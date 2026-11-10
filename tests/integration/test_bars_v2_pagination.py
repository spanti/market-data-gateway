# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[suspicious-url-open-usage]
"""End-to-end acceptance for revision-safe v2 history pagination (issue 10 A).

`GET /api/v2/bars` pages every raw Bar Revision with a signed opaque keyset
cursor over `(open_ts, revision, ingest_ts, row_fingerprint)`. The six
Revision-heavy scenarios recorded for the v1 Time Cursor are ported here as
positive v2 guarantees: every row is reachable exactly once for every limit.

Requests run against a real `GatewayRuntime` (live ingest revision bumps) or a
real ASGI app over a temp Dataset (cursor signing, Compaction, leases).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.api.history_cursor import CursorCodec, HistoryCursor
from mdgw.config import load_config
from mdgw.core.bar import Bar, RawBar
from mdgw.core.time import datetime_to_ns, ns_to_iso
from mdgw.ingest.fake import BarInput
from mdgw.runtime import GatewayRuntime
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import active_session_locks

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator, Sequence

    import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BTC = "binance:BTC/USDT"
ETH = "binance:ETH/USDT"
BASE_MINUTE = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
CURSOR_KEY = b"integration-test-cursor-key"
HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-bars-v2-pagination"
V2 = "/api/v2/bars"
V1 = "/api/v1/bars"


# -- fixtures ---------------------------------------------------------------


def _config(*, cursor_key_env: str | None = None):
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(
        update={"http_port": 0, "cursor_key_env": cursor_key_env}
    )
    return config.model_copy(update={"server": server})


class _ScriptedFeed:
    """Emit one scripted batch per `iter_bars()` call, then nothing.

    The live collector assigns `revision = max(stored) + 1` when a later batch
    changes the content at an existing `open_ts`, so batches produce genuine
    raw Revisions without writing the Store directly.
    """

    name = "fake"
    supports_history = False
    interval = "1m"

    def __init__(self, batches: Sequence[Sequence[BarInput]]) -> None:
        self.instrument_id = BTC
        self._batches = tuple(tuple(batch) for batch in batches)
        self._calls = 0

    @property
    def instrument_ids(self) -> tuple[str, ...]:
        return (self.instrument_id,)

    def iter_bars(self) -> Iterator[RawBar]:
        if self._calls >= len(self._batches):
            return
        batch = self._batches[self._calls]
        self._calls += 1
        for bar in batch:
            yield RawBar(
                instrument_id=self.instrument_id,
                interval="1m",
                open_ts=bar.open_ts,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                trade_count=bar.trade_count,
                revision=bar.revision,
            )


def _bar(minute: int, close: str) -> BarInput:
    return BarInput(
        open_ts=BASE_MINUTE + timedelta(minutes=minute),
        open=Decimal(100),
        high=Decimal(101),
        low=Decimal(99),
        close=Decimal(close),
        volume=Decimal(1),
        trade_count=1,
    )


def _runtime(
    tmp_path: Path,
    batches: Sequence[Sequence[BarInput]],
    *,
    cursor_key_env: str | None = None,
) -> GatewayRuntime:
    return GatewayRuntime(
        feed_handler=_ScriptedFeed(batches),
        config=_config(cursor_key_env=cursor_key_env),
        data_root=tmp_path,
    )


def _ingest_all(runtime: GatewayRuntime, batches: Sequence[Any]) -> None:
    runtime.start()
    for _ in batches:
        runtime.ingest()


@contextmanager
def _serve(
    store: ParquetBarStore, *, cursor_key: bytes = CURSOR_KEY
) -> Generator[AsgiServer]:
    server = AsgiServer()
    try:
        server.start(
            create_app(store, cursor_key=cursor_key),
            host=HOST,
            port=0,
            thread_name=THREAD_NAME,
        )
        yield server
    finally:
        server.stop()


def _store_bar(
    minute: int,
    *,
    revision: int = 0,
    instrument: str = BTC,
    close: str = "100",
    ingest: datetime | None = None,
) -> Bar:
    open_ts = BASE_MINUTE + timedelta(minutes=minute)
    close_ts = open_ts + timedelta(minutes=1)
    ingest_ts = ingest if ingest is not None else close_ts
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=instrument,
        interval="1m",
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=ingest_ts,
        feed="fake",
        seq=None,
        aggregation_source="external",
        open=Decimal(100),
        high=Decimal(101),
        low=Decimal(99),
        close=Decimal(close),
        volume=Decimal(1),
        trade_count=1,
        revision=revision,
    )


# -- HTTP helpers -----------------------------------------------------------


def _get(url: str) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), error.read()


def _json(url: str) -> dict[str, Any]:
    status, _headers, body = _get(url)
    assert status == 200, body
    return json.loads(body)


def _error(url: str) -> dict[str, Any]:
    status, _headers, body = _get(url)
    assert status == 400, (status, body)
    return json.loads(body)["error"]


def _url(  # ruff: ignore[too-many-arguments] - explicit query contract
    base_url: str,
    *,
    limit: int | str | None,
    instrument: str = BTC,
    interval: str = "1m",
    start: str | None = None,
    end: str | None = None,
    cursor: str | None = None,
    output_format: str | None = None,
) -> str:
    query = [f"interval={interval}", f"instrument={instrument.replace('/', '%2F')}"]
    if limit is not None:
        query.append(f"limit={limit}")
    if start is not None:
        query.append(f"start={start}")
    if end is not None:
        query.append(f"end={end}")
    if cursor is not None:
        query.append(f"cursor={cursor}")
    if output_format is not None:
        query.append(f"format={output_format}")
    return f"{base_url}{V2}?" + "&".join(query)


def _iso(minute: int) -> str:
    return ns_to_iso(datetime_to_ns(BASE_MINUTE + timedelta(minutes=minute)))


def _rows(payload: dict[str, Any]) -> list[tuple[str, int, Decimal]]:
    return [
        (
            bar["payload"]["open_ts"],
            bar["payload"]["revision"],
            Decimal(bar["payload"]["close"]),
        )
        for bar in payload["bars"]
    ]


def _keys(payload: dict[str, Any]) -> list[tuple[str, int]]:
    return [
        (bar["payload"]["open_ts"], bar["payload"]["revision"])
        for bar in payload["bars"]
    ]


class _Walk:
    def __init__(self) -> None:
        self.rows: list[tuple[str, int, Decimal]] = []
        self.pages: list[dict[str, Any]] = []
        self.cursors: list[str] = []

    @property
    def keys(self) -> list[tuple[str, int]]:
        return [(open_ts, revision) for open_ts, revision, _ in self.rows]


def _walk(
    base_url: str,
    *,
    limit: int,
    end: str | None = None,
    max_pages: int = 50,
    instrument: str = BTC,
) -> _Walk:
    walk = _Walk()
    cursor: str | None = None
    for page_number in range(max_pages):
        payload = _json(
            _url(
                base_url,
                limit=limit,
                instrument=instrument,
                end=end,
                cursor=cursor,
            )
        )
        walk.pages.append(payload)
        walk.rows.extend(_rows(payload))
        page = payload["page"]
        assert set(page) == {"has_more", "next_cursor"}
        if not page["has_more"]:
            assert page["next_cursor"] is None
            return walk
        assert isinstance(page["next_cursor"], str)
        assert page["next_cursor"]
        if cursor is not None:
            assert page["next_cursor"] != cursor, (
                f"cursor did not advance on page {page_number}"
            )
        assert page["next_cursor"] not in walk.cursors
        walk.cursors.append(page["next_cursor"])
        cursor = page["next_cursor"]
    msg = f"pagination did not terminate within {max_pages} pages"
    raise AssertionError(msg)


# -- the six revision-heavy scenarios --------------------------------------


def test_a_page_returns_every_raw_revision_sharing_an_open_ts(
    tmp_path: Path,
) -> None:
    batches = [
        [_bar(0, "100"), _bar(1, "100")],
        [_bar(0, "200"), _bar(1, "100")],
    ]
    with _runtime(tmp_path, batches) as runtime:
        _ingest_all(runtime, batches)
        payload = _json(_url(runtime.base_url, limit=10))

    assert payload["bars"]
    assert _rows(payload) == [
        (_iso(0), 0, Decimal("100.0")),
        (_iso(0), 1, Decimal("200.0")),
        (_iso(1), 0, Decimal("100.0")),
    ]
    assert payload["page"] == {"has_more": False, "next_cursor": None}


def test_limit_one_walk_returns_each_row_exactly_once_with_an_advancing_cursor(
    tmp_path: Path,
) -> None:
    batches = [
        [_bar(0, "100"), _bar(1, "100")],
        [_bar(0, "200"), _bar(1, "100")],
    ]
    with _runtime(tmp_path, batches) as runtime:
        _ingest_all(runtime, batches)
        first = _json(_url(runtime.base_url, limit=1))
        walk = _walk(runtime.base_url, limit=1)

    assert _keys(first) == [(_iso(0), 0)]
    assert first["page"]["has_more"] is True
    assert isinstance(first["page"]["next_cursor"], str)
    assert walk.keys == [
        (_iso(0), 0),
        (_iso(0), 1),
        (_iso(1), 0),
    ]
    assert len(walk.pages) == 3
    assert len(walk.cursors) == 2
    assert walk.cursors[0] != walk.cursors[1]
    # `next_cursor` is opaque: it is not the ISO timestamp.
    assert not walk.cursors[0].startswith("2024-")


def test_a_revision_group_larger_than_the_page_is_fully_traversable(
    tmp_path: Path,
) -> None:
    batches = [
        [_bar(0, "100"), _bar(1, "100")],
        [_bar(0, "200"), _bar(1, "100")],
        [_bar(0, "300"), _bar(1, "100")],
    ]
    with _runtime(tmp_path, batches) as runtime:
        _ingest_all(runtime, batches)
        walk = _walk(runtime.base_url, limit=2)

    assert walk.keys == [
        (_iso(0), 0),
        (_iso(0), 1),
        (_iso(0), 2),
        (_iso(1), 0),
    ]
    assert len(walk.pages) == 2
    assert [len(page["bars"]) for page in walk.pages] == [2, 2]


def test_a_page_ending_on_a_revision_group_boundary_is_complete(
    tmp_path: Path,
) -> None:
    batches = [
        [_bar(0, "100"), _bar(1, "100")],
        [_bar(0, "200"), _bar(1, "100")],
    ]
    with _runtime(tmp_path, batches) as runtime:
        _ingest_all(runtime, batches)
        walk = _walk(runtime.base_url, limit=2)

    assert walk.keys == [
        (_iso(0), 0),
        (_iso(0), 1),
        (_iso(1), 0),
    ]
    assert len(walk.pages) == 2
    assert _keys(walk.pages[0]) == [(_iso(0), 0), (_iso(0), 1)]
    assert _keys(walk.pages[1]) == [(_iso(1), 0)]


def test_last_page_reports_no_more_and_a_null_cursor(tmp_path: Path) -> None:
    batches = [[_bar(0, "100")], [_bar(0, "200")]]
    with _runtime(tmp_path, batches) as runtime:
        _ingest_all(runtime, batches)
        payload = _json(_url(runtime.base_url, limit=10))

    assert _keys(payload) == [(_iso(0), 0), (_iso(0), 1)]
    assert payload["page"] == {"has_more": False, "next_cursor": None}


def test_every_limit_has_no_livelock_and_no_unreachable_rows(
    tmp_path: Path,
) -> None:
    batches = [
        [_bar(0, "100"), _bar(2, "100")],
        [_bar(0, "200")],
        [_bar(0, "300")],
        [_bar(0, "400"), _bar(3, "100")],
    ]
    expected = [(_iso(0), revision) for revision in range(4)] + [
        (_iso(2), 0),
        (_iso(3), 0),
    ]
    with _runtime(tmp_path, batches) as runtime:
        _ingest_all(runtime, batches)
        for limit in range(1, 7):
            walk = _walk(runtime.base_url, limit=limit)
            assert walk.keys == expected, limit
            assert len(walk.rows) == len(set(walk.keys)) == 6


# -- window, ordering and snapshot semantics --------------------------------


def test_pages_are_globally_ordered_across_overlapping_files(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_store_bar(5), _store_bar(9)], asset_class="crypto")
    store.write_bars([_store_bar(0), _store_bar(7)], asset_class="crypto")
    store.write_bars([_store_bar(1), _store_bar(3)], asset_class="crypto")

    with _serve(store) as server:
        walk = _walk(server.base_url, limit=2)

    assert walk.keys == [(_iso(minute), 0) for minute in (0, 1, 3, 5, 7, 9)]
    assert active_session_locks(store.dataset_root) == []


def test_end_is_exclusive_and_carried_by_the_cursor(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_store_bar(minute) for minute in range(4)], asset_class="crypto")

    with _serve(store) as server:
        first = _json(_url(server.base_url, limit=1, end=_iso(2)))
        cursor = first["page"]["next_cursor"]
        assert isinstance(cursor, str)
        # Repeating the same `end` on a cursor page is allowed.
        second = _json(_url(server.base_url, limit=1, cursor=cursor, end=_iso(2)))
        walk = _walk(server.base_url, limit=1, end=_iso(2))
        mismatched = _error(
            _url(server.base_url, limit=1, cursor=cursor, end=_iso(3))
        )

    assert _keys(first) == [(_iso(0), 0)]
    assert _keys(second) == [(_iso(1), 0)]
    assert walk.keys == [(_iso(0), 0), (_iso(1), 0)]
    assert mismatched["code"] == "invalid_parameter"
    assert mismatched["details"]["field"] == "end"


def test_cursor_survives_compaction_and_a_new_manifest_snapshot(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    for minute in range(4):
        store.write_bars([_store_bar(minute)], asset_class="crypto")

    with _serve(store) as server:
        first = _json(_url(server.base_url, limit=2))
        cursor = first["page"]["next_cursor"]
        assert isinstance(cursor, str)
        cursor_page_before = _json(_url(server.base_url, limit=2, cursor=cursor))
        # Compaction replaces the Data Files behind the cursor position. The
        # cursor is logical, so it never pins the old Snapshot.
        report = store.compact(
            older_than=timedelta(seconds=0),
            now_ns=time.time_ns() + 1_000_000_000,
        )
        cursor_page_after = _json(_url(server.base_url, limit=2, cursor=cursor))

    assert report.new_files
    assert cursor_page_after == cursor_page_before
    assert _keys(cursor_page_after) == [(_iso(2), 0), (_iso(3), 0)]
    assert cursor_page_after["page"] == {
        "has_more": False,
        "next_cursor": None,
    }


# -- cursor validation ------------------------------------------------------


def _resign(payload: dict[str, object], key: bytes = CURSOR_KEY) -> str:
    raw = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    payload_part = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    signature = hmac.new(key, raw, hashlib.sha256).digest()
    signature_part = (
        base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")
    )
    return f"{payload_part}.{signature_part}"


def _payload(token: str) -> dict[str, object]:
    part = token.split(".", 1)[0]
    padded = part + "=" * (-len(part) % 4)
    return json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))


def test_cursor_and_start_together_are_rejected(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_store_bar(0), _store_bar(1)], asset_class="crypto")

    with _serve(store) as server:
        cursor = _json(_url(server.base_url, limit=1))["page"]["next_cursor"]
        assert isinstance(cursor, str)
        error = _error(
            _url(
                server.base_url,
                limit=1,
                cursor=cursor,
                start=_iso(1),
            )
        )

    assert error["code"] == "invalid_parameter"
    assert error["details"]["field"] == "cursor"


def test_tampered_and_malformed_cursors_are_rejected(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_store_bar(0), _store_bar(1)], asset_class="crypto")

    with _serve(store) as server:
        cursor = _json(_url(server.base_url, limit=1))["page"]["next_cursor"]
        assert isinstance(cursor, str)
        payload_part, signature_part = cursor.split(".")
        flipped = ("A" if signature_part[0] != "A" else "B") + signature_part[1:]
        errors = [
            _error(_url(server.base_url, limit=1, cursor="not-a-cursor")),
            _error(
                _url(
                    server.base_url,
                    limit=1,
                    cursor=f"{payload_part}.{flipped}",
                )
            ),
        ]

    for error in errors:
        assert error["code"] == "invalid_parameter"
        assert error["details"]["field"] == "cursor"
        assert error["details"]["reason"] == "invalid_cursor"


def test_unknown_cursor_version_is_rejected(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_store_bar(0), _store_bar(1)], asset_class="crypto")

    with _serve(store) as server:
        cursor = _json(_url(server.base_url, limit=1))["page"]["next_cursor"]
        assert isinstance(cursor, str)
        payload = _payload(cursor)
        payload["v"] = 2
        error = _error(
            _url(server.base_url, limit=1, cursor=_resign(payload))
        )

    assert error["code"] == "invalid_parameter"
    assert error["details"] == {
        "field": "cursor",
        "reason": "invalid_cursor",
    }


def test_cursor_signed_with_another_key_is_rejected(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_store_bar(0), _store_bar(1)], asset_class="crypto")

    with _serve(store) as server:
        cursor = _json(_url(server.base_url, limit=1))["page"]["next_cursor"]
        assert isinstance(cursor, str)
        error = _error(
            _url(
                server.base_url,
                limit=1,
                cursor=_resign(_payload(cursor), key=b"another-key"),
            )
        )

    assert error["code"] == "invalid_parameter"
    assert error["details"]["field"] == "cursor"


def test_cursor_query_mismatches_name_the_offending_parameter(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [
            _store_bar(0),
            _store_bar(1),
            _store_bar(0, instrument=ETH),
        ],
        asset_class="crypto",
    )

    with _serve(store) as server:
        cursor = _json(_url(server.base_url, limit=1))["page"]["next_cursor"]
        assert isinstance(cursor, str)
        instrument_error = _error(
            _url(server.base_url, limit=1, instrument=ETH, cursor=cursor)
        )
        interval_error = _error(
            _url(server.base_url, limit=1, interval="5m", cursor=cursor)
        )

    assert instrument_error["code"] == "invalid_parameter"
    assert instrument_error["details"]["field"] == "instrument"
    assert interval_error["code"] == "invalid_parameter"
    assert interval_error["details"]["field"] == "interval"


# -- limits and formats -----------------------------------------------------


def test_limit_is_required_and_bounded(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_store_bar(0)], asset_class="crypto")

    with _serve(store) as server:
        missing = _error(_url(server.base_url, limit=None))
        zero = _error(_url(server.base_url, limit=0))
        negative = _error(_url(server.base_url, limit=-1))
        not_integer = _error(_url(server.base_url, limit="many"))
        too_large = _error(_url(server.base_url, limit=100001))
        maximum = _json(_url(server.base_url, limit=100000))

    for error in (missing, zero, negative, not_integer):
        assert error["code"] == "invalid_parameter"
        assert error["details"]["field"] == "limit"
    assert too_large["code"] == "range_too_large"
    assert too_large["details"]["field"] == "limit"
    assert too_large["details"]["max_limit"] == 100000
    assert _keys(maximum) == [(_iso(0), 0)]


def test_v2_is_json_only(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_store_bar(0)], asset_class="crypto")

    with _serve(store) as server:
        explicit_json = _json(
            _url(server.base_url, limit=1, output_format="json")
        )
        csv_error = _error(_url(server.base_url, limit=1, output_format="csv"))
        arrow_error = _error(
            _url(server.base_url, limit=1, output_format="arrow")
        )

    assert _keys(explicit_json) == [(_iso(0), 0)]
    for error in (csv_error, arrow_error):
        assert error["code"] == "invalid_format"


def test_required_interval_and_instrument_keep_their_stable_codes(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_store_bar(0)], asset_class="crypto")

    with _serve(store) as server:
        bad_interval = _error(
            f"{server.base_url}{V2}?interval=2m&instrument={BTC}&limit=1"
        )
        bad_instrument = _error(
            f"{server.base_url}{V2}?interval=1m&instrument=nope&limit=1"
        )
        missing_instrument = _error(
            f"{server.base_url}{V2}?interval=1m&limit=1"
        )

    assert bad_interval["code"] == "invalid_interval"
    assert bad_instrument["code"] == "invalid_instrument"
    assert missing_instrument["code"] == "invalid_instrument"


# -- v1 coexistence ---------------------------------------------------------


def test_v1_paginated_json_is_byte_for_byte_unchanged(tmp_path: Path) -> None:
    """The v1 Time Cursor keeps its documented shape and known gap."""
    batches = [
        [_bar(0, "100"), _bar(1, "100")],
        [_bar(0, "200"), _bar(1, "100")],
    ]
    with _runtime(tmp_path, batches) as runtime:
        _ingest_all(runtime, batches)
        first = _json(
            f"{runtime.base_url}{V1}?interval=1m&instrument={BTC}&limit=1"
        )
        second = _json(
            f"{runtime.base_url}{V1}?interval=1m&instrument={BTC}&limit=1"
            f"&start={first['page']['next_start']}"
        )
        v2_page = _json(_url(runtime.base_url, limit=1))

    assert first["page"] == {"has_more": True, "next_start": _iso(0)}
    assert set(first["page"]) == {"has_more", "next_start"}
    # KNOWN CONTRACT GAP preserved for v1: the follow-up page repeats.
    assert second == first
    # v2 is a separate surface with a distinct page shape and no overlap.
    assert set(v2_page["page"]) == {"has_more", "next_cursor"}
    assert _keys(v2_page) == [(_iso(0), 0)]
    assert v2_page["page"]["next_cursor"] is not None


def test_named_cursor_key_environment_variable_is_used(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MDGW_IT_CURSOR_KEY", "integration-key")
    batches = [[_bar(0, "100")], [_bar(0, "200")]]
    with _runtime(
        tmp_path, batches, cursor_key_env="MDGW_IT_CURSOR_KEY"
    ) as runtime:
        _ingest_all(runtime, batches)
        walk = _walk(runtime.base_url, limit=1)

    assert walk.keys == [(_iso(0), 0), (_iso(0), 1)]


def test_cursor_key_is_verified_by_the_codec_used_by_the_app(
    tmp_path: Path,
) -> None:
    """A cursor from the app is a valid codec token for the injected key."""
    store = ParquetBarStore(tmp_path)
    store.write_bars([_store_bar(0), _store_bar(1)], asset_class="crypto")

    with _serve(store) as server:
        payload = _json(_url(server.base_url, limit=1))
    cursor = CursorCodec(CURSOR_KEY).decode(payload["page"]["next_cursor"])
    assert isinstance(cursor, HistoryCursor)
    assert cursor.open_ns == datetime_to_ns(BASE_MINUTE)
    assert cursor.revision == 0
