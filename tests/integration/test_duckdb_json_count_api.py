"""REST JSON history on the DuckDB count and non-paginated read seam.

The JSON row-ceiling count, the non-paginated JSON response and the paginated
page reader all use the DuckDB scan. These tests enter through the real ASGI
app on an ephemeral socket and compare the wire response with the Store read:
exact Message Envelope strings, decimal text, nanosecond timestamps, nulls,
duplicate and raw-Revision multiplicity, and the stable error envelopes.
"""

# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[suspicious-url-open-usage]
from __future__ import annotations

import json
import re
import struct
import urllib.error
import urllib.request
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest

from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.api.routes import bars as bars_routes
from mdgw.core.bar import Bar
from mdgw.core.envelope import table_to_messages
from mdgw.core.time import datetime_to_ns, ns_to_iso
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import active_session_locks

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

UTC = timezone.utc
INSTRUMENT = "binance:BTC/USDT"
OTHER_INSTRUMENT = "binance:ETH/USDT"
START = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-duckdb-json"
BARS = "/api/v1/bars"
NS_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z")


def _bar(
    minute: int,
    *,
    interval: str = "1m",
    instrument_id: str = INSTRUMENT,
    revision: int = 0,
    open_value: str = "65000.1",
    trade_count: int | None = None,
    seq: int | None = None,
    base: datetime = START,
) -> Bar:
    open_ts = base + timedelta(minutes=minute)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=instrument_id,
        interval=interval,
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=seq,
        aggregation_source="external",
        open=Decimal(open_value),
        high=Decimal("65010.0"),
        low=Decimal("64990.0"),
        close=Decimal("65005.5"),
        volume=Decimal("12.34"),
        trade_count=trade_count,
        revision=revision,
    )


def _write(store: ParquetBarStore, bars: list[Bar]) -> None:
    store.write_bars(bars, asset_class="crypto")


def _url(base_url: str, **query: object) -> str:
    encoded = "&".join(
        f"{key}={value}" for key, value in query.items() if value is not None
    )
    return f"{base_url}{BARS}?{encoded}"


def _get(url: str) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), error.read()


def _json(url: str) -> dict[str, Any]:
    status, _, body = _get(url)
    assert status == 200, body
    return json.loads(body)


@contextmanager
def _serve(app: Any) -> Generator[AsgiServer]:
    server = AsgiServer()
    try:
        server.start(app, host=HOST, port=0, thread_name=THREAD_NAME)
        yield server
    finally:
        server.stop()


def _canonical(messages: list[dict[str, Any]]) -> Counter:
    """Exact message content and multiplicity, ignoring scan order."""
    return Counter(json.dumps(message, sort_keys=True) for message in messages)


def _store_messages(
    store: ParquetBarStore,
    *,
    instrument_id: str | None = INSTRUMENT,
    interval: str = "1m",
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> list[dict[str, Any]]:
    """Store rows in REST message shape, used as the parity oracle."""
    table = store.read_bars(
        instrument_id=instrument_id,
        interval=interval,
        start_ns=start_ns,
        end_ns=end_ns,
    )
    return table_to_messages(table)


def _iso(minute: int, *, base: datetime = START) -> str:
    return ns_to_iso(datetime_to_ns(base + timedelta(minutes=minute)))


# -- value and multiplicity parity -----------------------------------------


def test_non_paginated_json_matches_the_store_for_multi_file_revisions(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    duplicate = _bar(0, revision=0)
    precise = "0.000000000000000001"
    _write(store, [duplicate, duplicate])
    _write(store, [_bar(0, revision=1), _bar(1, open_value=precise)])
    _write(store, [_bar(0, revision=2), _bar(2, revision=3, seq=7)])
    expected = _store_messages(store)

    with _serve(create_app(store)) as server:
        status, _, body = _get(_url(server.base_url, interval="1m"))
        with_instrument = _json(
            _url(server.base_url, interval="1m", instrument=INSTRUMENT)
        )

    assert status == 200
    payload = json.loads(body)
    assert set(payload) == {"bars"}
    assert len(payload["bars"]) == 6
    assert _canonical(payload["bars"]) == _canonical(expected)
    assert _canonical(with_instrument["bars"]) == _canonical(expected)

    revisions_at_start = sorted(
        bar["payload"]["revision"]
        for bar in payload["bars"]
        if bar["payload"]["open_ts"] == _iso(0)
    )
    assert revisions_at_start == [0, 0, 1, 2]
    precise_bar = next(
        bar for bar in payload["bars"] if bar["payload"]["open_ts"] == _iso(1)
    )
    assert precise_bar["payload"]["open"] == precise
    assert precise_bar["payload"]["trade_count"] is None
    seq_bar = next(
        bar for bar in payload["bars"] if bar["payload"]["open_ts"] == _iso(2)
    )
    assert seq_bar["seq"] == 7
    assert seq_bar["payload"]["revision"] == 3
    for message in payload["bars"]:
        assert NS_TIMESTAMP.fullmatch(message["event_ts"])
        assert NS_TIMESTAMP.fullmatch(message["ingest_ts"])
        assert NS_TIMESTAMP.fullmatch(message["payload"]["open_ts"])
        assert NS_TIMESTAMP.fullmatch(message["payload"]["close_ts"])
    assert active_session_locks(store.dataset_root) == []


def test_revision_heavy_non_paginated_json_preserves_every_row(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    for revision in range(4):
        _write(store, [_bar(0, revision=revision)])
    _write(store, [_bar(minute) for minute in range(1, 6)])
    expected = _store_messages(store)

    with _serve(create_app(store)) as server:
        payload = _json(_url(server.base_url, interval="1m"))

    assert len(payload["bars"]) == 9
    assert _canonical(payload["bars"]) == _canonical(expected)
    assert sorted(
        bar["payload"]["revision"]
        for bar in payload["bars"]
        if bar["payload"]["open_ts"] == _iso(0)
    ) == [0, 1, 2, 3]


def test_non_paginated_json_returns_empty_envelopes_for_empty_selections(
    tmp_path: Path,
) -> None:
    empty = ParquetBarStore(tmp_path / "empty")
    populated = ParquetBarStore(tmp_path / "populated")
    _write(populated, [_bar(0)])

    with _serve(create_app(empty)) as server:
        assert _json(_url(server.base_url, interval="1m")) == {"bars": []}
        assert active_session_locks(empty.dataset_root) == []
    with _serve(create_app(populated)) as server:
        other_interval = _json(_url(server.base_url, interval="5m"))
        outside_window = _json(
            _url(
                server.base_url,
                interval="1m",
                start=_iso(10),
                end=_iso(20),
            )
        )
        other_instrument = _json(
            _url(server.base_url, interval="1m", instrument=OTHER_INSTRUMENT)
        )

    assert other_interval == {"bars": []}
    assert outside_window == {"bars": []}
    assert other_instrument == {"bars": []}
    assert active_session_locks(populated.dataset_root) == []


# -- LIMIT and the JSON row ceiling ----------------------------------------


@pytest.mark.parametrize(
    ("limit", "expected_rows"),
    [(0, 0), (1, 1), (5, 5), (6, 6), (7, 6)],
)
def test_non_paginated_limit_only_caps_rows_from_the_matching_set(
    tmp_path: Path, limit: int, expected_rows: int
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(minute) for minute in range(5)])
    _write(store, [_bar(0, revision=1)])

    with _serve(create_app(store)) as server:
        payload = _json(_url(server.base_url, interval="1m", limit=limit))

    assert len(payload["bars"]) == expected_rows
    full = _canonical(_store_messages(store, instrument_id=None))
    returned = _canonical(payload["bars"])
    for message, count in returned.items():
        # LIMIT may select any scan-order rows, but never invents rows or
        # increases a stored row's multiplicity.
        assert count <= full[message]
    assert active_session_locks(store.dataset_root) == []


def test_json_ceiling_counts_raw_revisions_and_interacts_with_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(bars_routes, "JSON_ROW_LIMIT", 5)
    store = ParquetBarStore(tmp_path)
    for revision in range(3):
        _write(store, [_bar(0, revision=revision), _bar(1, revision=revision)])

    with _serve(create_app(store)) as server:
        status, _, body = _get(_url(server.base_url, interval="1m"))
        allowed = _json(_url(server.base_url, interval="1m", limit=5))
        small = _json(_url(server.base_url, interval="1m", limit=1))
        zero = _json(_url(server.base_url, interval="1m", limit=0))

    assert status == 400
    error = json.loads(body)["error"]
    assert error["code"] == "range_too_large"
    # Raw Revisions and duplicates are counted: no implicit dedup.
    assert error["details"] == {"row_count": 6, "limit": 5}
    assert len(allowed["bars"]) == 5
    assert len(small["bars"]) == 1
    assert zero == {"bars": []}
    assert active_session_locks(store.dataset_root) == []


def test_json_ceiling_boundary_is_inclusive_and_one_past_it_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(bars_routes, "JSON_ROW_LIMIT", 5)
    exact = ParquetBarStore(tmp_path / "exact")
    over = ParquetBarStore(tmp_path / "over")
    _write(exact, [_bar(minute) for minute in range(5)])
    _write(over, [_bar(minute) for minute in range(6)])

    with _serve(create_app(exact)) as server:
        payload = _json(_url(server.base_url, interval="1m"))
    with _serve(create_app(over)) as server:
        status, _, body = _get(_url(server.base_url, interval="1m"))

    assert len(payload["bars"]) == 5
    assert status == 400
    assert json.loads(body)["error"]["details"] == {
        "row_count": 6,
        "limit": 5,
    }
    assert active_session_locks(exact.dataset_root) == []
    assert active_session_locks(over.dataset_root) == []


# -- errors and resource release -------------------------------------------


def _corrupt_data_pages(path: Path) -> bytes:
    """Keep the Parquet footer; zero the already written data pages."""
    original = path.read_bytes()
    footer_length = struct.unpack("<I", original[-8:-4])[0]
    tail = original[-(footer_length + 8) :]
    path.write_bytes(
        original[:4] + b"\x00" * (len(original) - len(tail) - 4) + tail
    )
    return original


def test_json_releases_the_lease_after_range_too_large(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(bars_routes, "JSON_ROW_LIMIT", 2)
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(minute) for minute in range(3)])

    with _serve(create_app(store)) as server:
        status, _, body = _get(_url(server.base_url, interval="1m"))
        limited = _json(_url(server.base_url, interval="1m", limit=2))

    assert status == 400
    assert json.loads(body)["error"]["code"] == "range_too_large"
    assert len(limited["bars"]) == 2
    assert active_session_locks(store.dataset_root) == []


def test_json_count_failure_uses_the_stable_envelope_and_releases_the_lease(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])
    _write(store, [_bar(1)])
    manifest = json.loads(
        (store.dataset_root / "_manifest.json").read_text(encoding="utf-8")
    )
    victim = store.dataset_root / manifest["files"][1]["path"]
    original = victim.read_bytes()
    # Break the footer: the DuckDB exact count must throw, not miscount.
    victim.write_bytes(b"not a parquet file")

    with _serve(create_app(store)) as server:
        status, _, body = _get(_url(server.base_url, interval="1m"))
        victim.write_bytes(original)
        recovered = _json(_url(server.base_url, interval="1m"))

    assert status == 500
    error = json.loads(body)["error"]
    assert error["code"] == "internal_error"
    assert error["message"] == "internal server error"
    assert set(error) == {"code", "message", "details", "request_id"}
    leaked = body.decode("utf-8").lower()
    assert "read_parquet" not in leaked
    assert "duckdb" not in leaked
    assert "not a parquet" not in leaked
    assert len(recovered["bars"]) == 2
    assert active_session_locks(store.dataset_root) == []


def test_json_read_failure_after_a_successful_count_uses_the_envelope(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])
    _write(store, [_bar(1)])
    manifest = json.loads(
        (store.dataset_root / "_manifest.json").read_text(encoding="utf-8")
    )
    victim = store.dataset_root / manifest["files"][1]["path"]
    original = _corrupt_data_pages(victim)

    with _serve(create_app(store)) as server:
        status, _, body = _get(_url(server.base_url, interval="1m"))
        victim.write_bytes(original)
        recovered = _json(_url(server.base_url, interval="1m"))

    assert status == 500
    assert json.loads(body)["error"]["code"] == "internal_error"
    assert b'"error"' not in recovered  # it is a normal JSON body
    assert len(recovered["bars"]) == 2
    assert active_session_locks(store.dataset_root) == []


def test_json_schema_version_mismatch_keeps_its_stable_code(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])
    descriptor_path = store.dataset_root / "_dataset.json"
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    descriptor["schema_version"] = 2
    descriptor_path.write_text(json.dumps(descriptor), encoding="utf-8")

    with _serve(create_app(store)) as server:
        status, _, body = _get(_url(server.base_url, interval="1m"))

    assert status == 409
    error = json.loads(body)["error"]
    assert error["code"] == "schema_version_mismatch"
    assert set(error) == {"code", "message", "details", "request_id"}
    assert active_session_locks(store.dataset_root) == []
