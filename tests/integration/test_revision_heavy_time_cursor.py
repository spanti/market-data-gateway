# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[suspicious-url-open-usage]
"""Characterization of the Revision-heavy Time Cursor contract gap (ticket 03).

These tests pin the CURRENT public REST behaviour when one Instrument has
multiple raw Revisions at the same `open_ts`. They are not a fix and must not
be read as approving the behaviour they record: assertions that document the
known gap are marked ``KNOWN CONTRACT GAP``. The standalone record lives in
``docs/revision-heavy-time-cursor.md``.

Every request goes through `GatewayRuntime` on a real ephemeral socket, the
same seam as `tests/integration/test_bars_pagination.py`. The Feed Handler
below emits one batch per `ingest()` call so the second batch is a genuine
live correction and the Store keeps both raw Revisions of the same open_ts,
exactly as the production merge path does.
"""

from __future__ import annotations

import json
import operator
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mdgw.config import load_config
from mdgw.core.bar import RawBar
from mdgw.ingest.fake import BarInput
from mdgw.runtime import GatewayRuntime

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BTC = "binance:BTC/USDT"
BASE_MINUTE = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)


# -- local fixtures ---------------------------------------------------------
#
# Deliberately self-contained: ticket 01 owns reusable characterization data,
# so nothing here is imported from or shared with other pagination tests.


def _config():
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    return config.model_copy(update={"server": server})


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


def _iso(minute: int) -> str:
    return (
        BASE_MINUTE + timedelta(minutes=minute)
    ).strftime("%Y-%m-%dT%H:%M:%S.000000000Z")


class _ScriptedFeed:
    """Emit one scripted batch per `iter_bars()` call, then nothing.

    The live collector assigns `revision = max(stored) + 1` when a later batch
    changes the content at an existing `open_ts`, so two batches are enough to
    persist two raw Revisions without touching the Store directly.
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


def _runtime(
    tmp_path: Path, batches: Sequence[Sequence[BarInput]]
) -> GatewayRuntime:
    return GatewayRuntime(
        feed_handler=_ScriptedFeed(batches),
        config=_config(),
        data_root=tmp_path,
    )


def _ingest_all(runtime: GatewayRuntime, batches: Sequence[Any]) -> None:
    runtime.start()
    for _ in batches:
        runtime.ingest()


# -- REST helpers -----------------------------------------------------------


def _get(url: str) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), error.read()


def _get_json(url: str) -> dict[str, Any]:
    status, _headers, body = _get(url)
    assert status == 200, body
    return json.loads(body)


def _url(
    runtime: GatewayRuntime,
    *,
    limit: int | None,
    start: str | None = None,
) -> str:
    url = (
        f"{runtime.base_url}/api/v1/bars?interval=1m&instrument={BTC}"
    )
    if limit is not None:
        url += f"&limit={limit}"
    if start is not None:
        url += f"&start={start}"
    return url


def _rows(payload: dict[str, Any]) -> list[tuple[str, int]]:
    return [
        (bar["payload"]["open_ts"], bar["payload"]["revision"])
        for bar in payload["bars"]
    ]


def _revision_rows(
    payload: dict[str, Any],
) -> list[tuple[str, int, Decimal]]:
    return [
        (
            bar["payload"]["open_ts"],
            bar["payload"]["revision"],
            Decimal(bar["payload"]["close"]),
        )
        for bar in payload["bars"]
    ]


# -- in-page revisions ------------------------------------------------------


def test_a_page_returns_every_raw_revision_sharing_an_open_ts(
    tmp_path: Path,
) -> None:
    """Both Revisions at 00:00 appear; pagination does not dedupe a page."""
    batches = [
        [_bar(0, "100"), _bar(1, "100")],
        [_bar(0, "200"), _bar(1, "100")],
    ]
    with _runtime(tmp_path, batches) as runtime:
        _ingest_all(runtime, batches)
        plain = _get_json(_url(runtime, limit=None))
        page = _get_json(_url(runtime, limit=10))

    # The non-paginated JSON surface keeps every raw Revision row.
    assert len(plain["bars"]) == 3
    assert sorted(_revision_rows(plain), key=operator.itemgetter(0, 1)) == [
        (_iso(0), 0, Decimal(100)),
        (_iso(0), 1, Decimal(200)),
        (_iso(1), 0, Decimal(100)),
    ]

    # The paginated page returns the same three raw rows, ordered by open_ts.
    assert len(page["bars"]) == 3
    assert [(open_ts, revision) for open_ts, revision, _ in _revision_rows(page)] == [
        (_iso(0), 0),
        (_iso(0), 1),
        (_iso(1), 0),
    ]
    open_times = [open_ts for open_ts, _, _ in _revision_rows(page)]
    assert open_times == sorted(open_times)
    assert page["page"] == {"has_more": False, "next_start": None}


# -- duplicated timestamp spanning pages ------------------------------------


def test_page_boundary_inside_a_revision_group_repeats_rows_and_hides_the_rest(
    tmp_path: Path,
) -> None:
    """KNOWN CONTRACT GAP: with limit=1 the walk never leaves 00:00.

    Two Revisions share the first `open_ts`. The page returns only the first
    row, so `next_start` is that same `open_ts` and the inclusive `start`
    re-reads the group from its first row. The second Revision and the whole
    00:01 Bar are unreachable by following the documented Time Cursor.
    """
    batches = [
        [_bar(0, "100"), _bar(1, "100")],
        [_bar(0, "200"), _bar(1, "100")],
    ]
    with _runtime(tmp_path, batches) as runtime:
        _ingest_all(runtime, batches)
        first = _get_json(_url(runtime, limit=1))
        second = _get_json(
            _url(runtime, limit=1, start=first["page"]["next_start"])
        )
        third = _get_json(
            _url(runtime, limit=1, start=second["page"]["next_start"])
        )

    assert _rows(first) == [(_iso(0), 0)]
    assert first["page"] == {"has_more": True, "next_start": _iso(0)}

    # KNOWN CONTRACT GAP: the next page is byte-for-byte the same page. This
    # is overlap, not progress, and the loop is deterministic.
    assert second == first
    assert third == first

    # KNOWN CONTRACT GAP: because the walk repeats 00:00 forever, the second
    # Revision at 00:00 and the Bar at 00:01 are never returned. No amount of
    # following `next_start` reaches them.
    seen = _rows(first) + _rows(second) + _rows(third)
    assert (_iso(0), 1) not in seen
    assert (_iso(1), 0) not in seen


def test_a_revision_group_larger_than_the_page_never_reaches_the_remainder(
    tmp_path: Path,
) -> None:
    """KNOWN CONTRACT GAP: 00:00 has revisions 0..2; limit=2 stalls on 0..1."""
    batches = [
        [_bar(0, "100"), _bar(1, "100")],
        [_bar(0, "200"), _bar(1, "100")],
        [_bar(0, "300"), _bar(1, "100")],
    ]
    with _runtime(tmp_path, batches) as runtime:
        _ingest_all(runtime, batches)
        first = _get_json(_url(runtime, limit=2))
        second = _get_json(
            _url(runtime, limit=2, start=first["page"]["next_start"])
        )
        third = _get_json(
            _url(runtime, limit=2, start=second["page"]["next_start"])
        )

    assert _rows(first) == [(_iso(0), 0), (_iso(0), 1)]
    assert first["page"] == {"has_more": True, "next_start": _iso(0)}

    # KNOWN CONTRACT GAP: every follow-up page repeats the first two rows.
    assert second == first
    assert third == first

    seen = _rows(first) + _rows(second) + _rows(third)
    assert (_iso(0), 2) not in seen
    assert (_iso(1), 0) not in seen


# -- controls that still work ------------------------------------------------


def test_walk_is_complete_when_the_page_ends_on_a_revision_group_boundary(
    tmp_path: Path,
) -> None:
    """A group that exactly fills the page keeps the documented no-overlap walk."""
    batches = [
        [_bar(0, "100"), _bar(1, "100")],
        [_bar(0, "200"), _bar(1, "100")],
    ]
    with _runtime(tmp_path, batches) as runtime:
        _ingest_all(runtime, batches)
        first = _get_json(_url(runtime, limit=2))
        second = _get_json(
            _url(runtime, limit=2, start=first["page"]["next_start"])
        )

    assert _rows(first) == [(_iso(0), 0), (_iso(0), 1)]
    assert first["page"] == {"has_more": True, "next_start": _iso(1)}
    assert _rows(second) == [(_iso(1), 0)]
    assert second["page"] == {"has_more": False, "next_start": None}

    # All three raw Revisions were returned exactly once.
    seen = _rows(first) + _rows(second)
    assert sorted(seen) == [
        (_iso(0), 0),
        (_iso(0), 1),
        (_iso(1), 0),
    ]


def test_last_page_with_same_timestamp_revisions_reports_exhaustion(
    tmp_path: Path,
) -> None:
    batches = [[_bar(0, "100")], [_bar(0, "200")]]
    with _runtime(tmp_path, batches) as runtime:
        _ingest_all(runtime, batches)
        payload = _get_json(_url(runtime, limit=10))

    assert _rows(payload) == [(_iso(0), 0), (_iso(0), 1)]
    assert payload["page"] == {"has_more": False, "next_start": None}


# -- limit=0 ----------------------------------------------------------------


def test_limit_zero_probes_the_first_timestamp_and_never_advances(
    tmp_path: Path,
) -> None:
    """`limit=0` reports existence only; driving it by next_start loops."""
    batches = [
        [_bar(0, "100"), _bar(1, "100")],
        [_bar(0, "200"), _bar(1, "100")],
    ]
    with _runtime(tmp_path, batches) as runtime:
        _ingest_all(runtime, batches)
        probe = _get_json(_url(runtime, limit=0))
        again = _get_json(
            _url(runtime, limit=0, start=probe["page"]["next_start"])
        )
        first_real_page = _get_json(
            _url(runtime, limit=2, start=probe["page"]["next_start"])
        )

    assert probe == {
        "bars": [],
        "page": {"has_more": True, "next_start": _iso(0)},
    }
    # KNOWN CONTRACT GAP (limit=0 variant): `next_start` is the first
    # timestamp, so probing again at that inclusive start repeats the probe.
    assert again == probe

    # Switching to a real limit from the probe start returns the whole group,
    # because the probe itself returned no rows.
    assert _rows(first_real_page) == [(_iso(0), 0), (_iso(0), 1)]
    assert first_real_page["page"] == {
        "has_more": True,
        "next_start": _iso(1),
    }
