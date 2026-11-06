# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[suspicious-url-open-usage]
"""REST single-Instrument JSON pagination through DuckDB's ordered page query.

Ticket 07 replaces the Python per-batch concat/sort page reader with one
DuckDB query over the whole request Snapshot: `ORDER BY open_ts ASC` plus
`LIMIT limit+1`. These tests enter through the real ASGI app and freeze the
public Time Cursor contract:

* the page is the globally earliest rows across all selected Data Files,
  including overlapping files appended out of time order;
* `next_start` is the first unreturned row's `open_ts` and stays inclusive;
* `has_more` only considers rows inside the exclusive `end`;
* `limit=0` still probes the first row from the same Snapshot;
* raw Revisions are never deduplicated;
* Revision-heavy page boundaries reproduce the known contract gap recorded in
  ``docs/revision-heavy-time-cursor.md``; this ticket does not fix it;
* a global sort that exceeds the configured DuckDB budget fails with the
  stable `internal_error` envelope instead of returning a truncated page.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.api.routes import bars as bars_routes
from mdgw.core.bar import Bar
from mdgw.core.time import datetime_to_ns, ns_to_iso
from mdgw.store.duckdb_reader import DuckDBOptions
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import active_session_locks

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    import pytest

INSTRUMENT = "binance:BTC/USDT"
START = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-duckdb-pagination"
BARS = "/api/v1/bars"


def _bar(
    minute: int,
    *,
    revision: int = 0,
    seq: int | None = None,
    open_value: str = "100.5",
    base: datetime = START,
) -> Bar:
    open_ts = base + timedelta(minutes=minute)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=INSTRUMENT,
        interval="1m",
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=seq,
        aggregation_source="external",
        open=Decimal(open_value),
        high=Decimal(101),
        low=Decimal(99),
        close=Decimal(100),
        volume=Decimal(1),
        trade_count=None,
        revision=revision,
    )


def _write(store: ParquetBarStore, bars: list[Bar]) -> None:
    store.write_bars(bars, asset_class="crypto")


def _iso(minute: int) -> str:
    return ns_to_iso(datetime_to_ns(START + timedelta(minutes=minute)))


def _url(base_url: str, **query: object) -> str:
    encoded = "&".join(
        f"{key}={value}" for key, value in query.items() if value is not None
    )
    return f"{base_url}{BARS}?{encoded}"


def _get(url: str) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), error.read()


def _json(url: str) -> dict[str, Any]:
    status, _, body = _get(url)
    assert status == 200, body
    return json.loads(body)


def _header(headers: dict[str, str], name: str) -> str:
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return ""


@contextmanager
def _serve(store: ParquetBarStore) -> Generator[AsgiServer]:
    server = AsgiServer()
    try:
        server.start(create_app(store), host=HOST, port=0, thread_name=THREAD_NAME)
        yield server
    finally:
        server.stop()


def _open_times(payload: dict[str, Any]) -> list[str]:
    return [bar["payload"]["open_ts"] for bar in payload["bars"]]


def _rows(payload: dict[str, Any]) -> list[tuple[str, int]]:
    return [
        (bar["payload"]["open_ts"], bar["payload"]["revision"])
        for bar in payload["bars"]
    ]


def _walk(
    base_url: str,
    *,
    limit: int,
    end: str | None = None,
    max_pages: int = 100,
) -> tuple[list[str], list[dict[str, Any]]]:
    seen: list[str] = []
    pages: list[dict[str, Any]] = []
    cursor: str | None = None
    for _ in range(max_pages):
        payload = _json(
            _url(
                base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=limit,
                start=cursor,
                end=end,
            )
        )
        pages.append(payload)
        seen.extend(_open_times(payload))
        if not payload["page"]["has_more"]:
            assert payload["page"]["next_start"] is None
            return seen, pages
        assert payload["page"]["next_start"] is not None
        cursor = payload["page"]["next_start"]
    msg = "pagination did not terminate"
    raise AssertionError(msg)


# -- global ordering across files ------------------------------------------


def test_walk_across_overlapping_files_is_globally_ordered_without_gaps(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    # Append order differs from time order and the file ranges overlap.
    _write(store, [_bar(5, seq=5), _bar(9, seq=9)])
    _write(store, [_bar(0, seq=0), _bar(7, seq=7)])
    _write(store, [_bar(1, seq=1), _bar(3, seq=3)])

    with _serve(store) as server:
        seen, pages = _walk(server.base_url, limit=2)
    assert active_session_locks(store.dataset_root) == []

    expected = [_iso(minute) for minute in (0, 1, 3, 5, 7, 9)]
    assert seen == expected
    assert len(seen) == len(set(seen))
    assert [page["page"] for page in pages] == [
        {"has_more": True, "next_start": _iso(3)},
        {"has_more": True, "next_start": _iso(7)},
        {"has_more": False, "next_start": None},
    ]
    for page in pages:
        assert _open_times(page) == sorted(_open_times(page))
        assert len(page["bars"]) <= 2


def test_end_boundary_is_exclusive_and_outside_rows_never_signal_more(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0), _bar(1), _bar(2)])
    _write(store, [_bar(3), _bar(4), _bar(5)])

    with _serve(store) as server:
        first = _json(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=2,
                end=_iso(3),
            )
        )
        second = _json(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=2,
                start=first["page"]["next_start"],
                end=_iso(3),
            )
        )
        exact = _json(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=3,
                end=_iso(3),
            )
        )
        only_outside = _json(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=1,
                end=_iso(0),
            )
        )
        empty_window = _json(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=2,
                start=_iso(5),
                end=_iso(5),
            )
        )

    assert _open_times(first) == [_iso(0), _iso(1)]
    assert first["page"] == {"has_more": True, "next_start": _iso(2)}
    assert _open_times(second) == [_iso(2)]
    assert second["page"] == {"has_more": False, "next_start": None}
    assert _open_times(exact) == [_iso(0), _iso(1), _iso(2)]
    assert exact["page"] == {"has_more": False, "next_start": None}
    # The row at `end` is outside the window; rows beyond it are not counted.
    assert only_outside == {
        "bars": [],
        "page": {"has_more": False, "next_start": None},
    }
    assert empty_window == {
        "bars": [],
        "page": {"has_more": False, "next_start": None},
    }


def test_limit_zero_probes_the_globally_earliest_row(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(4), _bar(9)])
    _write(store, [_bar(0), _bar(2)])

    with _serve(store) as server:
        probe = _json(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=0,
            )
        )
        again = _json(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=0,
                start=probe["page"]["next_start"],
            )
        )
        page = _json(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=2,
            )
        )
        empty = _json(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=0,
                start=_iso(10),
            )
        )

    assert probe == {
        "bars": [],
        "page": {"has_more": True, "next_start": _iso(0)},
    }
    # Existing Time Cursor behaviour: `limit=0` itself never advances.
    assert again == probe
    assert _open_times(page) == [_iso(0), _iso(2)]
    assert empty == {
        "bars": [],
        "page": {"has_more": False, "next_start": None},
    }


# -- revision-heavy behaviour is preserved, not fixed ----------------------


def test_revision_heavy_page_boundary_reproduces_the_documented_overlap(
    tmp_path: Path,
) -> None:
    """KNOWN CONTRACT GAP: docs/revision-heavy-time-cursor.md section 3.1.

    Two raw Revisions share minute(0). `limit=1` returns the first and sets
    `next_start` to the same timestamp; the inclusive `start` re-reads the
    group from its first row, so the follow-up page is identical. Ticket 07
    must preserve this exactly; the fix belongs to the independent contract
    decision, not to this storage migration.
    """
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0, revision=0), _bar(1, revision=0)])
    _write(store, [_bar(0, revision=1)])

    with _serve(store) as server:
        first = _json(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=1,
            )
        )
        second = _json(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=1,
                start=first["page"]["next_start"],
            )
        )
        third = _json(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=1,
                start=second["page"]["next_start"],
            )
        )
        full_page = _json(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=10,
            )
        )

    assert _rows(first) == [(_iso(0), 0)]
    assert first["page"] == {"has_more": True, "next_start": _iso(0)}
    assert second == first
    assert third == first
    # The raw Revisions are still returned in full by a page that fits them.
    assert _rows(full_page) == [(_iso(0), 0), (_iso(0), 1), (_iso(1), 0)]


def test_paginated_ceiling_still_counts_raw_revisions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(bars_routes, "JSON_ROW_LIMIT", 5)
    store = ParquetBarStore(tmp_path)
    for revision in range(3):
        _write(store, [_bar(0, revision=revision)])
    _write(store, [_bar(1), _bar(2), _bar(3)])

    with _serve(store) as server:
        status, _, body = _get(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=10,
            )
        )
        page = _json(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=2,
            )
        )

    assert status == 400
    error = json.loads(body)["error"]
    assert error["code"] == "range_too_large"
    # Six raw rows: three Revisions at minute(0) plus three later Bars.
    assert error["details"] == {"row_count": 6, "limit": 5}
    assert _rows(page) == [(_iso(0), 0), (_iso(0), 1)]
    assert page["page"] == {"has_more": True, "next_start": _iso(0)}
    assert active_session_locks(store.dataset_root) == []


# -- explicit resource budget ----------------------------------------------


def test_sort_budget_exhaustion_is_an_error_not_a_short_page(
    tmp_path: Path,
) -> None:
    """A global sort over the budget fails; it never returns fewer rows."""
    writer = ParquetBarStore(tmp_path)
    _write(
        writer,
        [
            _bar(minute, seq=minute, open_value=f"{100 + minute % 100}")
            for minute in range(100_000)
        ],
    )

    temp_directory = tmp_path / "duckdb-temp"
    constrained = ParquetBarStore(
        tmp_path,
        duckdb=DuckDBOptions(
            memory_limit="8MB",
            threads=4,
            temp_directory=temp_directory,
            max_temp_directory_size="1MB",
        ),
    )
    with _serve(constrained) as server:
        status, headers, body = _get(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=100,
            )
        )
    assert status == 500
    assert _header(headers, "Content-Type").startswith("application/json")
    error = json.loads(body)["error"]
    assert error["code"] == "internal_error"
    assert error["message"] == "internal server error"
    leaked = body.decode("utf-8").lower()
    assert "duckdb" not in leaked
    assert "out of memory" not in leaked
    assert "read_parquet" not in leaked
    assert active_session_locks(constrained.dataset_root) == []

    # The same Snapshot under the configured budget returns the full page.
    healthy = ParquetBarStore(tmp_path)
    with _serve(healthy) as server:
        payload = _json(
            _url(
                server.base_url,
                interval="1m",
                instrument=INSTRUMENT,
                limit=100,
            )
        )
    assert _open_times(payload) == [_iso(minute) for minute in range(100)]
    assert payload["page"] == {
        "has_more": True,
        "next_start": _iso(100),
    }
    assert active_session_locks(healthy.dataset_root) == []
