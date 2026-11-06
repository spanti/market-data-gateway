# ruff: file-ignore[implicit-namespace-package]
"""Ordered earliest-row reads for single-Instrument JSON pagination.

Ticket 07 replaces the Python per-batch concat/sort page reader with one
DuckDB query: the whole Manifest Snapshot selection is ordered by `open_ts`
ascending and limited to `keep` rows. These tests use the `StoreRead` seam to
freeze the observable contract the REST page reader depends on:

* globally earliest rows across every selected Data File, not file order;
* raw Revisions and duplicate rows are preserved, never deduplicated;
* the half-open `[start, end)` window is applied before the row limit;
* rows sharing an `open_ts` keep the previously observable stable order
  (Manifest file order, then physical row order) without introducing a new
  public tie-break or cursor;
* `keep` is a positive row count; the empty selection keeps `BAR_SCHEMA`.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

from mdgw.core.bar import Bar
from mdgw.core.schema import BAR_SCHEMA
from mdgw.core.time import datetime_to_ns, ns_to_iso
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from pathlib import Path

    import pyarrow as pa

INSTRUMENT = "binance:BTC/USDT"
START = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)


def _bar(
    minute: int,
    *,
    revision: int = 0,
    seq: int | None = None,
    open_value: str = "100.5",
) -> Bar:
    open_ts = START + timedelta(minutes=minute)
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


def _rows(table: pa.Table) -> list[tuple[str, int, int | None]]:
    open_ts = table.column("open_ts").cast("int64").to_pylist()
    return [
        (ns_to_iso(ts), revision, seq)
        for ts, revision, seq in zip(
            open_ts,
            table.column("revision").to_pylist(),
            table.column("seq").to_pylist(),
            strict=True,
        )
    ]


def test_earliest_read_orders_the_whole_snapshot_by_open_ts(tmp_path: Path) -> None:
    """Files are append-ordered and overlap; the page must still be earliest-first."""
    store = ParquetBarStore(tmp_path)
    # Appended out of time order, with overlapping file time ranges.
    _write(store, [_bar(5, seq=5), _bar(9, seq=9)])
    _write(store, [_bar(0, revision=1, seq=0), _bar(7, seq=7)])
    _write(store, [_bar(0, revision=0, seq=0), _bar(1, seq=1)])
    _write(store, [_bar(3, seq=3)])

    with store.open_read(interval="1m") as read:
        page = read.read_earliest_arrow_bars(keep=4)
        total = read.count_arrow()

    assert page.schema == BAR_SCHEMA
    assert total == 7
    # Globally earliest four raw rows; both minute(0) Revisions are kept.
    assert [open_ts for open_ts, _, _ in _rows(page)] == [
        _iso(0),
        _iso(0),
        _iso(1),
        _iso(3),
    ]
    assert sorted(revision for _, revision, _ in _rows(page)) == [0, 0, 0, 1]
    # No dedup: the two raw Revisions at minute(0) are distinct rows.
    revisions_at_zero = {
        revision for open_ts, revision, _ in _rows(page) if open_ts == _iso(0)
    }
    assert revisions_at_zero == {0, 1}


def test_earliest_read_keeps_ties_in_the_previous_stable_storage_order(
    tmp_path: Path,
) -> None:
    """The pre-change page reader sorted stably over Manifest/file order.

    ADR 0012 does not define an in-timestamp order, but ticket 03 froze the
    observable current order. The DuckDB query has to reproduce it so
    Revision-heavy pagination does not change silently.
    """
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0, revision=1, open_value="200")])
    _write(store, [_bar(0, revision=0, open_value="100")])
    _write(store, [_bar(1, revision=0)])

    with store.open_read(interval="1m") as read:
        table = read.read_earliest_arrow_bars(keep=3)

    assert [(open_ts, revision) for open_ts, revision, _ in _rows(table)] == [
        (_iso(0), 1),
        (_iso(0), 0),
        (_iso(1), 0),
    ]


def test_earliest_read_applies_half_open_bounds_before_the_row_limit(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(minute) for minute in range(6)])

    with store.open_read(
        interval="1m",
        start_ns=datetime_to_ns(START + timedelta(minutes=1)),
        end_ns=datetime_to_ns(START + timedelta(minutes=5)),
    ) as read:
        bounded = read.read_earliest_arrow_bars(keep=10)
        probe = read.read_earliest_arrow_bars(keep=1)
        count = read.count_arrow()

    assert count == 4
    assert [open_ts for open_ts, _, _ in _rows(bounded)] == [
        _iso(1),
        _iso(2),
        _iso(3),
        _iso(4),
    ]
    # Rows outside `end` cannot occupy the probe row.
    assert _rows(probe) == [(_iso(1), 0, None)]


def test_earliest_read_handles_empty_and_oversized_keep(tmp_path: Path) -> None:
    empty = ParquetBarStore(tmp_path / "empty")
    populated = ParquetBarStore(tmp_path / "populated")
    _write(populated, [_bar(0), _bar(1)])

    with empty.open_read(interval="1m") as read:
        none = read.read_earliest_arrow_bars(keep=1)
    with populated.open_read(interval="1m") as read:
        all_rows = read.read_earliest_arrow_bars(keep=100)

    assert none.num_rows == 0
    assert none.schema == BAR_SCHEMA
    assert [(open_ts, revision) for open_ts, revision, _ in _rows(all_rows)] == [
        (_iso(0), 0),
        (_iso(1), 0),
    ]
    with (
        populated.open_read(interval="1m") as read,
        pytest.raises(ValueError, match="positive"),
    ):
        read.read_earliest_arrow_bars(keep=0)
