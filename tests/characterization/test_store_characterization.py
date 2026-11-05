"""Freeze `ParquetBarStore` read semantics before the engine replacement.

These assertions use only the Store's public read API:
`iter_batches`, `read_bars` and `count_bars`. They describe what a caller can
observe today, so the DuckDB implementation must reproduce it:

* Manifest authority and active-entry filtering;
* raw Revisions (no implicit deduplication);
* duplicate rows preserved with their multiplicity;
* `[start, end)` on `open_ts`, including `start == end`;
* interval restored from the Hive partition, exact Arrow schema;
* no total order for non-paginated reads (only row content is asserted).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from mdgw.core.schema import BAR_SCHEMA
from mdgw.core.time import datetime_to_ns

from .dataset_builder import (
    BASE,
    BTC,
    ETH,
    boundary_dataset,
    cross_partition_dataset,
    duplicate_rows_dataset,
    empty_dataset,
    inactive_entry_dataset,
    large_dataset,
    make_bar,
    minute,
    multi_file_dataset,
    revision_history_dataset,
    single_file_dataset,
    single_row_dataset,
)


def _scan(store, **filters) -> list[dict]:
    return [
        row
        for batch in store.iter_batches(**filters)
        for row in batch.to_pylist()
    ]


def _identities(rows: list[dict]) -> list[tuple[str, str, datetime, int]]:
    """Row identity including revision; deliberately order-insensitive."""
    return sorted(
        (
            row["instrument_id"],
            row["interval"],
            row["open_ts"],
            row["revision"],
        )
        for row in rows
    )


def test_empty_dataset_returns_no_rows_zero_count_and_contract_schema(
    tmp_path: Path,
) -> None:
    dataset = empty_dataset(tmp_path)

    assert list(dataset.store.iter_batches()) == []
    assert dataset.store.count_bars() == 0
    table = dataset.store.read_bars()
    assert table.num_rows == 0
    assert table.schema == BAR_SCHEMA


def test_single_row_round_trips_exact_values_and_restores_interval(
    tmp_path: Path,
) -> None:
    dataset = single_row_dataset(tmp_path)

    rows = _scan(dataset.store)

    assert len(rows) == 1
    row = rows[0]
    assert row["instrument_id"] == BTC
    # `interval` is not stored inside the Data File; the store restores it.
    assert row["interval"] == "1m"
    assert row["open_ts"] == BASE
    assert row["close_ts"] == minute(1)
    assert row["open"] == Decimal("65000.1")
    assert row["high"] == Decimal("101")
    assert row["volume"] == Decimal("1")
    assert row["open"].as_tuple().exponent == -18
    table = dataset.store.read_bars()
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 1


def test_single_file_and_multiple_files_return_every_row(
    tmp_path: Path,
) -> None:
    one = single_file_dataset(tmp_path / "one", rows=5)
    many = multi_file_dataset(tmp_path / "many", files=3, rows_per_file=2)

    assert len(one.files) == 1
    assert len(many.files) == 3
    assert one.store.count_bars() == 5
    assert many.store.count_bars() == 6
    assert [row["open_ts"] for row in _scan(one.store)] == [
        minute(index) for index in range(5)
    ]
    assert {row["open_ts"] for row in _scan(many.store)} == {
        minute(index) for index in range(6)
    }


def test_cross_partition_filters_by_instrument_interval_and_open_ts(
    tmp_path: Path,
) -> None:
    dataset = cross_partition_dataset(tmp_path)
    store = dataset.store

    assert store.count_bars() == 5
    assert store.count_bars(instrument_id=BTC) == 4
    assert store.count_bars(instrument_id=ETH) == 1
    assert store.count_bars(interval="1m") == 4
    assert store.count_bars(interval="5m") == 1
    assert store.count_bars(instrument_id=BTC, interval="1m") == 3
    # February rows only: the time filter is `[start, end)` on open_ts.
    february_start = datetime_to_ns(datetime(2026, 2, 1, tzinfo=UTC))
    assert store.count_bars(start_ns=february_start) == 2
    assert store.count_bars(end_ns=february_start) == 3
    five_minute = _scan(store, interval="5m")
    assert len(five_minute) == 1
    assert five_minute[0]["close_ts"] == minute(5)


def test_inactive_manifest_entries_are_never_read(tmp_path: Path) -> None:
    dataset = inactive_entry_dataset(tmp_path)

    assert len(dataset.files) == 2
    # The inactive file still exists on disk; only the Manifest hides it.
    assert dataset.files[1]["active"] is True
    assert dataset.store.count_bars() == 1
    assert [row["open_ts"] for row in _scan(dataset.store)] == [minute(0)]
    assert dataset.manifest()["files"][1]["active"] is False


def test_duplicate_rows_keep_their_multiplicity(tmp_path: Path) -> None:
    dataset = duplicate_rows_dataset(tmp_path)

    assert dataset.store.count_bars() == 3
    rows = _scan(dataset.store)
    assert len(rows) == 3
    assert {(row["open"], row["revision"]) for row in rows} == {
        (Decimal("42.5"), 0)
    }


def test_revisions_are_returned_raw_without_deduplication(
    tmp_path: Path,
) -> None:
    dataset = revision_history_dataset(tmp_path, revisions=3)

    assert dataset.store.count_bars() == 3
    rows = _scan(dataset.store)
    assert sorted(row["revision"] for row in rows) == [0, 1, 2]
    # Raw reads keep the exact stored price of every revision.
    assert sorted(row["open"] for row in rows) == [
        Decimal("100"),
        Decimal("101"),
        Decimal("102"),
    ]


def test_half_open_open_ts_boundaries_include_start_and_exclude_end(
    tmp_path: Path,
) -> None:
    dataset = boundary_dataset(tmp_path)
    store = dataset.store
    start = datetime_to_ns(minute(5))
    end = datetime_to_ns(minute(10))

    window = _scan(store, start_ns=start, end_ns=end)
    assert _identities(window) == sorted(
        [
            (BTC, "1m", minute(5), 0),
            (BTC, "1m", minute(5), 1),
            (BTC, "1m", minute(9), 0),
        ]
    )
    # A row exactly at `start` is included: all five rows start at >= 5m.
    assert store.count_bars(start_ns=start) == 5
    # A row exactly at `end` is excluded; one at `end` is the next inclusive
    # start, so it appears there and not in the previous window.
    second = _scan(store, start_ns=end, end_ns=datetime_to_ns(minute(15)))
    assert _identities(second) == [(BTC, "1m", minute(10), 0)]
    # `start == end` is an empty half-open interval, not a full read.
    assert _scan(store, start_ns=start, end_ns=start) == []
    assert store.count_bars(start_ns=start, end_ns=start) == 0
    # The same timestamp appears in two files and both rows survive.
    assert store.count_bars(start_ns=start, end_ns=datetime_to_ns(minute(6))) == 2


def test_limit_caps_raw_rows_not_identities(tmp_path: Path) -> None:
    dataset = duplicate_rows_dataset(tmp_path)
    store = dataset.store

    assert store.count_bars() == 3
    assert len(_scan(store, limit=2)) == 2
    assert store.read_bars(limit=0).num_rows == 0
    assert len(_scan(store, limit=0)) == 0
    assert store.count_bars() == 3  # count has no limit parameter


def test_scan_order_is_not_a_contract_but_content_is_complete(
    tmp_path: Path,
) -> None:
    dataset = boundary_dataset(tmp_path)
    # Manifest append order is 9, (5, 10), 5-revision, 15.
    expected = [
        (BTC, "1m", minute(9), 0),
        (BTC, "1m", minute(5), 0),
        (BTC, "1m", minute(10), 0),
        (BTC, "1m", minute(5), 1),
        (BTC, "1m", minute(15), 0),
    ]
    rows = _scan(dataset.store)
    # Do not freeze the physical scan order as a contract: only that every
    # stored row, and each duplicate/revision, is present exactly once.
    # DuckDB makes no non-paginated ordering promise.
    assert _identities(rows) == sorted(expected)


def test_large_dataset_scan_and_count(tmp_path: Path) -> None:
    dataset = large_dataset(tmp_path, 5_000)

    assert dataset.store.count_bars() == 5_000
    count = sum(batch.num_rows for batch in dataset.store.iter_batches())
    assert count == 5_000
    table = dataset.store.read_bars(limit=10)
    assert table.num_rows == 10
    assert table.schema == BAR_SCHEMA
    assert table.column("open_ts")[0].as_py() == BASE
    assert make_bar(open_ts=BASE).open == Decimal("100")
