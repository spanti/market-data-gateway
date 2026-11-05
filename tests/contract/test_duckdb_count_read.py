# ruff: file-ignore[implicit-namespace-package]
"""DuckDB count and materialized reads for the JSON REST surface.

The JSON row-ceiling count and the non-paginated JSON read use the one
DuckDB scan: `count_arrow()` and `read_arrow_bars()` preserve exact row
content, duplicate multiplicity and the Data Contract schema. Assertions are
order insensitive because non-paginated output has no ordering contract.
"""

from __future__ import annotations

import json
import struct
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

import duckdb
import pytest

from mdgw.core.bar import Bar
from mdgw.core.schema import BAR_SCHEMA
from mdgw.core.time import datetime_to_ns
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import active_session_locks

if TYPE_CHECKING:
    from pathlib import Path

    import pyarrow as pa

UTC = timezone.utc
INSTRUMENT = "binance:BTC/USDT"
OTHER_INSTRUMENT = "binance:ETH/USDT"
START = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)


def _bar(
    minute: int,
    *,
    instrument_id: str = INSTRUMENT,
    interval: str = "1m",
    revision: int = 0,
    open_value: str = "100.5",
) -> Bar:
    open_ts = START + timedelta(minutes=minute)
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
        seq=None,
        aggregation_source="external",
        open=Decimal(open_value),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal("1"),
        trade_count=None,
        revision=revision,
    )


def _write(store: ParquetBarStore, bars: list[Bar]) -> None:
    store.write_bars(bars, asset_class="crypto")


def _row_multiset(table: pa.Table) -> Counter:
    return Counter(
        tuple(row[name] for name in BAR_SCHEMA.names)
        for row in table.to_pylist()
    )


def test_count_arrow_counts_raw_revisions_and_duplicate_rows(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    duplicate = _bar(0, revision=0)
    _write(store, [duplicate, duplicate])
    _write(store, [_bar(0, revision=1), _bar(1)])
    _write(store, [_bar(0, revision=2), _bar(1)])

    with store.open_read(interval="1m") as read:
        assert read.count_arrow() == 6
        # The public count is the same query on the same Snapshot.
        assert read.count() == 6


def test_read_arrow_bars_preserves_multi_file_revisions(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    duplicate = _bar(0, revision=0)
    _write(store, [duplicate, duplicate])
    _write(store, [_bar(0, revision=1), _bar(1)])
    _write(store, [_bar(0, revision=2), _bar(1, revision=1)])

    with store.open_read(interval="1m") as read:
        actual = read.read_arrow_bars()

    assert actual.schema.equals(BAR_SCHEMA)
    assert actual.num_rows == 6
    assert Counter(
        (row["open_ts"].minute, row["revision"])
        for row in actual.to_pylist()
    ) == Counter(
        {(0, 0): 2, (0, 1): 1, (0, 2): 1, (1, 0): 1, (1, 1): 1}
    )


def test_count_and_read_apply_instrument_interval_and_half_open_bounds(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(minute) for minute in range(5)])
    _write(store, [_bar(0, instrument_id=OTHER_INSTRUMENT)])
    _write(store, [_bar(0, interval="5m")])

    def ns(minute: int) -> int:
        return datetime_to_ns(START + timedelta(minutes=minute))

    with store.open_read(
        instrument_id=INSTRUMENT,
        interval="1m",
        start_ns=ns(1),
        end_ns=ns(4),
    ) as read:
        assert read.count_arrow() == 3
        table = read.read_arrow_bars()

    assert table.schema.equals(BAR_SCHEMA)
    assert sorted(row["open_ts"].minute for row in table.to_pylist()) == [
        1,
        2,
        3,
    ]
    assert table.column("instrument_id").to_pylist() == [INSTRUMENT] * 3
    assert table.column("interval").to_pylist() == ["1m"] * 3

    with store.open_read(
        instrument_id=INSTRUMENT,
        interval="1m",
        start_ns=ns(3),
        end_ns=ns(3),
    ) as read:
        assert read.count_arrow() == 0
        assert read.read_arrow_bars().schema.equals(BAR_SCHEMA)
        assert read.read_arrow_bars().num_rows == 0


def test_count_and_read_on_an_empty_dataset_keep_the_contract_schema(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)

    with store.open_read(interval="1m") as read:
        assert read.count_arrow() == 0
        table = read.read_arrow_bars()

    assert table.schema.equals(BAR_SCHEMA)
    assert table.num_rows == 0
    assert active_session_locks(store.dataset_root) == []


def test_read_arrow_bars_limit_only_caps_rows_from_the_matching_set(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(minute) for minute in range(5)])
    _write(store, [_bar(minute, revision=1) for minute in range(2)])

    with store.open_read(interval="1m") as read:
        full = _row_multiset(read.read_arrow_bars())
    assert sum(full.values()) == 7

    for limit in (0, 1, 6, 7, 8):
        with store.open_read(interval="1m") as read:
            table = read.read_arrow_bars(limit=limit)
        assert table.schema.equals(BAR_SCHEMA)
        assert table.num_rows == min(limit, 7)
        partial = _row_multiset(table)
        for row, count in partial.items():
            # LIMIT may choose any scan-order rows, but never invents rows or
            # increases a row's multiplicity beyond what the Snapshot stores.
            assert count <= full[row]


def test_count_and_read_share_one_snapshot_across_a_concurrent_write(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0), _bar(1)])

    with store.open_read(interval="1m") as read:
        counted = read.count_arrow()
        rows = _row_multiset(read.read_arrow_bars())
        _write(store, [_bar(2)])

        assert read.count_arrow() == counted == 2
        assert _row_multiset(read.read_arrow_bars()) == rows

    # A fresh request-level Snapshot sees the new generation.
    assert store.count_bars(interval="1m") == 3


def test_revision_heavy_count_and_read_preserve_every_raw_revision(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    for revision in range(4):
        _write(store, [_bar(0, revision=revision)])
    _write(store, [_bar(minute) for minute in range(1, 6)])

    with store.open_read(interval="1m") as read:
        assert read.count_arrow() == 9
        actual = read.read_arrow_bars()

    assert actual.num_rows == 9
    revisions_at_start = sorted(
        row["revision"]
        for row in actual.to_pylist()
        if row["open_ts"] == START
    )
    assert revisions_at_start == [0, 1, 2, 3]


def _corrupt_data_pages(path: Path) -> bytes:
    """Keep the Parquet magic and footer; zero the already written pages."""
    original = path.read_bytes()
    footer_length = struct.unpack("<I", original[-8:-4])[0]
    tail = original[-(footer_length + 8) :]
    path.write_bytes(
        original[:4] + b"\x00" * (len(original) - len(tail) - 4) + tail
    )
    return original


def test_count_arrow_releases_the_lease_and_connection_after_failure(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])
    _write(store, [_bar(1)])
    manifest = json.loads(
        (store.dataset_root / "_manifest.json").read_text(encoding="utf-8")
    )
    victim = store.dataset_root / manifest["files"][1]["path"]
    # Break the footer, not just the pages: an exact count reads Parquet
    # metadata, while the response read would fail later on page data.
    original = victim.read_bytes()
    victim.write_bytes(b"not a parquet file")

    read = store.open_read(interval="1m")
    with pytest.raises((duckdb.Error, OSError)):
        read.count_arrow()
    read.close()
    assert active_session_locks(store.dataset_root) == []

    # A failed count must not poison the Store: after the file is repaired a
    # later read acquires a working connection and counts every row.
    victim.write_bytes(original)
    with store.open_read(interval="1m") as reopened:
        assert reopened.count_arrow() == 2


def test_read_arrow_bars_releases_the_lease_and_connection_after_failure(
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

    read = store.open_read(interval="1m")
    with pytest.raises((duckdb.Error, OSError)):
        read.read_arrow_bars()
    read.close()
    assert active_session_locks(store.dataset_root) == []

    victim.write_bytes(original)
    with store.open_read(interval="1m") as reopened:
        assert reopened.read_arrow_bars().num_rows == 2


def test_successful_count_and_read_release_the_lease(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0), _bar(1)])

    read = store.open_read(interval="1m")
    assert read.count_arrow() == 2
    assert read.read_arrow_bars().num_rows == 2
    assert active_session_locks(store.dataset_root)
    read.close()
    read.close()
    assert active_session_locks(store.dataset_root) == []
