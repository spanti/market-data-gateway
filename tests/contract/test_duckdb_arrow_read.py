"""DuckDB Relation Arrow Reader behind the ticket-04 StoreRead seam.

These tests compare the new DuckDB scan with the pre-migration PyArrow scan at
the Store level: exact per-row content, duplicate counts and the Data Contract
schema. Non-paginated output is not ordered, so comparisons are order
insensitive; the schema comparison is exact, including field order and
nullability.
"""

from __future__ import annotations

# ruff: file-ignore[implicit-namespace-package]
from __future__ import annotations

import json
import struct
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from pydantic import ValidationError

from mdgw.config import GatewayConfig
from mdgw.core.bar import Bar
from mdgw.core.schema import BAR_SCHEMA
from mdgw.core.time import datetime_to_ns
from mdgw.store.duckdb_reader import DuckDBUnavailableError
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import active_session_locks

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
    base: datetime = START,
    open_value: str = "100.5",
    trade_count: int | None = 1,
    seq: int | None = None,
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
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal("1"),
        trade_count=trade_count,
        revision=revision,
    )


def _write(store: ParquetBarStore, bars: list[Bar]) -> None:
    store.write_bars(bars, asset_class="crypto")


def _read_duckdb(read) -> pa.Table:
    batches = list(read.iter_arrow_batches())
    if not batches:
        return pa.Table.from_batches([], schema=BAR_SCHEMA)
    assert all(batch.schema.equals(BAR_SCHEMA) for batch in batches)
    return pa.Table.from_batches(batches, schema=BAR_SCHEMA)


def _row_multiset(table: pa.Table) -> Counter:
    return Counter(
        tuple(row[name] for name in BAR_SCHEMA.names)
        for row in table.to_pylist()
    )


def test_duckdb_arrow_read_returns_one_bar_with_the_contract_schema(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])

    with store.open_read(interval="1m") as read:
        table = _read_duckdb(read)

    assert table.schema.equals(BAR_SCHEMA)
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["instrument_id"] == INSTRUMENT
    assert row["interval"] == "1m"
    assert row["open"] == Decimal("100.5")
    assert row["open_ts"] == START


def test_duckdb_arrow_read_preserves_contract_types_and_nanoseconds(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    # 1 ns after the epoch and 1 ns before it: both are non-microsecond
    # multiples that would be rounded by a 1.x reader.
    store.write_bars(
        [
            _bar(0, open_value="0.000000000000000001", trade_count=0),
            _bar(1, open_value="-99999999999999999999.999999999999999999"),
        ],
        asset_class="crypto",
    )
    # A pre-epoch Bar lives in its own month partition.
    pre_epoch = datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
    store.write_bars(
        [
            Bar(
                schema_version=1,
                data_type="bar",
                instrument_id=INSTRUMENT,
                interval="1m",
                open_ts=pre_epoch,
                close_ts=pre_epoch + timedelta(minutes=1),
                event_ts=pre_epoch + timedelta(minutes=1),
                ingest_ts=pre_epoch + timedelta(minutes=1),
                feed="fake",
                seq=None,
                aggregation_source="external",
                open=Decimal("1"),
                high=Decimal("1"),
                low=Decimal("1"),
                close=Decimal("1"),
                volume=Decimal("1"),
                trade_count=None,
                revision=0,
            )
        ],
        asset_class="crypto",
    )

    with store.open_read(interval="1m") as read:
        table = _read_duckdb(read)

    assert table.schema.field("open").type == pa.decimal128(38, 18)
    assert table.schema.field("open_ts").type == pa.timestamp("ns", tz="UTC")
    assert table.schema.field("trade_count").type == pa.uint32()
    assert table.schema.field("trade_count").nullable is True
    assert table.schema.field("seq").nullable is True
    assert table.schema.field("instrument_id").nullable is False

    values = {row["open_ts"]: row for row in table.to_pylist()}
    first = values[datetime(2024, 1, 1, 0, 0, tzinfo=UTC)]
    assert first["open"] == Decimal("0.000000000000000001")
    second = values[datetime(2024, 1, 1, 0, 1, tzinfo=UTC)]
    assert second["open"] == Decimal("-99999999999999999999.999999999999999999")
    assert any(row["trade_count"] is None for row in values.values())


def test_duckdb_arrow_read_matches_pyarrow_for_multi_file_duplicates(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    duplicate = _bar(0, open_value="42.5")
    _write(store, [duplicate, duplicate])
    _write(store, [duplicate])
    _write(store, [_bar(1), _bar(2, revision=1)])

    with store.open_read(interval="1m") as read:
        expected = read.read_bars()
    with store.open_read(interval="1m") as read:
        actual = _read_duckdb(read)

    assert _row_multiset(actual) == _row_multiset(expected)
    assert actual.num_rows == expected.num_rows == 5


def test_duckdb_arrow_read_prunes_inactive_and_unlisted_files(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])
    _write(store, [_bar(1)])

    # Mark the second entry inactive after corrupting its file: a reader that
    # opened it would fail. A stray, unlisted Parquet-looking file must not be
    # globbed either.
    manifest = _manifest(store.dataset_root)
    inactive = manifest["files"][1]
    (store.dataset_root / inactive["path"]).write_bytes(b"not a parquet file")
    inactive["active"] = False
    _write_manifest(store.dataset_root, manifest)
    (store.dataset_root / "stray.parquet").write_bytes(b"not a parquet file")

    with store.open_read(interval="1m") as read:
        table = _read_duckdb(read)

    assert table.num_rows == 1
    assert table.column("open_ts").to_pylist() == [START]


def test_duckdb_arrow_read_honours_half_open_open_ts_bounds(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(minute) for minute in range(5)])

    def ns(minute: int) -> int:
        return datetime_to_ns(START + timedelta(minutes=minute))

    with store.open_read(
        interval="1m", start_ns=ns(1), end_ns=ns(4)
    ) as read:
        table = _read_duckdb(read)
    assert [value.minute for value in table.column("open_ts").to_pylist()] == [
        1,
        2,
        3,
    ]

    with store.open_read(interval="1m", start_ns=ns(3), end_ns=ns(3)) as read:
        assert _read_duckdb(read).num_rows == 0


def test_duckdb_arrow_read_returns_every_raw_revision(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    for revision in range(3):
        _write(store, [_bar(0, revision=revision)])

    with store.open_read(interval="1m") as read:
        expected = read.read_bars()
    with store.open_read(interval="1m") as read:
        actual = _read_duckdb(read)

    assert actual.num_rows == 3
    assert _row_multiset(actual) == _row_multiset(expected)
    assert sorted(row["revision"] for row in actual.to_pylist()) == [0, 1, 2]


def test_duckdb_arrow_read_prunes_other_instruments_and_intervals(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0, instrument_id=INSTRUMENT, interval="1m")])
    _write(store, [_bar(0, instrument_id=OTHER_INSTRUMENT, interval="1m")])
    _write(store, [_bar(0, instrument_id=INSTRUMENT, interval="5m")])

    with store.open_read(
        instrument_id=INSTRUMENT, interval="1m"
    ) as read:
        table = _read_duckdb(read)

    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["instrument_id"] == INSTRUMENT
    assert row["interval"] == "1m"


def test_duckdb_arrow_read_streams_multiple_batches(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(minute) for minute in range(7)])

    with store.open_read(interval="1m") as read:
        batches = list(read.iter_arrow_batches(batch_size=2))

    assert len(batches) > 1
    assert sum(batch.num_rows for batch in batches) == 7
    assert all(batch.schema.equals(BAR_SCHEMA) for batch in batches)


def test_duckdb_arrow_read_restores_interval_for_every_partition(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0, interval="1m"), _bar(1, interval="1m")])
    _write(store, [_bar(0, interval="5m")])

    with store.open_read() as read:
        table = _read_duckdb(read)

    assert table.num_rows == 3
    assert sorted(
        (row["interval"], row["open_ts"].minute) for row in table.to_pylist()
    ) == [("1m", 0), ("1m", 1), ("5m", 0)]


def test_duckdb_arrow_read_limit_caps_the_streamed_rows(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(minute) for minute in range(7)])

    with store.open_read(interval="1m") as read:
        batches = list(read.iter_arrow_batches(limit=3, batch_size=2))

    assert sum(batch.num_rows for batch in batches) == 3
    assert all(batch.schema.equals(BAR_SCHEMA) for batch in batches)


def test_duckdb_arrow_read_releases_the_lease_after_partial_iteration(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(minute) for minute in range(4)])

    read = store.open_read(interval="1m")
    iterator = read.iter_arrow_batches(batch_size=1)
    first = next(iterator)
    assert first.num_rows == 1
    assert active_session_locks(store.dataset_root)

    # The generator close runs the scanner finally path; read.close is the
    # duplicate-close-safe public release.
    iterator.close()
    read.close()
    read.close()
    assert active_session_locks(store.dataset_root) == []


def test_duckdb_arrow_read_failure_releases_the_lease(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])
    _write(store, [_bar(1)])
    manifest = _manifest(store.dataset_root)
    victim = store.dataset_root / manifest["files"][1]["path"]
    raw = victim.read_bytes()
    footer_length = struct.unpack("<I", raw[-8:-4])[0]
    tail = raw[-(footer_length + 8) :]
    victim.write_bytes(raw[:4] + b"\x00" * (len(raw) - len(tail) - 4) + tail)

    read = store.open_read(interval="1m")
    with pytest.raises((duckdb.Error, OSError)):
        list(read.iter_arrow_batches())
    read.close()
    assert active_session_locks(store.dataset_root) == []


def test_duckdb_arrow_missing_file_refreshes_the_snapshot_once(
    tmp_path: Path,
) -> None:
    writer = ParquetBarStore(tmp_path)
    _write(writer, [_bar(0)])
    _write(writer, [_bar(1)])

    read = writer.open_read(interval="1m")
    try:
        manifest = _manifest(writer.dataset_root)
        victim = manifest["files"][0]
        (writer.dataset_root / victim["path"]).unlink()
        victim["active"] = False
        _write_manifest(writer.dataset_root, manifest)

        table = pa.Table.from_batches(
            list(read.iter_arrow_batches()), schema=BAR_SCHEMA
        )
        assert table.column("open_ts").to_pylist() == [
            START + timedelta(minutes=1)
        ]
        assert all(entry["active"] for entry in read.snapshot.entries)
    finally:
        read.close()
    assert active_session_locks(writer.dataset_root) == []


def test_duckdb_storage_config_has_explicit_resource_defaults() -> None:
    storage = GatewayConfig().storage.duckdb
    assert storage.memory_limit == "1GB"
    assert storage.threads == 4
    assert storage.temp_directory is None
    assert storage.max_temp_directory_size == "10GB"
    assert storage.arrow_batch_size == 65_536

    options = storage.to_options()
    config = options.connection_config()
    assert config["timezone"] == "UTC"
    assert config["memory_limit"] == "1GB"
    assert config["threads"] == 4
    assert config["max_temp_directory_size"] == "10GB"
    assert "temp_directory" not in config


def test_duckdb_storage_config_rejects_invalid_resources() -> None:
    with pytest.raises(ValidationError):
        GatewayConfig.model_validate({"storage": {"duckdb": {"threads": 0}}})
    with pytest.raises(ValidationError):
        GatewayConfig.model_validate(
            {"storage": {"duckdb": {"arrow_batch_size": 0}}}
        )
    with pytest.raises(ValidationError, match="byte size"):
        GatewayConfig.model_validate(
            {"storage": {"duckdb": {"memory_limit": "lots"}}}
        )
    with pytest.raises(ValidationError, match="byte size"):
        GatewayConfig.model_validate(
            {"storage": {"duckdb": {"max_temp_directory_size": ""}}}
        )


def test_store_startup_rejects_duckdb_1x(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(duckdb, "__version__", "1.4.2")
    with pytest.raises(DuckDBUnavailableError, match="requires 2.x"):
        ParquetBarStore(tmp_path)


def _manifest(dataset_root: Path) -> dict:
    return json.loads((dataset_root / "_manifest.json").read_text(encoding="utf-8"))


def _write_manifest(dataset_root: Path, manifest: dict) -> None:
    (dataset_root / "_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
