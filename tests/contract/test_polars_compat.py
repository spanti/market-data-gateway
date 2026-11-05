"""M1 spike gates for the Polars side of the Data Contract.

Polars 2.1.0 is the candidate chosen by the spike. These tests prove that the
README-level claim "Polars preserves Arrow decimal128(38,18), UTC nanoseconds,
uint32 and nulls" holds for the real library, including the Parquet ->
DuckDB -> Arrow -> Polars path and the reverse path. They also characterise
the one hazard found: Polars silently rescales untyped Python ``Decimal``
values, so over-scale validation must happen before data reaches Polars.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mdgw.core.schema import BAR_SCHEMA

DECIMAL_38_18 = pa.decimal128(38, 18)
NS_UTC = pa.timestamp("ns", tz="UTC")

PHYSICAL_NAMES = [field.name for field in BAR_SCHEMA if field.name != "interval"]

DECIMAL_TEXTS = [
    "99999999999999999999.999999999999999999",
    "-99999999999999999999.999999999999999999",
    "-0.000000000000000001",
    "0.000000000000000000",
    "12345678901234567890.123456789012345678",
    None,
]

NS_VALUES = [1, 999, 1000, 123456789, 0, -1, -999, -1000, -123456789, None]


def decimal_texts(values: list[Decimal | None]) -> list[str | None]:
    return [None if value is None else format(value, "f") for value in values]


def read_via_duckdb(path: Path) -> pa.Table:
    connection = duckdb.connect(config={"timezone": "UTC", "threads": "2"})
    try:
        return connection.read_parquet(str(path)).to_arrow_table()
    finally:
        connection.close()


def bar_rows() -> list[dict[str, object]]:
    return [
        {
            "schema_version": 1,
            "data_type": "bar",
            "instrument_id": "binance:BTC/USDT",
            "interval": "1m",
            "open_ts": datetime(2024, 1, 1, tzinfo=UTC),
            "close_ts": datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
            "event_ts": datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
            "ingest_ts": datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
            "feed": "binance_spot",
            "seq": 2**63 - 1,
            "aggregation_source": "external",
            "open": Decimal("99999999999999999999.999999999999999999"),
            "high": Decimal("-99999999999999999999.999999999999999999"),
            "low": Decimal("-0.000000000000000001"),
            "close": Decimal("0"),
            "volume": Decimal("12345678901234567890.123456789012345678"),
            "trade_count": 2**32 - 1,
            "revision": 2**32 - 1,
        },
        {
            "schema_version": 1,
            "data_type": "bar",
            "instrument_id": "binance:ETH/USDT",
            "interval": "1m",
            "open_ts": datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
            "close_ts": datetime(2024, 1, 1, 0, 2, tzinfo=UTC),
            "event_ts": datetime(2024, 1, 1, 0, 2, tzinfo=UTC),
            "ingest_ts": datetime(2024, 1, 1, 0, 2, tzinfo=UTC),
            "feed": "binance_spot",
            "seq": None,
            "aggregation_source": "internal",
            "open": Decimal("1"),
            "high": Decimal("1"),
            "low": Decimal("1"),
            "close": Decimal("1"),
            "volume": Decimal("1"),
            "trade_count": None,
            "revision": 0,
        },
    ]


def write_bar_file(path: Path, rows: list[dict[str, object]] | None = None) -> None:
    table = pa.Table.from_pylist(
        bar_rows() if rows is None else rows, schema=BAR_SCHEMA
    )
    pq.write_table(table.select(PHYSICAL_NAMES), path, row_group_size=4096)


# ---------------------------------------------------------------------------
# version gate
# ---------------------------------------------------------------------------


def test_polars_version_is_the_verified_candidate() -> None:
    assert pl.__version__ == "2.1.0", (
        "Polars version changed; re-run the M1 type gates and update "
        "spike-report.md before changing this pin"
    )
    assert sys.version_info[:2] == (3, 13)


# ---------------------------------------------------------------------------
# decimal128(38,18)
# ---------------------------------------------------------------------------


def test_decimal128_38_18_roundtrip_through_polars_is_exact(tmp_path: Path) -> None:
    source = pa.table(
        {
            "id": pa.array(range(len(DECIMAL_TEXTS)), type=pa.int64()),
            "value": pa.array(
                [None if text is None else Decimal(text) for text in DECIMAL_TEXTS],
                type=DECIMAL_38_18,
            ),
        }
    )
    first = tmp_path / "decimal.parquet"
    pq.write_table(source, first)

    frame = pl.read_parquet(first)
    assert frame.schema["value"] == pl.Decimal(precision=38, scale=18)
    assert decimal_texts(frame["value"].to_list()) == DECIMAL_TEXTS
    assert frame["value"].null_count() == 1

    # Arrow -> Polars -> Arrow keeps the exact decimal type and values.
    exported = frame.to_arrow()
    assert exported.schema.field("value").type == DECIMAL_38_18
    assert not pa.types.is_floating(exported.schema.field("value").type)
    assert decimal_texts(exported["value"].to_pylist()) == DECIMAL_TEXTS

    # Polars -> Arrow -> Parquet -> DuckDB (reverse path) is exact as well.
    second = tmp_path / "decimal-reverse.parquet"
    pq.write_table(exported, second)
    again = read_via_duckdb(second)
    assert again.schema.field("value").type == DECIMAL_38_18
    assert decimal_texts(again["value"].to_pylist()) == DECIMAL_TEXTS


def test_polars_decimal_rescaling_is_a_validation_hazard() -> None:
    """Characterisation: Polars rescales untyped decimals instead of failing.

    Therefore Normalization (M4) must reject over-scale decimal input before
    constructing a typed Polars series; the storage pipeline itself only ever
    receives Arrow-validated ``decimal128(38,18)`` columns.
    """
    series = pl.Series(
        "value",
        [Decimal("1.1234567890123456789")],
        dtype=pl.Decimal(precision=38, scale=18),
        strict=True,
    )
    assert series.to_list() == [Decimal("1.123456789012345679")]

    wide = pl.Series(
        "value", [Decimal("0.0000000000000000001")], dtype=pl.Decimal(38, 18)
    )
    assert wide.to_list() == [Decimal("0E-18")]

    # The same silent rescale happens through an explicit cast.
    wide_dtype = pl.Series(
        "value",
        [Decimal("1.1234567890123456789")],
        dtype=pl.Decimal(precision=38, scale=19),
    )
    narrowed = wide_dtype.cast(pl.Decimal(precision=38, scale=18))
    assert narrowed.to_list() == [Decimal("1.123456789012345679")]


# ---------------------------------------------------------------------------
# UTC nanoseconds
# ---------------------------------------------------------------------------


def test_polars_receives_utc_nanoseconds_without_truncation(tmp_path: Path) -> None:
    source = pa.table({"ts": pa.array(NS_VALUES, type=NS_UTC)})
    first = tmp_path / "ns.parquet"
    pq.write_table(source, first)

    frame = pl.read_parquet(first)
    assert frame.schema["ts"] == pl.Datetime(time_unit="ns", time_zone="UTC")
    assert frame["ts"].cast(pl.Int64).to_list() == NS_VALUES
    assert frame["ts"].null_count() == 1

    exported = frame.to_arrow()
    assert exported.schema.field("ts").type == NS_UTC
    assert exported["ts"].cast(pa.int64()).to_pylist() == NS_VALUES

    second = tmp_path / "ns-reverse.parquet"
    pq.write_table(exported, second)
    again = read_via_duckdb(second)
    assert again.schema.field("ts").type == NS_UTC
    assert again["ts"].cast(pa.int64()).to_pylist() == NS_VALUES


# ---------------------------------------------------------------------------
# uint32, nulls and empty results
# ---------------------------------------------------------------------------


def test_uint32_bounds_and_nullable_integer_columns_through_polars(
    tmp_path: Path,
) -> None:
    source = pa.table(
        {
            "trade_count": pa.array([0, 2**32 - 1, None], type=pa.uint32()),
            "seq": pa.array([0, 2**63 - 1, None], type=pa.int64()),
        }
    )
    path = tmp_path / "uint32.parquet"
    pq.write_table(source, path)

    frame = pl.read_parquet(path)
    assert frame.schema["trade_count"] == pl.UInt32
    assert frame.schema["seq"] == pl.Int64
    assert frame["trade_count"].to_list() == [0, 2**32 - 1, None]
    assert frame["seq"].to_list() == [0, 2**63 - 1, None]

    exported = frame.to_arrow()
    assert exported.schema.field("trade_count").type == pa.uint32()
    assert exported.schema.field("seq").type == pa.int64()
    assert exported["trade_count"].to_pylist() == [0, 2**32 - 1, None]


def test_empty_result_keeps_contract_dtypes(tmp_path: Path) -> None:
    path = tmp_path / "empty.parquet"
    write_bar_file(path, rows=[])

    frame = pl.read_parquet(path)
    assert frame.height == 0
    assert frame.schema["open"] == pl.Decimal(precision=38, scale=18)
    assert frame.schema["open_ts"] == pl.Datetime(time_unit="ns", time_zone="UTC")
    assert frame.schema["trade_count"] == pl.UInt32
    assert frame.schema["revision"] == pl.UInt32

    exported = frame.to_arrow()
    assert exported.num_rows == 0
    assert exported.schema.field("open").type == DECIMAL_38_18
    assert exported.schema.field("open_ts").type == NS_UTC
    assert read_via_duckdb(path).num_rows == 0


def test_full_chain_parquet_duckdb_arrow_polars_and_back(tmp_path: Path) -> None:
    prices = [
        None if text is None else Decimal(text) for text in DECIMAL_TEXTS
    ] + [Decimal("1")] * 4
    trade_counts = [0, 2**32 - 1, None, 1, 2, 3, 4, 5, 6, 7]
    seqs = [None, 0, 2**63 - 1, None, 1, 2, 3, 4, 5, 6]
    source = pa.table(
        {
            "price": pa.array(prices, type=DECIMAL_38_18),
            "ts": pa.array(NS_VALUES, type=NS_UTC),
            "trade_count": pa.array(trade_counts, type=pa.uint32()),
            "seq": pa.array(seqs, type=pa.int64()),
        }
    )
    first = tmp_path / "chain.parquet"
    pq.write_table(source, first)

    # Parquet -> DuckDB -> Arrow.
    after_duckdb = read_via_duckdb(first)
    assert after_duckdb.schema.field("price").type == DECIMAL_38_18
    assert after_duckdb.schema.field("ts").type == NS_UTC
    assert after_duckdb.schema.field("trade_count").type == pa.uint32()
    assert after_duckdb.schema.field("seq").type == pa.int64()

    # Arrow -> Polars -> Arrow.
    frame = pl.from_arrow(after_duckdb)
    assert frame.schema["price"] == pl.Decimal(precision=38, scale=18)
    assert frame.schema["ts"] == pl.Datetime(time_unit="ns", time_zone="UTC")
    assert frame.schema["trade_count"] == pl.UInt32
    after_polars = frame.to_arrow()
    assert after_polars.schema.field("price").type == DECIMAL_38_18
    assert after_polars.schema.field("ts").type == NS_UTC
    assert after_polars.schema.field("trade_count").type == pa.uint32()

    # Arrow -> Parquet -> DuckDB -> Arrow (the "and back" half).
    second = tmp_path / "chain-reverse.parquet"
    pq.write_table(after_polars, second)
    final = read_via_duckdb(second)
    assert final.schema.field("price").type == DECIMAL_38_18
    assert final.schema.field("ts").type == NS_UTC
    assert final.schema.field("trade_count").type == pa.uint32()
    assert final.schema.field("seq").type == pa.int64()
    assert decimal_texts(final["price"].to_pylist()) == DECIMAL_TEXTS + [
        "1.000000000000000000"
    ] * 4
    assert final["ts"].cast(pa.int64()).to_pylist() == NS_VALUES
    assert final["trade_count"].to_pylist() == trade_counts
    assert final["seq"].to_pylist() == seqs
    for field in final.schema:
        assert not pa.types.is_floating(field.type), field


# ---------------------------------------------------------------------------
# full BAR_SCHEMA
# ---------------------------------------------------------------------------


def test_full_bar_schema_is_restorable_after_polars_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "bars.parquet"
    write_bar_file(path)

    frame = pl.read_parquet(path)
    assert frame.height == 2

    exported = frame.to_arrow()
    # Polars reports every Arrow field as nullable and widens strings to
    # large_string; the Store restores the exact contract schema afterwards.
    assert all(field.nullable for field in exported.schema)
    assert exported.schema.field("data_type").type == pa.large_string()

    with_interval = exported.append_column(
        "interval", pa.array(["1m"] * exported.num_rows, type=pa.string())
    )
    restored = with_interval.select(BAR_SCHEMA.names).cast(BAR_SCHEMA)
    assert restored.schema.equals(BAR_SCHEMA, check_metadata=False)
    assert [field.nullable for field in restored.schema] == [
        field.nullable for field in BAR_SCHEMA
    ]

    rows = restored.to_pylist()
    assert rows[0]["trade_count"] == 2**32 - 1
    assert rows[1]["trade_count"] is None
    assert rows[0]["seq"] == 2**63 - 1
    assert rows[1]["seq"] is None
    assert format(rows[0]["open"], "f") == DECIMAL_TEXTS[0]
    assert format(rows[1]["open"], "f") == "1.000000000000000000"
