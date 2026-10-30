"""Arrow representation of the Bar Data Contract (v1)."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import timedelta

import pyarrow as pa

from mdgw.core.bar import Bar
from mdgw.core.time import datetime_to_ns

UTC_TS = pa.timestamp("ns", tz="UTC")
PRICE = pa.decimal128(38, 18)
QUANTITY = pa.decimal128(38, 18)

# Bar Interval values are Data Contract strings, not a free-form field.
INTERVALS: dict[str, timedelta] = {
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "1d": timedelta(days=1),
}

# Logical schema: envelope columns plus Bar payload columns.
BAR_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.uint16(), nullable=False),
        pa.field("data_type", pa.string(), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("interval", pa.string(), nullable=False),
        pa.field("open_ts", UTC_TS, nullable=False),
        pa.field("close_ts", UTC_TS, nullable=False),
        pa.field("event_ts", UTC_TS, nullable=False),
        pa.field("ingest_ts", UTC_TS, nullable=False),
        pa.field("feed", pa.string(), nullable=False),
        pa.field("seq", pa.int64(), nullable=True),
        pa.field("aggregation_source", pa.string(), nullable=False),
        pa.field("open", PRICE, nullable=False),
        pa.field("high", PRICE, nullable=False),
        pa.field("low", PRICE, nullable=False),
        pa.field("close", PRICE, nullable=False),
        pa.field("volume", QUANTITY, nullable=False),
        pa.field("trade_count", pa.uint32(), nullable=True),
        pa.field("revision", pa.uint32(), nullable=False),
    ]
)

# `interval` is a Hive partition column, so it is not stored inside the Data File.
PARTITION_COLUMNS: tuple[str, ...] = (
    "asset_class",
    "venue",
    "symbol",
    "interval",
    "year",
    "month",
)
FILE_PARTITION_COLUMNS: tuple[str, ...] = ("interval",)
PHYSICAL_SCHEMA = pa.schema(
    [field for field in BAR_SCHEMA if field.name not in FILE_PARTITION_COLUMNS]
)


def _timestamp_array(values: Iterable) -> pa.Array:
    return pa.array([datetime_to_ns(value) for value in values], type=UTC_TS)


def _decimal_array(values: Iterable) -> pa.Array:
    return pa.array(list(values), type=PRICE)


def _quantity_array(values: Iterable) -> pa.Array:
    return pa.array(list(values), type=QUANTITY)


def bars_to_logical_table(bars: Iterable[Bar]) -> pa.Table:
    rows = list(bars)
    columns = {
        "schema_version": pa.array([bar.schema_version for bar in rows], type=pa.uint16()),
        "data_type": pa.array([bar.data_type for bar in rows], type=pa.string()),
        "instrument_id": pa.array([bar.instrument_id for bar in rows], type=pa.string()),
        "interval": pa.array([bar.interval for bar in rows], type=pa.string()),
        "open_ts": _timestamp_array(bar.open_ts for bar in rows),
        "close_ts": _timestamp_array(bar.close_ts for bar in rows),
        "event_ts": _timestamp_array(bar.event_ts for bar in rows),
        "ingest_ts": _timestamp_array(bar.ingest_ts for bar in rows),
        "feed": pa.array([bar.feed for bar in rows], type=pa.string()),
        "seq": pa.array([bar.seq for bar in rows], type=pa.int64()),
        "aggregation_source": pa.array(
            [bar.aggregation_source for bar in rows], type=pa.string()
        ),
        "open": _decimal_array(bar.open for bar in rows),
        "high": _decimal_array(bar.high for bar in rows),
        "low": _decimal_array(bar.low for bar in rows),
        "close": _decimal_array(bar.close for bar in rows),
        "volume": _quantity_array(bar.volume for bar in rows),
        "trade_count": pa.array([bar.trade_count for bar in rows], type=pa.uint32()),
        "revision": pa.array([bar.revision for bar in rows], type=pa.uint32()),
    }
    return pa.table(columns, schema=BAR_SCHEMA)


def bars_to_physical_table(bars: Iterable[Bar]) -> pa.Table:
    logical = bars_to_logical_table(bars)
    return logical.select(PHYSICAL_SCHEMA.names)


def empty_logical_table() -> pa.Table:
    return pa.table(
        {
            field.name: pa.array([], type=field.type)
            for field in BAR_SCHEMA
        },
        schema=BAR_SCHEMA,
    )


def arrow_type_name(dtype: pa.DataType) -> str:
    if pa.types.is_timestamp(dtype):
        return f"timestamp[{dtype.unit}, tz={dtype.tz}]"
    if pa.types.is_decimal(dtype):
        return f"decimal128({dtype.precision}, {dtype.scale})"
    return str(dtype)


def schema_json() -> dict:
    return {
        "fields": [
            {
                "name": field.name,
                "type": arrow_type_name(field.type),
                "nullable": field.nullable,
            }
            for field in BAR_SCHEMA
        ]
    }
