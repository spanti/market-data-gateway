"""Arrow representation of the Data Contract (v1): Bar, Instrument, Envelope."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import timedelta
from typing import Any

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

# Message Envelope columns (DESIGN §3.1). Every streaming message carries these
# alongside its payload columns, so they are also present in BAR_SCHEMA below.
MESSAGE_ENVELOPE_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.uint16(), nullable=False),
        pa.field("data_type", pa.string(), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("event_ts", UTC_TS, nullable=False),
        pa.field("ingest_ts", UTC_TS, nullable=False),
        pa.field("feed", pa.string(), nullable=False),
        pa.field("seq", pa.int64(), nullable=True),
    ]
)

# Bar payload columns (DESIGN §3.1). Instrument identity, event/ingestion time and
# feed live in the Message Envelope, so they are not repeated here.
BAR_PAYLOAD_SCHEMA = pa.schema(
    [
        pa.field("interval", pa.string(), nullable=False),
        pa.field("open_ts", UTC_TS, nullable=False),
        pa.field("close_ts", UTC_TS, nullable=False),
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

# Logical schema: envelope columns plus Bar payload columns. This is the Arrow
# schema used on ZMQ, in Arrow IPC responses and as the read-side Bar contract.
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

# Instrument Reference Data contract (DESIGN §3.1). Ticket 02 only defines the
# schema; YAML loading and the Instrument Snapshot belong to a later ticket.
INSTRUMENT_SCHEMA = pa.schema(
    [
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("venue", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("asset_class", pa.string(), nullable=False),
        pa.field("instrument_type", pa.string(), nullable=False),
        pa.field("base", pa.string(), nullable=False),
        pa.field("quote", pa.string(), nullable=False),
        pa.field("tick_size", PRICE, nullable=False),
        pa.field("lot_size", QUANTITY, nullable=False),
        pa.field("multiplier", QUANTITY, nullable=False),
        pa.field("timezone", pa.string(), nullable=False),
        pa.field("listing_ts", UTC_TS, nullable=False),
        pa.field("delisted_ts", UTC_TS, nullable=True),
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
FILE_PARTITION_COLUMNS: set[str] = {"interval"}

PHYSICAL_SCHEMA = pa.schema(
    [field for field in BAR_SCHEMA if field.name not in FILE_PARTITION_COLUMNS]
)


def _bar_to_dict(bar: Bar) -> dict[str, Any]:
    """将单个 Bar 对象高效转换为符合 Schema 格式的字典。"""
    return {
        "schema_version": bar.schema_version,
        "data_type": bar.data_type,
        "instrument_id": bar.instrument_id,
        "interval": bar.interval,
        # 在 Python 层仅对时间戳做必要的纳秒转换
        "open_ts": datetime_to_ns(bar.open_ts),
        "close_ts": datetime_to_ns(bar.close_ts),
        "event_ts": datetime_to_ns(bar.event_ts),
        "ingest_ts": datetime_to_ns(bar.ingest_ts),
        "feed": bar.feed,
        "seq": bar.seq,
        "aggregation_source": bar.aggregation_source,
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "trade_count": bar.trade_count,
        "revision": bar.revision,
    }


def bars_to_logical_table(bars: Iterable[Bar]) -> pa.Table:
    """通过 PyArrow 的 C++ 批量序列化能力，高效将 Bar 集合转为逻辑表。"""
    # 单次遍历生成字典列表
    pylist = [_bar_to_dict(bar) for bar in bars]
    
    if not pylist:
        return empty_logical_table()
        
    # 利用 PyArrow 核心的 from_pylist 批量构造，避开了 18 次列循环
    return pa.Table.from_pylist(pylist, schema=BAR_SCHEMA)


def bars_to_physical_table(bars: Iterable[Bar]) -> pa.Table:
    """生成物理表（剔除了物理存储分区列）。"""
    logical = bars_to_logical_table(bars)
    return logical.select(PHYSICAL_SCHEMA.names)


def empty_logical_table() -> pa.Table:
    """创建一个符合 BAR_SCHEMA 结构的空 Arrow 表。"""
    return pa.Table.from_pylist([], schema=BAR_SCHEMA)


def arrow_type_name(dtype: pa.DataType) -> str:
    """将 Arrow 类型对象转换为可读的契约文本字符串。"""
    if pa.types.is_timestamp(dtype):
        return f"timestamp[{dtype.unit}, tz={dtype.tz}]"
    if pa.types.is_decimal(dtype):
        return f"decimal128({dtype.precision}, {dtype.scale})"
    return str(dtype)


def schema_to_json(schema: pa.Schema) -> dict[str, list[dict[str, Any]]]:
    """Render an Arrow schema as the JSON form stored in `_dataset.json`."""
    return {
        "fields": [
            {
                "name": field.name,
                "type": arrow_type_name(field.type),
                "nullable": field.nullable,
            }
            for field in schema
        ]
    }


def schema_json() -> dict[str, list[dict[str, Any]]]:
    """JSON representation of the Bar Data Contract."""
    return schema_to_json(BAR_SCHEMA)


def instrument_schema_json() -> dict[str, list[dict[str, Any]]]:
    """JSON representation of the Instrument Data Contract."""
    return schema_to_json(INSTRUMENT_SCHEMA)


def message_envelope_schema_json() -> dict[str, list[dict[str, Any]]]:
    """JSON representation of the Message Envelope contract."""
    return schema_to_json(MESSAGE_ENVELOPE_SCHEMA)
