"""Data Contract v1 ``BAR_SCHEMA`` and JSON Message Envelope decoding.

This module intentionally duplicates the Arrow schema definition instead of
importing ``mdgw``: the SDK must be usable without the gateway package. A
contract test asserts it stays equal to ``mdgw.core.schema.BAR_SCHEMA``.

The JSON wire form (WebSocket / REST JSON) encodes timestamps as ISO-8601
nanosecond strings and decimals as exact decimal strings. Decoding goes through
``Decimal`` and integer nanoseconds; no value ever passes through ``float``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from decimal import Decimal
from typing import Any

import pyarrow as pa

from mdgw_client.time import iso_to_ns

UTC_TS = pa.timestamp("ns", tz="UTC")
PRICE = pa.decimal128(38, 18)
QUANTITY = pa.decimal128(38, 18)

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

TIMESTAMP_COLUMNS = ("open_ts", "close_ts", "event_ts", "ingest_ts")
DECIMAL_COLUMNS = ("open", "high", "low", "close", "volume")


def empty_table() -> pa.Table:
    """An empty Table carrying the complete Bar schema."""
    return pa.Table.from_pylist([], schema=BAR_SCHEMA)


def _payload(message: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = message.get("payload")
    if not isinstance(payload, Mapping):
        msg = "Message Envelope is missing its 'payload' object"
        raise ValueError(msg)  # ruff: ignore[type-check-without-type-error] - ValueError is the envelope-error contract
    return payload


def _decimal(value: Any) -> Decimal:
    """Decode a JSON decimal without ever constructing a float.

    Data Contract v1 emits exact decimal strings. Floats are rejected because
    ``Decimal(float)`` would capture the binary approximation.
    """
    if isinstance(value, (bool, float)):
        msg = "decimal fields must be exact strings, not floats"
        raise ValueError(msg)  # ruff: ignore[type-check-without-type-error] - tests and callers catch ValueError
    return Decimal(value)


def message_to_row(message: Mapping[str, Any]) -> dict[str, Any]:
    """Flatten one JSON Message Envelope into a Bar schema row."""
    payload = _payload(message)
    return {
        "schema_version": message["schema_version"],
        "data_type": message["data_type"],
        "instrument_id": message["instrument_id"],
        "interval": payload["interval"],
        "open_ts": iso_to_ns(payload["open_ts"]),
        "close_ts": iso_to_ns(payload["close_ts"]),
        "event_ts": iso_to_ns(message["event_ts"]),
        "ingest_ts": iso_to_ns(message["ingest_ts"]),
        "feed": message["feed"],
        "seq": message["seq"],
        "aggregation_source": payload["aggregation_source"],
        "open": _decimal(payload["open"]),
        "high": _decimal(payload["high"]),
        "low": _decimal(payload["low"]),
        "close": _decimal(payload["close"]),
        "volume": _decimal(payload["volume"]),
        "trade_count": payload["trade_count"],
        "revision": payload["revision"],
    }


def messages_to_table(messages: Iterable[Mapping[str, Any]]) -> pa.Table:
    """Decode JSON Message Envelopes into the canonical Arrow Bar Table."""
    rows = [message_to_row(message) for message in messages]
    if not rows:
        return empty_table()
    return pa.Table.from_pylist(rows, schema=BAR_SCHEMA)


def ipc_to_table(payload: bytes) -> pa.Table:
    """Decode one Arrow IPC stream, as carried by the ZMQ wire format."""
    return pa.ipc.open_stream(pa.BufferReader(payload)).read_all()


def table_to_ipc(table: pa.Table) -> bytes:
    """Encode a Table as an Arrow IPC stream (tests and stubs only)."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()
