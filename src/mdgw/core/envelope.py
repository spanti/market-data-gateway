"""JSON form of the Message Envelope and its Bar payload (Data Contract v1).

Arrow keeps exact types (`decimal128(38, 18)`, `timestamp[ns, tz=UTC]`); JSON
cannot, so decimals become exact decimal strings and timestamps become
ISO-8601 nanosecond strings. Both encoders and decoders live here so the wire
format has one definition.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pyarrow as pa

from mdgw.core.schema import BAR_SCHEMA, empty_logical_table
from mdgw.core.time import iso_to_ns, ns_to_iso

TIMESTAMP_COLUMNS = ("open_ts", "close_ts", "event_ts", "ingest_ts")


def decimal_to_str(value: Decimal) -> str:
    """Exact decimal text with at least one fractional digit, never a float."""
    text = format(value.normalize(), "f")
    return text if "." in text else f"{text}.0"


def table_to_messages(table: pa.Table) -> list[dict[str, Any]]:
    """Convert an Arrow Table of Bars into JSON-serializable messages."""
    if table.num_rows == 0:
        return []

    for column in TIMESTAMP_COLUMNS:
        if column in table.column_names:
            table = table.set_column(
                table.column_names.index(column),
                column,
                table.column(column).cast(pa.int64()),
            )

    messages: list[dict[str, Any]] = [
        {
            "schema_version": row["schema_version"],
            "data_type": row["data_type"],
            "instrument_id": row["instrument_id"],
            "event_ts": ns_to_iso(row["event_ts"]),
            "ingest_ts": ns_to_iso(row["ingest_ts"]),
            "feed": row["feed"],
            "seq": row["seq"],
            "payload": {
                "interval": row["interval"],
                "open_ts": ns_to_iso(row["open_ts"]),
                "close_ts": ns_to_iso(row["close_ts"]),
                "aggregation_source": row["aggregation_source"],
                "open": decimal_to_str(row["open"]),
                "high": decimal_to_str(row["high"]),
                "low": decimal_to_str(row["low"]),
                "close": decimal_to_str(row["close"]),
                "volume": decimal_to_str(row["volume"]),
                "trade_count": row["trade_count"],
                "revision": row["revision"],
            },
        }
        for row in table.to_pylist()
    ]
    return messages


def messages_to_table(messages: list[dict[str, Any]]) -> pa.Table:
    """Decode JSON Message Envelopes back into the canonical Arrow Bar Table."""
    if not messages:
        return empty_logical_table()

    rows: list[dict[str, Any]] = []
    for message in messages:
        payload = message["payload"]
        rows.append(
            {
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
                "open": Decimal(payload["open"]),
                "high": Decimal(payload["high"]),
                "low": Decimal(payload["low"]),
                "close": Decimal(payload["close"]),
                "volume": Decimal(payload["volume"]),
                "trade_count": payload["trade_count"],
                "revision": payload["revision"],
            }
        )
    return pa.Table.from_pylist(rows, schema=BAR_SCHEMA)
