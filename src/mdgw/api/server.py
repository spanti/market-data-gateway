"""REST API for Bar history (JSON only in this ticket)."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pyarrow as pa
from aiohttp import web
import logging

from mdgw.core.schema import INTERVALS
from mdgw.core.time import ns_to_iso
from mdgw.store.parquet_dataset import ParquetBarStore

logger = logging.getLogger(__name__)
TIMESTAMP_COLUMNS = ("open_ts", "close_ts", "event_ts", "ingest_ts")


def create_app(store: ParquetBarStore) -> web.Application:
    """Create an aiohttp web application for the Bar history API."""
    app = web.Application()
    app.router.add_get("/api/v1/bars", _make_bars_handler(store))
    return app

def _validate_request(request: web.Request) -> tuple[str, str | None] | web.Response:
    """Validate the request query parameters for the Bar history API.

    Returns a tuple of (interval, instrument) if valid, or a web.Response with an error if invalid.
    """
    interval = request.query.get("interval")
    if not interval or interval not in INTERVALS:
        return _error(
            code="invalid_interval",
            message="query parameter 'interval' is required and must be a Bar Interval",
        )

    instrument = request.query.get("instrument")
    if instrument is not None and ":" not in instrument:
        return _error(
            code="invalid_instrument",
            message="query parameter 'instrument' must be an InstrumentId",
        )

    return interval, instrument

def _make_bars_handler(store: ParquetBarStore):
    """Create a request handler for the Bar history API."""
    async def handle_bars(request: web.Request) -> web.Response:
        validation_result = _validate_request(request)
        if isinstance(validation_result, web.Response):
            return validation_result
        interval, instrument = validation_result

        try:
            table = await asyncio.to_thread(
                store.read_bars, instrument_id=instrument, interval=interval
            )
        except Exception as exc:  # pragma: no cover - defensive error boundary
            return _error("internal_error", str(exc), status=500)

        return web.json_response({"bars": table_to_messages(table)})

    return handle_bars


def table_to_messages(table: pa.Table) -> list[dict]:
    """Convert an Arrow Table of Bars into a list of JSON-serializable messages."""
    if table.num_rows == 0:
        return []

    for col in TIMESTAMP_COLUMNS:
        if col in table.column_names:
            table = table.set_column(
                table.column_names.index(col),
                col,
                table.column(col).cast(pa.int64())
            )

    raw_rows = table.to_pylist()
    messages: list[dict[str, Any]] = []

    for row in raw_rows:
        messages.append(
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
        )
    return messages


def decimal_to_str(value: Decimal) -> str:
    """Exact decimal text with at least one fractional digit, never a float."""
    text = format(value.normalize(), "f")
    return text if "." in text else f"{text}.0"

def _error(code: str, message: str, *, status: int = 400) -> web.Response:
    return web.json_response(
        {
            "error": {
                "code": code,
                "message": message,
                "details": {},
                "request_id": None,
            }
        },
        status=status,
    )
