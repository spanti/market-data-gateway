"""REST API for Bar history (JSON only in this ticket)."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pyarrow as pa
from aiohttp import web

from mdgw.core.schema import INTERVALS
from mdgw.core.time import ns_to_iso
from mdgw.store.parquet_dataset import ParquetBarStore

TIMESTAMP_COLUMNS = ("open_ts", "close_ts", "event_ts", "ingest_ts")


def create_app(store: ParquetBarStore) -> web.Application:
    app = web.Application()
    app.router.add_get("/api/v1/bars", _make_bars_handler(store))
    return app


def _make_bars_handler(store: ParquetBarStore):
    async def handle_bars(request: web.Request) -> web.Response:
        interval = request.query.get("interval")
        if not interval or interval not in INTERVALS:
            return _error(
                "invalid_interval",
                "query parameter 'interval' is required and must be a Bar Interval",
            )

        instrument = request.query.get("instrument")
        if instrument is not None and ":" not in instrument:
            return _error(
                "invalid_instrument",
                "query parameter 'instrument' must be an InstrumentId",
            )

        try:
            table = await asyncio.to_thread(
                store.read_bars, instrument_id=instrument, interval=interval
            )
        except Exception as exc:  # pragma: no cover - defensive error boundary
            return _error("internal_error", str(exc), status=500)

        return web.json_response({"bars": table_to_messages(table)})

    return handle_bars


def table_to_messages(table: pa.Table) -> list[dict]:
    if table.num_rows == 0:
        return []

    columns = {
        name: table.column(name).to_pylist() for name in table.column_names
    }
    timestamps = {
        name: table.column(name).cast(pa.int64()).to_pylist()
        for name in TIMESTAMP_COLUMNS
    }

    messages: list[dict] = []
    for index in range(table.num_rows):
        messages.append(
            {
                "schema_version": columns["schema_version"][index],
                "data_type": columns["data_type"][index],
                "instrument_id": columns["instrument_id"][index],
                "event_ts": ns_to_iso(timestamps["event_ts"][index]),
                "ingest_ts": ns_to_iso(timestamps["ingest_ts"][index]),
                "feed": columns["feed"][index],
                "seq": columns["seq"][index],
                "payload": {
                    "interval": columns["interval"][index],
                    "open_ts": ns_to_iso(timestamps["open_ts"][index]),
                    "close_ts": ns_to_iso(timestamps["close_ts"][index]),
                    "aggregation_source": columns["aggregation_source"][index],
                    "open": decimal_to_str(columns["open"][index]),
                    "high": decimal_to_str(columns["high"][index]),
                    "low": decimal_to_str(columns["low"][index]),
                    "close": decimal_to_str(columns["close"][index]),
                    "volume": decimal_to_str(columns["volume"][index]),
                    "trade_count": columns["trade_count"][index],
                    "revision": columns["revision"][index],
                },
            }
        )
    return messages


def decimal_to_str(value: Decimal) -> str:
    """Exact decimal text with at least one fractional digit, never a float."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0")
        if text.endswith("."):
            text += "0"
    else:
        text += ".0"
    return text


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
