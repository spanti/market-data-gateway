"""REST API for Bar history: JSON, Arrow IPC stream, CSV and file export.

The API is a thin adapter over `ParquetBarStore`: every format reads through the
same batch iterator, so JSON / Arrow / CSV / export cannot drift apart.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from aiohttp import web

from mdgw.core.envelope import decimal_to_str, table_to_messages
from mdgw.core.errors import (
    INTERNAL_ERROR,
    INSTRUMENT_NOT_FOUND,
    INVALID_FORMAT,
    INVALID_INSTRUMENT,
    INVALID_INTERVAL,
    INVALID_RANGE,
    RANGE_TOO_LARGE,
    GatewayError,
)
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import BAR_SCHEMA, INTERVALS
from mdgw.core.time import iso_to_ns, ns_to_iso
from mdgw.reference import InstrumentStore, instrument_to_json
from mdgw.store.parquet_dataset import ParquetBarStore

logger = logging.getLogger(__name__)

JSON_ROW_LIMIT = 100_000
BARS_FORMATS = frozenset({"json", "arrow", "csv"})
EXPORT_FORMATS = frozenset({"parquet", "arrow"})
TIMESTAMP_COLUMNS = ("open_ts", "close_ts", "event_ts", "ingest_ts")
DECIMAL_COLUMNS = ("open", "high", "low", "close", "volume")

ARROW_STREAM_CONTENT_TYPE = "application/vnd.apache.arrow.stream"
ARROW_FILE_CONTENT_TYPE = "application/vnd.apache.arrow.file"
PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"
CSV_CONTENT_TYPE = "text/csv; charset=utf-8"

_END_OF_BATCHES = object()
REQUEST_ID_KEY = web.RequestKey("request_id", str)


@dataclass(frozen=True, slots=True)
class BarQuery:
    """Validated history query shared by `/bars` and `/bars/export`."""

    interval: str
    instrument: str | None
    start_ns: int | None
    end_ns: int | None
    limit: int | None
    format: str


@web.middleware
async def _request_id_middleware(request: web.Request, handler) -> web.StreamResponse:
    request[REQUEST_ID_KEY] = uuid4().hex
    return await handler(request)


def create_app(
    store: ParquetBarStore,
    *,
    instrument_store: InstrumentStore | None = None,
) -> web.Application:
    """Create an aiohttp web application for the Bar history API.

    Instrument endpoints read only the `InstrumentStore` snapshot; they never
    parse the YAML truth source at request time.
    """
    app = web.Application(middlewares=[_request_id_middleware])
    app.router.add_get(
        "/api/v1/instruments", _make_instruments_handler(instrument_store)
    )
    app.router.add_get(
        "/api/v1/instruments/{instrument_id:.+}",
        _make_instrument_handler(instrument_store),
    )
    app.router.add_get("/api/v1/bars", _make_bars_handler(store))
    app.router.add_get("/api/v1/bars/export", _make_export_handler(store))
    app.router.add_get("/api/v1/status", _make_status_handler(store))
    return app


def _make_instruments_handler(instrument_store: InstrumentStore | None):
    """Create the handler for the Instrument list endpoint."""

    async def handle_instruments(request: web.Request) -> web.StreamResponse:
        include_delisted = _parse_include_delisted(request)
        if isinstance(include_delisted, web.Response):
            return include_delisted

        instruments = (
            []
            if instrument_store is None
            else instrument_store.list(
                venue=request.query.get("venue"),
                asset_class=request.query.get("asset_class"),
                include_delisted=include_delisted,
            )
        )
        return web.json_response(
            {"instruments": [instrument_to_json(item) for item in instruments]}
        )

    return handle_instruments


def _make_instrument_handler(instrument_store: InstrumentStore | None):
    """Create the handler for one Instrument lookup, including Delisted ones."""

    async def handle_instrument(request: web.Request) -> web.StreamResponse:
        instrument_id = request.match_info["instrument_id"]
        try:
            InstrumentId.parse(instrument_id)
        except ValueError:
            return _error(
                request,
                code=INVALID_INSTRUMENT,
                message="path parameter 'instrument_id' must be an InstrumentId",
            )

        instrument = (
            None if instrument_store is None else instrument_store.get(instrument_id)
        )
        if instrument is None:
            return _error(
                request,
                code=INSTRUMENT_NOT_FOUND,
                message=f"unknown Instrument {instrument_id!r}",
                status=404,
            )
        return web.json_response({"instrument": instrument_to_json(instrument)})

    return handle_instrument


def _parse_include_delisted(request: web.Request) -> bool | web.Response:
    raw = request.query.get("include_delisted", "false").strip().lower()
    if raw in {"true", "1"}:
        return True
    if raw in {"false", "0", ""}:
        return False
    return _error(
        request,
        code=INVALID_RANGE,
        message="query parameter 'include_delisted' must be a boolean",
    )


def _validate_request(
    request: web.Request,
    *,
    formats: frozenset[str],
    default_format: str,
) -> BarQuery | web.Response:
    """Validate query parameters for a Bar history request.

    Returns a `BarQuery` when valid, or a JSON error response otherwise.
    """
    interval = request.query.get("interval")
    if not interval or interval not in INTERVALS:
        return _error(
            request,
            code=INVALID_INTERVAL,
            message="query parameter 'interval' is required and must be a Bar Interval",
        )

    instrument = request.query.get("instrument")
    if instrument is not None:
        try:
            InstrumentId.parse(instrument)
        except ValueError:
            return _error(
                request,
                code=INVALID_INSTRUMENT,
                message="query parameter 'instrument' must be an InstrumentId",
            )

    output_format = request.query.get("format", default_format)
    if output_format not in formats:
        return _error(
            request,
            code=INVALID_FORMAT,
            message=(
                "query parameter 'format' must be one of: "
                + ", ".join(sorted(formats))
            ),
        )

    try:
        start_ns = _parse_optional_ns(request.query.get("start"))
        end_ns = _parse_optional_ns(request.query.get("end"))
    except ValueError as exc:
        return _error(request, code=INVALID_RANGE, message=str(exc))

    if start_ns is not None and end_ns is not None and start_ns > end_ns:
        return _error(
            request,
            code=INVALID_RANGE,
            message="query parameter 'start' must not be after 'end'",
        )

    limit = None
    raw_limit = request.query.get("limit")
    if raw_limit is not None:
        try:
            limit = int(raw_limit)
        except ValueError:
            return _error(
                request,
                code=INVALID_RANGE,
                message="query parameter 'limit' must be an integer",
            )
        if limit < 0:
            return _error(
                request,
                code=INVALID_RANGE,
                message="query parameter 'limit' must not be negative",
            )

    return BarQuery(
        interval=interval,
        instrument=instrument,
        start_ns=start_ns,
        end_ns=end_ns,
        limit=limit,
        format=output_format,
    )


def _make_bars_handler(store: ParquetBarStore):
    """Create a request handler for the Bar history API."""

    async def handle_bars(request: web.Request) -> web.StreamResponse:
        query = _validate_request(
            request, formats=BARS_FORMATS, default_format="json"
        )
        if isinstance(query, web.Response):
            return query

        try:
            if query.format == "json":
                return await _respond_json(request, store, query)
            if query.format == "arrow":
                return await _stream_arrow(request, store, query)
            return await _stream_csv(request, store, query)
        except GatewayError as exc:
            return _error(
                request,
                code=exc.code,
                message=exc.message,
                status=exc.status,
                details=exc.details,
            )
        except Exception as exc:  # pragma: no cover - defensive error boundary
            logger.exception("Bar history request failed")
            return _error(
                request, code=INTERNAL_ERROR, message=str(exc), status=500
            )

    return handle_bars


def _make_export_handler(store: ParquetBarStore):
    """Create a request handler that downloads the result as one file."""

    async def handle_export(request: web.Request) -> web.StreamResponse:
        query = _validate_request(
            request, formats=EXPORT_FORMATS, default_format="parquet"
        )
        if isinstance(query, web.Response):
            return query

        try:
            path = await asyncio.to_thread(_write_export_file, store, query)
            content_type = (
                PARQUET_CONTENT_TYPE
                if query.format == "parquet"
                else ARROW_FILE_CONTENT_TYPE
            )
            suffix = "parquet" if query.format == "parquet" else "arrow"
            return await _send_file(
                request,
                path,
                content_type=content_type,
                filename=f"{query.interval}-bars.{suffix}",
            )
        except GatewayError as exc:
            return _error(
                request,
                code=exc.code,
                message=exc.message,
                status=exc.status,
                details=exc.details,
            )
        except Exception as exc:  # pragma: no cover - defensive error boundary
            logger.exception("Bar export failed")
            return _error(
                request, code=INTERNAL_ERROR, message=str(exc), status=500
            )

    return handle_export


def _make_status_handler(store: ParquetBarStore):
    """Create a request handler for Dataset operational status."""

    async def handle_status(request: web.Request) -> web.StreamResponse:
        try:
            payload = await asyncio.to_thread(store.status)
        except GatewayError as exc:
            return _error(
                request,
                code=exc.code,
                message=exc.message,
                status=exc.status,
                details=exc.details,
            )
        except Exception as exc:  # pragma: no cover - defensive error boundary
            logger.exception("Dataset status request failed")
            return _error(
                request, code=INTERNAL_ERROR, message=str(exc), status=500
            )
        return web.json_response(payload)

    return handle_status


async def _respond_json(
    request: web.Request, store: ParquetBarStore, query: BarQuery
) -> web.Response:
    total = await asyncio.to_thread(
        store.count_bars,
        instrument_id=query.instrument,
        interval=query.interval,
        start_ns=query.start_ns,
        end_ns=query.end_ns,
    )
    if total > JSON_ROW_LIMIT:
        return _error(
            request,
            code=RANGE_TOO_LARGE,
            message=(
                f"JSON output is limited to {JSON_ROW_LIMIT} Bars; "
                "use format=arrow or format=csv for larger ranges"
            ),
            details={"row_count": total, "limit": JSON_ROW_LIMIT},
        )

    table = await asyncio.to_thread(
        store.read_bars,
        instrument_id=query.instrument,
        interval=query.interval,
        start_ns=query.start_ns,
        end_ns=query.end_ns,
        limit=query.limit,
    )
    return web.json_response({"bars": table_to_messages(table)})


async def _stream_arrow(
    request: web.Request, store: ParquetBarStore, query: BarQuery
) -> web.StreamResponse:
    await asyncio.to_thread(store.validate_schema_version)

    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": ARROW_STREAM_CONTENT_TYPE,
            "Content-Disposition": f'inline; filename="{query.interval}-bars.arrow"',
        },
    )
    await response.prepare(request)

    sink = _ChunkSink()
    writer = pa.ipc.new_stream(sink, BAR_SCHEMA)
    wrote_any = False
    try:
        async for batch in _iter_batches(store, query):
            writer.write_batch(batch)
            wrote_any = True
            await _flush(response, sink)
        if not wrote_any:
            writer.write_batch(_empty_batch())
        writer.close()
        await _flush(response, sink)
    finally:
        await response.write_eof()
    return response


async def _stream_csv(
    request: web.Request, store: ParquetBarStore, query: BarQuery
) -> web.StreamResponse:
    await asyncio.to_thread(store.validate_schema_version)

    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": CSV_CONTENT_TYPE,
            "Content-Disposition": f'inline; filename="{query.interval}-bars.csv"',
        },
    )
    await response.prepare(request)

    try:
        await response.write((",".join(BAR_SCHEMA.names) + "\n").encode("utf-8"))
        async for batch in _iter_batches(store, query):
            text = _batch_to_csv(batch)
            if text:
                await response.write(text.encode("utf-8"))
    finally:
        await response.write_eof()
    return response


async def _iter_batches(store: ParquetBarStore, query: BarQuery):
    """Iterate Bar batches on a worker thread so the event loop stays free."""
    iterator = store.iter_batches(
        instrument_id=query.instrument,
        interval=query.interval,
        start_ns=query.start_ns,
        end_ns=query.end_ns,
        limit=query.limit,
    )
    while True:
        batch = await asyncio.to_thread(next, iterator, _END_OF_BATCHES)
        if batch is _END_OF_BATCHES:
            return
        yield batch


async def _flush(response: web.StreamResponse, sink: "_ChunkSink") -> None:
    chunk = sink.drain()
    if chunk:
        await response.write(chunk)


def _batch_to_csv(batch: pa.RecordBatch) -> str:
    """Render one batch as CSV without ever routing a price through float."""
    columns: dict[str, list[Any]] = {}
    for name in TIMESTAMP_COLUMNS:
        columns[name] = [
            ns_to_iso(value)
            for value in batch.column(name).cast(pa.int64()).to_pylist()
        ]
    for name in DECIMAL_COLUMNS:
        columns[name] = [
            decimal_to_str(value) for value in batch.column(name).to_pylist()
        ]
    for name in BAR_SCHEMA.names:
        if name not in columns:
            columns[name] = batch.column(name).to_pylist()

    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for index in range(batch.num_rows):
        writer.writerow([columns[name][index] for name in BAR_SCHEMA.names])
    return buffer.getvalue()


def _write_export_file(store: ParquetBarStore, query: BarQuery) -> Path:
    """Materialize the query into one temporary Parquet/Arrow file."""
    tempdir = Path(tempfile.mkdtemp(prefix="mdgw-export-"))
    suffix = "parquet" if query.format == "parquet" else "arrow"
    path = tempdir / f"{query.interval}-bars.{suffix}"

    try:
        batches = store.iter_batches(
            instrument_id=query.instrument,
            interval=query.interval,
            start_ns=query.start_ns,
            end_ns=query.end_ns,
            limit=query.limit,
        )
        if query.format == "parquet":
            writer = pq.ParquetWriter(
                path,
                BAR_SCHEMA,
                version=store.format_version,
                use_deprecated_int96_timestamps=False,
                compression=store.compression,
            )
            try:
                for batch in batches:
                    writer.write_batch(batch)
            finally:
                writer.close()
        else:
            with pa.OSFile(str(path), "wb") as sink:
                with pa.ipc.new_file(sink, BAR_SCHEMA) as writer:
                    for batch in batches:
                        writer.write_batch(batch)
    except Exception:
        shutil.rmtree(tempdir, ignore_errors=True)
        raise
    return path


async def _send_file(
    request: web.Request,
    path: Path,
    *,
    content_type: str,
    filename: str,
) -> web.StreamResponse:
    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": content_type,
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(path.stat().st_size),
        },
    )
    try:
        await response.prepare(request)
        with path.open("rb") as handle:
            while True:
                chunk = await asyncio.to_thread(handle.read, 1_048_576)
                if not chunk:
                    break
                await response.write(chunk)
    finally:
        if response.prepared:
            await response.write_eof()
        _remove_export(path)
    return response


class _ChunkSink:
    """File-like sink that collects IPC bytes until the handler drains them."""

    def __init__(self) -> None:
        self._chunks: list[bytes] = []
        self.closed = False

    def write(self, data: bytes) -> int:
        self._chunks.append(bytes(data))
        return len(data)

    def close(self) -> None:
        self.closed = True

    def drain(self) -> bytes:
        chunk = b"".join(self._chunks)
        self._chunks.clear()
        return chunk


def _empty_batch() -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(
        [pa.array([], type=field.type) for field in BAR_SCHEMA],
        schema=BAR_SCHEMA,
    )


def _parse_optional_ns(value: str | None) -> int | None:
    if value is None:
        return None
    return iso_to_ns(value)


def _remove_export(path: Path) -> None:
    """Remove a finished export file and the temporary directory holding it."""
    try:
        path.unlink(missing_ok=True)
        parent = path.parent
        if parent.name.startswith("mdgw-export-"):
            parent.rmdir()
    except OSError:  # pragma: no cover - best-effort cleanup
        pass


def _error(
    request: web.Request,
    code: str,
    message: str,
    *,
    status: int = 400,
    details: dict[str, Any] | None = None,
) -> web.Response:
    return web.json_response(
        {
            "error": {
                "code": code,
                "message": message,
                "details": details or {},
                "request_id": request.get(REQUEST_ID_KEY),
            }
        },
        status=status,
    )
