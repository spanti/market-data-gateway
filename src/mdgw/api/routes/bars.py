"""Bar history endpoints: JSON, Arrow IPC stream, CSV and file export.

Every format reads through the same batch iterator, so JSON / Arrow / CSV /
export cannot drift apart. Store calls run on worker threads; streaming bodies
are never materialised in full.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import shutil
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import APIRouter, Query
from starlette.background import BackgroundTask
from starlette.responses import JSONResponse, Response, StreamingResponse

from mdgw.api.formats import (
    ARROW_FILE_MEDIA_TYPE,
    ARROW_STREAM_MEDIA_TYPE,
    BARS_FORMATS,
    CSV_MEDIA_TYPE,
    EXPORT_FORMATS,
    PARQUET_MEDIA_TYPE,
    content_disposition,
)
from mdgw.core.envelope import decimal_to_str, table_to_messages
from mdgw.core.errors import (
    INVALID_FORMAT,
    RANGE_TOO_LARGE,
    GatewayError,
    InvalidInstrumentError,
    InvalidIntervalError,
    InvalidRangeError,
)
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import BAR_SCHEMA, INTERVALS
from mdgw.core.time import iso_to_ns, ns_to_iso

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator

    from mdgw.store.parquet_dataset import ParquetBarStore, StoreRead

logger = logging.getLogger(__name__)

JSON_ROW_LIMIT = 100_000
TIMESTAMP_COLUMNS = ("open_ts", "close_ts", "event_ts", "ingest_ts")
DECIMAL_COLUMNS = ("open", "high", "low", "close", "volume")

_EXPORT_PREFIX = "mdgw-export-"
_READ_CHUNK_SIZE = 1_048_576
_END_OF_BATCHES = object()

_INTERVAL_DESCRIPTION = "Bar Interval, one of 1m, 5m, 15m, 1h, 1d"
_INSTRUMENT_DESCRIPTION = "InstrumentId filter, for example binance:BTC/USDT"
_START_DESCRIPTION = "Inclusive open_ts bound as an ISO-8601 timestamp"
_END_DESCRIPTION = "Exclusive open_ts bound as an ISO-8601 timestamp"
_LIMIT_DESCRIPTION = "Maximum number of Bars to return"


class _InvalidFormatError(GatewayError):
    """The requested output format is not supported by this endpoint."""

    code = INVALID_FORMAT
    status = 400


class _RangeTooLargeError(GatewayError):
    """A JSON response would exceed the configured ceiling."""

    code = RANGE_TOO_LARGE
    status = 400


@dataclass(frozen=True, slots=True)
class _ExportSpec:
    """Everything that differs between the two file-export formats."""

    content_type: str
    suffix: str
    writer: Callable[
        [Iterator[pa.RecordBatch], Path, ParquetBarStore], None
    ]


@dataclass(frozen=True, slots=True)
class _RawQuery:
    interval: str
    instrument: str | None
    start: str | None
    end: str | None
    limit: str | None
    format: str | None


@dataclass(frozen=True, slots=True)
class BarQuery:
    """Validated history query shared by `/bars` and `/bars/export`."""

    interval: str
    instrument: str | None
    start_ns: int | None
    end_ns: int | None
    limit: int | None
    format: str


def create_router(*, store: ParquetBarStore) -> APIRouter:
    """Build the Bar history router for one application instance."""
    router = APIRouter()

    @router.get("/api/v1/bars")
    async def list_bars(  # ruff: ignore[too-many-arguments,too-many-positional-arguments] - query contract
        interval: Annotated[str, Query(description=_INTERVAL_DESCRIPTION)],
        instrument: Annotated[
            str | None, Query(description=_INSTRUMENT_DESCRIPTION)
        ] = None,
        start: Annotated[
            str | None, Query(description=_START_DESCRIPTION)
        ] = None,
        end: Annotated[
            str | None, Query(description=_END_DESCRIPTION)
        ] = None,
        limit: Annotated[
            str | None, Query(description=_LIMIT_DESCRIPTION)
        ] = None,
        output_format: Annotated[
            str | None, Query(alias="format")
        ] = None,
    ) -> Response:
        query = _validate_query(
            _RawQuery(
                interval=interval,
                instrument=instrument,
                start=start,
                end=end,
                limit=limit,
                format=output_format,
            ),
            formats=BARS_FORMATS,
            default_format="json",
        )
        if query.format == "json":
            return await _respond_json(store, query)
        if query.format == "arrow":
            return await _stream_arrow(store, query)
        return await _stream_csv(store, query)

    @router.get("/api/v1/bars/export")
    async def export_bars(  # ruff: ignore[too-many-arguments,too-many-positional-arguments] - query contract
        interval: Annotated[str, Query(description=_INTERVAL_DESCRIPTION)],
        instrument: Annotated[
            str | None, Query(description=_INSTRUMENT_DESCRIPTION)
        ] = None,
        start: Annotated[
            str | None, Query(description=_START_DESCRIPTION)
        ] = None,
        end: Annotated[
            str | None, Query(description=_END_DESCRIPTION)
        ] = None,
        limit: Annotated[
            str | None, Query(description=_LIMIT_DESCRIPTION)
        ] = None,
        output_format: Annotated[
            str | None, Query(alias="format")
        ] = None,
    ) -> Response:
        query = _validate_query(
            _RawQuery(
                interval=interval,
                instrument=instrument,
                start=start,
                end=end,
                limit=limit,
                format=output_format,
            ),
            formats=EXPORT_FORMATS,
            default_format="parquet",
        )
        return await _export_file(store, query)

    return router


# -- validation -------------------------------------------------------------


def _validate_query(
    raw: _RawQuery,
    *,
    formats: frozenset[str],
    default_format: str,
) -> BarQuery:
    """Turn raw query strings into a `BarQuery`, raising stable Gateway errors."""
    if not raw.interval or raw.interval not in INTERVALS:
        message = "query parameter 'interval' is required and must be a Bar Interval"
        raise InvalidIntervalError(message)

    instrument = raw.instrument
    if instrument is not None:
        try:
            InstrumentId.parse(instrument)
        except ValueError:
            message = "query parameter 'instrument' must be an InstrumentId"
            raise InvalidInstrumentError(message) from None

    output_format = (
        default_format if raw.format is None else raw.format
    )
    if output_format not in formats:
        message = (
            "query parameter 'format' must be one of: "
            + ", ".join(sorted(formats))
        )
        raise _InvalidFormatError(message)

    start_ns = _parse_optional_ns(raw.start)
    end_ns = _parse_optional_ns(raw.end)
    if start_ns is not None and end_ns is not None and start_ns > end_ns:
        message = "query parameter 'start' must not be after 'end'"
        raise InvalidRangeError(message)

    return BarQuery(
        interval=raw.interval,
        instrument=instrument,
        start_ns=start_ns,
        end_ns=end_ns,
        limit=_parse_limit(raw.limit),
        format=output_format,
    )


def _parse_optional_ns(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return iso_to_ns(value)
    except ValueError as exc:
        raise InvalidRangeError(str(exc)) from exc


def _parse_limit(raw_limit: str | None) -> int | None:
    if raw_limit is None:
        return None
    try:
        limit = int(raw_limit)
    except ValueError:
        message = "query parameter 'limit' must be an integer"
        raise InvalidRangeError(message) from None
    if limit < 0:
        message = "query parameter 'limit' must not be negative"
        raise InvalidRangeError(message)
    return limit


# -- storage access ---------------------------------------------------------
#
# One HTTP request owns one `StoreRead`: its row-ceiling count, its existence
# probe and its actual read all scan the same Manifest Snapshot under the same
# Dataset Read Lease. The helpers below only translate the validated
# `BarQuery` into the Store's primitive filters.


def _query_filters(query: BarQuery) -> dict[str, Any]:
    return {
        "instrument_id": query.instrument,
        "interval": query.interval,
        "start_ns": query.start_ns,
        "end_ns": query.end_ns,
    }


def _open_store_read(store: ParquetBarStore, query: BarQuery) -> StoreRead:
    return store.open_read(**_query_filters(query))


def _read_earliest_bars(read: StoreRead, *, keep: int) -> pa.Table:
    """Return the globally earliest ``keep`` Bars in ascending `open_ts` order.

    Storage order is not a time order (Manifest entries are append-ordered and
    Data Files may overlap), so a plain `limit` read cannot produce a page.
    Scanning every matching row and retaining only the smallest ``keep`` rows
    keeps memory bounded by the page size while preserving Arrow types exactly
    (no Python datetime/decimal round-trip).
    """
    selected: pa.Table | None = None
    for batch in read.iter_batches(limit=None):
        table = pa.Table.from_batches([batch], schema=BAR_SCHEMA)
        if selected is not None:
            table = pa.concat_tables([selected, table])
        selected = table.sort_by([("open_ts", "ascending")]).slice(0, keep)
    if selected is None:
        return pa.Table.from_batches([], schema=BAR_SCHEMA)
    return selected


# -- JSON -------------------------------------------------------------------


async def _respond_json(store: ParquetBarStore, query: BarQuery) -> Response:
    payload = await asyncio.to_thread(_build_json_payload, store, query)
    return JSONResponse(payload)


def _build_json_payload(store: ParquetBarStore, query: BarQuery) -> dict:
    """Count and read on one Snapshot, so the ceiling cannot disagree with rows."""
    with _open_store_read(store, query) as read:
        # The exact raw-row count runs through DuckDB on this request's
        # Snapshot and Lease. It is a separate query from the read, so no
        # window SQL or implicit dedup can change the ceiling.
        total = read.count_arrow()
        # `limit` caps the response, so only the rows that would actually be
        # rendered count against the JSON ceiling.
        effective_total = (
            total if query.limit is None else min(total, query.limit)
        )
        if effective_total > JSON_ROW_LIMIT:
            message = (
                f"JSON output is limited to {JSON_ROW_LIMIT} Bars; "
                "use format=arrow or format=csv for larger ranges"
            )
            raise _RangeTooLargeError(
                message,
                details={"row_count": total, "limit": JSON_ROW_LIMIT},
            )

        # Pagination metadata is a single-Instrument feature: it needs both
        # `instrument` and `limit` (limit=0 counts) to be unambiguous.
        paginated = query.instrument is not None and query.limit is not None
        if not paginated:
            # Non-paginated JSON has no ordering contract: read through
            # DuckDB, where LIMIT only caps returned rows.
            table = read.read_arrow_bars(limit=query.limit)
            return {"bars": table_to_messages(table)}

        assert query.limit is not None  # narrowed by `paginated`
        limit = query.limit
        # The paginated JSON surface has an explicit ordering contract:
        # open_ts ascending. Storage order is not ordered (the Manifest is
        # append-ordered and files may overlap), so reading `limit + 1` rows
        # first would pick the wrong page. Stream every matching row and
        # retain only the smallest `limit + 1` by `open_ts`, then render them
        # in ascending order.
        table = _read_earliest_bars(read, keep=limit + 1)

        has_more = table.num_rows > limit
        next_start: str | None = None
        if has_more:
            next_ns = table.column("open_ts").cast(pa.int64())[limit].as_py()
            next_start = ns_to_iso(next_ns)
        return {
            "bars": table_to_messages(table.slice(0, limit)),
            "page": {"has_more": has_more, "next_start": next_start},
        }


# -- Arrow stream -----------------------------------------------------------


async def _stream_arrow(
    store: ParquetBarStore,
    query: BarQuery,
) -> Response:
    # Acquire before the response starts so a schema mismatch or a Manifest
    # race still yields the stable JSON error envelope.
    read = await asyncio.to_thread(_open_store_read, store, query)
    try:
        return StreamingResponse(
            _arrow_body(read, query),
            headers={
                "Content-Type": ARROW_STREAM_MEDIA_TYPE,
                "Content-Disposition": content_disposition(
                    "inline", f"{query.interval}-bars.arrow"
                ),
            },
            # Safety net when the client disconnects before the body iterator
            # is ever started; the normal path closes in `_arrow_body`.
            background=BackgroundTask(read.close),
        )
    except BaseException:
        read.close()
        raise


async def _arrow_body(
    read: StoreRead,
    query: BarQuery,
) -> AsyncIterator[bytes]:
    sink = _ChunkSink()
    writer = pa.ipc.new_stream(sink, BAR_SCHEMA)
    wrote_any = False
    closed = False
    try:
        # Only this call site streams the DuckDB Relation Arrow Reader.
        # CSV and export keep the existing PyArrow scan.
        async for batch in _iter_arrow_batches(read, query):
            writer.write_batch(batch)
            wrote_any = True
            chunk = sink.drain()
            if chunk:
                yield chunk
        if not wrote_any:
            # An empty Arrow stream still carries the canonical schema.
            writer.write_batch(_empty_batch())
        writer.close()
        closed = True
        chunk = sink.drain()
        if chunk:
            yield chunk
    finally:
        if not closed:
            writer.close()
            sink.drain()
        read.close()


# -- CSV stream -------------------------------------------------------------


async def _stream_csv(
    store: ParquetBarStore,
    query: BarQuery,
) -> Response:
    read = await asyncio.to_thread(_open_store_read, store, query)
    try:
        return StreamingResponse(
            _csv_body(read, query),
            headers={
                "Content-Type": CSV_MEDIA_TYPE,
                "Content-Disposition": content_disposition(
                    "inline", f"{query.interval}-bars.csv"
                ),
            },
            background=BackgroundTask(read.close),
        )
    except BaseException:
        read.close()
        raise


async def _csv_body(
    read: StoreRead,
    query: BarQuery,
) -> AsyncIterator[bytes]:
    try:
        yield (",".join(BAR_SCHEMA.names) + "\n").encode("utf-8")
        async for batch in _iter_batches(read, query):
            text = _batch_to_csv(batch)
            if text:
                yield text.encode("utf-8")
    finally:
        read.close()


async def _iter_batches(
    read: StoreRead,
    query: BarQuery,
) -> AsyncIterator[pa.RecordBatch]:
    """Iterate the existing PyArrow Bar batches on worker threads."""
    iterator = read.iter_batches(limit=query.limit)
    async for batch in _pump_batches(iterator):
        yield batch


async def _iter_arrow_batches(
    read: StoreRead,
    query: BarQuery,
) -> AsyncIterator[pa.RecordBatch]:
    """Iterate DuckDB Relation Arrow Reader batches on worker threads."""
    iterator = read.iter_arrow_batches(limit=query.limit)
    async for batch in _pump_batches(iterator):
        yield batch


async def _pump_batches(
    iterator: Iterator[pa.RecordBatch],
) -> AsyncIterator[pa.RecordBatch]:
    """Move one blocking batch pull at a time off the event loop.

    On client disconnect `next` may still be running on its worker thread; the
    StoreRead close path interrupts the DuckDB scan and the lease is released
    by the caller either way.
    """
    try:
        while True:
            batch = await asyncio.to_thread(next, iterator, _END_OF_BATCHES)
            if batch is _END_OF_BATCHES:
                return
            yield batch
    finally:
        with suppress(ValueError):  # pragma: no cover - generator executing
            iterator.close()


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


# -- file export ------------------------------------------------------------


async def _export_file(
    store: ParquetBarStore,
    query: BarQuery,
) -> Response:
    path = await asyncio.to_thread(_write_export_file, store, query)
    try:
        stat = await asyncio.to_thread(path.stat)
    except BaseException:
        _remove_export(path)
        raise

    spec = _EXPORT_SPECS[query.format]
    return StreamingResponse(
        _file_body(path),
        headers={
            "Content-Type": spec.content_type,
            "Content-Disposition": content_disposition(
                "attachment", f"{query.interval}-bars.{spec.suffix}"
            ),
            "Content-Length": str(stat.st_size),
        },
    )


async def _file_body(path: Path) -> AsyncIterator[bytes]:
    handle = None
    try:
        handle = await asyncio.to_thread(path.open, "rb")
        while True:
            chunk = await asyncio.to_thread(handle.read, _READ_CHUNK_SIZE)
            if not chunk:
                break
            yield chunk
    finally:
        if handle is not None:
            handle.close()
        _remove_export(path)


def _write_export_file(store: ParquetBarStore, query: BarQuery) -> Path:
    """Materialize the query into one temporary Parquet/Arrow file."""
    spec = _EXPORT_SPECS[query.format]
    tempdir = Path(tempfile.mkdtemp(prefix=_EXPORT_PREFIX))
    path = tempdir / f"{query.interval}-bars.{spec.suffix}"

    try:
        with _open_store_read(store, query) as read:
            batches = read.iter_batches(limit=query.limit)
            spec.writer(batches, path, store)
    except Exception:
        shutil.rmtree(tempdir, ignore_errors=True)
        raise
    return path


def _write_parquet_export(
    batches: Iterator[pa.RecordBatch],
    path: Path,
    store: ParquetBarStore,
) -> None:
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


def _write_arrow_export(
    batches: Iterator[pa.RecordBatch],
    path: Path,
    _store: ParquetBarStore,
) -> None:
    with (
        pa.OSFile(str(path), "wb") as sink,
        pa.ipc.new_file(sink, BAR_SCHEMA) as writer,
    ):
        for batch in batches:
            writer.write_batch(batch)


# One mapping owns the `format` -> media type, suffix and writer choice, so the
# response headers and the materialized file cannot disagree.
_EXPORT_SPECS: dict[str, _ExportSpec] = {
    "parquet": _ExportSpec(
        PARQUET_MEDIA_TYPE, "parquet", _write_parquet_export
    ),
    "arrow": _ExportSpec(
        ARROW_FILE_MEDIA_TYPE, "arrow", _write_arrow_export
    ),
}


def _remove_export(path: Path) -> None:
    """Remove a finished export file and the temporary directory holding it."""
    try:
        path.unlink(missing_ok=True)
        parent = path.parent
        if parent.name.startswith(_EXPORT_PREFIX):
            parent.rmdir()
    except OSError:  # pragma: no cover - best-effort cleanup
        pass


# -- helpers ----------------------------------------------------------------


class _ChunkSink:
    """File-like sink that collects IPC bytes until the response drains them."""

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


__all__ = [
    "BARS_FORMATS",
    "EXPORT_FORMATS",
    "JSON_ROW_LIMIT",
    "BarQuery",
    "create_router",
]
