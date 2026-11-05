"""DuckDB 2.0 Relation Arrow Reader for Manifest-pruned Bar scans.

DuckDB is only ever handed the explicit Data File list of one Manifest
Snapshot. It never globs the Dataset root, and Manifest pruning (active entry,
Instrument, Interval, partition and file time range) happens before the
connection is opened. Filter values are bound parameters; no caller input is
interpolated into SQL.

The Arrow boundary is the Data Contract: DuckDB exports every field nullable
and cannot see the Hive `interval` column, so each RecordBatch is rebuilt with
`BAR_SCHEMA` (field order, nullability and types) and the per-partition
`interval` is restored explicitly. Decimal and timestamp columns stay typed
Arrow/Parquet values; no SQL cast or Python row conversion touches them.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from mdgw.core.schema import BAR_SCHEMA, PHYSICAL_SCHEMA
from mdgw.core.time import ns_to_iso

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from pathlib import Path

    from mdgw.store.parquet_dataset import BarReadQuery

DUCKDB_MAJOR_VERSION = 2

_SELECT_PHYSICAL = ", ".join(PHYSICAL_SCHEMA.names)
_END_OF_STREAM = object()


class DuckDBUnavailableError(RuntimeError):
    """DuckDB is missing or not the supported major version."""


def ensure_supported_duckdb() -> None:
    """Fail fast when the runtime cannot provide the DuckDB 2.x reader.

    Called when a Store is constructed (application startup). There is no
    1.x fallback and no engine switch.
    """
    try:
        import duckdb  # ruff: ignore[import-outside-top-level] - fail-fast probe
    except ImportError as exc:  # pragma: no cover - dependency is declared
        msg = (
            "DuckDB is required for Arrow history reads; "
            "install the declared duckdb>=2.0,<3 dependency"
        )
        raise DuckDBUnavailableError(msg) from exc

    version = str(getattr(duckdb, "__version__", ""))
    try:
        major = int(version.split(".", 1)[0])
    except ValueError:  # pragma: no cover - defensive against odd builds
        msg = f"cannot determine the DuckDB version from {version!r}"
        raise DuckDBUnavailableError(msg) from None
    if major != DUCKDB_MAJOR_VERSION:
        msg = (
            f"DuckDB {version} is unsupported; the storage read path requires "
            f"{DUCKDB_MAJOR_VERSION}.x"
        )
        raise DuckDBUnavailableError(msg)


@dataclass(frozen=True, slots=True)
class DuckDBOptions:
    """Explicit DuckDB read resources; never an engine switch."""

    memory_limit: str = "1GB"
    threads: int = 4
    # None keeps DuckDB's own default temporary directory.
    temp_directory: Path | None = None
    max_temp_directory_size: str = "10GB"
    arrow_batch_size: int = 65_536

    def __post_init__(self) -> None:
        if self.threads < 1:
            msg = "DuckDB threads must be positive"
            raise ValueError(msg)
        if self.arrow_batch_size < 1:
            msg = "DuckDB Arrow batch size must be positive"
            raise ValueError(msg)

    def connection_config(self) -> dict[str, Any]:
        """Connection settings applied for the whole Reader lifetime."""
        config: dict[str, Any] = {
            # Contract timestamps are `timestamp[ns, tz=UTC]`; without this the
            # export carries the machine-local timezone.
            "timezone": "UTC",
            # Non-paginated Bar output has no ordering contract; letting the
            # scan stream without insertion-order preservation is measurable
            # and cannot change any documented result order.
            "preserve_insertion_order": "false",
            "memory_limit": self.memory_limit,
            "threads": self.threads,
            "max_temp_directory_size": self.max_temp_directory_size,
        }
        if self.temp_directory is not None:
            config["temp_directory"] = str(self.temp_directory)
        return config


class _ScannerClosedError(Exception):
    """The owning StoreRead was closed while the scanner was running."""


class DuckDBConnectionCache:
    """Reuse at most one idle DuckDB connection between Store reads.

    Every checkout is exclusive for one Reader lifetime; the connection is
    never shared by concurrent Readers. Only a scan that reached a clean end
    is cached, and at most one connection is retained, so process memory does
    not grow with request concurrency. Cancelled or failed scans close their
    connection. Reusing the idle connection removes DuckDB's per-request
    connection setup cost while keeping request-scoped ownership.
    """

    def __init__(self, options: DuckDBOptions) -> None:
        self._options = options
        self._lock = threading.Lock()
        self._idle: Any = None
        self._closed = False

    def acquire(self) -> Any:
        with self._lock:
            connection, self._idle = self._idle, None
            closed = self._closed
        if connection is not None:
            return connection
        if closed:
            raise _ScannerClosedError
        import duckdb  # ruff: ignore[import-outside-top-level] - optional-heavy dependency

        return duckdb.connect(
            ":memory:", config=self._options.connection_config()
        )

    def release(self, connection: Any) -> None:
        with self._lock:
            if not self._closed and self._idle is None:
                self._idle = connection
                return
        _close_connection(connection)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            connection, self._idle = self._idle, None
        if connection is not None:
            _close_connection(connection)


class DuckDBArrowScanner:
    """One DuckDB connection, one Arrow Reader at a time, closed together.

    The scanner is created per `StoreRead.iter_arrow_batches()` call. The
    connection is checked out exclusively for the Reader lifetime: the Reader
    is closed before the next interval group starts and the connection is
    released with the scanner. A clean scan returns it to the cache; a
    cancelled or failed scan closes it. `close()` interrupts an in-flight
    query from another thread; the worker thread that owns the blocking call
    releases the Reader and the connection once that call returns.
    """

    def __init__(self, cache: DuckDBConnectionCache) -> None:
        self._cache = cache
        self._lock = threading.Lock()
        self._closed = False
        self._in_flight = 0
        self._exhausted = False
        self._failed = False
        self._connection: Any = None
        self._reader: Any = None

    def batches(
        self,
        groups: Sequence[tuple[str, Sequence[str]]],
        query: BarReadQuery,
        *,
        batch_size: int,
        limit: int | None,
    ) -> Iterator[pa.RecordBatch]:
        """Yield contract RecordBatches for the selected interval groups."""
        remaining = limit
        try:  # ruff: ignore[too-many-nested-blocks,too-many-statements-in-try-clause] - explicit resource scopes
            if remaining is not None and remaining <= 0:
                self._exhausted = True
                return
            self._connect()
            for interval, files in groups:
                if remaining is not None and remaining <= 0:
                    self._exhausted = True
                    return
                reader = self._open_reader(
                    files, query, batch_size=batch_size, limit=remaining
                )
                try:
                    for submitted in self._reader_batches(reader, interval):
                        batch = submitted
                        if remaining is not None:
                            if batch.num_rows > remaining:
                                batch = batch.slice(0, remaining)
                            remaining -= batch.num_rows
                        yield batch
                        if remaining is not None and remaining <= 0:
                            self._exhausted = True
                            return
                finally:
                    self._close_reader()
            self._exhausted = True
        except BaseException:
            self._failed = True
            raise
        finally:
            self.close()

    def count(self, files: Sequence[str], query: BarReadQuery) -> int:
        """Exact raw-row count for the selected Data Files of one Snapshot.

        The count runs as its own query, separate from the response read, on
        the same Manifest Snapshot and Read Lease. There is no window SQL,
        `DISTINCT` or other implicit dedup: every stored Revision and duplicate
        row is counted. The Arrow Reader is closed and the connection is
        released before this returns; a failed count closes the connection.
        """
        try:  # ruff: ignore[too-many-statements-in-try-clause] - explicit resource scope
            self._connect()
            sql, params = _build_count_sql(files, query)
            reader = self._open_sql_reader(sql, params, batch_size=1)
            try:
                counted = self._next_batch(reader)
            finally:
                self._close_reader()
            if counted is _END_OF_STREAM:  # pragma: no cover - defensive
                msg = "DuckDB returned no row for the Bar count"
                raise RuntimeError(msg)  # ruff: ignore[raise-within-try]
            self._exhausted = True
            return int(counted.column(0)[0].as_py())
        except BaseException:
            self._failed = True
            raise
        finally:
            self.close()

    def close(self) -> None:
        """Cancel an in-flight query and release Reader/connection resources."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            connection = self._connection
            release_now = self._in_flight == 0
        if connection is not None:
            # Interrupt is safe from another thread and also a no-op on an
            # idle connection. The worker thread releases the resources.
            with suppress(Exception):  # pragma: no cover - defensive engine path
                connection.interrupt()
        if release_now:
            self._release()

    # -- internals ---------------------------------------------------------

    def _connect(self) -> None:
        with self._busy():
            self._connection = self._cache.acquire()

    def _reader_batches(
        self, reader: Any, interval: str
    ) -> Iterator[pa.RecordBatch]:
        while True:
            physical = self._next_batch(reader)
            if physical is _END_OF_STREAM:
                return
            yield _restore_partition_batch(physical, interval)

    @contextmanager
    def _busy(self):
        """Mark a blocking DuckDB call so `close()` cannot race its release."""
        with self._lock:
            if self._closed:
                raise _ScannerClosedError
            self._in_flight += 1
        try:
            yield
        finally:
            with self._lock:
                self._in_flight -= 1
                release = self._closed and self._in_flight == 0
            if release:
                self._release()

    def _open_reader(
        self,
        files: Sequence[str],
        query: BarReadQuery,
        *,
        batch_size: int,
        limit: int | None,
    ):
        sql, params = _build_scan_sql(files, query, limit=limit)
        return self._open_sql_reader(sql, params, batch_size=batch_size)

    def _open_sql_reader(
        self,
        sql: str,
        params: dict[str, Any],
        *,
        batch_size: int,
    ):
        with self._busy():
            connection = self._connection
            if connection is None:
                raise _ScannerClosedError
            relation = connection.sql(sql, params=params)
            if relation is None:  # pragma: no cover - defensive DBAPI contract
                msg = "DuckDB returned no relation for the Bar scan"
                raise RuntimeError(msg)
            reader = relation.to_arrow_reader(batch_size=batch_size)
            self._reader = reader
            return reader

    def _next_batch(self, reader):
        with self._busy():
            try:
                return reader.read_next_batch()
            except StopIteration:
                return _END_OF_STREAM

    def _close_reader(self) -> None:
        with self._lock:
            reader, self._reader = self._reader, None
        if reader is not None:
            with suppress(Exception):  # pragma: no cover - defensive close path
                reader.close()

    def _release(self) -> None:
        with self._lock:
            reader, connection = self._reader, self._connection
            reusable = self._exhausted and not self._failed
            self._reader = None
            self._connection = None
        if reader is not None:
            with suppress(Exception):  # pragma: no cover - defensive close path
                reader.close()
        if connection is not None:
            if reusable:
                # Returns it to the one-idle cache; the connection stays
                # exclusive because it is no longer checked out.
                self._cache.release(connection)
            else:
                _close_connection(connection)


def _close_connection(connection: Any) -> None:
    with suppress(Exception):  # pragma: no cover - defensive close path
        connection.close()


def _predicate_clauses(
    query: BarReadQuery, params: dict[str, Any]
) -> list[str]:
    """Bound-value predicates shared by the count and read queries."""
    clauses: list[str] = []
    if query.instrument_id is not None:
        clauses.append("instrument_id = $instrument")
        params["instrument"] = query.instrument_id
    if query.start_ns is not None:
        # TIMESTAMPTZ_NS keeps the nanosecond bound exact; the value travels
        # as a bound string parameter, never as interpolated SQL.
        clauses.append("open_ts >= $start::TIMESTAMPTZ_NS")
        params["start"] = ns_to_iso(int(query.start_ns))
    if query.end_ns is not None:
        clauses.append("open_ts < $end::TIMESTAMPTZ_NS")
        params["end"] = ns_to_iso(int(query.end_ns))
    return clauses


def _build_scan_sql(
    files: Sequence[str],
    query: BarReadQuery,
    *,
    limit: int | None,
) -> tuple[str, dict[str, Any]]:
    """Static SQL plus bound values; no caller input becomes SQL text."""
    params: dict[str, Any] = {"files": [str(path) for path in files]}
    clauses = _predicate_clauses(query, params)
    # The SQL text is static; every value below is a bound `$parameter`.
    select = _SELECT_PHYSICAL
    sql = (
        f"SELECT {select} "  # ruff: ignore[hardcoded-sql-expression] - bound values
        "FROM read_parquet($files, hive_partitioning = false)"
    )
    if clauses:
        predicate = " AND ".join(clauses)
        sql = f"{sql} WHERE {predicate}"
    if limit is not None:
        sql = f"{sql} LIMIT $row_limit"
        params["row_limit"] = int(limit)
    return sql, params


def _build_count_sql(
    files: Sequence[str],
    query: BarReadQuery,
) -> tuple[str, dict[str, Any]]:
    """Static exact-count SQL; raw rows, never a deduplicated relation."""
    params: dict[str, Any] = {"files": [str(path) for path in files]}
    clauses = _predicate_clauses(query, params)
    sql = (
        "SELECT count(*) AS row_count "
        "FROM read_parquet($files, hive_partitioning = false)"
    )
    if clauses:
        predicate = " AND ".join(clauses)
        sql = f"{sql} WHERE {predicate}"
    return sql, params


def _restore_partition_batch(
    physical: pa.RecordBatch, interval: str
) -> pa.RecordBatch:
    """Rebuild the logical contract schema, restoring the Hive `interval`."""
    arrays: list[pa.Array] = []
    for field in BAR_SCHEMA:
        if field.name == "interval":
            arrays.append(
                pa.array([interval] * physical.num_rows, type=field.type)
            )
        else:
            arrays.append(physical.column(field.name))
    return pa.RecordBatch.from_arrays(arrays, schema=BAR_SCHEMA)


__all__ = [
    "DUCKDB_MAJOR_VERSION",
    "DuckDBArrowScanner",
    "DuckDBConnectionCache",
    "DuckDBOptions",
    "DuckDBUnavailableError",
    "ensure_supported_duckdb",
]
