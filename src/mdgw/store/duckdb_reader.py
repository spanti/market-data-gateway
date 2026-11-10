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

#: Versioned marker prefixed to every canonical row-fingerprint input. A
#: change to the marker, the column order or the hashing recipe requires a
#: cursor-version bump so old cursors fail closed instead of addressing a
#: different physical row set.
ROW_FINGERPRINT_ALGORITHM = "mdgw-row-fingerprint-v1"

#: Internal Arrow column carrying the row fingerprint for ORDER BY/keyset. It
#: is stripped before any JSON rendering.
PAGE_FINGERPRINT_COLUMN = "_row_fingerprint"

#: Page-read Arrow schema: the contract Bar columns plus the internal
#: fingerprint. Only Store/route code sees the extra column.
PAGE_SCHEMA = BAR_SCHEMA.append(
    pa.field(PAGE_FINGERPRINT_COLUMN, pa.string(), nullable=False)
)

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
class PagePosition:
    """The total-order key of the last row returned on a v2 page.

    The key is ``(open_ts, revision, ingest_ts, row_fingerprint)``; ``open_ns``,
    ``revision`` and ``ingest_ns`` are UTC nanoseconds/ints, ``fingerprint`` is
    the lowercase hex SHA-256 produced by :data:`ROW_FINGERPRINT_ALGORITHM`.
    """

    open_ns: int
    revision: int
    ingest_ns: int
    fingerprint: str


@dataclass(frozen=True, slots=True)
class ReplayScanBranch:
    """One distinct Replay subscription's explicit Data File scan.

    The Store resolves the branch's Data Files from the Session's fixed
    Manifest Snapshot; DuckDB only ever sees this explicit list. `open_ts`
    starts at `open_start_ns`, widened by one Bar Interval from the inclusive
    `close_start_ns`, so a Bar closing exactly at the window start is not
    missed. `close_end_ns` bounds both the physical `open_ts` scan and the
    inclusive/exclusive `close_ts` window.
    """

    files: tuple[str, ...]
    instrument_id: str
    interval: str
    open_start_ns: int
    close_start_ns: int
    close_end_ns: int


@dataclass(frozen=True, slots=True)
class DuckDBOptions:
    """Explicit DuckDB read resources; never an engine switch."""

    memory_limit: str = "1GB"
    threads: int = 4
    # None is resolved by ParquetBarStore to `<store.root>/.duckdb_tmp`.
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
        self._active = 0
        self._closed = False

    @property
    def active_connections(self) -> int:
        """Connections currently checked out by a Reader."""
        with self._lock:
            return self._active

    def temp_bytes(self) -> int:
        """Best-effort bytes currently held in the configured temp directory.

        DuckDB spills global sorts and aggregations here; the files are not an
        authoritative data copy. Missing directories and files that disappear
        during the walk are ignored.
        """
        temp_directory = self._options.temp_directory
        if temp_directory is None:
            return 0
        total = 0
        try:
            for path in temp_directory.rglob("*"):
                try:
                    if path.is_file():
                        total += path.stat().st_size
                except OSError:  # pragma: no cover - transient spill cleanup
                    continue
        except OSError:  # pragma: no cover - defensive filesystem path
            return total
        return total

    def acquire(self) -> Any:
        with self._lock:
            connection, self._idle = self._idle, None
            closed = self._closed
            if connection is not None:
                self._active += 1
        if connection is not None:
            return connection
        if closed:
            msg = "DuckDB connection cache is closed"
            raise _ScannerClosedError(msg)
        import duckdb  # ruff: ignore[import-outside-top-level] - optional-heavy dependency

        connection = duckdb.connect(
            ":memory:", config=self._options.connection_config()
        )
        with self._lock:
            self._active += 1
        return connection

    def release(self, connection: Any) -> None:
        with self._lock:
            self._active = max(0, self._active - 1)
            if not self._closed and self._idle is None:
                self._idle = connection
                return
        _close_connection(connection)

    def discard(self, connection: Any) -> None:
        """Close a checked-out connection that must not be reused."""
        with self._lock:
            self._active = max(0, self._active - 1)
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
        self._release_done = False
        self._released = threading.Event()
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

    def earliest_batches(
        self,
        files: Sequence[str],
        interval: str,
        query: BarReadQuery,
        *,
        batch_size: int,
        keep: int,
    ) -> Iterator[pa.RecordBatch]:
        """Yield the globally earliest ``keep`` raw rows ordered by `open_ts`.

        This is the paginated JSON page reader. One static query reads every
        Data File of the Snapshot selection, filters the request window and
        lets DuckDB order the whole selection by `open_ts` ascending before it
        applies the row limit. A page therefore cannot be assembled from
        per-batch minima.

        The storage-order tie-break (Manifest file position, then physical row
        number) reproduces the stable-sort order the previous page reader
        produced for rows sharing an `open_ts`. It is not a public tie-break
        and carries no cursor state. No `DISTINCT`, `QUALIFY` or other dedup is
        involved: raw Revisions and duplicate rows stay raw. A sort that cannot
        fit the configured DuckDB memory/temp budget raises instead of
        returning a short page.
        """
        try:  # ruff: ignore[too-many-statements-in-try-clause] - explicit resource scope
            self._connect()
            sql, params = _build_earliest_sql(files, query, keep=keep)
            reader = self._open_sql_reader(sql, params, batch_size=batch_size)
            try:
                yield from self._reader_batches(reader, interval)
            finally:
                self._close_reader()
            self._exhausted = True
        except BaseException:
            self._failed = True
            raise
        finally:
            self.close()

    def ordered_page_batches(  # ruff: ignore[too-many-arguments] - keyword-only page scope
        self,
        files: Sequence[str],
        interval: str,
        query: BarReadQuery,
        *,
        after: PagePosition | None = None,
        keep: int,
        batch_size: int,
    ) -> Iterator[pa.RecordBatch]:
        """Yield raw rows in the revision-safe v2 total order.

        One static DuckDB query selects every Data File of the request
        Snapshot, computes the canonical row fingerprint in SQL, applies the
        strict keyset predicate after ``after`` (when supplied) and orders by
        ``(open_ts, revision, ingest_ts, row_fingerprint)`` ascending before
        limiting to ``keep`` rows. The fingerprint is carried as an internal
        column so the route can encode the last returned row as the next
        cursor; it is never part of the contract Arrow/JSON row.
        """
        try:  # ruff: ignore[too-many-statements-in-try-clause] - explicit resource scope
            self._connect()
            sql, params = _build_ordered_page_sql(
                files, query, after=after, keep=keep
            )
            reader = self._open_sql_reader(sql, params, batch_size=batch_size)
            try:
                yield from self._reader_page_batches(reader, interval)
            finally:
                self._close_reader()
            self._exhausted = True
        except BaseException:
            self._failed = True
            raise
        finally:
            self.close()

    def iter_replay_batches(
        self,
        branches: Sequence[ReplayScanBranch],
        *,
        batch_size: int,
    ) -> Iterator[pa.RecordBatch]:
        """Yield the five-key ordered Replay batches as DuckDB produces them.

        This is the only Replay read seam: the query does the close-time
        filter, Revision choice, duplicate-subscription resolution and total
        order, but each Arrow RecordBatch is handed to the caller immediately
        instead of being accumulated. The Reader and connection are closed on
        exhaustion, exception and cancellation; `close()` from another thread
        interrupts the blocking `read_next_batch()` call.
        """
        try:  # ruff: ignore[too-many-statements-in-try-clause] - explicit resource scope
            self._connect()
            sql, params = _build_replay_sql(branches)
            reader = self._open_sql_reader(sql, params, batch_size=batch_size)
            try:
                while True:
                    batch = self._next_batch(reader)
                    if batch is _END_OF_STREAM:
                        break
                    yield batch
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

    def wait_released(self, timeout: float | None = None) -> bool:
        """Wait until the Reader and connection are actually released.

        `close()` may only interrupt a blocking DuckDB call; the worker thread
        that owns it performs the release after that call returns. Callers that
        must keep a Dataset Read Lease until the Reader is closed use this to
        observe the real release, including the no-resource path.
        """
        return self._released.wait(timeout)

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

    def _reader_page_batches(
        self, reader: Any, interval: str
    ) -> Iterator[pa.RecordBatch]:
        while True:
            physical = self._next_batch(reader)
            if physical is _END_OF_STREAM:
                return
            yield _restore_partition_page_batch(physical, interval)

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
        """Close the Reader/connection once and signal actual release."""
        with self._lock:
            if self._release_done:
                return
            self._release_done = True
            reader, connection = self._reader, self._connection
            reusable = self._exhausted and not self._failed
            self._reader = None
            self._connection = None
        try:
            if reader is not None:
                with suppress(Exception):  # pragma: no cover - defensive close path
                    reader.close()
            if connection is not None:
                if reusable:
                    # Returns it to the one-idle cache; the connection stays
                    # exclusive because it is no longer checked out.
                    self._cache.release(connection)
                else:
                    self._cache.discard(connection)
        finally:
            self._released.set()


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


def _build_earliest_sql(
    files: Sequence[str],
    query: BarReadQuery,
    *,
    keep: int,
) -> tuple[str, dict[str, Any]]:
    """Static globally ordered page SQL: `open_ts` ASC, then storage order.

    `filename` and `file_row_number` are only referenced by `ORDER BY`, so the
    projected Arrow batch still contains exactly the physical Bar columns. The
    tie-break reproduces the previous stable sort for equal timestamps without
    changing the documented `open_ts`-only ordering promise.
    """
    if keep < 1:
        msg = "earliest row limit must be positive"
        raise ValueError(msg)
    params: dict[str, Any] = {
        "files": [str(path) for path in files],
        "row_limit": int(keep),
    }
    clauses = _predicate_clauses(query, params)
    sql = (
        f"SELECT {_SELECT_PHYSICAL} "  # ruff: ignore[hardcoded-sql-expression] - bound values
        "FROM read_parquet($files, hive_partitioning = false, "
        "filename = true, file_row_number = true)"
    )
    if clauses:
        sql = f"{sql} WHERE {' AND '.join(clauses)}"
    sql = (
        f"{sql} ORDER BY open_ts ASC, "
        "list_position($files, filename) ASC, file_row_number ASC "
        "LIMIT $row_limit"
    )
    return sql, params


def _row_fingerprint_sql() -> str:
    """Canonical row fingerprint as static DuckDB SQL.

    Algorithm (ADR-0018): take ``PHYSICAL_SCHEMA.names`` in schema order, emit
    ``coalesce(CAST(col AS VARCHAR), chr(0))`` for each, join the fields with
    ``chr(31)`` and prefix the versioned marker, then SHA-256 the UTF-8 bytes.
    ``sha256`` returns the lowercase hex digest. The marker travels as a bound
    parameter; column names are schema constants.
    """
    fields = ", ".join(
        f"coalesce(CAST({name} AS VARCHAR), chr(0))"
        for name in PHYSICAL_SCHEMA.names
    )
    return (
        "sha256(concat_ws(chr(31), $fingerprint_marker, "
        f"{fields}))"
    )


def _build_ordered_page_sql(
    files: Sequence[str],
    query: BarReadQuery,
    *,
    after: PagePosition | None,
    keep: int,
) -> tuple[str, dict[str, Any]]:
    """Static revision-safe page SQL over the total order, with keyset seek.

    The inner query computes the internal fingerprint column; the outer query
    applies the strict keyset predicate after the last row of the previous
    page. Every caller value (file list, Instrument, time bounds, cursor key)
    is a named bound parameter, so the SQL text stays static.
    """
    if keep < 1:
        msg = "ordered page row limit must be positive"
        raise ValueError(msg)
    params: dict[str, Any] = {
        "files": [str(path) for path in files],
        "row_limit": int(keep),
        "fingerprint_marker": ROW_FINGERPRINT_ALGORITHM,
    }
    clauses = _predicate_clauses(query, params)
    inner_where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    fingerprint = _row_fingerprint_sql()
    sql = (
        f"SELECT * FROM (SELECT {_SELECT_PHYSICAL}, "  # ruff: ignore[hardcoded-sql-expression] - schema constants only
        f"{fingerprint} AS {PAGE_FINGERPRINT_COLUMN} "
        "FROM read_parquet($files, hive_partitioning = false)"
        f"{inner_where}) AS page_rows"
    )
    if after is not None:
        sql += (
            " WHERE (open_ts > $after_open::TIMESTAMPTZ_NS)"
            " OR (open_ts = $after_open::TIMESTAMPTZ_NS"
            " AND revision > $after_revision)"
            " OR (open_ts = $after_open::TIMESTAMPTZ_NS"
            " AND revision = $after_revision"
            " AND ingest_ts > $after_ingest::TIMESTAMPTZ_NS)"
            " OR (open_ts = $after_open::TIMESTAMPTZ_NS"
            " AND revision = $after_revision"
            " AND ingest_ts = $after_ingest::TIMESTAMPTZ_NS"
            f" AND {PAGE_FINGERPRINT_COLUMN} > $after_fingerprint)"
        )
        params["after_open"] = ns_to_iso(int(after.open_ns))
        params["after_revision"] = int(after.revision)
        params["after_ingest"] = ns_to_iso(int(after.ingest_ns))
        params["after_fingerprint"] = str(after.fingerprint)
    sql = (
        f"{sql} ORDER BY open_ts ASC, revision ASC, ingest_ts ASC, "
        f"{PAGE_FINGERPRINT_COLUMN} ASC LIMIT $row_limit"
    )
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


def _build_replay_sql(
    branches: Sequence[ReplayScanBranch],
) -> tuple[str, dict[str, Any]]:
    """Static Replay SQL: one branch per distinct subscription, then five keys.

    Only the branch count is structural SQL text. Every caller value (file
    list, Instrument, Interval, time bounds) is a named bound parameter, so a
    caller can never inject SQL, a path list member or an expression. The
    outer `QUALIFY` keeps one Bar per `(instrument_id, interval, open_ts)`:
    highest Revision first, latest `ingest_ts` on a Revision tie. The final
    `ORDER BY` is the Replay Protocol's five-key total order; `seq` is numeric
    before NULL.
    """
    if not branches:
        msg = "a Replay query needs at least one scan branch"
        raise ValueError(msg)

    params: dict[str, Any] = {}
    selects: list[str] = []
    for index, branch in enumerate(branches):
        files_key = f"files_{index}"
        instrument_key = f"instrument_{index}"
        interval_key = f"interval_{index}"
        open_start_key = f"open_start_{index}"
        close_start_key = f"close_start_{index}"
        close_end_key = f"close_end_{index}"
        params[files_key] = [str(path) for path in branch.files]
        params[instrument_key] = branch.instrument_id
        params[interval_key] = branch.interval
        params[open_start_key] = ns_to_iso(branch.open_start_ns)
        params[close_start_key] = ns_to_iso(branch.close_start_ns)
        params[close_end_key] = ns_to_iso(branch.close_end_ns)
        selects.append(
            f"SELECT {_SELECT_PHYSICAL}, "  # ruff: ignore[hardcoded-sql-expression] - bound values only
            f"CAST(${interval_key} AS VARCHAR) AS interval "
            f"FROM read_parquet(${files_key}, hive_partitioning = false) "
            f"WHERE instrument_id = ${instrument_key} "
            f"AND open_ts >= ${open_start_key}::TIMESTAMPTZ_NS "
            f"AND open_ts < ${close_end_key}::TIMESTAMPTZ_NS "
            f"AND close_ts >= ${close_start_key}::TIMESTAMPTZ_NS "
            f"AND close_ts < ${close_end_key}::TIMESTAMPTZ_NS"
        )
    sql = (
        "WITH replay_candidates AS (\n  "
        + "\n  UNION ALL\n  ".join(selects)
        + "\n)\nSELECT * FROM replay_candidates\n"
        "QUALIFY row_number() OVER ("
        "PARTITION BY instrument_id, interval, open_ts "
        "ORDER BY revision DESC, ingest_ts DESC) = 1\n"
        "ORDER BY close_ts ASC, instrument_id ASC, seq ASC NULLS LAST, "
        "interval ASC, open_ts ASC"
    )
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


def _restore_partition_page_batch(
    physical: pa.RecordBatch, interval: str
) -> pa.RecordBatch:
    """Rebuild the page schema: contract columns plus the fingerprint."""
    arrays: list[pa.Array] = []
    for field in BAR_SCHEMA:
        if field.name == "interval":
            arrays.append(
                pa.array([interval] * physical.num_rows, type=field.type)
            )
        else:
            arrays.append(physical.column(field.name))
    arrays.append(physical.column(PAGE_FINGERPRINT_COLUMN))
    return pa.RecordBatch.from_arrays(arrays, schema=PAGE_SCHEMA)


__all__ = [
    "DUCKDB_MAJOR_VERSION",
    "PAGE_FINGERPRINT_COLUMN",
    "PAGE_SCHEMA",
    "ROW_FINGERPRINT_ALGORITHM",
    "DuckDBArrowScanner",
    "DuckDBConnectionCache",
    "DuckDBOptions",
    "DuckDBUnavailableError",
    "PagePosition",
    "ReplayScanBranch",
    "ensure_supported_duckdb",
]
