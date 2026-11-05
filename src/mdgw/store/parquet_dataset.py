"""Parquet Dataset storage for Bars (v1 Data Contract layout).

`_manifest.json` is authoritative: reads only open active Data Files listed in
the Manifest, and Compaction / Garbage Collection switch the Manifest before
touching old files. Data Files themselves are immutable.
"""

from __future__ import annotations

import statistics
import threading
import time
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from mdgw.core.errors import SchemaVersionMismatch
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import (
    BAR_SCHEMA,
    PARTITION_COLUMNS,
    UTC_TS,
    bars_to_physical_table,
    empty_logical_table,
    schema_json,
)
from mdgw.core.time import datetime_to_ns, ns_to_iso
from mdgw.normalize.version import NORMALIZATION_RULE_VERSIONS, normalize_rule_versions
from mdgw.store.duckdb_reader import (
    DuckDBArrowScanner,
    DuckDBConnectionCache,
    DuckDBOptions,
    ReplayScanBranch,
    ensure_supported_duckdb,
)
from mdgw.store.maintenance import (
    CompactionReport,
    GCReport,
    compact_store,
    gc_store,
)
from mdgw.store.manifest import (
    DATASET_DESCRIPTOR_NAME,
    DATASET_NAME,
    PARTITION_FIELDS,
    active_manifest_entries,
    instrument_id_for_partition,
    manifest_generation,
    read_dataset_descriptor,
    read_manifest,
    write_dataset_descriptor,
    write_manifest,
)
from mdgw.store.session_locks import DEFAULT_LOCK_TIMEOUT_S, DatasetReadLease

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Sequence

    from mdgw.core.bar import Bar

DATASET_SCHEMA_VERSION = 1
FILE_NAMING = "data-<start_ns>-<end_ns>-<uuid>.parquet"
DEFAULT_BATCH_SIZE = 65_536
# One Manifest refresh is enough for the Compaction/GC race: the switching
# process has already written the new Manifest before it unlinks the old files.
MANIFEST_REFRESH_ATTEMPTS = 2
# A candidate selection, lease acquisition and generation re-read may race
# with a writer. Retry a bounded number of times; never mix generations.
READ_LEASE_ATTEMPTS = 3
# StoreRead filters are optional; this sentinel distinguishes "not passed"
# from an explicit `None` (= no filter on that dimension).
_UNSET = object()
# `interval` is a Hive partition column, not a physical column in Data Files.
PHYSICAL_READ_COLUMNS: tuple[str, ...] = tuple(
    name for name in BAR_SCHEMA.names if name != "interval"
)


class ParquetBarStore:
    """Writes immutable Data Files and reads them back through pyarrow.dataset."""

    def __init__(  # ruff: ignore[too-many-arguments] - keyword-only store tuning knobs
        self,
        root: Path,
        *,
        compression: str = "zstd",
        row_group_size: int = 1_000_000,
        format_version: str = "2.6",
        schema_version: int = DATASET_SCHEMA_VERSION,
        session_lock_timeout_s: int = DEFAULT_LOCK_TIMEOUT_S,
        read_lease_heartbeat_s: float | None = None,
        duckdb: DuckDBOptions | None = None,
        rule_versions: Sequence[str] | None = None,
    ) -> None:
        # The DuckDB 2.x Relation Arrow Reader is a formal runtime dependency,
        # not an optional engine: fail at Store/app startup, never at request
        # time and never with a 1.x fallback.
        ensure_supported_duckdb()
        self.duckdb = duckdb if duckdb is not None else DuckDBOptions()
        self.duckdb_connections = DuckDBConnectionCache(self.duckdb)
        self.root = Path(root)
        self.dataset_root = self.root / DATASET_NAME
        self.compression = compression
        self.row_group_size = row_group_size
        self.format_version = format_version
        self.schema_version = schema_version
        self.session_lock_timeout_s = session_lock_timeout_s
        # A lease must be refreshed well before maintenance treats it as
        # expired. Keep the default in seconds, not tied to the exact timeout.
        if read_lease_heartbeat_s is None:
            read_lease_heartbeat_s = min(
                5.0, max(0.05, float(session_lock_timeout_s) / 3.0)
            )
        if read_lease_heartbeat_s <= 0:
            msg = "read_lease_heartbeat_s must be positive"
            raise ValueError(msg)
        self.read_lease_heartbeat_s = float(read_lease_heartbeat_s)
        # Audit metadata only: this never participates in Schema compatibility.
        self.rule_versions = (
            normalize_rule_versions(rule_versions)
            if rule_versions is not None
            else tuple(sorted(NORMALIZATION_RULE_VERSIONS))
        )
        self._lock = threading.Lock()

    # -- writes ------------------------------------------------------------

    def write_bars(
        self,
        bars: Sequence[Bar],
        *,
        asset_class: str,
        rule_versions: Sequence[str] | None = None,
    ) -> list[dict]:
        """Write Bars and stamp their Manifest entries with the Rule Versions.

        ``rule_versions`` defaults to the Store's configured set, so live and
        test call sites do not have to repeat it. Callers that intentionally
        use different Normalization rules (Backfill/archive scalar rules) pass
        their own set explicitly; this is audit metadata, not an engine switch.
        """
        versions = (
            self.rule_versions
            if rule_versions is None
            else normalize_rule_versions(rule_versions)
        )
        groups: dict[tuple[str, str, str, str, int, int], list[Bar]] = defaultdict(list)
        for bar in bars:
            instrument = InstrumentId.parse(bar.instrument_id)
            groups[
                asset_class,
                instrument.venue,
                instrument.file_symbol,
                bar.interval,
                bar.open_ts.year,
                bar.open_ts.month,
            ].append(bar)

        written: list[dict] = []
        for key, group in sorted(groups.items()):
            written.append(self._write_partition(key, group, versions))

        if written:
            last_flushed_ns = max(
                datetime_to_ns(bar.ingest_ts) for bar in bars
            )
            with self._lock:
                self._ensure_dataset_descriptor(last_flushed_ns)
                self._append_manifest(written)
        return written

    def _write_partition(
        self,
        key: tuple[str, str, str, str, int, int],
        bars: Sequence[Bar],
        rule_versions: Sequence[str],
    ) -> dict:
        asset_class, venue, file_symbol, interval, year, month = key
        partition = {
            "asset_class": asset_class,
            "venue": venue,
            "symbol": file_symbol,
            "interval": interval,
            "year": f"{year:04d}",
            "month": f"{month:02d}",
        }
        directory = self.dataset_root
        for partition_field in PARTITION_COLUMNS:
            directory /= f"{partition_field}={partition[partition_field]}"
        directory.mkdir(parents=True, exist_ok=True)

        start_ns = min(datetime_to_ns(bar.open_ts) for bar in bars)
        end_ns = max(datetime_to_ns(bar.close_ts) for bar in bars)
        filename = f"data-{start_ns}-{end_ns}-{uuid4()}.parquet"
        path = directory / filename

        pq.write_table(
            bars_to_physical_table(bars),
            path,
            version=self.format_version,
            use_deprecated_int96_timestamps=False,
            compression=self.compression,
            row_group_size=self.row_group_size,
        )

        return {
            "path": path.relative_to(self.dataset_root).as_posix(),
            "start_ns": start_ns,
            "end_ns": end_ns,
            "row_count": len(bars),
            "active": True,
            "size_bytes": path.stat().st_size,
            "created_ns": time.time_ns(),
            "partition": partition,
            "max_ingest_ns": max(datetime_to_ns(bar.ingest_ts) for bar in bars),
            "rule_versions": list(rule_versions),
        }

    def _ensure_dataset_descriptor(self, last_flushed_ns: int | None = None) -> None:
        descriptor = read_dataset_descriptor(self.dataset_root)
        descriptor.update(
            {
                "schema_version": self.schema_version,
                "data_type": "bar",
                "arrow_schema": schema_json(),
                "partitioning": list(PARTITION_COLUMNS),
                "partitioning_flavor": "hive",
                "file_naming": FILE_NAMING,
            }
        )
        if last_flushed_ns is not None:
            descriptor["last_flushed_ns"] = max(
                int(descriptor.get("last_flushed_ns") or 0), int(last_flushed_ns)
            )
        write_dataset_descriptor(self.dataset_root, descriptor)

    def _append_manifest(self, files: Sequence[dict]) -> None:
        manifest = read_manifest(self.dataset_root)
        manifest.setdefault("schema_version", self.schema_version)
        manifest.setdefault("data_type", "bar")
        manifest.setdefault("files", []).extend(files)
        write_manifest(self.dataset_root, manifest)

    def validate_schema_version(self) -> None:
        """Reject a Dataset written under a different Data Contract version."""
        descriptor_path = self.dataset_root / DATASET_DESCRIPTOR_NAME
        if not descriptor_path.exists():
            return
        descriptor = read_dataset_descriptor(self.dataset_root)

        found = descriptor.get("schema_version")
        if found != self.schema_version:
            msg = (
                f"Dataset schema_version {found!r} does not match "
                f"the expected {self.schema_version}"
            )
            raise SchemaVersionMismatch(msg)

    # -- reads -------------------------------------------------------------

    def open_read(  # ruff: ignore[too-many-arguments] - keyword-only query scope
        self,
        *,
        instrument_id: str | None = None,
        interval: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
        queries: Sequence[BarReadQuery] | None = None,
        batch_size: int | None = None,
        lease_id: str | None = None,
        subscriptions: Sequence[dict[str, str]] | None = None,
        lease_heartbeat_s: float | None = None,
    ) -> StoreRead:
        """Open one fixed Manifest Snapshot protected by a Dataset Read Lease.

        Candidate entries are selected by Instrument, Interval, partition and
        file time range before any Data File is opened. The lease covers only
        the selected partitions and months. Afterwards the Manifest generation
        is re-read: if a writer switched it, the lease is released and the
        selection is retried, so one read never mixes Manifest generations.

        `queries` lets a Replay Session acquire one lease for the union of its
        subscriptions while issuing per-subscription scans on the same
        Snapshot.
        """
        if queries is None:
            query_specs: tuple[BarReadQuery, ...] = (
                BarReadQuery(
                    instrument_id=instrument_id,
                    interval=interval,
                    start_ns=start_ns,
                    end_ns=end_ns,
                ),
            )
        else:
            if any(
                value is not None
                for value in (instrument_id, interval, start_ns, end_ns)
            ):
                msg = "pass either query filters or structured queries, not both"
                raise ValueError(msg)
            query_specs = tuple(queries)
        self.validate_schema_version()
        heartbeat_s = (
            self.read_lease_heartbeat_s
            if lease_heartbeat_s is None
            else float(lease_heartbeat_s)
        )
        snapshot, lease = self._acquire_snapshot(
            query_specs,
            lease_id=lease_id,
            subscriptions=subscriptions,
            heartbeat_s=heartbeat_s,
        )
        default_query = query_specs[0] if len(query_specs) == 1 else None
        return StoreRead(
            self,
            snapshot,
            lease,
            default_query=default_query,
            acquisition_queries=query_specs,
            lease_id=lease_id,
            subscriptions=subscriptions,
            heartbeat_s=heartbeat_s,
            batch_size=(
                batch_size
                if batch_size is not None
                else DEFAULT_BATCH_SIZE
            ),
        )

    def iter_batches(  # ruff: ignore[too-many-arguments] - keyword-only query filters
        self,
        *,
        instrument_id: str | None = None,
        interval: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
        limit: int | None = None,
        batch_size: int | None = None,
    ):
        """Yield Bar RecordBatches from one fixed Manifest Snapshot.

        `start_ns` is inclusive and `end_ns` is exclusive, both applied to
        `open_ts`. `limit` caps the total number of rows yielded. This legacy
        one-shot form delegates to `open_read`; callers that need several
        observations (count, existence probe, read) should hold one StoreRead.
        """
        with self.open_read(
            instrument_id=instrument_id,
            interval=interval,
            start_ns=start_ns,
            end_ns=end_ns,
            batch_size=batch_size,
        ) as read:
            yield from read.iter_batches(limit=limit)

    def count_bars(
        self,
        *,
        instrument_id: str | None = None,
        interval: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
    ) -> int:
        """Count matching Bars, used to enforce the JSON row ceiling."""
        with self.open_read(
            instrument_id=instrument_id,
            interval=interval,
            start_ns=start_ns,
            end_ns=end_ns,
        ) as read:
            return read.count()

    def read_bars(
        self,
        *,
        instrument_id: str | None = None,
        interval: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
        limit: int | None = None,
    ) -> pa.Table:
        with self.open_read(
            instrument_id=instrument_id,
            interval=interval,
            start_ns=start_ns,
            end_ns=end_ns,
        ) as read:
            return read.read_bars(limit=limit)

    def _active_entries(self) -> list[dict]:
        """Active Manifest entries, including ones whose file may be gone.

        A missing file is not silently dropped here: the caller opens every
        selected file before yielding and refreshes the Manifest once if a
        concurrent Compaction/GC already removed it. Kept as an overridable
        seam for the maintenance-race tests.
        """
        return active_manifest_entries(
            self.dataset_root, existing_only=False
        )

    def _acquire_snapshot(
        self,
        query_specs: Sequence[BarReadQuery],
        *,
        lease_id: str | None,
        subscriptions: Sequence[dict[str, str]] | None,
        heartbeat_s: float,
    ) -> tuple[ManifestSnapshot, DatasetReadLease | None]:
        """Select candidates, take a lease and re-verify the Manifest generation."""
        for attempt in range(READ_LEASE_ATTEMPTS):
            generation = manifest_generation(self.dataset_root)
            entries = self._active_entries()
            selected = _select_for_queries(entries, query_specs)
            partitions, months = _lease_scope(selected)
            lease: DatasetReadLease | None = None
            if selected or subscriptions:
                lease = DatasetReadLease.acquire(
                    self.dataset_root,
                    lease_id or f"read-{uuid4().hex}",
                    partitions=partitions,
                    months=months,
                    subscriptions=subscriptions,
                    heartbeat_s=heartbeat_s,
                )
            if lease is not None:
                self._after_lease_acquired(lease)
            if manifest_generation(self.dataset_root) == generation:
                snapshot = ManifestSnapshot(
                    generation=generation,
                    entries=tuple(selected),
                    schema_version=self.schema_version,
                )
                return snapshot, lease
            if lease is not None:
                lease.close()
            if attempt + 1 >= READ_LEASE_ATTEMPTS:
                break
        msg = "Manifest changed repeatedly while acquiring a read lease"
        raise ManifestSnapshotRaceError(msg)

    def _after_lease_acquired(self, lease: DatasetReadLease) -> None:
        """Race-test seam: runs after the lease exists, before the re-read."""

    # -- observability -----------------------------------------------------

    def status(self) -> dict:
        """Operational status used by `/api/v1/status`."""
        self.validate_schema_version()
        entries = active_manifest_entries(self.dataset_root)
        descriptor = read_dataset_descriptor(self.dataset_root)

        overall_last_ns = descriptor.get("last_flushed_ns")
        if overall_last_ns is None and entries:
            overall_last_ns = max(int(entry["max_ingest_ns"]) for entry in entries)

        by_interval: dict[str, list[dict]] = defaultdict(list)
        for entry in entries:
            by_interval[str(entry["partition"].get("interval", ""))].append(entry)

        intervals: dict[str, dict] = {}
        for interval in sorted(by_interval):
            group = by_interval[interval]
            last_ns = max(int(entry["max_ingest_ns"]) for entry in group)
            intervals[interval] = {
                "last_flushed_ts": ns_to_iso(last_ns) if last_ns else None,
                "active_file_count": len(group),
                "median_file_size_bytes": _median_file_size(group),
            }

        return {
            "dataset": DATASET_NAME,
            "schema_version": self.schema_version,
            "last_flushed_ts": ns_to_iso(int(overall_last_ns))
            if overall_last_ns
            else None,
            "active_file_count": len(entries),
            "median_file_size_bytes": _median_file_size(entries),
            "intervals": intervals,
        }

    # -- maintenance -------------------------------------------------------

    def compact(
        self,
        *,
        older_than,
        now_ns: int | None = None,
    ) -> CompactionReport:
        """Merge small Data Files per partition and switch the Manifest."""
        return compact_store(
            self,
            older_than=older_than,
            now_ns=now_ns,
            timeout_s=self.session_lock_timeout_s,
        )

    def gc(
        self,
        *,
        bars_days: int,
        apply: bool,
        now=None,
    ) -> GCReport:
        """Delete whole month partitions older than the Retention window."""
        return gc_store(
            self,
            bars_days=bars_days,
            apply=apply,
            now=now,
            timeout_s=self.session_lock_timeout_s,
        )


@dataclass(frozen=True, slots=True)
class BarReadQuery:
    """Structured filter for one scan window.

    `start_ns` is inclusive and `end_ns` is exclusive, both on `open_ts`.
    """

    instrument_id: str | None = None
    interval: str | None = None
    start_ns: int | None = None
    end_ns: int | None = None


@dataclass(frozen=True, slots=True)
class ReplayReadQuery:
    """One distinct Replay subscription and its DuckDB scan window.

    `open_start_ns` is the physical `open_ts` lower bound, widened by one Bar
    Interval from the inclusive `close_start_ns`, so a Bar closing exactly at
    the Replay window start is not missed. `close_end_ns` is exclusive.
    Identical `(instrument_id, interval)` subscriptions are resolved to one
    query before it reaches the Store.
    """

    instrument_id: str
    interval: str
    open_start_ns: int
    close_start_ns: int
    close_end_ns: int


@dataclass(frozen=True, slots=True)
class ManifestSnapshot:
    """One Manifest read: stable generation plus its selected active entries."""

    generation: str
    entries: tuple[dict[str, Any], ...]
    schema_version: int | None = None


class ManifestSnapshotRaceError(RuntimeError):
    """The Manifest kept changing while a read snapshot was being acquired."""


class StoreRead:
    """One Manifest Snapshot and one Dataset Read Lease.

    `count()`, `iter_batches()` and `read_bars()` all scan the same Snapshot,
    so an HTTP request cannot mix generations between its row-ceiling check,
    its existence probe and its actual read. The lease is held until `close()`
    (or the context manager exits) on success, exception, cancellation and
    repeated close.
    """

    def __init__(  # ruff: ignore[too-many-arguments] - explicit read state
        self,
        store: ParquetBarStore,
        snapshot: ManifestSnapshot,
        lease: DatasetReadLease | None,
        *,
        default_query: BarReadQuery | None,
        acquisition_queries: Sequence[BarReadQuery],
        lease_id: str | None,
        subscriptions: Sequence[dict[str, str]] | None,
        heartbeat_s: float,
        batch_size: int,
    ) -> None:
        self._store = store
        self.snapshot = snapshot
        self.lease = lease
        self.batch_size = batch_size
        self._default_query = default_query
        self._acquisition_queries = tuple(acquisition_queries)
        self._lease_id = lease_id
        self._subscriptions = tuple(subscriptions or ())
        self._heartbeat_s = heartbeat_s
        self._closed = False
        self._close_lock = threading.Lock()
        self._scans = 0
        self._yielded = False
        self._active_scanner: DuckDBArrowScanner | None = None

    @property
    def closed(self) -> bool:
        return self._closed

    def iter_batches(  # ruff: ignore[too-many-arguments] - keyword-only query filters
        self,
        *,
        instrument_id: Any = _UNSET,
        interval: Any = _UNSET,
        start_ns: Any = _UNSET,
        end_ns: Any = _UNSET,
        limit: int | None = None,
        batch_size: int | None = None,
    ):
        """Yield Bar RecordBatches from this read's fixed Snapshot."""
        self._ensure_open()
        query = self._resolve_query(instrument_id, interval, start_ns, end_ns)
        self._scans += 1
        effective_batch_size = batch_size or self.batch_size
        opened = self._open_files(query)

        remaining = limit
        try:
            for parquet_file, partition_interval in opened:
                for physical_batch in parquet_file.iter_batches(
                    columns=list(PHYSICAL_READ_COLUMNS),
                    batch_size=effective_batch_size,
                ):
                    batch = _with_partition_interval(
                        physical_batch, partition_interval
                    )
                    batch = _filter_batch(
                        batch,
                        instrument_id=query.instrument_id,
                        interval=query.interval,
                        start_ns=query.start_ns,
                        end_ns=query.end_ns,
                    )
                    if batch.num_rows == 0:
                        continue
                    if remaining is not None:
                        if remaining <= 0:
                            return
                        if batch.num_rows > remaining:
                            batch = batch.slice(0, remaining)
                        remaining -= batch.num_rows
                    self._yielded = True
                    yield batch
        finally:
            _close_parquet_files(opened)

    def iter_arrow_batches(  # ruff: ignore[too-many-arguments] - keyword-only query filters
        self,
        *,
        instrument_id: Any = _UNSET,
        interval: Any = _UNSET,
        start_ns: Any = _UNSET,
        end_ns: Any = _UNSET,
        limit: int | None = None,
        batch_size: int | None = None,
    ):
        """Yield contract Bar RecordBatches from DuckDB's Relation Arrow Reader.

        Call sites that explicitly choose this scan (the REST Arrow stream and
        the non-paginated JSON read) use DuckDB. It reads exactly the active
        Data Files of this read's fixed Manifest Snapshot, streams bounded
        batches and holds the Read Lease until the generator finishes, fails or
        is closed.
        """
        self._ensure_open()
        query = self._resolve_query(instrument_id, interval, start_ns, end_ns)
        self._scans += 1
        effective_batch_size = (
            batch_size
            if batch_size is not None
            else self._store.duckdb.arrow_batch_size
        )
        groups = self._arrow_scan_groups(query)
        if not groups or limit == 0:
            return

        scanner = DuckDBArrowScanner(self._store.duckdb_connections)
        with self._close_lock:
            if self._closed:
                msg = "StoreRead is closed"
                raise RuntimeError(msg)
            self._active_scanner = scanner
        try:
            for batch in scanner.batches(
                groups,
                query,
                batch_size=effective_batch_size,
                limit=limit,
            ):
                self._yielded = True
                yield batch
        finally:
            scanner.close()
            with self._close_lock:
                if self._active_scanner is scanner:
                    self._active_scanner = None

    def count(
        self,
        *,
        instrument_id: Any = _UNSET,
        interval: Any = _UNSET,
        start_ns: Any = _UNSET,
        end_ns: Any = _UNSET,
    ) -> int:
        """Count matching Bars on this read's fixed Snapshot."""
        return sum(
            batch.num_rows
            for batch in self.iter_batches(
                instrument_id=instrument_id,
                interval=interval,
                start_ns=start_ns,
                end_ns=end_ns,
            )
        )

    def count_arrow(
        self,
        *,
        instrument_id: Any = _UNSET,
        interval: Any = _UNSET,
        start_ns: Any = _UNSET,
        end_ns: Any = _UNSET,
    ) -> int:
        """Exact raw-row count through DuckDB on this read's Snapshot.

        This is the JSON row-ceiling count. It executes as a separate query on
        the same Snapshot and Lease as the response read, so both observe one
        Manifest generation. Every Revision and duplicate row is counted.
        """
        self._ensure_open()
        query = self._resolve_query(
            instrument_id, interval, start_ns, end_ns
        )
        self._scans += 1
        files = [
            file
            for _interval, group in self._arrow_scan_groups(query)
            for file in group
        ]
        if not files:
            return 0
        scanner = DuckDBArrowScanner(self._store.duckdb_connections)
        with self._close_lock:
            if self._closed:
                msg = "StoreRead is closed"
                raise RuntimeError(msg)
            self._active_scanner = scanner
        try:
            return scanner.count(files, query)
        finally:
            scanner.close()
            with self._close_lock:
                if self._active_scanner is scanner:
                    self._active_scanner = None

    def read_replay_bars(
        self, queries: Sequence[ReplayReadQuery]
    ) -> pa.Table:
        """Materialize the deduplicated, five-key ordered Bars of a Replay query.

        This is the Replay Source seam. It reads exactly the active Data Files
        selected from this read's fixed Manifest Snapshot; DuckDB never globs
        the Dataset root. One `UNION ALL` branch is emitted per distinct
        `(instrument_id, interval)` subscription, each with its own explicit
        file list and bound close-time window. Deduplication, Revision choice
        and the total order happen inside one DuckDB query, so the REST
        raw-Revision semantics cannot leak into Replay.

        M5A keeps the full materialization: every batch is consumed before
        this returns. A failure at any point raises; it is never turned into
        an empty or short result. The Arrow export is restored to
        `BAR_SCHEMA` (field order and nullability) before it crosses the Store
        boundary.
        """
        self._ensure_open()
        branches = self._replay_scan_branches(queries)
        if not branches:
            return empty_logical_table()
        self._scans += 1
        scanner = DuckDBArrowScanner(self._store.duckdb_connections)
        with self._close_lock:
            if self._closed:
                msg = "StoreRead is closed"
                raise RuntimeError(msg)
            self._active_scanner = scanner
        try:
            batches = scanner.replay_batches(
                branches,
                batch_size=self._store.duckdb.arrow_batch_size,
            )
        finally:
            scanner.close()
            with self._close_lock:
                if self._active_scanner is scanner:
                    self._active_scanner = None
        if not batches:
            return empty_logical_table()
        table = pa.Table.from_batches(batches)
        return table.select(BAR_SCHEMA.names).cast(BAR_SCHEMA)

    def read_bars(
        self,
        *,
        instrument_id: Any = _UNSET,
        interval: Any = _UNSET,
        start_ns: Any = _UNSET,
        end_ns: Any = _UNSET,
        limit: int | None = None,
    ) -> pa.Table:
        batches = list(
            self.iter_batches(
                instrument_id=instrument_id,
                interval=interval,
                start_ns=start_ns,
                end_ns=end_ns,
                limit=limit,
            )
        )
        if not batches:
            return empty_logical_table()
        return pa.Table.from_batches(batches, schema=BAR_SCHEMA)

    def read_arrow_bars(
        self,
        *,
        instrument_id: Any = _UNSET,
        interval: Any = _UNSET,
        start_ns: Any = _UNSET,
        end_ns: Any = _UNSET,
        limit: int | None = None,
    ) -> pa.Table:
        """Materialize contract Bars through the DuckDB scan on this Snapshot.

        This is the non-paginated JSON read. Row selection follows the DuckDB
        scan order; `limit` only caps the number of rows and no global
        `ORDER BY` is added. The Arrow schema is restored per batch.
        """
        batches = list(
            self.iter_arrow_batches(
                instrument_id=instrument_id,
                interval=interval,
                start_ns=start_ns,
                end_ns=end_ns,
                limit=limit,
            )
        )
        if not batches:
            return empty_logical_table()
        return pa.Table.from_batches(batches, schema=BAR_SCHEMA)

    def read_earliest_arrow_bars(self, *, keep: int) -> pa.Table:
        """Materialize the globally earliest ``keep`` raw Bar rows.

        This is the single-Instrument paginated JSON page read. One DuckDB
        query orders the whole request Snapshot selection by `open_ts`
        ascending and stops after ``keep`` rows, so there is no Python
        per-batch concat/sort loop. Equal `open_ts` rows keep the previously
        observable stable order (Manifest file order, then physical row
        order); raw Revisions are never deduplicated.

        The request always filters one Bar Interval (the REST `interval`
        parameter). A read without an Interval filter would need per-row
        partition restoration, so it is rejected rather than guessing.
        """
        if keep < 1:
            msg = "keep must be a positive row count"
            raise ValueError(msg)
        self._ensure_open()
        query = self._resolve_query(_UNSET, _UNSET, _UNSET, _UNSET)
        self._scans += 1
        groups = self._arrow_scan_groups(query)
        if not groups:
            return empty_logical_table()
        if len(groups) != 1:
            msg = "earliest Bar reads require a single Bar Interval"
            raise RuntimeError(msg)
        interval, files = groups[0]
        scanner = DuckDBArrowScanner(self._store.duckdb_connections)
        with self._close_lock:
            if self._closed:
                msg = "StoreRead is closed"
                raise RuntimeError(msg)
            self._active_scanner = scanner
        try:
            batches = list(
                scanner.earliest_batches(
                    files,
                    interval,
                    query,
                    batch_size=self._store.duckdb.arrow_batch_size,
                    keep=keep,
                )
            )
        finally:
            scanner.close()
            with self._close_lock:
                if self._active_scanner is scanner:
                    self._active_scanner = None
        if not batches:
            return empty_logical_table()
        return pa.Table.from_batches(batches, schema=BAR_SCHEMA)

    def close(self) -> None:
        """Release the lease and any DuckDB scan. Safe from any thread."""
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
            lease = self.lease
            self.lease = None
            scanner = self._active_scanner
            self._active_scanner = None
        if scanner is not None:
            scanner.close()
        if lease is not None:
            lease.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    # -- internals ---------------------------------------------------------

    def _ensure_open(self) -> None:
        if self._closed:
            msg = "StoreRead is closed"
            raise RuntimeError(msg)

    def _resolve_query(
        self,
        instrument_id: Any,
        interval: Any,
        start_ns: Any,
        end_ns: Any,
    ) -> BarReadQuery:
        if all(
            value is _UNSET
            for value in (instrument_id, interval, start_ns, end_ns)
        ):
            return self._default_query or BarReadQuery()
        return BarReadQuery(
            instrument_id=None if instrument_id is _UNSET else instrument_id,
            interval=None if interval is _UNSET else interval,
            start_ns=None if start_ns is _UNSET else start_ns,
            end_ns=None if end_ns is _UNSET else end_ns,
        )

    def _open_files(
        self, query: BarReadQuery
    ) -> list[tuple[pq.ParquetFile, str]]:
        """Open every selected file; refresh once if one disappeared."""
        for attempt in range(MANIFEST_REFRESH_ATTEMPTS):
            entries = _select_entries(
                self.snapshot.entries,
                instrument_id=query.instrument_id,
                interval=query.interval,
                start_ns=query.start_ns,
                end_ns=query.end_ns,
            )
            try:
                return _open_parquet_files(self._store.dataset_root, entries)
            except FileNotFoundError:
                # A concurrent writer ignored the lease (or deleted the file
                # externally). Only a read that has not produced a row and has
                # not completed an earlier scan may switch snapshots.
                if (
                    self._scans > 1
                    or self._yielded
                    or attempt + 1 >= MANIFEST_REFRESH_ATTEMPTS
                ):
                    raise
                self._reacquire()
        return []

    def _replay_scan_branches(
        self, queries: Sequence[ReplayReadQuery]
    ) -> tuple[ReplayScanBranch, ...]:
        """Explicit Data Files per distinct Replay subscription.

        Selection is re-run against this read's fixed Snapshot entries, so a
        subscription can only see the Data Files the Session leased at
        creation. A missing file is an explicit read failure: the Snapshot is
        fixed for the whole Session and is never silently replaced by newer
        data.
        """
        dataset_root = self._store.dataset_root
        branches: list[ReplayScanBranch] = []
        for query in queries:
            entries = _select_entries(
                self.snapshot.entries,
                instrument_id=query.instrument_id,
                interval=query.interval,
                start_ns=query.open_start_ns,
                end_ns=query.close_end_ns,
            )
            paths = tuple(
                str(dataset_root / str(entry["path"])) for entry in entries
            )
            missing = [path for path in paths if not Path(path).is_file()]
            if missing:
                msg = (
                    "Data File is missing from the selected Snapshot: "
                    f"{missing[0]}"
                )
                raise FileNotFoundError(msg)
            if not paths:
                continue
            branches.append(
                ReplayScanBranch(
                    files=paths,
                    instrument_id=query.instrument_id,
                    interval=query.interval,
                    open_start_ns=query.open_start_ns,
                    close_start_ns=query.close_start_ns,
                    close_end_ns=query.close_end_ns,
                )
            )
        return tuple(branches)

    def _arrow_scan_groups(
        self, query: BarReadQuery
    ) -> tuple[tuple[str, tuple[str, ...]], ...]:
        """Explicit Data Files per partition interval, after a miss retry.

        Selecting entries and checking that their files exist happens before a
        DuckDB connection is opened, exactly like the PyArrow path. A missing
        file before any row was produced may switch to a newer Snapshot once;
        after that it is a read failure, never a silently short result.
        """
        dataset_root = self._store.dataset_root
        for attempt in range(MANIFEST_REFRESH_ATTEMPTS):
            entries = _select_entries(
                self.snapshot.entries,
                instrument_id=query.instrument_id,
                interval=query.interval,
                start_ns=query.start_ns,
                end_ns=query.end_ns,
            )
            paths = [dataset_root / str(entry["path"]) for entry in entries]
            missing = [path for path in paths if not path.is_file()]
            if not missing:
                return _group_scan_files(entries, dataset_root)
            if (
                self._scans > 1
                or self._yielded
                or attempt + 1 >= MANIFEST_REFRESH_ATTEMPTS
            ):
                msg = f"Data File is missing from the selected Snapshot: {missing[0]}"
                raise FileNotFoundError(msg)
            self._reacquire()
        return ()

    def _reacquire(self) -> None:
        lease = self.lease
        self.lease = None
        if lease is not None:
            lease.close()
        snapshot, new_lease = self._store._acquire_snapshot(
            self._acquisition_queries,
            lease_id=self._lease_id,
            subscriptions=self._subscriptions,
            heartbeat_s=self._heartbeat_s,
        )
        self.snapshot = snapshot
        self.lease = new_lease


def _group_scan_files(
    entries: Sequence[dict], dataset_root: Path
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    """Group the selected Data Files by their Hive partition Interval.

    The physical Parquet file omits `interval`; every file in one partition
    carries the same value, so the Store restores it per batch without making
    DuckDB infer partitions from the Dataset root layout.
    """
    groups: dict[str, list[str]] = {}
    for entry in entries:
        partition = entry.get("partition") or {}
        interval = str(partition.get("interval", ""))
        groups.setdefault(interval, []).append(
            str(dataset_root / str(entry["path"]))
        )
    return tuple(
        (interval, tuple(files)) for interval, files in groups.items()
    )


def _select_for_queries(
    entries: Sequence[dict], query_specs: Sequence[BarReadQuery]
) -> list[dict]:
    """Union of the Manifest entries selected by every scan window."""
    selected: list[dict] = []
    seen: set[str] = set()
    for query in query_specs:
        for entry in _select_entries(
            entries,
            instrument_id=query.instrument_id,
            interval=query.interval,
            start_ns=query.start_ns,
            end_ns=query.end_ns,
        ):
            path = str(entry["path"])
            if path in seen:
                continue
            seen.add(path)
            selected.append(entry)
    return selected


def _lease_scope(
    entries: Sequence[dict],
) -> tuple[tuple[dict[str, str], ...], tuple[str, ...]]:
    """Exact partitions and months protected by a read lease."""
    partitions: list[dict[str, str]] = []
    seen_partitions: set[tuple[str, ...]] = set()
    months: set[str] = set()
    for entry in entries:
        partition = entry.get("partition") or {}
        key = tuple(str(partition.get(field, "")) for field in PARTITION_FIELDS)
        if key not in seen_partitions:
            seen_partitions.add(key)
            partitions.append(
                {
                    field: str(partition[field])
                    for field in PARTITION_FIELDS
                    if field in partition
                }
            )
        year = str(partition.get("year", ""))
        month = str(partition.get("month", ""))
        if year and month:
            months.add(f"{year}-{month}")
    return tuple(partitions), tuple(sorted(months))


def _open_parquet_files(
    dataset_root: Path, entries: Sequence[dict]
) -> list[tuple[pq.ParquetFile, str]]:
    opened: list[tuple[pq.ParquetFile, str]] = []
    try:
        for entry in entries:
            path = Path(dataset_root) / str(entry["path"])
            opened.append(
                (
                    pq.ParquetFile(path),
                    str(entry["partition"].get("interval", "")),
                )
            )
    except BaseException:
        _close_parquet_files(opened)
        raise
    return opened


def _select_entries(
    entries: Sequence[dict],
    *,
    instrument_id: str | None,
    interval: str | None,
    start_ns: int | None,
    end_ns: int | None,
) -> list[dict]:
    """Manifest entries that can contain rows for the query.

    Partition and file-range pruning happens before any Data File is opened, so
    a query does not pay for unrelated partitions even though the scan itself
    is row-filtered.
    """
    selected: list[dict] = []
    for entry in entries:
        if not entry.get("active", True):
            continue
        partition = entry.get("partition") or {}
        if interval is not None and str(partition.get("interval", "")) != interval:
            continue
        if (
            instrument_id is not None
            and instrument_id_for_partition(partition) != instrument_id
        ):
            continue
        entry_start = int(entry.get("start_ns") or 0)
        entry_end = int(entry.get("end_ns") or 0)
        if entry_start or entry_end:
            if end_ns is not None and entry_start and entry_start >= end_ns:
                continue
            if start_ns is not None and entry_end and entry_end <= start_ns:
                continue
        selected.append(entry)
    return selected


def _with_partition_interval(
    physical_batch: pa.RecordBatch, partition_interval: str
) -> pa.RecordBatch:
    """Rebuild the logical schema, restoring the Hive `interval` column."""
    arrays: list[pa.Array] = []
    for name in BAR_SCHEMA.names:
        if name == "interval":
            arrays.append(
                pa.array(
                    [partition_interval] * physical_batch.num_rows,
                    type=BAR_SCHEMA.field("interval").type,
                )
            )
        else:
            arrays.append(physical_batch.column(name))
    return pa.RecordBatch.from_arrays(arrays, schema=BAR_SCHEMA)


def _filter_batch(
    batch: pa.RecordBatch,
    *,
    instrument_id: str | None,
    interval: str | None,
    start_ns: int | None,
    end_ns: int | None,
) -> pa.RecordBatch:
    mask = None

    def combine(clause) -> None:
        nonlocal mask
        mask = clause if mask is None else pc.and_(mask, clause)

    if instrument_id is not None:
        combine(
            pc.equal(
                batch.column("instrument_id"),
                pa.scalar(instrument_id, type=pa.string()),
            )
        )
    if interval is not None:
        combine(
            pc.equal(
                batch.column("interval"), pa.scalar(interval, type=pa.string())
            )
        )
    if start_ns is not None:
        combine(
            pc.greater_equal(
                batch.column("open_ts"), pa.scalar(start_ns, type=UTC_TS)
            )
        )
    if end_ns is not None:
        combine(
            pc.less(batch.column("open_ts"), pa.scalar(end_ns, type=UTC_TS))
        )
    return batch if mask is None else batch.filter(mask)


def _close_parquet_files(files: Sequence[tuple[pq.ParquetFile, str]]) -> None:
    for parquet_file, _ in files:
        # Best-effort close on an error path: never mask the original failure.
        with suppress(Exception):  # pragma: no cover - defensive close path
            parquet_file.close()


def _median_file_size(entries: Sequence[dict]) -> int | float | None:
    if not entries:
        return None
    value = statistics.median(int(entry.get("size_bytes") or 0) for entry in entries)
    return int(value) if float(value).is_integer() else value
