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
from pathlib import Path
from typing import TYPE_CHECKING
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
from mdgw.store.maintenance import (
    CompactionReport,
    GCReport,
    compact_store,
    gc_store,
)
from mdgw.store.manifest import (
    DATASET_DESCRIPTOR_NAME,
    DATASET_NAME,
    active_manifest_entries,
    instrument_id_for_partition,
    read_dataset_descriptor,
    read_manifest,
    write_dataset_descriptor,
    write_manifest,
)
from mdgw.store.session_locks import DEFAULT_LOCK_TIMEOUT_S

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Sequence

    from mdgw.core.bar import Bar

DATASET_SCHEMA_VERSION = 1
FILE_NAMING = "data-<start_ns>-<end_ns>-<uuid>.parquet"
DEFAULT_BATCH_SIZE = 65_536
# One Manifest refresh is enough for the Compaction/GC race: the switching
# process has already written the new Manifest before it unlinks the old files.
MANIFEST_REFRESH_ATTEMPTS = 2
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
    ) -> None:
        self.root = Path(root)
        self.dataset_root = self.root / DATASET_NAME
        self.compression = compression
        self.row_group_size = row_group_size
        self.format_version = format_version
        self.schema_version = schema_version
        self.session_lock_timeout_s = session_lock_timeout_s
        self._lock = threading.Lock()

    # -- writes ------------------------------------------------------------

    def write_bars(self, bars: Sequence[Bar], *, asset_class: str) -> list[dict]:
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
            written.append(self._write_partition(key, group))

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

    def iter_batches(  # ruff: ignore[too-many-arguments] - keyword-only query filters
        self,
        *,
        instrument_id: str | None = None,
        interval: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
        limit: int | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        """Yield Bar RecordBatches from active Data Files only.

        `start_ns` is inclusive and `end_ns` is exclusive, both applied to
        `open_ts`. `limit` caps the total number of rows yielded.

        Compaction/GC switch the Manifest before deleting the old Data Files.
        A reader can therefore have selected paths from the previous Manifest
        when the files disappear. Every file is opened before the first batch
        is yielded; if an open reports a missing file, the Manifest is re-read
        once and the query is retried against the new snapshot. Files already
        opened keep working even if Compaction unlinks them (POSIX) or the
        unlink is refused while the handle is open (Windows).
        """
        self.validate_schema_version()
        opened = self._open_active_files(
            instrument_id=instrument_id,
            interval=interval,
            start_ns=start_ns,
            end_ns=end_ns,
        )
        if not opened:
            return

        remaining = limit
        try:
            for parquet_file, partition_interval in opened:
                for physical_batch in parquet_file.iter_batches(
                    columns=list(PHYSICAL_READ_COLUMNS),
                    batch_size=batch_size,
                ):
                    batch = _with_partition_interval(
                        physical_batch, partition_interval
                    )
                    batch = _filter_batch(
                        batch,
                        instrument_id=instrument_id,
                        interval=interval,
                        start_ns=start_ns,
                        end_ns=end_ns,
                    )
                    if batch.num_rows == 0:
                        continue
                    if remaining is not None:
                        if remaining <= 0:
                            return
                        if batch.num_rows > remaining:
                            batch = batch.slice(0, remaining)
                        remaining -= batch.num_rows
                    yield batch
        finally:
            for parquet_file, _ in opened:
                parquet_file.close()

    def count_bars(
        self,
        *,
        instrument_id: str | None = None,
        interval: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
    ) -> int:
        """Count matching Bars, used to enforce the JSON row ceiling."""
        return sum(
            batch.num_rows
            for batch in self.iter_batches(
                instrument_id=instrument_id,
                interval=interval,
                start_ns=start_ns,
                end_ns=end_ns,
            )
        )

    def read_bars(
        self,
        *,
        instrument_id: str | None = None,
        interval: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
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

    def _active_entries(self) -> list[dict]:
        """Active Manifest entries, including ones whose file may be gone.

        A missing file is not silently dropped here: the caller opens every
        selected file before yielding and refreshes the Manifest once if a
        concurrent Compaction/GC already removed it.
        """
        return active_manifest_entries(
            self.dataset_root, existing_only=False
        )

    def _open_active_files(
        self,
        *,
        instrument_id: str | None,
        interval: str | None,
        start_ns: int | None,
        end_ns: int | None,
    ) -> list[tuple[pq.ParquetFile, str]]:
        """Open every selected Data File, refreshing the Manifest once on a race."""
        for attempt in range(MANIFEST_REFRESH_ATTEMPTS):
            entries = self._active_entries()
            selected = _select_entries(
                entries,
                instrument_id=instrument_id,
                interval=interval,
                start_ns=start_ns,
                end_ns=end_ns,
            )
            opened: list[tuple[pq.ParquetFile, str]] = []
            try:
                for entry in selected:
                    path = self.dataset_root / str(entry["path"])
                    opened.append(
                        (
                            pq.ParquetFile(path),
                            str(entry["partition"].get("interval", "")),
                        )
                    )
            except FileNotFoundError:
                _close_parquet_files(opened)
                if attempt + 1 >= MANIFEST_REFRESH_ATTEMPTS:
                    raise
                continue
            except BaseException:
                _close_parquet_files(opened)
                raise
            return opened
        return []

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
