"""Compaction and Retention/Garbage Collection for the Bar Dataset.

Both operations are Manifest-first: a new Data File is written and the
Manifest is switched atomically before any old Data File is removed. Data
Files themselves are never rewritten.
"""

from __future__ import annotations

import os
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pyarrow.dataset as pds
import pyarrow.parquet as pq

from mdgw.core.schema import PHYSICAL_SCHEMA
from mdgw.core.time import datetime_to_ns, to_utc, utc_now
from mdgw.store.manifest import (
    PARTITION_FIELDS,
    instrument_id_for_partition,
    normalize_manifest_entry,
    partition_key,
    partition_label,
    read_manifest,
    write_manifest,
)
from mdgw.store.session_locks import (
    DEFAULT_LOCK_TIMEOUT_S,
    SESSIONS_DIR_NAME,
    SessionLock,
    active_session_locks,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mdgw.store.parquet_dataset import ParquetBarStore

NANOS_PER_DAY = 86_400 * 1_000_000_000

_DURATION_RE = re.compile(r"^(?P<amount>\d+)(?P<unit>[smhdw])$", re.IGNORECASE)
_DURATION_UNITS = {
    "s": 1,
    "m": 60,
    "h": 3_600,
    "d": 86_400,
    "w": 604_800,
}


@dataclass(frozen=True, slots=True)
class CompactionReport:
    older_than_ns: int
    merged_partitions: tuple[str, ...] = ()
    new_files: tuple[str, ...] = ()
    removed_files: tuple[str, ...] = ()
    skipped_locked: tuple[str, ...] = ()
    skipped_single: tuple[str, ...] = ()
    failures: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GCReport:
    dry_run: bool
    bars_days: int
    removed_partitions: tuple[str, ...] = ()
    removed_files: tuple[str, ...] = ()
    skipped_current_month: tuple[str, ...] = ()
    skipped_locked: tuple[str, ...] = ()
    failures: tuple[str, ...] = ()


def parse_duration(value: str) -> timedelta:
    """Parse operator durations such as `24h`, `7d`, `30m` or `0s`."""
    match = _DURATION_RE.match(value.strip())
    if match is None:
        raise ValueError(
            f"invalid duration {value!r}; expected a number followed by s/m/h/d/w"
        )
    seconds = int(match.group("amount")) * _DURATION_UNITS[match.group("unit").lower()]
    return timedelta(seconds=seconds)


def duration_to_ns(value: timedelta | str | int) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        value = parse_duration(value)
    return (
        value.days * NANOS_PER_DAY
        + value.seconds * 1_000_000_000
        + value.microseconds * 1_000
    )


def partition_is_locked(
    partition: dict[str, Any], locks: list[SessionLock]
) -> bool:
    """A lock with no subscriptions protects the whole Dataset; otherwise match."""
    instrument_id = instrument_id_for_partition(partition)
    interval = str(partition.get("interval", ""))
    for lock in locks:
        if not lock.subscriptions:
            return True
        for subscription in lock.subscriptions:
            locked_instrument = subscription.get("instrument_id")
            locked_interval = subscription.get("interval")
            if locked_instrument is not None and locked_instrument != instrument_id:
                continue
            if locked_interval is not None and locked_interval != interval:
                continue
            return True
    return False


def compact_store(
    store: "ParquetBarStore",
    *,
    older_than: timedelta | str | int,
    now_ns: int | None = None,
    timeout_s: int = DEFAULT_LOCK_TIMEOUT_S,
) -> CompactionReport:
    """Compact the store by merging eligible files older than the specified duration.
    Args:
        store: The ParquetBarStore instance to compact.
        older_than: Duration specifying which files are eligible for compaction.
        now_ns: Current time in nanoseconds. If None, the current system time is used.
        timeout_s: Lock acquisition timeout in seconds.

    Returns:
        A CompactionReport summarizing the compaction results.
    """
    store.validate_schema_version()
    dataset_root = store.dataset_root
    now_ns = time.time_ns() if now_ns is None else int(now_ns)
    older_than_ns = duration_to_ns(older_than)
    cutoff_ns = now_ns - older_than_ns
    locks = active_session_locks(dataset_root, timeout_s=timeout_s, now_ns=now_ns)

    with store._lock:  # noqa: SLF001 - shared writer/compaction lock
        manifest = read_manifest(dataset_root)
        entries = [
            normalize_manifest_entry(raw, dataset_root)
            for raw in manifest.get("files", [])
        ]
        groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
        for entry in entries:
            if not entry["active"]:
                continue
            if not (dataset_root / entry["path"]).exists():
                continue
            groups[partition_key(entry["partition"])].append(entry)

        merged_partitions: list[str] = []
        new_files: list[str] = []
        skipped_locked: list[str] = []
        skipped_single: list[str] = []
        failures: list[str] = []
        pending_inactive: set[str] = set()

        for key in sorted(groups):
            group = groups[key]
            partition = group[0]["partition"]
            label = partition_label(partition)

            if partition_is_locked(partition, locks):
                skipped_locked.append(label)
                continue

            eligible = [
                entry for entry in group if entry["created_ns"] <= cutoff_ns
            ]
            if len(eligible) < 2:
                skipped_single.append(label)
                continue

            try:
                new_entry = _write_compacted_file(store, partition, eligible)
            except Exception as exc:  # defensive: leave the Manifest untouched
                failures.append(f"{label}: {exc}")
                continue

            eligible_paths = {entry["path"] for entry in eligible}
            for entry in entries:
                if entry["path"] in eligible_paths:
                    entry["active"] = False
            entries.append(new_entry)
            pending_inactive.update(eligible_paths)
            merged_partitions.append(label)
            new_files.append(new_entry["path"])

        if not new_files:
            return CompactionReport(
                older_than_ns=older_than_ns,
                skipped_locked=tuple(skipped_locked),
                skipped_single=tuple(skipped_single),
                failures=tuple(failures),
            )

        manifest["files"] = entries
        try:
            write_manifest(dataset_root, manifest)
        except Exception:
            for relative_path in new_files:
                (dataset_root / relative_path).unlink(missing_ok=True)
            raise

        removed_files: list[str] = []
        for relative_path in sorted(pending_inactive):
            path = dataset_root / relative_path
            try:
                path.unlink()
            except FileNotFoundError:
                removed_files.append(relative_path)
            except OSError as exc:
                # The inactive record stays in the Manifest and the failure is
                # reported so an operator can retry.
                failures.append(f"{relative_path}: {exc}")
            else:
                removed_files.append(relative_path)

        return CompactionReport(
            older_than_ns=older_than_ns,
            merged_partitions=tuple(merged_partitions),
            new_files=tuple(new_files),
            removed_files=tuple(removed_files),
            skipped_locked=tuple(skipped_locked),
            skipped_single=tuple(skipped_single),
            failures=tuple(failures),
        )


def gc_store(
    store: "ParquetBarStore",
    *,
    bars_days: int,
    apply: bool,
    now: datetime | None = None,
    timeout_s: int = DEFAULT_LOCK_TIMEOUT_S,
) -> GCReport:
    """Garbage collect old data in the store based on the specified retention period.

    Args:
        store: The ParquetBarStore instance to perform GC on.
        bars_days: Number of days to retain bars.
        apply: If True, perform the actual deletion; otherwise, perform a dry run.
        now: Current datetime. If None, the current system time is used.
        timeout_s: Lock acquisition timeout in seconds.

    Returns:
        A GCReport summarizing the GC results.
    """
    store.validate_schema_version()
    dataset_root = store.dataset_root
    dry_run = not apply
    bars_days = int(bars_days)
    if bars_days <= 0:
        return GCReport(dry_run=dry_run, bars_days=bars_days)

    now = to_utc(now) if now is not None else utc_now()
    now_ns = datetime_to_ns(now)
    cutoff_ns = now_ns - bars_days * NANOS_PER_DAY
    locks = active_session_locks(dataset_root, timeout_s=timeout_s, now_ns=now_ns)

    with store._lock:  # noqa: SLF001 - shared writer/GC lock
        manifest = read_manifest(dataset_root)
        entries = [
            normalize_manifest_entry(raw, dataset_root)
            for raw in manifest.get("files", [])
        ]

        months: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for entry in entries:
            if not entry["active"]:
                continue
            if not (dataset_root / entry["path"]).exists():
                continue
            partition = entry["partition"]
            months[(partition["year"], partition["month"])].append(entry)

        removed_partitions: list[str] = []
        skipped_current: list[str] = []
        skipped_locked: list[str] = []
        to_remove: list[dict[str, Any]] = []

        for year, month in sorted(months):
            label = f"{year}-{month}"
            if not (year.isdigit() and month.isdigit()):
                continue
            if (int(year), int(month)) == (now.year, now.month):
                skipped_current.append(label)
                continue
            if _month_end_ns(int(year), int(month)) > cutoff_ns:
                continue
            group = months[(year, month)]
            if any(partition_is_locked(entry["partition"], locks) for entry in group):
                skipped_locked.append(label)
                continue
            removed_partitions.append(label)
            to_remove.extend(group)

        removed_files = [entry["path"] for entry in to_remove]
        if dry_run or not to_remove:
            return GCReport(
                dry_run=dry_run,
                bars_days=bars_days,
                removed_partitions=tuple(removed_partitions),
                removed_files=tuple(removed_files),
                skipped_current_month=tuple(skipped_current),
                skipped_locked=tuple(skipped_locked),
            )

        removed_paths = set(removed_files)
        for entry in entries:
            if entry["path"] in removed_paths:
                entry["active"] = False
        manifest["files"] = entries
        write_manifest(dataset_root, manifest)

        failures: list[str] = []
        for relative_path in removed_files:
            try:
                (dataset_root / relative_path).unlink()
            except FileNotFoundError:
                pass
            except OSError as exc:
                failures.append(f"{relative_path}: {exc}")

        _prune_empty_directories(dataset_root)

        return GCReport(
            dry_run=dry_run,
            bars_days=bars_days,
            removed_partitions=tuple(removed_partitions),
            removed_files=tuple(removed_files),
            skipped_current_month=tuple(skipped_current),
            skipped_locked=tuple(skipped_locked),
            failures=tuple(failures),
        )


def _write_compacted_file(
    store: "ParquetBarStore",
    partition: dict[str, Any],
    eligible: list[dict[str, Any]],
) -> dict[str, Any]:
    dataset_root = store.dataset_root
    paths = [str(dataset_root / entry["path"]) for entry in eligible]
    collected = pds.dataset(paths, format="parquet", partitioning="hive")
    table = collected.to_table(columns=PHYSICAL_SCHEMA.names).select(
        PHYSICAL_SCHEMA.names
    )
    if table.schema != PHYSICAL_SCHEMA:
        table = table.cast(PHYSICAL_SCHEMA)

    start_ns = min(entry["start_ns"] for entry in eligible)
    end_ns = max(entry["end_ns"] for entry in eligible)
    max_ingest_ns = max(int(entry.get("max_ingest_ns") or 0) for entry in eligible)
    directory = _partition_directory(dataset_root, partition)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"data-{start_ns}-{end_ns}-{uuid4()}.parquet"

    pq.write_table(
        table,
        path,
        version=store.format_version,
        use_deprecated_int96_timestamps=False,
        compression=store.compression,
        row_group_size=store.row_group_size,
    )

    return {
        "path": path.relative_to(dataset_root).as_posix(),
        "start_ns": start_ns,
        "end_ns": end_ns,
        "row_count": table.num_rows,
        "active": True,
        "size_bytes": path.stat().st_size,
        "created_ns": time.time_ns(),
        "partition": dict(partition),
        "max_ingest_ns": max_ingest_ns,
    }


def _partition_directory(dataset_root: Path, partition: dict[str, Any]) -> Path:
    directory = dataset_root
    for field in PARTITION_FIELDS:
        directory = directory / f"{field}={partition[field]}"
    return directory


def _month_end_ns(year: int, month: int) -> int:
    if month == 12:
        next_year, next_month = year + 1, 1
    else:
        next_year, next_month = year, month + 1
    return datetime_to_ns(datetime(next_year, next_month, 1, tzinfo=timezone.utc))


def _prune_empty_directories(dataset_root: Path) -> None:
    for directory, _, _ in os.walk(dataset_root, topdown=False):
        path = Path(directory)
        if path == dataset_root or path.name == SESSIONS_DIR_NAME:
            continue
        try:
            path.rmdir()
        except OSError:
            pass
