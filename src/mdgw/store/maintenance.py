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
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pyarrow.dataset as pds
import pyarrow.parquet as pq
from pyarrow import ArrowException

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


@dataclass(frozen=True, slots=True)
class _MergeRecord:
    label: str
    partition: dict[str, Any]
    entries: tuple[dict[str, Any], ...]
    new_entry: dict[str, Any]


@dataclass(slots=True)
class _CompactionOutcome:
    merged_partitions: list[str] = field(default_factory=list)
    new_files: list[str] = field(default_factory=list)
    removed_files: list[str] = field(default_factory=list)
    skipped_locked: list[str] = field(default_factory=list)
    skipped_single: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    pending_inactive: dict[str, dict[str, Any]] = field(default_factory=dict)
    merges: list[_MergeRecord] = field(default_factory=list)


@dataclass(slots=True)
class _GCOutcome:
    removed_partitions: list[str] = field(default_factory=list)
    skipped_current_month: list[str] = field(default_factory=list)
    skipped_locked: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    to_remove: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class _CompactionContext:
    store: ParquetBarStore
    entries: list[dict[str, Any]]
    cutoff_ns: int
    locks: list[SessionLock]
    outcome: _CompactionOutcome


@dataclass(slots=True)
class _GCContext:
    now: datetime
    cutoff_ns: int
    locks: list[SessionLock]
    outcome: _GCOutcome


def parse_duration(value: str) -> timedelta:
    """Parse operator durations such as `24h`, `7d`, `30m` or `0s`."""
    match = _DURATION_RE.match(value.strip())
    if match is None:
        msg = f"invalid duration {value!r}; expected a number followed by s/m/h/d/w"
        raise ValueError(msg)
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


def _month_label(partition: dict[str, Any]) -> str:
    return f"{partition.get('year', '')}-{partition.get('month', '')}"


def partition_is_locked(
    partition: dict[str, Any], locks: list[SessionLock]
) -> bool:
    """Whether a lock protects this partition.

    A legacy lock with no scope records protects the whole Dataset. A Replay
    Session lock carries subscriptions and matches Instrument + Interval; a
    short-lived Dataset Read Lease carries the exact partitions and months it
    selected.
    """
    instrument_id = instrument_id_for_partition(partition)
    interval = str(partition.get("interval", ""))
    key = partition_key(partition)
    month = _month_label(partition)
    for lock in locks:
        if not lock.subscriptions and not lock.partitions and not lock.months:
            return True
        if lock.partitions:
            if any(
                partition_key(candidate) == key
                for candidate in lock.partitions
            ):
                return True
        elif lock.months and month in lock.months:
            # Month-only scope is coarser than an exact partition list; GC's
            # month grouping still protects the whole month it belongs to.
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
    store: ParquetBarStore,
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

    with store._lock:  # shared writer/compaction lock
        locks = active_session_locks(
            dataset_root, timeout_s=timeout_s, now_ns=now_ns
        )
        manifest = read_manifest(dataset_root)
        entries = _load_entries(manifest, dataset_root)
        outcome = _compact_entries(store, entries, cutoff_ns, locks)
        if not outcome.new_files:
            return _compaction_report(older_than_ns, outcome)

        # Second guard: a lease may have appeared while compacted files were
        # being written. Never switch the Manifest for a partition that is
        # now protected.
        _recheck_compaction_before_switch(
            dataset_root, entries, outcome, timeout_s=timeout_s, now_ns=now_ns
        )
        if not outcome.new_files:
            return _compaction_report(older_than_ns, outcome)

        manifest["files"] = entries
        _write_manifest_or_rollback(dataset_root, manifest, outcome.new_files)
        # Third guard, immediately before the unlink: a lease acquired after
        # the previous check still cannot lose its files silently.
        _recheck_compaction_before_unlink(
            dataset_root, outcome, timeout_s=timeout_s, now_ns=now_ns
        )
        _remove_superseded_files(dataset_root, outcome)
        return _compaction_report(older_than_ns, outcome)


def gc_store(
    store: ParquetBarStore,
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

    with store._lock:  # shared writer/GC lock
        locks = active_session_locks(
            dataset_root, timeout_s=timeout_s, now_ns=now_ns
        )
        manifest = read_manifest(dataset_root)
        entries = _load_entries(manifest, dataset_root)
        outcome = _collect_expired_partitions(
            entries, dataset_root, now, cutoff_ns, locks
        )
        if dry_run or not outcome.to_remove:
            return _gc_report(dry_run, bars_days, outcome)

        # Second guard before the Manifest switch, then a third one right
        # before the unlink; a read lease acquired mid-maintenance keeps its
        # whole month.
        _recheck_gc_before_removal(
            dataset_root, outcome, timeout_s=timeout_s, now_ns=now_ns
        )
        if not outcome.to_remove:
            return _gc_report(dry_run, bars_days, outcome)

        _deactivate_entries(entries, {entry["path"] for entry in outcome.to_remove})
        manifest["files"] = entries
        write_manifest(dataset_root, manifest)
        skipped_paths = _recheck_gc_before_removal(
            dataset_root, outcome, timeout_s=timeout_s, now_ns=now_ns
        )
        if skipped_paths:
            # A lease appeared after the Manifest switch. Put its entries back
            # so the data stays reachable and a later GC retries the month.
            _reactivate_entries(entries, skipped_paths)
            manifest["files"] = entries
            write_manifest(dataset_root, manifest)
        _unlink_removed_files(dataset_root, outcome)
        _prune_empty_directories(dataset_root)
        return _gc_report(dry_run, bars_days, outcome)


def _load_entries(
    manifest: dict[str, Any], dataset_root: Path
) -> list[dict[str, Any]]:
    return [
        normalize_manifest_entry(raw, dataset_root)
        for raw in manifest.get("files", [])
    ]


def _active_existing_entries(
    entries: list[dict[str, Any]], dataset_root: Path
) -> list[dict[str, Any]]:
    return [
        entry
        for entry in entries
        if entry["active"] and (dataset_root / entry["path"]).exists()
    ]


def _group_by_partition(
    entries: list[dict[str, Any]],
) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        groups[partition_key(entry["partition"])].append(entry)
    return groups


def _compact_entries(
    store: ParquetBarStore,
    entries: list[dict[str, Any]],
    cutoff_ns: int,
    locks: list[SessionLock],
) -> _CompactionOutcome:
    context = _CompactionContext(
        store=store,
        entries=entries,
        cutoff_ns=cutoff_ns,
        locks=locks,
        outcome=_CompactionOutcome(),
    )
    groups = _group_by_partition(_active_existing_entries(entries, store.dataset_root))
    for key in sorted(groups):
        _compact_group(groups[key], context)
    return context.outcome


def _compact_group(
    group: list[dict[str, Any]], context: _CompactionContext
) -> None:
    partition = group[0]["partition"]
    label = partition_label(partition)

    if partition_is_locked(partition, context.locks):
        context.outcome.skipped_locked.append(label)
        return

    eligible = [
        entry for entry in group if entry["created_ns"] <= context.cutoff_ns
    ]
    if len(eligible) < 2:
        context.outcome.skipped_single.append(label)
        return

    try:
        new_entry = _write_compacted_file(context.store, partition, eligible)
    except (OSError, ArrowException, ValueError) as exc:
        # Defensive: leave the Manifest untouched when a Data File cannot merge.
        context.outcome.failures.append(f"{label}: {exc}")
        return

    eligible_paths = {entry["path"] for entry in eligible}
    for entry in context.entries:
        if entry["path"] in eligible_paths:
            entry["active"] = False
    context.entries.append(new_entry)
    for entry in eligible:
        context.outcome.pending_inactive[entry["path"]] = dict(partition)
    context.outcome.merged_partitions.append(label)
    context.outcome.new_files.append(new_entry["path"])
    context.outcome.merges.append(
        _MergeRecord(
            label=label,
            partition=dict(partition),
            entries=tuple(eligible),
            new_entry=new_entry,
        )
    )


def _write_manifest_or_rollback(
    dataset_root: Path, manifest: dict[str, Any], new_files: list[str]
) -> None:
    try:
        write_manifest(dataset_root, manifest)
    except Exception:
        for relative_path in new_files:
            (dataset_root / relative_path).unlink(missing_ok=True)
        raise


def _append_locked(outcome: _CompactionOutcome | _GCOutcome, label: str) -> None:
    if label not in outcome.skipped_locked:
        outcome.skipped_locked.append(label)


def _recheck_compaction_before_switch(
    dataset_root: Path,
    entries: list[dict[str, Any]],
    outcome: _CompactionOutcome,
    *,
    timeout_s: int,
    now_ns: int,
) -> None:
    """Roll back a merge whose partition gained a lease after the first scan."""
    if not outcome.merges:
        return
    locks = active_session_locks(
        dataset_root, timeout_s=timeout_s, now_ns=now_ns
    )
    for merge in list(outcome.merges):
        if not partition_is_locked(merge.partition, locks):
            continue
        for entry in merge.entries:
            entry["active"] = True
            outcome.pending_inactive.pop(entry["path"], None)
        entries[:] = [
            entry for entry in entries if entry is not merge.new_entry
        ]
        with suppress(OSError):
            (dataset_root / merge.new_entry["path"]).unlink()
        if merge.new_entry["path"] in outcome.new_files:
            outcome.new_files.remove(merge.new_entry["path"])
        if merge.label in outcome.merged_partitions:
            outcome.merged_partitions.remove(merge.label)
        outcome.merges.remove(merge)
        _append_locked(outcome, merge.label)


def _recheck_compaction_before_unlink(
    dataset_root: Path,
    outcome: _CompactionOutcome,
    *,
    timeout_s: int,
    now_ns: int,
) -> None:
    """Keep files whose partition is now leased instead of unlinking them."""
    if not outcome.pending_inactive:
        return
    locks = active_session_locks(
        dataset_root, timeout_s=timeout_s, now_ns=now_ns
    )
    for relative_path, partition in list(outcome.pending_inactive.items()):
        if not partition_is_locked(partition, locks):
            continue
        del outcome.pending_inactive[relative_path]
        _append_locked(outcome, partition_label(partition))


def _remove_superseded_files(
    dataset_root: Path, outcome: _CompactionOutcome
) -> None:
    for relative_path in sorted(outcome.pending_inactive):
        path = dataset_root / relative_path
        try:
            path.unlink()
        except FileNotFoundError:
            outcome.removed_files.append(relative_path)
        except OSError as exc:
            # The inactive record stays in the Manifest and the failure is
            # reported so an operator can retry.
            outcome.failures.append(f"{relative_path}: {exc}")
        else:
            outcome.removed_files.append(relative_path)


def _compaction_report(
    older_than_ns: int, outcome: _CompactionOutcome
) -> CompactionReport:
    return CompactionReport(
        older_than_ns=older_than_ns,
        merged_partitions=tuple(outcome.merged_partitions),
        new_files=tuple(outcome.new_files),
        removed_files=tuple(outcome.removed_files),
        skipped_locked=tuple(outcome.skipped_locked),
        skipped_single=tuple(outcome.skipped_single),
        failures=tuple(outcome.failures),
    )


def _group_by_month(
    entries: list[dict[str, Any]],
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    months: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        partition = entry["partition"]
        months[partition["year"], partition["month"]].append(entry)
    return months


def _collect_expired_partitions(
    entries: list[dict[str, Any]],
    dataset_root: Path,
    now: datetime,
    cutoff_ns: int,
    locks: list[SessionLock],
) -> _GCOutcome:
    context = _GCContext(
        now=now,
        cutoff_ns=cutoff_ns,
        locks=locks,
        outcome=_GCOutcome(),
    )
    months = _group_by_month(_active_existing_entries(entries, dataset_root))
    for (year, month), group in sorted(months.items()):
        _collect_expired_month(group, year, month, context)
    return context.outcome


def _collect_expired_month(
    group: list[dict[str, Any]],
    year: str,
    month: str,
    context: _GCContext,
) -> None:
    label = f"{year}-{month}"
    if not (year.isdigit() and month.isdigit()):
        return
    if (int(year), int(month)) == (context.now.year, context.now.month):
        context.outcome.skipped_current_month.append(label)
        return
    if _month_end_ns(int(year), int(month)) > context.cutoff_ns:
        return
    if any(partition_is_locked(entry["partition"], context.locks) for entry in group):
        context.outcome.skipped_locked.append(label)
        return
    context.outcome.removed_partitions.append(label)
    context.outcome.to_remove.extend(group)


def _deactivate_entries(
    entries: list[dict[str, Any]], removed_paths: set[str]
) -> None:
    for entry in entries:
        if entry["path"] in removed_paths:
            entry["active"] = False


def _reactivate_entries(
    entries: list[dict[str, Any]], kept_paths: set[str]
) -> None:
    for entry in entries:
        if entry["path"] in kept_paths:
            entry["active"] = True


def _recheck_gc_before_removal(
    dataset_root: Path,
    outcome: _GCOutcome,
    *,
    timeout_s: int,
    now_ns: int,
) -> set[str]:
    """Skip whole months that gained a read lease during GC.

    Returns the Data File paths that were dropped from the removal set, so the
    caller can put their Manifest entries back after a late Manifest switch.
    """
    if not outcome.to_remove:
        return set()
    before_paths = {entry["path"] for entry in outcome.to_remove}
    locks = active_session_locks(
        dataset_root, timeout_s=timeout_s, now_ns=now_ns
    )
    by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in outcome.to_remove:
        by_month[_month_label(entry["partition"])].append(entry)

    kept: list[dict[str, Any]] = []
    skipped: set[str] = set()
    for label in outcome.removed_partitions:
        group = by_month.pop(label, [])
        if any(partition_is_locked(entry["partition"], locks) for entry in group):
            skipped.add(label)
        else:
            kept.extend(group)
    for label, group in by_month.items():
        if any(partition_is_locked(entry["partition"], locks) for entry in group):
            skipped.add(label)
        else:
            kept.extend(group)

    outcome.to_remove = kept
    outcome.removed_partitions = [
        label
        for label in outcome.removed_partitions
        if label not in skipped
    ]
    for label in sorted(skipped):
        _append_locked(outcome, label)
    return before_paths - {entry["path"] for entry in kept}


def _unlink_removed_files(dataset_root: Path, outcome: _GCOutcome) -> None:
    for entry in outcome.to_remove:
        relative_path = entry["path"]
        try:
            (dataset_root / relative_path).unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            outcome.failures.append(f"{relative_path}: {exc}")


def _gc_report(
    dry_run: bool, bars_days: int, outcome: _GCOutcome
) -> GCReport:
    return GCReport(
        dry_run=dry_run,
        bars_days=bars_days,
        removed_partitions=tuple(outcome.removed_partitions),
        removed_files=tuple(entry["path"] for entry in outcome.to_remove),
        skipped_current_month=tuple(outcome.skipped_current_month),
        skipped_locked=tuple(outcome.skipped_locked),
        failures=tuple(outcome.failures),
    )


def _write_compacted_file(
    store: ParquetBarStore,
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
    for partition_field in PARTITION_FIELDS:
        directory /= f"{partition_field}={partition[partition_field]}"
    return directory


def _month_end_ns(year: int, month: int) -> int:
    if month == 12:
        next_year, next_month = year + 1, 1
    else:
        next_year, next_month = year, month + 1
    return datetime_to_ns(datetime(next_year, next_month, 1, tzinfo=UTC))


def _prune_empty_directories(dataset_root: Path) -> None:
    for directory, _, _ in os.walk(dataset_root, topdown=False):
        path = Path(directory)
        if path == dataset_root or path.name == SESSIONS_DIR_NAME:
            continue
        with suppress(OSError):
            path.rmdir()
