"""Independent, versioned Quarantine Dataset.

Rejected inputs are persisted as immutable Parquet files under
``<root>/year=YYYY/month=MM`` and listed in the Quarantine Manifest
(``_quarantine_manifest.json``). The Dataset owns a descriptor
(``_quarantine_dataset.json``) with an independent ``schema_version``; it is
never the Bars Dataset and is never consulted by the Bars Store, REST or
Replay.

A write is atomic: Parquet files are written first, then the Manifest is
appended. If the Manifest append fails the new files are removed and
``QuarantineWriteError`` is raised, so callers can stay fail-closed.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from mdgw.core.schema import UTC_TS, schema_to_json
from mdgw.core.time import datetime_to_ns, ns_to_iso, to_utc, utc_now

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

NANOS_PER_DAY = 86_400 * 1_000_000_000
QUARANTINE_DEFAULT_RETENTION_DAYS = 30

QUARANTINE_SCHEMA_VERSION = 1
QUARANTINE_DATASET_NAME = "quarantine"
QUARANTINE_DESCRIPTOR_NAME = "_quarantine_dataset.json"
QUARANTINE_MANIFEST_NAME = "_quarantine_manifest.json"
QUARANTINE_FILE_NAMING = "quarantine-<start_ns>-<end_ns>-<uuid>.parquet"
QUARANTINE_PARTITIONING: tuple[str, ...] = ("year", "month")

# Stable Source Stage vocabulary. Exception text, file paths and Instrument
# identities must never become a stage.
SOURCE_STAGE_LIVE_NORMALIZE = "live_normalize"
SOURCE_STAGE_LIVE_INSTRUMENT_VALIDATE = "live_instrument_validate"
SOURCE_STAGE_CCXT_LIVE_ROW = "ccxt_live_row"
SOURCE_STAGE_CCXT_HISTORY_ROW = "ccxt_history_row"
SOURCE_STAGE_ARCHIVE_FILE = "archive_file"
SOURCE_STAGES = frozenset(
    {
        SOURCE_STAGE_LIVE_NORMALIZE,
        SOURCE_STAGE_LIVE_INSTRUMENT_VALIDATE,
        SOURCE_STAGE_CCXT_LIVE_ROW,
        SOURCE_STAGE_CCXT_HISTORY_ROW,
        SOURCE_STAGE_ARCHIVE_FILE,
    }
)

# Stable Reason Code vocabulary. This is an operations contract, not a place
# for exception messages.
REASON_UNCLOSED_BAR = "unclosed_bar"
REASON_FLOAT_VALUE = "float_value"
REASON_INVALID_INTERVAL = "invalid_interval"
REASON_DECIMAL_SCALE_EXCEEDED = "decimal_scale_exceeded"
REASON_INVALID_INSTRUMENT = "invalid_instrument"
REASON_UNKNOWN_INSTRUMENT = "unknown_instrument"
REASON_MALFORMED_ROW = "malformed_row"
REASON_ARCHIVE_CHECKSUM_MISMATCH = "archive_checksum_mismatch"
REASON_ARCHIVE_FORMAT_ERROR = "archive_format_error"
REASON_ARCHIVE_TIME_UNIT_UNSUPPORTED = "archive_time_unit_unsupported"
REASON_ARCHIVE_FIELD_ERROR = "archive_field_error"
REASON_CODES = frozenset(
    {
        REASON_UNCLOSED_BAR,
        REASON_FLOAT_VALUE,
        REASON_INVALID_INTERVAL,
        REASON_DECIMAL_SCALE_EXCEEDED,
        REASON_INVALID_INSTRUMENT,
        REASON_UNKNOWN_INSTRUMENT,
        REASON_MALFORMED_ROW,
        REASON_ARCHIVE_CHECKSUM_MISMATCH,
        REASON_ARCHIVE_FORMAT_ERROR,
        REASON_ARCHIVE_TIME_UNIT_UNSUPPORTED,
        REASON_ARCHIVE_FIELD_ERROR,
    }
)

MAX_DETAIL_STRING = 512
MAX_RAW_STRING = 1_024
MAX_CONTAINER_ITEMS = 32
MAX_SANITIZER_DEPTH = 4

QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("quarantine_schema_version", pa.uint16(), nullable=False),
        pa.field("recorded_ts", UTC_TS, nullable=False),
        pa.field("source_stage", pa.string(), nullable=False),
        pa.field("reason_code", pa.string(), nullable=False),
        pa.field("rule_version", pa.string(), nullable=False),
        pa.field("feed", pa.string(), nullable=True),
        pa.field("instrument_id", pa.string(), nullable=True),
        pa.field("interval", pa.string(), nullable=True),
        pa.field("open_ts", UTC_TS, nullable=True),
        pa.field("source_location", pa.string(), nullable=False),
        pa.field("details", pa.string(), nullable=False),
        pa.field("raw_record", pa.string(), nullable=True),
    ]
)


class QuarantineWriteError(RuntimeError):
    """A Quarantine record could not be durably written (fail-closed)."""


@dataclass(frozen=True, slots=True)
class QuarantineGCReport:
    """Observable result of one Quarantine Retention GC pass.

    A dry run reports what would be removed without touching the Manifest or
    the Data Files. Only complete UTC months whose end is at or before the
    cutoff are eligible; the current UTC month is always protected.
    """

    dry_run: bool
    retention_days: int
    removed_partitions: tuple[str, ...] = ()
    removed_files: tuple[str, ...] = ()
    removed_bytes: int = 0
    skipped_current_month: tuple[str, ...] = ()
    failures: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.failures


@dataclass(frozen=True, slots=True)
class QuarantineRecord:
    """One rejection, before it is serialized into the Quarantine Dataset.

    ``details`` is a structured diagnostics mapping; it is sanitized and
    serialized as a JSON object. ``raw_record`` is the rejected input when it
    is available. Neither is ever used as a program branch or a metric label.
    """

    source_stage: str
    reason_code: str
    rule_version: str
    source_location: str
    details: Mapping[str, Any] = field(default_factory=dict)
    raw_record: Mapping[str, Any] | None = None
    recorded_ts: datetime | None = None
    feed: str | None = None
    instrument_id: str | None = None
    interval: str | None = None
    open_ts: datetime | None = None


def live_batch_source_location(ingest_ts: datetime, row_index: int) -> str:
    """Stable, readable location of one row in a live Collector batch."""
    return f"live:batch:{ns_to_iso(datetime_to_ns(to_utc(ingest_ts)))}:row={row_index}"


def _sanitize(value: Any, *, max_string: int, depth: int = 0) -> Any:
    """Make a value JSON-safe and truncate unbounded diagnostic strings."""
    if depth >= MAX_SANITIZER_DEPTH:
        return "<truncated>"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:max_string]
    if isinstance(value, Mapping):
        return {
            str(key)[:128]: _sanitize(
                item, max_string=max_string, depth=depth + 1
            )
            for key, item in list(value.items())[:MAX_CONTAINER_ITEMS]
        }
    if isinstance(value, (list, tuple)):
        return [
            _sanitize(item, max_string=max_string, depth=depth + 1)
            for item in list(value)[:MAX_CONTAINER_ITEMS]
        ]
    return str(value)[:max_string]


def _json_object(
    value: Mapping[str, Any] | None, *, max_string: int
) -> str | None:
    if value is None:
        return None
    sanitized = _sanitize(value, max_string=max_string)
    return json.dumps(sanitized, ensure_ascii=False, sort_keys=True)


def _file_name(start_ns: int, end_ns: int) -> str:
    return f"quarantine-{start_ns}-{end_ns}-{uuid4()}.parquet"


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


class QuarantineStore:
    """Append-only writer/reader for the Quarantine Dataset."""

    def __init__(  # ruff: ignore[too-many-arguments] - explicit Store settings
        self,
        root: Path,
        *,
        schema_version: int = QUARANTINE_SCHEMA_VERSION,
        compression: str = "zstd",
        row_group_size: int = 1_000_000,
        format_version: str = "2.6",
        retention_days: int = QUARANTINE_DEFAULT_RETENTION_DAYS,
    ) -> None:
        self.root = Path(root)
        self.schema_version = int(schema_version)
        self.compression = compression
        self.row_group_size = row_group_size
        self.format_version = format_version
        if int(retention_days) < 0:
            msg = "quarantine retention_days must be zero (keep forever) or positive"
            raise ValueError(msg)
        self.retention_days = int(retention_days)
        self.descriptor_path = self.root / QUARANTINE_DESCRIPTOR_NAME
        self.manifest_path = self.root / QUARANTINE_MANIFEST_NAME
        # `_lock` guards counters; `_io_lock` serializes Manifest updates so a
        # GC pass cannot race an append from the collector thread.
        self._lock = threading.Lock()
        self._io_lock = threading.Lock()
        self._records_written = 0
        self._write_failures = 0
        self._last_write_ts: datetime | None = None
        self._by_stage_reason: dict[tuple[str, str], int] = {}
        self._gc_runs = 0
        self._last_gc: dict[str, Any] | None = None

    # -- writes ------------------------------------------------------------

    def write_records(
        self, records: Sequence[QuarantineRecord]
    ) -> list[dict[str, Any]]:
        """Durably append records and return the Manifest entries written.

        The vocabulary is validated before any I/O. A write failure removes
        the new Data Files, increments ``write_failures`` and raises
        ``QuarantineWriteError`` so the caller never treats the input as
        acknowledged.
        """
        items = list(records)
        if not items:
            return []
        self._validate(items)

        write_ts = utc_now()
        created_paths: list[Path] = []
        entries: list[dict[str, Any]] = []
        try:
            table = self._build_table(items, write_ts)
            groups = self._group_indices(items, write_ts)
            with self._io_lock:
                created_paths, entries = self._write_data_files(table, groups)
                self._append_manifest(entries)
        except Exception as exc:
            for path in created_paths:
                with suppress(OSError):
                    path.unlink(missing_ok=True)
            with self._lock:
                self._write_failures += 1
            logger.exception(
                "quarantine write failed; rejected input is not acknowledged",
                extra={"event": "quarantine.write_failed"},
            )
            msg = (
                "Quarantine write failed; the corresponding input must not be "
                "acknowledged"
            )
            raise QuarantineWriteError(msg) from exc

        with self._lock:
            self._records_written += len(items)
            self._last_write_ts = write_ts
            for record in items:
                key = (record.source_stage, record.reason_code)
                self._by_stage_reason[key] = self._by_stage_reason.get(key, 0) + 1
        return entries

    @staticmethod
    def _group_indices(
        items: Sequence[QuarantineRecord], write_ts: datetime
    ) -> dict[tuple[int, int], list[int]]:
        groups: dict[tuple[int, int], list[int]] = {}
        for index, record in enumerate(items):
            recorded = to_utc(
                write_ts if record.recorded_ts is None else record.recorded_ts
            )
            groups.setdefault((recorded.year, recorded.month), []).append(index)
        return groups

    def _write_data_files(
        self,
        table: pa.Table,
        groups: Mapping[tuple[int, int], list[int]],
    ) -> tuple[list[Path], list[dict[str, Any]]]:
        self._ensure_descriptor()
        created_paths: list[Path] = []
        entries: list[dict[str, Any]] = []
        for (year, month), indices in sorted(groups.items()):
            group = table.take(pa.array(indices, type=pa.int64()))
            directory = self.root / f"year={year:04d}" / f"month={month:02d}"
            directory.mkdir(parents=True, exist_ok=True)
            recorded_ns = [
                datetime_to_ns(value)
                for value in group.column("recorded_ts").to_pylist()
            ]
            start_ns = min(recorded_ns)
            end_ns = max(recorded_ns)
            temporary = directory / f".{uuid4().hex}.parquet.tmp"
            path = directory / _file_name(start_ns, end_ns)
            try:
                pq.write_table(
                    group,
                    temporary,
                    version=self.format_version,
                    use_deprecated_int96_timestamps=False,
                    compression=self.compression,
                    row_group_size=self.row_group_size,
                )
                temporary.replace(path)
            except Exception:
                temporary.unlink(missing_ok=True)
                raise
            created_paths.append(path)
            entries.append(
                {
                    "path": path.relative_to(self.root).as_posix(),
                    "start_ns": start_ns,
                    "end_ns": end_ns,
                    "row_count": group.num_rows,
                    "active": True,
                    "size_bytes": path.stat().st_size,
                    "created_ns": time.time_ns(),
                    "partition": {
                        "year": f"{year:04d}",
                        "month": f"{month:02d}",
                    },
                }
            )
        return created_paths, entries

    @staticmethod
    def _validate(records: Sequence[QuarantineRecord]) -> None:
        for record in records:
            if record.source_stage not in SOURCE_STAGES:
                msg = f"unknown quarantine source_stage: {record.source_stage!r}"
                raise ValueError(msg)
            if record.reason_code not in REASON_CODES:
                msg = f"unknown quarantine reason_code: {record.reason_code!r}"
                raise ValueError(msg)
            if not record.source_location:
                msg = "quarantine source_location must not be empty"
                raise ValueError(msg)

    def _build_table(
        self, records: Sequence[QuarantineRecord], write_ts: datetime
    ) -> pa.Table:
        rows = []
        for record in records:
            recorded = to_utc(
                write_ts if record.recorded_ts is None else record.recorded_ts
            )
            open_ts = None if record.open_ts is None else to_utc(record.open_ts)
            rows.append(
                {
                    "quarantine_schema_version": self.schema_version,
                    "recorded_ts": datetime_to_ns(recorded),
                    "source_stage": record.source_stage,
                    "reason_code": record.reason_code,
                    "rule_version": record.rule_version,
                    "feed": record.feed,
                    "instrument_id": record.instrument_id,
                    "interval": record.interval,
                    "open_ts": None
                    if open_ts is None
                    else datetime_to_ns(open_ts),
                    "source_location": record.source_location,
                    "details": _json_object(
                        record.details, max_string=MAX_DETAIL_STRING
                    ),
                    "raw_record": _json_object(
                        record.raw_record, max_string=MAX_RAW_STRING
                    ),
                }
            )
        return pa.Table.from_pylist(rows, schema=QUARANTINE_SCHEMA)

    def _ensure_descriptor(self) -> None:
        descriptor = (
            json.loads(self.descriptor_path.read_text(encoding="utf-8"))
            if self.descriptor_path.exists()
            else {}
        )
        descriptor.update(
            {
                "dataset": QUARANTINE_DATASET_NAME,
                "schema_version": self.schema_version,
                "arrow_schema": schema_to_json(QUARANTINE_SCHEMA),
                "partitioning": list(QUARANTINE_PARTITIONING),
                "partitioning_flavor": "hive",
                "file_naming": QUARANTINE_FILE_NAMING,
            }
        )
        _write_json_atomic(self.descriptor_path, descriptor)

    def _append_manifest(self, entries: Sequence[dict[str, Any]]) -> None:
        manifest = self.read_manifest()
        manifest.setdefault("dataset", QUARANTINE_DATASET_NAME)
        manifest.setdefault("schema_version", self.schema_version)
        manifest.setdefault("files", []).extend(entries)
        _write_json_atomic(self.manifest_path, manifest)

    # -- reads -------------------------------------------------------------

    def read_manifest(self) -> dict[str, Any]:
        if not self.manifest_path.exists():
            return {
                "dataset": QUARANTINE_DATASET_NAME,
                "schema_version": self.schema_version,
                "files": [],
            }
        return json.loads(self.manifest_path.read_text(encoding="utf-8"))

    def active_files(self) -> list[dict[str, Any]]:
        """Normalized active entries, skipping missing Data Files."""
        files: list[dict[str, Any]] = []
        for raw in self.read_manifest().get("files", []):
            entry = dict(raw)
            if not entry.get("active", True):
                continue
            entry.setdefault("row_count", 0)
            entry.setdefault("partition", {})
            if not (self.root / str(entry["path"])).exists():
                continue
            files.append(entry)
        return files

    def read_records(self) -> pa.Table:
        """Return every active Quarantine record as one Arrow table."""
        tables = [
            pq.read_table(self.root / str(entry["path"]))
            for entry in self.active_files()
        ]
        if not tables:
            return pa.Table.from_batches([], schema=QUARANTINE_SCHEMA)
        return pa.concat_tables(tables).select(QUARANTINE_SCHEMA.names)

    # -- retention / GC ----------------------------------------------------

    def gc(
        self,
        retention_days: int | None = None,
        *,
        apply: bool = False,
        now: datetime | None = None,
    ) -> QuarantineGCReport:
        """Delete Quarantine Data Files in whole UTC months past Retention.

        The default is a dry run. Only complete UTC months whose month end is
        at or before the cutoff are eligible; the current UTC month is always
        skipped. ``retention_days=0`` means keep forever. The Quarantine
        Manifest is switched atomically before any Data File is unlinked, and
        empty month directories are pruned afterwards.
        """
        retention = (
            self.retention_days
            if retention_days is None
            else int(retention_days)
        )
        if retention < 0:
            msg = "quarantine retention_days must be zero (keep forever) or positive"
            raise ValueError(msg)
        current = to_utc(now) if now is not None else utc_now()
        dry_run = not apply
        if retention == 0:
            report = QuarantineGCReport(
                dry_run=dry_run, retention_days=retention
            )
            self._record_gc(report, current)
            return report

        cutoff_ns = datetime_to_ns(current) - retention * NANOS_PER_DAY
        with self._io_lock:
            manifest = self.read_manifest()
            entries = [dict(raw) for raw in manifest.get("files", [])]
            (
                expired_months,
                skipped_current,
                to_remove,
            ) = self._expired_entries(entries, current, cutoff_ns)
            removed_files = tuple(str(entry["path"]) for entry in to_remove)
            removed_bytes = sum(
                self._entry_size(entry) for entry in to_remove
            )
            if dry_run or not to_remove:
                report = QuarantineGCReport(
                    dry_run=dry_run,
                    retention_days=retention,
                    removed_partitions=tuple(expired_months),
                    removed_files=removed_files,
                    removed_bytes=removed_bytes,
                    skipped_current_month=tuple(skipped_current),
                )
                self._record_gc(report, current)
                return report

            removed_paths = set(removed_files)
            for entry in entries:
                if str(entry.get("path")) in removed_paths:
                    entry["active"] = False
            manifest["files"] = entries
            try:
                _write_json_atomic(self.manifest_path, manifest)
            except Exception as exc:
                logger.exception(
                    "quarantine GC manifest update failed",
                    extra={"event": "quarantine.gc_manifest_failed"},
                )
                report = QuarantineGCReport(
                    dry_run=False,
                    retention_days=retention,
                    removed_partitions=tuple(expired_months),
                    skipped_current_month=tuple(skipped_current),
                    failures=(f"manifest: {exc}",),
                )
                self._record_gc(report, current)
                return report

            report = QuarantineGCReport(
                dry_run=False,
                retention_days=retention,
                removed_partitions=tuple(expired_months),
                removed_files=removed_files,
                removed_bytes=removed_bytes,
                skipped_current_month=tuple(skipped_current),
                failures=self._unlink_removed(to_remove, expired_months),
            )
            self._record_gc(report, current)
            return report

    @staticmethod
    def _expired_entries(
        entries: Sequence[dict[str, Any]],
        current: datetime,
        cutoff_ns: int,
    ) -> tuple[list[str], list[str], list[dict[str, Any]]]:
        """Select whole months past the cutoff, protecting the current month."""
        expired_months: list[str] = []
        skipped_current: list[str] = []
        to_remove: list[dict[str, Any]] = []
        months = QuarantineStore._entries_by_month(entries)
        for (year, month), group in sorted(months.items()):
            label = f"{year:04d}-{month:02d}"
            if (year, month) == (current.year, current.month):
                skipped_current.append(label)
                continue
            # A month is only complete once its last instant is at or before
            # the cutoff, matching the Bars GC month-end rule.
            if _month_end_ns(year, month) > cutoff_ns:
                continue
            expired_months.append(label)
            to_remove.extend(group)
        return expired_months, skipped_current, to_remove

    @staticmethod
    def _entries_by_month(
        entries: Sequence[dict[str, Any]],
    ) -> dict[tuple[int, int], list[dict[str, Any]]]:
        months: dict[tuple[int, int], list[dict[str, Any]]] = {}
        for entry in entries:
            if not entry.get("active", True):
                continue
            year_month = _entry_month(entry)
            if year_month is None:
                continue
            months.setdefault(year_month, []).append(entry)
        return months

    def _entry_size(self, entry: Mapping[str, Any]) -> int:
        size = entry.get("size_bytes")
        if size is not None:
            return int(size)
        with suppress(OSError):
            return (self.root / str(entry["path"])).stat().st_size
        return 0

    def _unlink_removed(
        self,
        to_remove: Sequence[Mapping[str, Any]],
        expired_months: Sequence[str],
    ) -> tuple[str, ...]:
        """Unlink inactive Data Files, then prune their empty month dirs."""
        failures: list[str] = []
        for entry in to_remove:
            path = self.root / str(entry["path"])
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:
                # The entry stays inactive in the Manifest; report it so an
                # operator can retry.
                failures.append(f"{entry['path']}: {exc}")
        self._prune_empty_months(
            {(int(label[:4]), int(label[5:])) for label in expired_months}
        )
        return tuple(failures)

    def _prune_empty_months(self, months: set[tuple[int, int]]) -> None:
        years: set[int] = set()
        for year, month in months:
            directory = self.root / f"year={year:04d}" / f"month={month:02d}"
            try:
                directory.rmdir()
            except OSError:
                continue
            years.add(year)
        for year in years:
            with suppress(OSError):
                (self.root / f"year={year:04d}").rmdir()

    def _record_gc(
        self, report: QuarantineGCReport, now: datetime
    ) -> None:
        with self._lock:
            self._gc_runs += 1
            self._last_gc = {
                "ts": ns_to_iso(datetime_to_ns(to_utc(now))),
                "dry_run": report.dry_run,
                "retention_days": report.retention_days,
                "removed_partitions": list(report.removed_partitions),
                "removed_files": len(report.removed_files),
                "removed_bytes": report.removed_bytes,
                "skipped_current_month": list(report.skipped_current_month),
                "failures": list(report.failures),
            }

    # -- observability -----------------------------------------------------

    def stats(self) -> dict[str, Any]:
        with self._lock:
            records_written = self._records_written
            write_failures = self._write_failures
            last_write_ts = self._last_write_ts
            by_stage_reason = dict(self._by_stage_reason)
            gc_runs = self._gc_runs
            last_gc = None if self._last_gc is None else dict(self._last_gc)
        by_stage: dict[str, int] = {}
        by_reason: dict[str, int] = {}
        combined = []
        for (stage, reason), count in sorted(by_stage_reason.items()):
            by_stage[stage] = by_stage.get(stage, 0) + count
            by_reason[reason] = by_reason.get(reason, 0) + count
            combined.append(
                {
                    "source_stage": stage,
                    "reason_code": reason,
                    "count": count,
                }
            )
        try:
            active_files = self.active_files()
        except (OSError, ValueError):
            active_files = []
        return {
            "schema_version": self.schema_version,
            "root": str(self.root),
            "retention_days": self.retention_days,
            "records_written": records_written,
            "write_failures": write_failures,
            "last_write_ts": None
            if last_write_ts is None
            else ns_to_iso(datetime_to_ns(last_write_ts)),
            "active_file_count": len(active_files),
            "active_bytes": sum(
                int(entry.get("size_bytes") or 0) for entry in active_files
            ),
            "gc_runs": gc_runs,
            "last_gc": last_gc,
            "records_by_source_stage": by_stage,
            "records_by_reason_code": by_reason,
            "records_by_stage_and_reason": combined,
        }


def _entry_month(entry: Mapping[str, Any]) -> tuple[int, int] | None:
    partition = entry.get("partition")
    if isinstance(partition, Mapping):
        year = str(partition.get("year", ""))
        month = str(partition.get("month", ""))
        if year.isdigit() and month.isdigit():
            return int(year), int(month)
    year_text: str | None = None
    month_text: str | None = None
    for part in Path(str(entry.get("path", ""))).parts:
        if part.startswith("year="):
            year_text = part[len("year=") :]
        elif part.startswith("month="):
            month_text = part[len("month=") :]
    if (
        year_text is not None
        and month_text is not None
        and year_text.isdigit()
        and month_text.isdigit()
    ):
        return int(year_text), int(month_text)
    return None


def _month_end_ns(year: int, month: int) -> int:
    if month == 12:
        following = datetime(year + 1, 1, 1, tzinfo=UTC)
    else:
        following = datetime(year, month + 1, 1, tzinfo=UTC)
    return datetime_to_ns(following)


__all__ = [
    "QUARANTINE_DATASET_NAME",
    "QUARANTINE_DEFAULT_RETENTION_DAYS",
    "QUARANTINE_DESCRIPTOR_NAME",
    "QUARANTINE_FILE_NAMING",
    "QUARANTINE_MANIFEST_NAME",
    "QUARANTINE_PARTITIONING",
    "QUARANTINE_SCHEMA",
    "QUARANTINE_SCHEMA_VERSION",
    "REASON_ARCHIVE_CHECKSUM_MISMATCH",
    "REASON_ARCHIVE_FIELD_ERROR",
    "REASON_ARCHIVE_FORMAT_ERROR",
    "REASON_ARCHIVE_TIME_UNIT_UNSUPPORTED",
    "REASON_CODES",
    "REASON_DECIMAL_SCALE_EXCEEDED",
    "REASON_FLOAT_VALUE",
    "REASON_INVALID_INSTRUMENT",
    "REASON_INVALID_INTERVAL",
    "REASON_MALFORMED_ROW",
    "REASON_UNCLOSED_BAR",
    "REASON_UNKNOWN_INSTRUMENT",
    "SOURCE_STAGES",
    "SOURCE_STAGE_ARCHIVE_FILE",
    "SOURCE_STAGE_CCXT_HISTORY_ROW",
    "SOURCE_STAGE_CCXT_LIVE_ROW",
    "SOURCE_STAGE_LIVE_INSTRUMENT_VALIDATE",
    "SOURCE_STAGE_LIVE_NORMALIZE",
    "QuarantineGCReport",
    "QuarantineRecord",
    "QuarantineStore",
    "QuarantineWriteError",
    "live_batch_source_location",
]
