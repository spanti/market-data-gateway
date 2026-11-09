"""Export Job service: validation, quotas, execution and download lookup.

The service is the only place that knows the Export root and the exact CSV
byte contract. It never writes the Bars Dataset: execution opens one
``ParquetBarStore.open_read`` (Manifest Snapshot + Dataset Read Lease), renders
each batch through the shared ``mdgw.api.csv_render`` module, and publishes
the finished file with an atomic rename from a temporary file in the Export
root. Only export metadata and export files are ever cleaned up.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tempfile
import time
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from mdgw.admin.errors import (
    ConflictError,
    ExportExpiredError,
    InvalidParameterError,
    JobNotFoundError,
)
from mdgw.admin.export.models import (
    CANCELLED,
    CANCELLING,
    EXPIRED,
    FORMAT_CSV,
    INTERNAL_ERROR,
    INTERRUPTED_ERROR,
    QUEUED,
    SUCCEEDED,
    ExportJobRecord,
    ExportJobRequest,
    ExportJobResult,
)
from mdgw.admin.export.store import ExportStore
from mdgw.admin.export.worker import ExportExecutor
from mdgw.api.csv_render import batch_to_csv, header_bytes
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import INTERVALS
from mdgw.core.time import iso_to_ns, ns_to_iso

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable

    from mdgw.config import AdminConfig
    from mdgw.store.parquet_dataset import ParquetBarStore, StoreRead

logger = logging.getLogger(__name__)

NANOS_PER_SECOND = 1_000_000_000
NANOS_PER_DAY = 86_400 * NANOS_PER_SECOND
_TEMP_SUFFIX = ".csv.tmp"
_TEMP_GLOB = ".*" + _TEMP_SUFFIX
_UNSAFE_FILE_CHARS = re.compile(r"[^A-Za-z0-9._-]+")
_MAX_INSTRUMENT_PART = 80
_RESULT_NAME_RE = re.compile(r"-(?P<job_id>exp_[0-9a-f]{32})\.csv$")


class _JobCancelledError(Exception):
    """Cooperative cancellation observed between batches."""


class _JobInterruptedError(Exception):
    """The process is stopping and the job cannot be confirmed."""


class ExportService:
    """Owns the Export Job store, executor and result files."""

    def __init__(  # ruff: ignore[too-many-arguments] - explicit dependencies
        self,
        *,
        config: AdminConfig,
        storage_root: Path,
        db_path: Path,
        store: ParquetBarStore | None,
        clock: Callable[[], int] | None = None,
        read_opener: Callable[..., StoreRead] | None = None,
    ) -> None:
        self.config = config
        self.export_config = config.export
        self.storage_root = Path(storage_root)
        self.export_root = self._resolve_export_root()
        self.export_root.mkdir(parents=True, exist_ok=True)
        self._ensure_export_root_is_independent()
        self._clock = time.time_ns if clock is None else clock
        self._dataset_store = store
        self._open_reader = (
            read_opener
            if read_opener is not None
            else (None if store is None else store.open_read)
        )
        self._store = ExportStore(db_path)
        self._closed = False
        self._started = False
        self._executor = ExportExecutor(
            run_job=self._run_job,
            expire_now=self.expire_now,
            concurrency=self.export_config.global_concurrency,
        )

    # -- lifecycle ---------------------------------------------------------

    @property
    def export_store(self) -> ExportStore:
        return self._store

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def available(self) -> bool:
        return (
            not self._closed
            and self._open_reader is not None
            and self._executor.running
        )

    def start(self) -> None:
        """Recover unconfirmed work, then start workers and expiry cleanup."""
        if self._closed:
            msg = "ExportService is closed"
            raise RuntimeError(msg)
        if self._started:
            return
        self._store.recover_interrupted(self.now_ns())
        self._remove_stale_temp_files()
        self._remove_orphan_results()
        self._executor.start()
        for job_id in self._store.queued_job_ids():
            self._executor.submit(job_id)
        self.expire_now()
        self._started = True

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._executor.stop()
        self._store.close()

    def now_ns(self) -> int:
        return int(self._clock())

    def set_read_opener(
        self, opener: Callable[..., StoreRead] | None
    ) -> None:
        """Swap the Store read seam (alternate storage adapter or tests)."""
        self._open_reader = opener

    # -- request handling --------------------------------------------------

    def parse_request(self, body: Mapping[str, Any]) -> ExportJobRequest:
        """Validate the create body and the 31..366 day range policy."""
        if not isinstance(body, Mapping):
            msg = "the request body must be an object"
            raise InvalidParameterError(msg, details={"field": "body"})
        instrument = body.get("instrument_id")
        if not isinstance(instrument, str) or not instrument:
            msg = "instrument_id is required"
            raise InvalidParameterError(
                msg,
                details={
                    "field": "instrument_id",
                    "reason": "invalid_instrument",
                },
            )
        try:
            InstrumentId.parse(instrument)
        except ValueError as exc:
            msg = "instrument_id must be an InstrumentId"
            raise InvalidParameterError(
                msg,
                details={
                    "field": "instrument_id",
                    "reason": "invalid_instrument",
                },
            ) from exc

        interval = body.get("interval")
        if not isinstance(interval, str) or interval not in INTERVALS:
            msg = "interval must be a Bar Interval"
            raise InvalidParameterError(
                msg,
                details={"field": "interval", "allowed": list(INTERVALS)},
            )

        if body.get("format") != FORMAT_CSV:
            msg = "format must be 'csv'"
            raise InvalidParameterError(
                msg,
                details={"field": "format", "allowed": [FORMAT_CSV]},
            )

        start_ns = _parse_time(body.get("start"), "start")
        end_ns = _parse_time(body.get("end"), "end")
        if start_ns >= end_ns:
            msg = "start must be before end"
            raise InvalidParameterError(
                msg,
                details={"field": "end", "reason": "invalid_range"},
            )

        duration_ns = end_ns - start_ns
        min_ns = self.export_config.min_days * NANOS_PER_DAY
        max_ns = self.export_config.max_days * NANOS_PER_DAY
        if duration_ns < min_ns:
            msg = (
                "ranges shorter than the configured minimum use the direct "
                "CSV download"
            )
            raise InvalidParameterError(
                msg,
                details={
                    "field": "end",
                    "reason": "use_direct_download",
                    "min_days": self.export_config.min_days,
                },
            )
        if duration_ns > max_ns:
            msg = "the requested range exceeds the Export Job maximum"
            raise InvalidParameterError(
                msg,
                details={
                    "field": "end",
                    "reason": "range_too_large",
                    "max_days": self.export_config.max_days,
                },
            )
        range_days = max(1, (duration_ns + NANOS_PER_DAY - 1) // NANOS_PER_DAY)
        return ExportJobRequest(
            instrument_id=instrument,
            interval=interval,
            start_ns=start_ns,
            end_ns=end_ns,
            range_days=range_days,
            format=FORMAT_CSV,
        )

    @staticmethod
    def new_job_id() -> str:
        return f"exp_{uuid4().hex}"

    @staticmethod
    def request_hash(request: ExportJobRequest) -> str:
        """Canonical body hash for Idempotency-Key replay detection."""
        payload = json.dumps(
            {
                "instrument_id": request.instrument_id,
                "interval": request.interval,
                "start_ns": request.start_ns,
                "end_ns": request.end_ns,
                "format": request.format,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def lookup_idempotent(
        self, subject: str, idempotency_key: str, request_hash: str
    ) -> ExportJobRecord | None:
        """Return the original job for a replayed key, rejecting body reuse."""
        existing = self._store.get_idempotent(subject, idempotency_key)
        if existing is None:
            return None
        record, stored_hash = existing
        if stored_hash != request_hash:
            msg = "the Idempotency-Key was reused with a different body"
            raise ConflictError(
                msg,
                details={
                    "reason": "idempotency_key_reuse",
                    "idempotency_key": idempotency_key,
                },
            )
        return record

    def create(  # ruff: ignore[too-many-arguments] - explicit create context
        self,
        request: ExportJobRequest,
        *,
        job_id: str,
        subject: str,
        display_name: str,
        idempotency_key: str | None = None,
        request_hash: str | None = None,
    ) -> tuple[ExportJobRecord, bool]:
        """Persist one queued job (quota-checked) and schedule it."""
        now = self.now_ns()
        record = ExportJobRecord(
            job_id=job_id,
            status=QUEUED,
            instrument_id=request.instrument_id,
            interval=request.interval,
            start_ns=request.start_ns,
            end_ns=request.end_ns,
            range_days=request.range_days,
            format=request.format,
            created_by_subject=subject,
            created_by_display_name=display_name,
            created_at_ns=now,
        )
        stored, replayed = self._store.insert(
            record,
            max_running_per_user=self.export_config.max_running_per_user,
            global_concurrency=self.export_config.global_concurrency,
            storage_quota_bytes=self.export_config.storage_quota_bytes,
            now_ns=now,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
        )
        if not replayed:
            self._executor.submit(stored.job_id)
        return stored, replayed

    # -- reads / cancellation ---------------------------------------------

    def get(self, job_id: str) -> ExportJobRecord | None:
        return self._store.get(job_id)

    def list_page(
        self,
        *,
        owner_subject: str | None,
        after: tuple[int, str] | None,
        limit: int,
    ) -> list[ExportJobRecord]:
        return self._store.list_page(
            owner_subject=owner_subject, after=after, limit=limit
        )

    def cancel(self, job_id: str) -> bool:
        """Request cooperative cancellation; False when not cancellable."""
        return self._store.request_cancel(job_id, self.now_ns())

    def expire_now(self) -> list[str]:
        """Expire due jobs and delete only their Export result files."""
        names = self._store.expire_due(self.now_ns())
        for name in names:
            self._remove_export_file(name)
        return names

    def result_path(self, record: ExportJobRecord) -> Path:
        """Return the validated result path for a succeeded job."""
        result = record.result
        file_name = None if result is None else result.get("file_name")
        if not isinstance(file_name, str) or not file_name:
            msg = "the export has no result"
            raise ConflictError(
                msg,
                details={"reason": "job_not_succeeded", "status": record.status},
            )
        path = self._export_path(file_name)
        if not path.is_file():
            msg = "the export file is no longer available"
            raise ConflictError(
                msg,
                details={"reason": "file_unavailable", "status": record.status},
            )
        return path

    def download_check(self, job_id: str) -> tuple[ExportJobRecord, Path]:
        """Resolve a downloadable job or raise the contract error."""
        record = self._store.get(job_id)
        if record is None:
            msg = "export job not found"
            raise JobNotFoundError(msg, details={"job_id": job_id})
        now = self.now_ns()
        if record.effective_status(now) == EXPIRED:
            self.expire_now()
            msg = "the export file has expired"
            raise ExportExpiredError(
                msg,
                details={
                    "job_id": job_id,
                    "expires_at": (
                        None
                        if record.expires_at_ns is None
                        else ns_to_iso(record.expires_at_ns)
                    ),
                },
            )
        if record.status != SUCCEEDED:
            msg = "the export job has not succeeded"
            raise ConflictError(
                msg,
                details={
                    "reason": "job_not_succeeded",
                    "status": record.status,
                },
            )
        return record, self.result_path(record)

    # -- execution ---------------------------------------------------------

    def _run_job(  # ruff: ignore[too-many-branches] - explicit state machine
        self, job_id: str, stop_event: threading.Event
    ) -> None:
        record = self._store.get(job_id)
        if record is None:
            return
        if record.status in {CANCELLING, CANCELLED}:
            self._store.mark_cancelled(job_id, self.now_ns())
            return
        if record.status != QUEUED:
            return
        if not self._store.mark_running(job_id, self.now_ns()):
            latest = self._store.get(job_id)
            if latest is not None and latest.cancel_requested:
                self._store.mark_cancelled(job_id, self.now_ns())
            return

        temp_path: Path | None = None
        published: Path | None = None
        try:  # ruff: ignore[too-many-statements-in-try-clause] - one job lifecycle
            _raise_if_stopped(stop_event)
            _raise_if_cancelled(self._store, job_id)
            temp_path, processed_rows, processed_bytes = self._render_export(
                job_id, record, stop_event
            )
            succeeded, final_path = self._publish_result(
                job_id,
                record,
                temp_path,
                processed_rows=processed_rows,
                processed_bytes=processed_bytes,
            )
            temp_path = None
            published = final_path
            if succeeded:
                return
            # A cancellation won the race after the file was published.
            self._remove_path(published)
            published = None
            self._store.mark_cancelled(job_id, self.now_ns())
        except _JobCancelledError:
            self._discard(temp_path, published)
            self._store.mark_cancelled(job_id, self.now_ns())
        except _JobInterruptedError:
            self._discard(temp_path, published)
            if self._store.cancel_requested(job_id):
                self._store.mark_cancelled(job_id, self.now_ns())
            else:
                self._store.mark_failed(job_id, INTERRUPTED_ERROR, self.now_ns())
        except Exception:
            logger.exception(
                "export job failed",
                extra={"event": "admin.export.job_failed", "job_id": job_id},
            )
            self._discard(temp_path, published)
            # If the failure happened after publish, the unique job id keys
            # exactly one result file; never leave an untracked orphan.
            self._remove_export_file(_file_name(record))
            if self._store.cancel_requested(job_id):
                self._store.mark_cancelled(job_id, self.now_ns())
            else:
                self._store.mark_failed(job_id, INTERNAL_ERROR, self.now_ns())

    def _render_export(
        self,
        job_id: str,
        record: ExportJobRecord,
        stop_event: threading.Event,
    ) -> tuple[Path, int, int]:
        """Stream the CSV into a temporary file outside the Dataset."""
        temp_path = self._new_temp_path(job_id)
        processed_rows = 0
        processed_bytes = 0
        read: StoreRead | None = None
        try:  # ruff: ignore[too-many-statements-in-try-clause] - one CSV scan
            with temp_path.open("wb") as handle:
                header = header_bytes()
                handle.write(header)
                processed_bytes = len(header)
                read = self._open_read(job_id, record)
                for batch in read.iter_arrow_batches():
                    _raise_if_stopped(stop_event)
                    _raise_if_cancelled(self._store, job_id)
                    text = batch_to_csv(batch)
                    if not text:
                        continue
                    data = text.encode("utf-8")
                    handle.write(data)
                    processed_bytes += len(data)
                    processed_rows += batch.num_rows
                    self._store.update_progress(
                        job_id,
                        processed_rows=processed_rows,
                        processed_bytes=processed_bytes,
                    )
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            self._remove_path(temp_path)
            raise
        finally:
            if read is not None:
                read.close()
        return temp_path, processed_rows, processed_bytes

    def _publish_result(
        self,
        job_id: str,
        record: ExportJobRecord,
        temp_path: Path,
        *,
        processed_rows: int,
        processed_bytes: int,
    ) -> tuple[bool, Path]:
        """Atomically publish the file and record the exact completion data."""
        result = ExportJobResult(
            file_name=_file_name(record),
            size_bytes=temp_path.stat().st_size,
            sha256=_sha256_file(temp_path),
            row_count=processed_rows,
        )
        final_path = self._export_path(result.file_name)
        temp_path.replace(final_path)
        finished_ns = self.now_ns()
        expires_at_ns = (
            finished_ns + self.export_config.retention_s * NANOS_PER_SECOND
        )
        succeeded = self._store.mark_succeeded(
            job_id,
            result=result,
            expires_at_ns=expires_at_ns,
            finished_ns=finished_ns,
            processed_rows=processed_rows,
            processed_bytes=processed_bytes,
        )
        return succeeded, final_path

    def _open_read(self, job_id: str, record: ExportJobRecord) -> StoreRead:
        opener = self._open_reader
        if opener is None:
            msg = "the Bars Store is not attached to the export executor"
            raise RuntimeError(msg)
        return opener(
            instrument_id=record.instrument_id,
            interval=record.interval,
            start_ns=record.start_ns,
            end_ns=record.end_ns,
            lease_id=job_id,
        )

    # -- export root / files ----------------------------------------------

    def _resolve_export_root(self) -> Path:
        configured = self.export_config.root
        if configured is None:
            return Path(self.storage_root) / "exports"
        path = Path(configured)
        if not path.is_absolute():
            path = Path(self.storage_root) / path
        return path

    def _ensure_export_root_is_independent(self) -> None:
        export_root = self.export_root.resolve()
        bars_root = (self.storage_root / "bars").resolve()
        if export_root == bars_root or bars_root in export_root.parents:
            msg = (
                "admin.export.root must not be inside the Bars Dataset "
                f"({bars_root})"
            )
            raise ValueError(msg)

    def _export_path(self, file_name: str) -> Path:
        if (
            not file_name
            or Path(file_name).name != file_name
            or file_name in {".", ".."}
            or "\x00" in file_name
        ):
            msg = "invalid export file name"
            raise ValueError(msg)
        return self.export_root / file_name

    def _new_temp_path(self, job_id: str) -> Path:
        handle, name = tempfile.mkstemp(
            prefix=f".{job_id}-",
            suffix=_TEMP_SUFFIX,
            dir=self.export_root,
        )
        os.close(handle)
        return Path(name)

    def _remove_export_file(self, file_name: str) -> None:
        try:
            path = self._export_path(file_name)
        except ValueError:  # pragma: no cover - corrupt metadata
            logger.warning(
                "refusing to delete an unsafe export file name",
                extra={"event": "admin.export.unsafe_file_name"},
            )
            return
        self._remove_path(path)

    def _remove_stale_temp_files(self) -> None:
        for path in self.export_root.glob(_TEMP_GLOB):
            self._remove_path(path)

    def _remove_orphan_results(self) -> None:
        """Delete published files whose job never recorded success.

        A crash between the atomic rename and the metadata update leaves a
        file with no downloadable job. The unique job id embedded in the
        generated name identifies it; unrelated files are never touched.
        """
        for path in self.export_root.glob("*.csv"):
            match = _RESULT_NAME_RE.search(path.name)
            if match is None:
                continue
            record = self._store.get(match.group("job_id"))
            if record is None or record.status != SUCCEEDED:
                self._remove_path(path)

    def _discard(self, temp_path: Path | None, published: Path | None) -> None:
        if temp_path is not None:
            self._remove_path(temp_path)
        if published is not None:
            self._remove_path(published)

    @staticmethod
    def _remove_path(path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except OSError:  # pragma: no cover - best-effort cleanup
            logger.warning(
                "failed to remove an export file",
                extra={"event": "admin.export.file_remove_failed"},
            )


def _raise_if_stopped(stop_event: threading.Event) -> None:
    if stop_event.is_set():
        raise _JobInterruptedError


def _raise_if_cancelled(store: ExportStore, job_id: str) -> None:
    if store.cancel_requested(job_id):
        raise _JobCancelledError


def _parse_time(value: object, field: str) -> int:
    if not isinstance(value, str):
        msg = f"{field} is required"
        raise InvalidParameterError(
            msg, details={"field": field, "reason": "invalid_timestamp"}
        )
    try:
        return iso_to_ns(value)
    except ValueError as exc:
        msg = f"{field} must be a UTC ISO-8601 timestamp"
        raise InvalidParameterError(
            msg, details={"field": field, "reason": "invalid_timestamp"}
        ) from exc


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _file_name(record: ExportJobRecord) -> str:
    instrument = _UNSAFE_FILE_CHARS.sub("_", record.instrument_id)[
        :_MAX_INSTRUMENT_PART
    ]
    return f"{instrument}-{record.interval}-{record.job_id}.csv"


__all__ = ["ExportService"]
