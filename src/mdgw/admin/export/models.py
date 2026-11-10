"""Export Job wire and persistence models.

The JSON shape is the Operator Console contract section 6.3 exactly:
``status`` is one of seven stable values, ``progress`` distinguishes known
work from an indeterminate total (never a fabricated percentage), and
``download_url`` is a same-origin relative path containing only the opaque
job id. Server file-system paths are never part of the wire.

Times are stored as integer UTC nanoseconds; the wire emits nine fractional
digits (``…Z``) for the job range and metadata.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from mdgw.core.time import ns_to_iso

if TYPE_CHECKING:
    from collections.abc import Mapping

QUEUED = "queued"
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"
CANCELLING = "cancelling"
CANCELLED = "cancelled"
EXPIRED = "expired"

EXPORT_STATUSES: tuple[str, ...] = (
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLING,
    CANCELLED,
    EXPIRED,
)

#: Statuses that count against the per-user and global concurrency quotas.
ACTIVE_STATUSES: tuple[str, ...] = (QUEUED, RUNNING, CANCELLING)

FORMAT_CSV = "csv"
EXPORT_FORMATS: tuple[str, ...] = (FORMAT_CSV,)

CSV_CONTENT_TYPE = "text/csv; charset=utf-8"
DOWNLOAD_PATH_TEMPLATE = "/api/v1/admin/export-jobs/{job_id}/download"


def download_url(job_id: str) -> str:
    """The only download URL the wire ever emits: opaque id, same origin."""
    return DOWNLOAD_PATH_TEMPLATE.format(job_id=job_id)


@dataclass(frozen=True)
class ExportJobRecord:
    """One persisted Export Job row."""

    job_id: str
    status: str
    instrument_id: str
    interval: str
    start_ns: int
    end_ns: int
    range_days: int
    format: str
    created_by_subject: str
    created_by_display_name: str
    created_at_ns: int
    processed_rows: int = 0
    processed_bytes: int = 0
    total_rows: int | None = None
    result: Mapping[str, Any] | None = None
    error: Mapping[str, Any] | None = None
    started_at_ns: int | None = None
    finished_at_ns: int | None = None
    cancelled_at_ns: int | None = None
    expires_at_ns: int | None = None
    cancel_requested: bool = False

    @property
    def active(self) -> bool:
        return self.status in ACTIVE_STATUSES

    def effective_status(self, now_ns: int) -> str:
        """Report ``expired`` once the retention window has passed.

        The stored row may still say ``succeeded`` until cleanup runs; the
        download endpoint and the wire must never offer a gone file.
        """
        if (
            self.status == SUCCEEDED
            and self.expires_at_ns is not None
            and now_ns >= self.expires_at_ns
        ):
            return EXPIRED
        return self.status

    def progress(self) -> dict[str, Any]:
        """Truthful progress: a null total is unknown, never a fake percent."""
        total = self.total_rows
        percent = (
            None
            if total is None or total <= 0
            else round(min(self.processed_rows, total) * 100.0 / total, 2)
        )
        return {
            "processed_rows": self.processed_rows,
            "processed_bytes": self.processed_bytes,
            "total_rows": total,
            "percent": percent,
            "indeterminate": total is None,
        }

    def to_wire(self, now_ns: int) -> dict[str, Any]:
        status = self.effective_status(now_ns)
        downloadable = status == SUCCEEDED and self.result is not None
        return {
            "job_id": self.job_id,
            "status": status,
            "instrument_id": self.instrument_id,
            "interval": self.interval,
            "start": ns_to_iso(self.start_ns),
            "end": ns_to_iso(self.end_ns),
            "range_days": self.range_days,
            "format": self.format,
            "created_at": ns_to_iso(self.created_at_ns),
            "created_by": {
                "subject": self.created_by_subject,
                "display_name": self.created_by_display_name,
            },
            "progress": self.progress(),
            "result": None if self.result is None else dict(self.result),
            "error": None if self.error is None else dict(self.error),
            "cancelled_at": (
                None
                if self.cancelled_at_ns is None
                else ns_to_iso(self.cancelled_at_ns)
            ),
            "expires_at": (
                None
                if self.expires_at_ns is None
                else ns_to_iso(self.expires_at_ns)
            ),
            "download_url": download_url(self.job_id) if downloadable else None,
        }


@dataclass(frozen=True)
class ExportJobRequest:
    """A validated create request (UTC ns bounds, half-open range)."""

    instrument_id: str
    interval: str
    start_ns: int
    end_ns: int
    range_days: int
    format: str = FORMAT_CSV


@dataclass(frozen=True)
class ExportJobResult:
    """The completion record stored in ``result_json``."""

    file_name: str
    size_bytes: int
    sha256: str
    row_count: int
    content_type: str = CSV_CONTENT_TYPE

    def to_json(self) -> dict[str, Any]:
        return {
            "file_name": self.file_name,
            "content_type": self.content_type,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "row_count": self.row_count,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ExportJobResult:
        return cls(
            file_name=str(value["file_name"]),
            size_bytes=int(value["size_bytes"]),
            sha256=str(value["sha256"]),
            row_count=int(value["row_count"]),
            content_type=str(
                value.get("content_type", CSV_CONTENT_TYPE)
            ),
        )


@dataclass(frozen=True)
class ExportJobError:
    """The stable failure record stored in ``error_json``."""

    code: str
    message: str
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": dict(self.details),
        }


INTERRUPTED_ERROR = ExportJobError(
    code="interrupted",
    message="the export worker was interrupted by an API restart",
)

INTERNAL_ERROR = ExportJobError(
    code="internal_error",
    message="export execution failed",
)


def encode_json(value: Mapping[str, Any] | None) -> str | None:
    if value is None:
        return None
    return json.dumps(dict(value), sort_keys=True, separators=(",", ":"))


def decode_json(value: str | None) -> dict[str, Any] | None:
    if value is None:
        return None
    parsed = json.loads(value)
    return parsed if isinstance(parsed, dict) else None


__all__ = [
    "ACTIVE_STATUSES",
    "CANCELLED",
    "CANCELLING",
    "CSV_CONTENT_TYPE",
    "EXPIRED",
    "EXPORT_FORMATS",
    "EXPORT_STATUSES",
    "FAILED",
    "FORMAT_CSV",
    "INTERNAL_ERROR",
    "INTERRUPTED_ERROR",
    "QUEUED",
    "RUNNING",
    "SUCCEEDED",
    "ExportJobError",
    "ExportJobRecord",
    "ExportJobRequest",
    "ExportJobResult",
    "decode_json",
    "download_url",
    "encode_json",
]
