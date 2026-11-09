"""The single Backfill Job status vocabulary.

Both the Admin API (persistence/wire/filter validation) and the Collector
executor import these names, so a status can never mean one thing on the
control plane and another in the management database.
"""

from __future__ import annotations

QUEUED = "queued"
RUNNING = "running"
SUCCEEDED = "succeeded"
PARTIALLY_SUCCEEDED = "partially_succeeded"
FAILED = "failed"
CANCELLING = "cancelling"
CANCELLED = "cancelled"
UNKNOWN = "unknown"

#: Every status the Admin wire may emit.
BACKFILL_STATUSES: tuple[str, ...] = (
    QUEUED,
    RUNNING,
    SUCCEEDED,
    PARTIALLY_SUCCEEDED,
    FAILED,
    CANCELLING,
    CANCELLED,
)

#: Statuses that block a second job for the same (instrument_id, interval).
ACTIVE_STATUSES: tuple[str, ...] = (QUEUED, RUNNING, CANCELLING)

#: Statuses whose job can be retried as a new linked job.
RETRYABLE_STATUSES: tuple[str, ...] = (
    FAILED,
    PARTIALLY_SUCCEEDED,
    CANCELLED,
)

#: Statuses after which a job never changes again.
TERMINAL_STATUSES: tuple[str, ...] = (
    SUCCEEDED,
    PARTIALLY_SUCCEEDED,
    FAILED,
    CANCELLED,
)

__all__ = [
    "ACTIVE_STATUSES",
    "BACKFILL_STATUSES",
    "CANCELLED",
    "CANCELLING",
    "FAILED",
    "PARTIALLY_SUCCEEDED",
    "QUEUED",
    "RETRYABLE_STATUSES",
    "RUNNING",
    "SUCCEEDED",
    "TERMINAL_STATUSES",
    "UNKNOWN",
]
