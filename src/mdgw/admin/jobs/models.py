"""Backfill Job wire and persistence models (contract section 7.2).

The management database stores integer UTC nanoseconds; the wire emits the
contract's nine-fractional-digit ``…Z`` timestamps. ``counters`` keep the five
documented names and meanings:

* ``scanned_bars`` — expected 1m source identities inspected;
* ``gap_bars`` — missing 1m source identities;
* ``backfilled_bars`` — source Bars fetched and validated;
* ``written_bars`` — Bars persisted, including generated parent Bars;
* ``rejected_bars`` — rows rejected by Validation/Quarantine.

``resolved_gap_ids`` stays empty in this ticket: the persistent Gap store and
the resolution rules belong to ticket 07. The Collector runner exposes the
linkage hook (see ``mdgw.service.backfill_jobs``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from mdgw.backfill_job_plan import plan_for_interval
from mdgw.core.time import ns_to_iso

if TYPE_CHECKING:
    from collections.abc import Mapping

QUEUED = "queued"
RUNNING = "running"
SUCCEEDED = "succeeded"
PARTIALLY_SUCCEEDED = "partially_succeeded"
FAILED = "failed"
CANCELLING = "cancelling"
CANCELLED = "cancelled"

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

#: Statuses whose job can be retried as a NEW linked job.
RETRYABLE_STATUSES: tuple[str, ...] = (
    FAILED,
    PARTIALLY_SUCCEEDED,
    CANCELLED,
)

TERMINAL_STATUSES: tuple[str, ...] = (
    SUCCEEDED,
    PARTIALLY_SUCCEEDED,
    FAILED,
    CANCELLED,
)

COUNTER_KEYS: tuple[str, ...] = (
    "scanned_bars",
    "gap_bars",
    "backfilled_bars",
    "written_bars",
    "rejected_bars",
)

INTERRUPTED = "interrupted"
HISTORY_UNSUPPORTED = "history_unsupported"
COLLECTOR_REJECTED = "collector_rejected"
COLLECTOR_UNAVAILABLE = "collector_unavailable"
BACKFILL_FAILED = "backfill_failed"


@dataclass(frozen=True)
class BackfillJobError:
    """The stable ``{code, message, details}`` record stored in ``error_json``."""

    code: str
    message: str
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": dict(self.details),
        }


INTERRUPTED_ERROR = BackfillJobError(
    code=INTERRUPTED,
    message="the backfill job was interrupted by a process restart",
)


def empty_counters() -> dict[str, int]:
    return dict.fromkeys(COUNTER_KEYS, 0)


def normalize_counters(
    value: Mapping[str, Any] | None,
) -> dict[str, int]:
    """Coerce a collector/DB counter mapping onto the five documented keys."""
    counters = empty_counters()
    if value is None:
        return counters
    for key in COUNTER_KEYS:
        raw = value.get(key, 0)
        try:
            counters[key] = max(0, int(raw))
        except (TypeError, ValueError):
            counters[key] = 0
    return counters


def counters_to_json(counters: Mapping[str, Any] | None) -> str:
    return json.dumps(
        normalize_counters(counters), sort_keys=True, separators=(",", ":")
    )


def counters_from_json(value: str | None) -> dict[str, int]:
    if value is None:
        return empty_counters()
    parsed = json.loads(value)
    return (
        normalize_counters(parsed) if isinstance(parsed, dict) else empty_counters()
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


def encode_range(range_ns: Mapping[str, Any] | None) -> str | None:
    """Persist ``current_range`` as ``{"start_ns","end_ns"}``."""
    if range_ns is None:
        return None
    try:
        return encode_json(
            {
                "start_ns": int(range_ns["start_ns"]),
                "end_ns": int(range_ns["end_ns"]),
            }
        )
    except (KeyError, TypeError, ValueError):
        return None


def decode_range(value: str | None) -> dict[str, int] | None:
    parsed = decode_json(value)
    if parsed is None:
        return None
    try:
        return {
            "start_ns": int(parsed["start_ns"]),
            "end_ns": int(parsed["end_ns"]),
        }
    except (KeyError, TypeError, ValueError):
        return None


def encode_gap_ids(gap_ids: Any) -> str:
    values = [
        str(gap_id) for gap_id in (gap_ids or ()) if isinstance(gap_id, str)
    ]
    return json.dumps(values, separators=(",", ":"))


def decode_gap_ids(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    parsed = json.loads(value)
    if not isinstance(parsed, list):
        return ()
    return tuple(
        str(gap_id) for gap_id in parsed if isinstance(gap_id, str)
    )


@dataclass(frozen=True)
class BackfillJobRecord:
    """One persisted Backfill Job row."""

    job_id: str
    status: str
    instrument_id: str
    interval: str
    start_ns: int
    end_ns: int
    plan: Mapping[str, Any]
    current_range: Mapping[str, int] | None
    counters: Mapping[str, int]
    requested_by_subject: str
    requested_by_display_name: str
    requested_by_access_role: str
    reason: str
    retry_of_job_id: str | None
    created_at_ns: int
    started_at_ns: int | None
    finished_at_ns: int | None
    error: Mapping[str, Any] | None
    resolved_gap_ids: tuple[str, ...]
    dispatched_at_ns: int | None
    updated_at_ns: int = 0

    @property
    def active(self) -> bool:
        return self.status in ACTIVE_STATUSES

    @property
    def terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def to_wire(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "instrument_id": self.instrument_id,
            "interval": self.interval,
            "start": ns_to_iso(self.start_ns),
            "end": ns_to_iso(self.end_ns),
            "plan": dict(self.plan),
            "current_range": (
                None
                if self.current_range is None
                else {
                    "start": ns_to_iso(int(self.current_range["start_ns"])),
                    "end": ns_to_iso(int(self.current_range["end_ns"])),
                }
            ),
            "counters": normalize_counters(self.counters),
            "requested_by": {
                "subject": self.requested_by_subject,
                "display_name": self.requested_by_display_name,
                "access_role": self.requested_by_access_role,
            },
            "reason": self.reason,
            "retry_of_job_id": self.retry_of_job_id,
            "created_at": ns_to_iso(self.created_at_ns),
            "started_at": (
                None
                if self.started_at_ns is None
                else ns_to_iso(self.started_at_ns)
            ),
            "finished_at": (
                None
                if self.finished_at_ns is None
                else ns_to_iso(self.finished_at_ns)
            ),
            "error": None if self.error is None else dict(self.error),
            "resolved_gap_ids": list(self.resolved_gap_ids),
        }


@dataclass(frozen=True)
class BackfillJobRequest:
    """A validated create/retry request (UTC ns bounds, half-open range)."""

    instrument_id: str
    interval: str
    start_ns: int
    end_ns: int
    reason: str

    @property
    def plan(self) -> dict[str, object]:
        return plan_for_interval(self.interval)


__all__ = [
    "ACTIVE_STATUSES",
    "BACKFILL_FAILED",
    "BACKFILL_STATUSES",
    "CANCELLED",
    "CANCELLING",
    "COLLECTOR_REJECTED",
    "COLLECTOR_UNAVAILABLE",
    "COUNTER_KEYS",
    "FAILED",
    "HISTORY_UNSUPPORTED",
    "INTERRUPTED",
    "INTERRUPTED_ERROR",
    "PARTIALLY_SUCCEEDED",
    "QUEUED",
    "RETRYABLE_STATUSES",
    "RUNNING",
    "SUCCEEDED",
    "TERMINAL_STATUSES",
    "BackfillJobError",
    "BackfillJobRecord",
    "BackfillJobRequest",
    "counters_from_json",
    "counters_to_json",
    "decode_gap_ids",
    "decode_json",
    "decode_range",
    "empty_counters",
    "encode_gap_ids",
    "encode_json",
    "encode_range",
    "normalize_counters",
]
