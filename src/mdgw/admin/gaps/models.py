"""Persistent Gap wire/persistence models (contract section 7.4).

Gaps are immutable history aside from the one natural-key row's discovery
bookkeeping and resolution state. The wire exposes first/last discovery and an
occurrence count; the append-only per-observation rows stay internal (A11).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mdgw.core.time import ns_to_iso

if TYPE_CHECKING:
    from collections.abc import Iterable

GAP_OPEN = "open"
GAP_RESOLVED = "resolved"
GAP_STATUSES: tuple[str, ...] = (GAP_OPEN, GAP_RESOLVED)

SOURCE_COLLECTOR_PROCESS = "collector_process"
SOURCE_BACKFILL_JOB = "backfill_job"
SOURCE_QUERY = "query"
SOURCES: tuple[str, ...] = (
    SOURCE_COLLECTOR_PROCESS,
    SOURCE_BACKFILL_JOB,
    SOURCE_QUERY,
)

PHASE_DISCOVERED = "discovered"
PHASE_RESOLVED = "resolved"
PHASES: tuple[str, ...] = (PHASE_DISCOVERED, PHASE_RESOLVED)

#: Stable low-cardinality reasons. The first two are the Collector's existing
#: values; ``history_missing`` is additive for 1m identities a Backfill Job
#: observed as missing even though its Feed Handler supports history.
GAP_HISTORY_UNSUPPORTED = "history_unsupported"
GAP_BACKFILL_LIMIT = "backfill_limit_exceeded"
GAP_HISTORY_MISSING = "history_missing"
GAP_REASONS: tuple[str, ...] = (
    GAP_HISTORY_UNSUPPORTED,
    GAP_BACKFILL_LIMIT,
    GAP_HISTORY_MISSING,
)


@dataclass(frozen=True)
class GapRecord:
    """One persisted Gap row."""

    gap_id: str
    status: str
    feed: str
    instrument_id: str
    interval: str
    start_ns: int | None
    end_ns: int | None
    reason: str
    first_discovered_at_ns: int
    last_discovered_at_ns: int
    occurrence_count: int
    resolved_at_ns: int | None
    resolved_by_job_id: str | None
    related_job_ids: tuple[str, ...]
    created_at_ns: int
    updated_at_ns: int

    @property
    def open(self) -> bool:
        return self.status == GAP_OPEN

    def to_wire(self) -> dict[str, Any]:
        """Emit exactly the contract fields with UTC RFC3339 ``Z`` values."""
        return {
            "gap_id": self.gap_id,
            "status": self.status,
            "feed": self.feed,
            "instrument_id": self.instrument_id,
            "interval": self.interval,
            "start": (
                None if self.start_ns is None else ns_to_iso(self.start_ns)
            ),
            "end": None if self.end_ns is None else ns_to_iso(self.end_ns),
            "reason": self.reason,
            "first_discovered_at": ns_to_iso(self.first_discovered_at_ns),
            "last_discovered_at": ns_to_iso(self.last_discovered_at_ns),
            "occurrence_count": self.occurrence_count,
            "resolved_at": (
                None
                if self.resolved_at_ns is None
                else ns_to_iso(self.resolved_at_ns)
            ),
            "resolved_by_job_id": self.resolved_by_job_id,
            "related_job_ids": list(self.related_job_ids),
            "created_at": ns_to_iso(self.created_at_ns),
            "updated_at": ns_to_iso(self.updated_at_ns),
        }


@dataclass(frozen=True)
class GapOccurrence:
    """One append-only observation of a Gap (internal history)."""

    occurrence_id: int
    gap_id: str
    observed_at_ns: int
    source: str
    phase: str
    job_id: str | None
    start_ns: int | None
    end_ns: int | None


def encode_job_ids(job_ids: Iterable[str]) -> str:
    values = _unique_strings(job_ids)
    return json.dumps(values, separators=(",", ":"))


def decode_job_ids(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return ()
    if not isinstance(parsed, list):
        return ()
    return tuple(str(entry) for entry in parsed if isinstance(entry, str))


def merge_job_id(existing: Iterable[str], job_id: str | None) -> tuple[str, ...]:
    """Append one job id while preserving order and never duplicating it."""
    values = list(_unique_strings(existing))
    if job_id is not None and job_id not in values:
        values.append(job_id)
    return tuple(values)


def _unique_strings(values: Iterable[str]) -> list[str]:
    unique: list[str] = []
    for value in values:
        text = str(value)
        if text and text not in unique:
            unique.append(text)
    return unique


def validate_reason(reason: str) -> str:
    """Reject empty reasons; unknown stable codes stay accepted additively."""
    if not isinstance(reason, str) or not reason.strip():
        msg = "gap reason must be a non-empty stable string"
        raise ValueError(msg)
    return reason.strip()


def is_known_reason(reason: str) -> bool:
    return reason in GAP_REASONS


__all__ = [
    "GAP_BACKFILL_LIMIT",
    "GAP_HISTORY_MISSING",
    "GAP_HISTORY_UNSUPPORTED",
    "GAP_OPEN",
    "GAP_REASONS",
    "GAP_RESOLVED",
    "GAP_STATUSES",
    "PHASES",
    "PHASE_DISCOVERED",
    "PHASE_RESOLVED",
    "SOURCES",
    "SOURCE_BACKFILL_JOB",
    "SOURCE_COLLECTOR_PROCESS",
    "SOURCE_QUERY",
    "GapOccurrence",
    "GapRecord",
    "decode_job_ids",
    "encode_job_ids",
    "is_known_reason",
    "merge_job_id",
    "validate_reason",
]
