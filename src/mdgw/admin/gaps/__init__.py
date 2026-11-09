"""Persistent Unrecoverable Backfill Gaps (Admin ticket 07)."""

from mdgw.admin.gaps.models import (
    GAP_BACKFILL_LIMIT,
    GAP_HISTORY_MISSING,
    GAP_HISTORY_UNSUPPORTED,
    GAP_OPEN,
    GAP_REASONS,
    GAP_RESOLVED,
    GAP_STATUSES,
    SOURCE_BACKFILL_JOB,
    SOURCE_COLLECTOR_PROCESS,
    SOURCE_QUERY,
    SOURCES,
    GapOccurrence,
    GapRecord,
)

__all__ = [
    "GAP_BACKFILL_LIMIT",
    "GAP_HISTORY_MISSING",
    "GAP_HISTORY_UNSUPPORTED",
    "GAP_OPEN",
    "GAP_REASONS",
    "GAP_RESOLVED",
    "GAP_STATUSES",
    "SOURCES",
    "SOURCE_BACKFILL_JOB",
    "SOURCE_COLLECTOR_PROCESS",
    "SOURCE_QUERY",
    "GapOccurrence",
    "GapRecord",
]
