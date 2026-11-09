"""Gap validation, Collector-snapshot ingest and job reconciliation (A11).

Only this service writes persistent Gaps. It consumes two evidence streams:

* the Collector's process-local ``gap_unrecoverable`` snapshot (source
  ``collector_process``); an unavailable Collector or an empty snapshot never
  fabricates or deletes a row;
* a finished Backfill Job's observed missing 1m ranges and outcome (source
  ``backfill_job``), polled through the ticket 06 status document.

Resolution is deliberately narrow: only a ``succeeded`` job resolves open gaps
fully contained in its requested range. ``partially_succeeded``, ``failed``
and ``cancelled`` jobs may only add themselves to ``related_job_ids``.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from mdgw.admin.errors import InvalidParameterError
from mdgw.admin.gaps.models import (
    GAP_HISTORY_MISSING,
    SOURCE_BACKFILL_JOB,
    SOURCE_COLLECTOR_PROCESS,
)
from mdgw.core.schema import INTERVALS
from mdgw.core.time import iso_to_ns
from mdgw.service.backfill_jobs import SUCCEEDED, TERMINAL_STATES

if TYPE_CHECKING:
    from collections.abc import Callable

    from mdgw.admin.gaps.models import GapRecord
    from mdgw.admin.gaps.store import GapStore
    from mdgw.admin.jobs.models import BackfillJobRecord

logger = logging.getLogger(__name__)

BASE_INTERVAL = "1m"
INVALID_CURSOR_REASON = "invalid_cursor"


class BackfillGapService:
    """Validation and reconciliation over one ``GapStore``."""

    def __init__(
        self,
        *,
        store: GapStore,
        clock: Callable[[], int],
    ) -> None:
        self.store = store
        self._clock = clock

    def now_ns(self) -> int:
        return int(self._clock())

    # -- Collector snapshot ------------------------------------------------

    def apply_collector_snapshot(self, result: Any) -> int:
        """Upsert every valid in-memory Gap event; return the accepted count.

        A malformed entry is skipped: the snapshot is observational evidence,
        never an instruction to remove history. A Collector outage is handled
        by the caller and leaves every row untouched.
        """
        if not isinstance(result, Mapping):
            return 0
        entries = result.get("gaps")
        if not isinstance(entries, list):
            return 0
        accepted = 0
        for entry in entries:
            event = _parse_gap_event(entry)
            if event is None:
                continue
            self.store.upsert_discovery(
                source=SOURCE_COLLECTOR_PROCESS,
                feed=event["feed"],
                instrument_id=event["instrument_id"],
                interval=event["interval"],
                start_ns=event["from_ns"],
                end_ns=event["to_ns"],
                reason=event["reason"],
                observed_at_ns=event["recorded_at_ns"],
            )
            accepted += 1
        return accepted

    # -- finished Backfill Job --------------------------------------------

    def reconcile_finished_job(
        self,
        job: BackfillJobRecord,
        result: Mapping[str, Any],
    ) -> tuple[str, ...]:
        """Record one terminal job's evidence; return the Gap ids it resolved.

        A clean ``succeeded`` job resolves pre-existing open gaps contained in
        its requested range. A success document that still reports observed
        holes is contradictory and resolves nothing; those holes are recorded
        as new evidence instead.
        """
        state = result.get("state")
        if not isinstance(state, str) or state not in TERMINAL_STATES:
            state = job.status
        if state not in TERMINAL_STATES:
            return ()

        feed = _feed_name(result.get("feed"), job.instrument_id)
        now = self.now_ns()
        observed = _parse_gap_ranges(result.get("gap_ranges"))
        resolved: list[str] = []

        # A success document that still reports a hole contradicts itself.
        # Resolve nothing in that case; the observation below records the hole.
        if state == SUCCEEDED and not observed:
            intervals = [BASE_INTERVAL]
            if job.interval != BASE_INTERVAL:
                intervals.append(job.interval)
            for interval in intervals:
                for gap_id in self.store.resolve_contained(
                    feed=feed,
                    instrument_id=job.instrument_id,
                    interval=interval,
                    start_ns=job.start_ns,
                    end_ns=job.end_ns,
                    job_id=job.job_id,
                    observed_at_ns=now,
                ):
                    if gap_id not in resolved:
                        resolved.append(gap_id)

        for start_ns, end_ns in observed:
            self.store.upsert_discovery(
                source=SOURCE_BACKFILL_JOB,
                feed=feed,
                instrument_id=job.instrument_id,
                interval=BASE_INTERVAL,
                start_ns=start_ns,
                end_ns=end_ns,
                reason=GAP_HISTORY_MISSING,
                observed_at_ns=now,
                job_id=job.job_id,
            )

        if state != SUCCEEDED:
            intervals = [BASE_INTERVAL]
            if job.interval != BASE_INTERVAL:
                intervals.append(job.interval)
            for interval in intervals:
                self.store.add_related_contained(
                    feed=feed,
                    instrument_id=job.instrument_id,
                    interval=interval,
                    start_ns=job.start_ns,
                    end_ns=job.end_ns,
                    job_id=job.job_id,
                    now_ns=now,
                )
        return tuple(resolved)

    # -- reads -------------------------------------------------------------

    def list_page(  # ruff: ignore[too-many-arguments] - explicit filters
        self,
        *,
        status: str | None,
        feed: str | None,
        instrument_id: str | None,
        interval: str | None,
        reason: str | None,
        discovered_from_ns: int | None,
        discovered_to_ns: int | None,
        resolved_from_ns: int | None,
        resolved_to_ns: int | None,
        after: tuple[int, str] | None,
        limit: int,
    ) -> list[GapRecord]:
        return self.store.list_gaps(
            status=status,
            feed=feed,
            instrument_id=instrument_id,
            interval=interval,
            reason=reason,
            discovered_from_ns=discovered_from_ns,
            discovered_to_ns=discovered_to_ns,
            resolved_from_ns=resolved_from_ns,
            resolved_to_ns=resolved_to_ns,
            after=after,
            limit=limit,
        )

    def get(self, gap_id: str) -> GapRecord | None:
        return self.store.get(gap_id)


# -- parsing ---------------------------------------------------------------


def _parse_gap_event(entry: Any) -> dict[str, Any] | None:
    if not isinstance(entry, Mapping):
        return None
    feed = entry.get("feed")
    instrument_id = entry.get("instrument_id")
    interval = entry.get("interval")
    reason = entry.get("reason")
    recorded_at_ns = entry.get("recorded_at_ns")
    from_ns = entry.get("from_ns")
    to_ns = entry.get("to_ns")
    if not (
        _non_empty_string(feed)
        and _non_empty_string(instrument_id)
        and isinstance(interval, str)
        and interval in INTERVALS
        and _non_empty_string(reason)
    ):
        return None
    if not _valid_ns(recorded_at_ns):
        return None
    if not _valid_optional_ns(from_ns) or not _valid_optional_ns(to_ns):
        return None
    if from_ns is not None and to_ns is not None and int(from_ns) >= int(to_ns):
        return None
    return {
        "feed": feed.strip(),
        "instrument_id": instrument_id.strip(),
        "interval": interval,
        "from_ns": None if from_ns is None else int(from_ns),
        "to_ns": None if to_ns is None else int(to_ns),
        "reason": reason.strip(),
        "recorded_at_ns": int(recorded_at_ns),
    }


def _parse_gap_ranges(value: Any) -> list[tuple[int, int]]:
    if not isinstance(value, (list, tuple)):
        return []
    ranges: list[tuple[int, int]] = []
    for entry in value:
        start_ns: Any = None
        end_ns: Any = None
        if isinstance(entry, Mapping):
            start_ns = entry.get("start_ns")
            end_ns = entry.get("end_ns")
        elif isinstance(entry, (list, tuple)) and len(entry) == 2:
            start_ns, end_ns = entry
        if not _valid_ns(start_ns) or not _valid_ns(end_ns):
            continue
        start, end = int(start_ns), int(end_ns)
        if start >= end:
            continue
        ranges.append((start, end))
    return ranges


def _non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _valid_ns(value: Any) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, int)
        and value >= 0
    )


def _valid_optional_ns(value: Any) -> bool:
    return value is None or _valid_ns(value)


def _feed_name(value: Any, instrument_id: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    # Defensive fallback for a Collector that predates the feed field: the
    # instrument's venue is the only stable source available.
    return instrument_id.split(":", 1)[0]


def parse_time_filter(value: str | None, field: str) -> int | None:
    """Parse one UTC query timestamp; invalid values are stable 400s."""
    if value is None or not value.strip():
        return None
    try:
        return iso_to_ns(value)
    except ValueError:
        msg = f"{field} must be a UTC ISO-8601 timestamp"
        raise InvalidParameterError(
            msg,
            details={"field": field, "reason": "invalid_timestamp"},
        ) from None


__all__ = [
    "BASE_INTERVAL",
    "INVALID_CURSOR_REASON",
    "BackfillGapService",
    "parse_time_filter",
]
