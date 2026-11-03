"""Collector health: stale detection and unrecoverable-gap events.

Staleness is tracked per `(feed, instrument_id, interval)` using the Bar's
`event_ts` (its `close_ts`). The threshold is `max(2 * interval, stale_after_s)`
so a coarse interval is not declared stale just because it only closes hourly.

`gap_unrecoverable` events are an in-memory, append-only audit list for v1.
Ticket 12 can export the same snapshot through metrics without changing this
module's contract.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING

from mdgw.core.schema import INTERVALS
from mdgw.core.time import datetime_to_ns, ns_to_iso, to_utc, utc_now

if TYPE_CHECKING:
    from datetime import datetime

    from mdgw.core.bar import Bar

GAP_HISTORY_UNSUPPORTED = "history_unsupported"
GAP_BACKFILL_LIMIT = "backfill_limit_exceeded"


@dataclass(frozen=True, slots=True)
class GapEvent:
    """One known hole that Backfill could not close."""

    instrument_id: str
    interval: str
    from_ts: datetime | None
    to_ts: datetime | None
    reason: str
    recorded_at: datetime


def stale_threshold_s(interval: str, stale_after_s: float) -> float:
    """`max(2 * Bar Interval, configured minimum)`, in seconds."""
    interval_s = INTERVALS[interval].total_seconds()
    return max(2 * interval_s, float(stale_after_s))


class HealthTracker:
    """Thread-safe last-event and gap bookkeeping for one collector process."""

    def __init__(self, *, stale_after_s: float = 120.0) -> None:
        self.stale_after_s = float(stale_after_s)
        self._last_event: dict[tuple[str, str, str], datetime | None] = {}
        self._gaps: list[GapEvent] = []
        self._lock = threading.Lock()

    def register(
        self,
        feed: str,
        instrument_id: str,
        interval: str,
        last_event_ts: datetime | None,
    ) -> None:
        """Register an expected key with its starting freshness baseline.

        `last_event_ts=None` means no Bar has ever arrived for this key. It
        initialises an absent key only; it never clears a known event. A
        non-None value updates only when it is later than the existing one, so
        registering the same key again can never move the baseline backwards.
        """
        key = (feed, instrument_id, interval)
        normalized = None if last_event_ts is None else to_utc(last_event_ts)
        with self._lock:
            if key not in self._last_event:
                self._last_event[key] = normalized
                return
            previous = self._last_event[key]
            if (
                normalized is not None
                and (previous is None or normalized > previous)
            ):
                self._last_event[key] = normalized

    def record_bar(self, bar: Bar) -> None:
        """Track a persisted Bar's `event_ts` (its `close_ts`)."""
        self.record_event(bar.feed, bar.instrument_id, bar.interval, bar.event_ts)

    def record_event(
        self,
        feed: str,
        instrument_id: str,
        interval: str,
        event_ts: datetime,
    ) -> None:
        event_ts = to_utc(event_ts)
        key = (feed, instrument_id, interval)
        with self._lock:
            previous = self._last_event.get(key)
            if previous is None or event_ts > previous:
                self._last_event[key] = event_ts

    def record_gap(  # ruff: ignore[too-many-arguments] - keyword-only gap fields
        self,
        *,
        instrument_id: str,
        interval: str,
        from_ts: datetime | None,
        to_ts: datetime | None,
        reason: str,
        recorded_at: datetime | None = None,
    ) -> GapEvent:
        """Append one `gap_unrecoverable` event."""
        event = GapEvent(
            instrument_id=instrument_id,
            interval=interval,
            from_ts=None if from_ts is None else to_utc(from_ts),
            to_ts=None if to_ts is None else to_utc(to_ts),
            reason=reason,
            recorded_at=to_utc(recorded_at) if recorded_at is not None else utc_now(),
        )
        with self._lock:
            self._gaps.append(event)
        return event

    def gap_events(self) -> tuple[GapEvent, ...]:
        with self._lock:
            return tuple(self._gaps)

    def stale(self, now: datetime | None = None) -> list[dict]:
        """Return only the stale `(feed, instrument, interval)` entries."""
        return [entry for entry in self.tracked(now) if entry["stale"]]

    def tracked(self, now: datetime | None = None) -> list[dict]:
        """Return every tracked key with its freshness assessment."""
        now = to_utc(now) if now is not None else utc_now()
        with self._lock:
            rows = list(self._last_event.items())

        entries: list[dict] = []
        for (feed, instrument_id, interval), last_event in sorted(rows):
            threshold_s = stale_threshold_s(interval, self.stale_after_s)
            if last_event is None:
                # Registered but never received: stale with no measurable age.
                entries.append(
                    {
                        "feed": feed,
                        "instrument_id": instrument_id,
                        "interval": interval,
                        "last_event_ts": None,
                        "age_s": None,
                        "threshold_s": threshold_s,
                        "stale": True,
                    }
                )
                continue
            age_s = (now - last_event).total_seconds()
            entries.append(
                {
                    "feed": feed,
                    "instrument_id": instrument_id,
                    "interval": interval,
                    "last_event_ts": ns_to_iso(datetime_to_ns(last_event)),
                    "age_s": age_s,
                    "threshold_s": threshold_s,
                    "stale": age_s > threshold_s,
                }
            )
        return entries

    def snapshot(self, now: datetime | None = None) -> dict:
        """Assertable health view; suitable for `/metrics` wiring in ticket 12."""
        tracked = self.tracked(now)
        stale = [entry for entry in tracked if entry["stale"]]
        with self._lock:
            gaps = tuple(self._gaps)
        return {
            "stale": stale,
            "stale_count": len(stale),
            "tracked": tracked,
            "gap_unrecoverable": [_gap_to_dict(event) for event in gaps],
            "gap_unrecoverable_count": len(gaps),
        }


def _ts_to_iso(value: datetime | None) -> str | None:
    return None if value is None else ns_to_iso(datetime_to_ns(value))


def _gap_to_dict(event: GapEvent) -> dict:
    return {
        "instrument_id": event.instrument_id,
        "interval": event.interval,
        "from_ts": _ts_to_iso(event.from_ts),
        "to_ts": _ts_to_iso(event.to_ts),
        "reason": event.reason,
        "recorded_at": _ts_to_iso(event.recorded_at),
    }
