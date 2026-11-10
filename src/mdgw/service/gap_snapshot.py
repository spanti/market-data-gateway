"""Collector-side projection of the in-memory ``gap_unrecoverable`` list.

The Admin API polls the allow-listed ``gaps.snapshot`` control action and turns
this process-local list into persistent Gap records. This module only shapes
already-recorded :class:`~mdgw.state.health.GapEvent` values: it never scans
the Dataset, compares ``seq`` values or invents a range.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mdgw.core.time import datetime_to_ns, utc_now

if TYPE_CHECKING:
    from mdgw.service.collector_core import CollectorCore


def _snapshot_ns(value: int | None) -> int:
    return datetime_to_ns(utc_now()) if value is None else int(value)


def gaps_snapshot_result(
    core: CollectorCore,
    *,
    collector_instance_id: str,
    now_ns: int | None = None,
) -> dict[str, Any]:
    """Return the bounded JSON result for the ``gaps.snapshot`` action.

    Bounds are UTC integer nanoseconds (the collector's internal time unit) so
    the API never has to re-parse prose or lose sub-millisecond precision.
    """
    snapshot_at = _snapshot_ns(now_ns)
    gaps = [
        {
            "feed": event.feed,
            "instrument_id": event.instrument_id,
            "interval": event.interval,
            "from_ns": (
                None
                if event.from_ts is None
                else datetime_to_ns(event.from_ts)
            ),
            "to_ns": (
                None if event.to_ts is None else datetime_to_ns(event.to_ts)
            ),
            "reason": event.reason,
            "recorded_at_ns": datetime_to_ns(event.recorded_at),
        }
        for event in core.gap_events()
    ]
    return {
        "collector_instance_id": collector_instance_id,
        "snapshot_at_ns": snapshot_at,
        "gaps": gaps,
    }


__all__ = ["gaps_snapshot_result"]
