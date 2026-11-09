"""The fixed Backfill Job execution plan (Admin contract section 7.2).

A Backfill Job never asks a Venue for a native high-interval history: the base
interval is always ``1m`` and every higher target interval is produced by the
Collector's internal Bar Aggregation. This module is the single place both the
Admin API (which persists and returns the plan) and the Collector job runner
(which executes it) read the plan from, so the wire can never claim a native
high-interval call that the executor does not make.
"""

from __future__ import annotations

from mdgw.core.schema import INTERVALS

BASE_INTERVAL = "1m"

PLAN_NATIVE_INTERVAL = "native_interval"
PLAN_ONE_MINUTE_PLUS_AGGREGATION = "one_minute_plus_aggregation"
PLAN_MODES: tuple[str, ...] = (
    PLAN_NATIVE_INTERVAL,
    PLAN_ONE_MINUTE_PLUS_AGGREGATION,
)

#: The UI displays this exact text; it must not imply a native venue call.
AGGREGATION_DESCRIPTION = "补采 1m + 内部 Bar Aggregation"
NATIVE_DESCRIPTION = "补采 1m Bar"

#: Backfill Jobs accept the five canonical Bar Intervals.
BACKFILL_INTERVALS: tuple[str, ...] = (
    "1m",
    "5m",
    "15m",
    "1h",
    "1d",
)


def plan_for_interval(interval: str) -> dict[str, object]:
    """Return the persisted/wire plan for one target Bar Interval."""
    if interval not in BACKFILL_INTERVALS or interval not in INTERVALS:
        msg = f"unsupported Backfill target interval: {interval!r}"
        raise ValueError(msg)
    if interval == BASE_INTERVAL:
        mode = PLAN_NATIVE_INTERVAL
        description = NATIVE_DESCRIPTION
    else:
        mode = PLAN_ONE_MINUTE_PLUS_AGGREGATION
        description = AGGREGATION_DESCRIPTION
    return {
        "mode": mode,
        "source_intervals": [BASE_INTERVAL],
        "target_interval": interval,
        "description": description,
    }


def source_interval_for(interval: str) -> str:
    """The only interval a Backfill Job ever fetches from the Feed Handler."""
    if interval not in BACKFILL_INTERVALS or interval not in INTERVALS:
        msg = f"unsupported Backfill target interval: {interval!r}"
        raise ValueError(msg)
    return BASE_INTERVAL


__all__ = [
    "AGGREGATION_DESCRIPTION",
    "BACKFILL_INTERVALS",
    "BASE_INTERVAL",
    "NATIVE_DESCRIPTION",
    "PLAN_MODES",
    "PLAN_NATIVE_INTERVAL",
    "PLAN_ONE_MINUTE_PLUS_AGGREGATION",
    "plan_for_interval",
    "source_interval_for",
]
