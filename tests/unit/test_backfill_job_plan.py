# ruff: file-ignore[implicit-namespace-package]
"""Backfill Job plan and wire model (contract section 7.2).

The plan is fixed by the contract: a 1m target is a native-interval Backfill,
every higher interval is always "补采 1m + 内部 Bar Aggregation" and must never
name a native high-interval venue call.
"""

from __future__ import annotations

import pytest

from mdgw import backfill_job_status
from mdgw.admin.jobs import models
from mdgw.admin.jobs.models import (
    QUEUED,
    BackfillJobRecord,
    empty_counters,
)
from mdgw.backfill_job_plan import (
    AGGREGATION_DESCRIPTION,
    BASE_INTERVAL,
    NATIVE_DESCRIPTION,
    PLAN_MODES,
    PLAN_NATIVE_INTERVAL,
    PLAN_ONE_MINUTE_PLUS_AGGREGATION,
    plan_for_interval,
)
from mdgw.service import backfill_jobs


def test_one_minute_uses_the_native_interval_plan() -> None:
    plan = plan_for_interval("1m")

    assert plan == {
        "mode": PLAN_NATIVE_INTERVAL,
        "source_intervals": ["1m"],
        "target_interval": "1m",
        "description": NATIVE_DESCRIPTION,
    }


@pytest.mark.parametrize("interval", ["5m", "15m", "1h", "1d"])
def test_higher_intervals_always_aggregate_internally(interval: str) -> None:
    plan = plan_for_interval(interval)

    assert plan["mode"] == PLAN_ONE_MINUTE_PLUS_AGGREGATION
    assert plan["source_intervals"] == [BASE_INTERVAL]
    assert plan["target_interval"] == interval
    assert plan["description"] == AGGREGATION_DESCRIPTION
    assert PLAN_MODES == (
        PLAN_NATIVE_INTERVAL,
        PLAN_ONE_MINUTE_PLUS_AGGREGATION,
    )


def test_an_unknown_interval_is_rejected() -> None:
    with pytest.raises(ValueError, match="unsupported Backfill target interval"):
        plan_for_interval("2h")


def test_job_wire_matches_the_contract_shape() -> None:
    record = BackfillJobRecord(
        job_id="bf_0123456789abcdef0123456789abcdef",
        status=QUEUED,
        instrument_id="binance:BTC/USDT",
        interval="1h",
        start_ns=1_725_148_800_000_000_000,
        end_ns=1_725_235_200_000_000_000,
        plan=plan_for_interval("1h"),
        current_range=None,
        counters=empty_counters(),
        requested_by_subject="subject-1",
        requested_by_display_name="Operator User",
        requested_by_access_role="Operator",
        reason="recover exchange outage",
        retry_of_job_id=None,
        created_at_ns=1_762_689_600_000_000_000,
        started_at_ns=None,
        finished_at_ns=None,
        error=None,
        resolved_gap_ids=(),
        dispatched_at_ns=None,
    )

    wire = record.to_wire()

    assert wire == {
        "job_id": "bf_0123456789abcdef0123456789abcdef",
        "status": "queued",
        "instrument_id": "binance:BTC/USDT",
        "interval": "1h",
        "start": "2024-09-01T00:00:00.000000000Z",
        "end": "2024-09-02T00:00:00.000000000Z",
        "plan": {
            "mode": "one_minute_plus_aggregation",
            "source_intervals": ["1m"],
            "target_interval": "1h",
            "description": "补采 1m + 内部 Bar Aggregation",
        },
        "current_range": None,
        "counters": {
            "scanned_bars": 0,
            "gap_bars": 0,
            "backfilled_bars": 0,
            "written_bars": 0,
            "rejected_bars": 0,
        },
        "requested_by": {
            "subject": "subject-1",
            "display_name": "Operator User",
            "access_role": "Operator",
        },
        "reason": "recover exchange outage",
        "retry_of_job_id": None,
        "created_at": "2025-11-09T12:00:00.000000000Z",
        "started_at": None,
        "finished_at": None,
        "error": None,
        "resolved_gap_ids": [],
    }


def test_the_backfill_status_vocabulary_has_one_source() -> None:
    assert models.BACKFILL_STATUSES is backfill_job_status.BACKFILL_STATUSES
    assert models.ACTIVE_STATUSES is backfill_job_status.ACTIVE_STATUSES
    assert models.TERMINAL_STATUSES is backfill_job_status.TERMINAL_STATUSES
    assert models.RETRYABLE_STATUSES is backfill_job_status.RETRYABLE_STATUSES
    assert set(backfill_jobs.ACTIVE_STATES) == set(
        backfill_job_status.ACTIVE_STATUSES
    )
    assert set(backfill_jobs.TERMINAL_STATES) == set(
        backfill_job_status.TERMINAL_STATUSES
    )
