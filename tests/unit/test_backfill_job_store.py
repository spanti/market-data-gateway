# ruff: file-ignore[implicit-namespace-package]
"""Backfill Job persistence: migration, conflict, idempotency, transitions.

These tests exercise only the public ``BackfillJobStore`` methods; the admin
migration runner remains the only schema owner.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from mdgw.admin.db import AdminStore
from mdgw.admin.errors import ConflictError
from mdgw.admin.jobs.models import (
    CANCELLED,
    CANCELLING,
    FAILED,
    PARTIALLY_SUCCEEDED,
    QUEUED,
    RUNNING,
    SUCCEEDED,
    BackfillJobRecord,
    empty_counters,
)
from mdgw.admin.jobs.store import BackfillJobStore
from mdgw.backfill_job_plan import plan_for_interval

if TYPE_CHECKING:
    from pathlib import Path

SECOND = 1_000_000_000
BASE_NS = 1_700_000_000 * SECOND
MINUTE_NS = 60 * SECOND
HOUR_NS = 60 * MINUTE_NS


def _open(tmp_path: Path) -> BackfillJobStore:
    path = tmp_path / "admin" / "admin.db"
    AdminStore(path).close()
    return BackfillJobStore(path)


def _record(  # ruff: ignore[too-many-arguments] - explicit record fields
    job_id: str = "bf_one",
    *,
    instrument_id: str = "binance:BTC/USDT",
    interval: str = "1h",
    status: str = QUEUED,
    start_ns: int = BASE_NS,
    end_ns: int = BASE_NS + HOUR_NS,
    retry_of_job_id: str | None = None,
    created_at_ns: int = BASE_NS,
) -> BackfillJobRecord:
    return BackfillJobRecord(
        job_id=job_id,
        status=status,
        instrument_id=instrument_id,
        interval=interval,
        start_ns=start_ns,
        end_ns=end_ns,
        plan=plan_for_interval(interval),
        current_range=None,
        counters=empty_counters(),
        requested_by_subject="subject-1",
        requested_by_display_name="Operator User",
        requested_by_access_role="Operator",
        reason="recover exchange outage",
        retry_of_job_id=retry_of_job_id,
        created_at_ns=created_at_ns,
        started_at_ns=None,
        finished_at_ns=None,
        error=None,
        resolved_gap_ids=(),
        dispatched_at_ns=None,
        updated_at_ns=created_at_ns,
    )


def test_the_migration_persists_and_reads_a_queued_job(tmp_path: Path) -> None:
    store = _open(tmp_path)
    record = _record()
    try:
        stored, replayed = store.insert(record)
        loaded = store.get("bf_one")
    finally:
        store.close()

    assert replayed is False
    assert stored == record
    assert loaded == record


def test_an_active_job_for_the_same_pair_is_rejected(tmp_path: Path) -> None:
    store = _open(tmp_path)
    try:
        store.insert(_record("bf_first"))
        with pytest.raises(ConflictError) as raised:
            store.insert(_record("bf_second"))
    finally:
        store.close()

    assert raised.value.details == {
        "reason": "active_job",
        "active_job_id": "bf_first",
    }


def test_a_terminal_job_does_not_block_a_new_job(tmp_path: Path) -> None:
    store = _open(tmp_path)
    try:
        store.insert(_record("bf_first"))
        store.apply_status(
            "bf_first",
            status=SUCCEEDED,
            counters=empty_counters(),
            current_range=None,
            started_at_ns=BASE_NS,
            finished_at_ns=BASE_NS + HOUR_NS,
            error=None,
            resolved_gap_ids=(),
            now_ns=BASE_NS + HOUR_NS,
        )
        stored, _replayed = store.insert(
            _record("bf_second", created_at_ns=BASE_NS + 1)
        )
    finally:
        store.close()

    assert stored.job_id == "bf_second"


def test_an_idempotency_key_replays_the_original_job_and_rejects_reuse(
    tmp_path: Path,
) -> None:
    store = _open(tmp_path)
    try:
        first, replayed = store.insert(
            _record(),
            idempotency_key="idem-1",
            action="create",
            request_hash="hash-a",
        )
        again, replayed_again = store.insert(
            _record(job_id="bf_other"),
            idempotency_key="idem-1",
            action="create",
            request_hash="hash-a",
        )
        with pytest.raises(ConflictError) as raised:
            store.insert(
                _record(job_id="bf_other"),
                idempotency_key="idem-1",
                action="create",
                request_hash="hash-b",
            )
    finally:
        store.close()

    assert replayed is False
    assert replayed_again is True
    assert again.job_id == first.job_id
    assert raised.value.details["reason"] == "idempotency_key_reuse"


def test_status_transitions_persist_progress_and_ignore_late_terminal_events(
    tmp_path: Path,
) -> None:
    store = _open(tmp_path)
    counters = {
        "scanned_bars": 60,
        "gap_bars": 2,
        "backfilled_bars": 57,
        "written_bars": 70,
        "rejected_bars": 1,
    }
    try:
        store.insert(_record())
        store.mark_dispatched("bf_one", BASE_NS + SECOND)
        store.apply_status(
            "bf_one",
            status=RUNNING,
            counters={**empty_counters(), "scanned_bars": 10},
            current_range={"start_ns": BASE_NS, "end_ns": BASE_NS + MINUTE_NS},
            started_at_ns=BASE_NS + SECOND,
            finished_at_ns=None,
            error=None,
            resolved_gap_ids=(),
            now_ns=BASE_NS + SECOND,
        )
        store.apply_status(
            "bf_one",
            status=PARTIALLY_SUCCEEDED,
            counters=counters,
            current_range={"start_ns": BASE_NS, "end_ns": BASE_NS + HOUR_NS},
            started_at_ns=BASE_NS + SECOND,
            finished_at_ns=BASE_NS + 2 * SECOND,
            error=None,
            resolved_gap_ids=("gap_one",),
            now_ns=BASE_NS + 2 * SECOND,
        )
        late = store.apply_status(
            "bf_one",
            status=FAILED,
            counters=empty_counters(),
            current_range=None,
            started_at_ns=None,
            finished_at_ns=BASE_NS + 3 * SECOND,
            error={"code": "late", "message": "late", "details": {}},
            resolved_gap_ids=(),
            now_ns=BASE_NS + 3 * SECOND,
        )
        loaded = store.get("bf_one")
    finally:
        store.close()

    assert late is False
    assert loaded is not None
    assert loaded.status == PARTIALLY_SUCCEEDED
    assert loaded.dispatched_at_ns == BASE_NS + SECOND
    assert dict(loaded.counters) == counters
    # The contract reports current_range null once the job is finished.
    assert loaded.current_range is None
    assert loaded.resolved_gap_ids == ("gap_one",)
    assert loaded.finished_at_ns == BASE_NS + 2 * SECOND


def test_an_unconfirmed_running_job_becomes_interrupted_never_success(
    tmp_path: Path,
) -> None:
    store = _open(tmp_path)
    try:
        store.insert(_record())
        store.apply_status(
            "bf_one",
            status=RUNNING,
            counters=empty_counters(),
            current_range=None,
            started_at_ns=BASE_NS,
            finished_at_ns=None,
            error=None,
            resolved_gap_ids=(),
            now_ns=BASE_NS,
        )
        changed = store.mark_interrupted("bf_one", BASE_NS + SECOND)
        loaded = store.get("bf_one")
        second = store.mark_interrupted("bf_one", BASE_NS + 3 * SECOND)
    finally:
        store.close()

    assert changed is True
    assert second is False
    assert loaded is not None
    assert loaded.status == FAILED
    assert loaded.error is not None
    assert loaded.error["code"] == "interrupted"
    assert loaded.finished_at_ns == BASE_NS + SECOND


def test_cancelling_a_queued_job_starts_cooperatively(tmp_path: Path) -> None:
    store = _open(tmp_path)
    try:
        store.insert(_record())
        record = store.request_cancel("bf_one", BASE_NS + SECOND)
        loaded = store.get("bf_one")
    finally:
        store.close()

    assert record is not None
    assert record.status == CANCELLING
    assert loaded is not None
    assert loaded.status == CANCELLING
    assert loaded.finished_at_ns is None


def test_cancelling_a_dispatched_job_waits_for_collector_confirmation(
    tmp_path: Path,
) -> None:
    store = _open(tmp_path)
    try:
        store.insert(_record())
        store.mark_dispatched("bf_one", BASE_NS + SECOND)
        store.apply_status(
            "bf_one",
            status=RUNNING,
            counters=empty_counters(),
            current_range=None,
            started_at_ns=BASE_NS + SECOND,
            finished_at_ns=None,
            error=None,
            resolved_gap_ids=(),
            now_ns=BASE_NS + SECOND,
        )
        record = store.request_cancel("bf_one", BASE_NS + 2 * SECOND)
        loaded = store.get("bf_one")
        confirmed = store.apply_status(
            "bf_one",
            status=CANCELLED,
            counters=empty_counters(),
            current_range=None,
            started_at_ns=BASE_NS + SECOND,
            finished_at_ns=BASE_NS + 3 * SECOND,
            error=None,
            resolved_gap_ids=(),
            now_ns=BASE_NS + 3 * SECOND,
        )
        final = store.get("bf_one")
    finally:
        store.close()

    assert record is not None
    assert record.status == CANCELLING
    assert loaded is not None
    assert loaded.status == CANCELLING
    assert confirmed is True
    assert final is not None
    assert final.status == CANCELLED


def test_a_late_running_snapshot_does_not_overwrite_cancelling(
    tmp_path: Path,
) -> None:
    store = _open(tmp_path)
    try:
        store.insert(_record())
        store.mark_dispatched("bf_one", BASE_NS + SECOND)
        store.request_cancel("bf_one", BASE_NS + 2 * SECOND)
        changed = store.apply_status(
            "bf_one",
            status=RUNNING,
            counters=empty_counters(),
            current_range=None,
            started_at_ns=BASE_NS + SECOND,
            finished_at_ns=None,
            error=None,
            resolved_gap_ids=(),
            now_ns=BASE_NS + 3 * SECOND,
        )
        loaded = store.get("bf_one")
    finally:
        store.close()

    assert changed is False
    assert loaded is not None
    assert loaded.status == CANCELLING


def test_a_terminal_job_is_not_cancellable(tmp_path: Path) -> None:
    store = _open(tmp_path)
    try:
        store.insert(_record())
        store.apply_status(
            "bf_one",
            status=SUCCEEDED,
            counters=empty_counters(),
            current_range=None,
            started_at_ns=BASE_NS,
            finished_at_ns=BASE_NS + SECOND,
            error=None,
            resolved_gap_ids=(),
            now_ns=BASE_NS + SECOND,
        )
        cancelled = store.request_cancel("bf_one", BASE_NS + 2 * SECOND)
    finally:
        store.close()

    assert cancelled is None


def test_a_retry_links_the_new_job_without_mutating_the_old_one(
    tmp_path: Path,
) -> None:
    store = _open(tmp_path)
    try:
        store.insert(_record())
        store.apply_status(
            "bf_one",
            status=FAILED,
            counters=empty_counters(),
            current_range=None,
            started_at_ns=BASE_NS,
            finished_at_ns=BASE_NS + SECOND,
            error={"code": "backfill_failed", "message": "x", "details": {}},
            resolved_gap_ids=(),
            now_ns=BASE_NS + SECOND,
        )
        before = store.get("bf_one")
        retried, replayed = store.insert_retry(
            _record(
                "bf_retry",
                retry_of_job_id="bf_one",
                created_at_ns=BASE_NS + 2 * SECOND,
            ),
            idempotency_key="idem-retry",
            request_hash="hash-retry",
        )
        after = store.get("bf_one")
    finally:
        store.close()

    assert replayed is False
    assert retried.retry_of_job_id == "bf_one"
    assert after == before


def test_list_pages_filter_and_order_by_created_at_desc(tmp_path: Path) -> None:
    store = _open(tmp_path)
    try:
        store.insert(_record("bf_a", created_at_ns=BASE_NS))
        store.insert(
            _record(
                "bf_b",
                instrument_id="binance:ETH/USDT",
                interval="1m",
                created_at_ns=BASE_NS + 1,
            )
        )
        store.insert(
            _record(
                "bf_c",
                instrument_id="binance:ETH/USDT",
                interval="5m",
                created_at_ns=BASE_NS + 2,
            )
        )
        page = store.list_jobs(
            status=None,
            instrument_id="binance:ETH/USDT",
            interval=None,
            after=None,
            limit=10,
        )
        after = store.list_jobs(
            status=QUEUED,
            instrument_id=None,
            interval=None,
            after=(BASE_NS + 2, "bf_c"),
            limit=10,
        )
    finally:
        store.close()

    assert [record.job_id for record in page] == ["bf_c", "bf_b"]
    assert [record.job_id for record in after] == ["bf_b", "bf_a"]
