# ruff: file-ignore[implicit-namespace-package]
"""Gap service: Collector snapshot ingest and finished-job reconciliation (A11).

The service is the only writer of persistent Gap rows. A Collector outage or
an empty snapshot never fabricates or deletes history; a successful job
resolves only fully contained open gaps; every other terminal outcome may only
link itself to gaps it attempted.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mdgw.admin.db import AdminStore
from mdgw.admin.gaps.models import (
    GAP_HISTORY_MISSING,
    GAP_HISTORY_UNSUPPORTED,
    SOURCE_BACKFILL_JOB,
    SOURCE_COLLECTOR_PROCESS,
)
from mdgw.admin.gaps.service import BackfillGapService
from mdgw.admin.gaps.store import GapStore
from mdgw.admin.jobs.models import (
    PARTIALLY_SUCCEEDED,
    SUCCEEDED,
    BackfillJobRecord,
    empty_counters,
)
from mdgw.backfill_job_plan import plan_for_interval

if TYPE_CHECKING:
    from pathlib import Path

SECOND = 1_000_000_000
BASE_NS = 1_700_000_000 * SECOND
MINUTE_NS = 60 * SECOND
HOUR_NS = 60 * MINUTE_NS
FEED = "binance_spot"
BTC = "binance:BTC/USDT"


class Clock:
    def __init__(self, now_ns: int = BASE_NS + 3 * HOUR_NS) -> None:
        self.now_ns = now_ns

    def __call__(self) -> int:
        return self.now_ns


def _service(tmp_path: Path) -> tuple[BackfillGapService, GapStore, Clock]:
    path = tmp_path / "admin" / "admin.db"
    AdminStore(path).close()
    store = GapStore(path)
    clock = Clock()
    return BackfillGapService(store=store, clock=clock), store, clock


def _job(
    *,
    job_id: str = "bf_one",
    status: str = SUCCEEDED,
    interval: str = "1m",
    start_ns: int = BASE_NS,
    end_ns: int = BASE_NS + HOUR_NS,
) -> BackfillJobRecord:
    return BackfillJobRecord(
        job_id=job_id,
        status=status,
        instrument_id=BTC,
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
        retry_of_job_id=None,
        created_at_ns=BASE_NS,
        started_at_ns=BASE_NS,
        finished_at_ns=BASE_NS + HOUR_NS,
        error=None,
        resolved_gap_ids=(),
        dispatched_at_ns=BASE_NS,
        updated_at_ns=BASE_NS + HOUR_NS,
    )


def _event(  # ruff: ignore[too-many-arguments] - explicit observation
    *,
    feed: str = FEED,
    instrument_id: str = BTC,
    interval: str = "1m",
    from_ns: int | None = BASE_NS,
    to_ns: int | None = BASE_NS + HOUR_NS,
    reason: str = GAP_HISTORY_UNSUPPORTED,
    recorded_at_ns: int = BASE_NS + MINUTE_NS,
) -> dict:
    return {
        "feed": feed,
        "instrument_id": instrument_id,
        "interval": interval,
        "from_ns": from_ns,
        "to_ns": to_ns,
        "reason": reason,
        "recorded_at_ns": recorded_at_ns,
    }


def test_collector_snapshot_upserts_and_an_empty_snapshot_deletes_nothing(
    tmp_path: Path,
) -> None:
    service, store, clock = _service(tmp_path)
    try:
        accepted = service.apply_collector_snapshot(
            {"gaps": [_event()], "snapshot_at_ns": clock.now_ns}
        )
        after_first = store.list_gaps(limit=10)
        # A later empty snapshot (for example after a Collector restart) must
        # not delete persistent history.
        accepted_empty = service.apply_collector_snapshot(
            {"gaps": [], "snapshot_at_ns": clock.now_ns + MINUTE_NS}
        )
        after_empty = store.list_gaps(limit=10)
        occurrences = store.occurrences(after_first[0].gap_id)
    finally:
        store.close()

    assert accepted == 1
    assert accepted_empty == 0
    assert after_first[0].feed == FEED
    assert after_first[0].reason == GAP_HISTORY_UNSUPPORTED
    assert after_first[0].occurrence_count == 1
    assert after_empty == after_first
    assert [(row.source, row.phase) for row in occurrences] == [
        (SOURCE_COLLECTOR_PROCESS, "discovered")
    ]


def test_malformed_snapshot_entries_are_skipped_without_touching_history(
    tmp_path: Path,
) -> None:
    service, store, _clock = _service(tmp_path)
    try:
        service.apply_collector_snapshot({"gaps": [_event()]})
        accepted = service.apply_collector_snapshot(
            {
                "gaps": [
                    {"feed": "", "instrument_id": BTC},
                    {"feed": FEED, "instrument_id": BTC, "interval": "1m",
                     "from_ns": 10, "to_ns": 5, "reason": "x",
                     "recorded_at_ns": 1},
                    "not-an-object",
                ]
            }
        )
        rows = store.list_gaps(limit=10)
    finally:
        store.close()

    assert accepted == 0
    assert len(rows) == 1
    assert rows[0].occurrence_count == 1


def test_a_successful_job_resolves_contained_gaps_and_returns_their_ids(
    tmp_path: Path,
) -> None:
    service, store, _clock = _service(tmp_path)
    try:
        contained = store.upsert_discovery(
            source=SOURCE_COLLECTOR_PROCESS,
            feed=FEED,
            instrument_id=BTC,
            interval="1m",
            start_ns=BASE_NS + MINUTE_NS,
            end_ns=BASE_NS + 10 * MINUTE_NS,
            reason=GAP_HISTORY_UNSUPPORTED,
            observed_at_ns=BASE_NS,
        )
        outside = store.upsert_discovery(
            source=SOURCE_COLLECTOR_PROCESS,
            feed=FEED,
            instrument_id=BTC,
            interval="1m",
            start_ns=BASE_NS + HOUR_NS,
            end_ns=BASE_NS + HOUR_NS + MINUTE_NS,
            reason=GAP_HISTORY_UNSUPPORTED,
            observed_at_ns=BASE_NS,
        )
        resolved = service.reconcile_finished_job(
            _job(status=SUCCEEDED),
            {"state": SUCCEEDED, "feed": FEED, "gap_ranges": []},
        )
        loaded_contained = store.get(contained.gap_id)
        loaded_outside = store.get(outside.gap_id)
    finally:
        store.close()

    assert resolved == (contained.gap_id,)
    assert loaded_contained.status == "resolved"
    assert loaded_contained.resolved_by_job_id == "bf_one"
    assert loaded_outside.status == "open"


def test_non_successful_outcomes_link_but_never_resolve(tmp_path: Path) -> None:
    for outcome in ("partially_succeeded", "failed", "cancelled"):
        service, store, _clock = _service(tmp_path / outcome)
        try:
            existing = store.upsert_discovery(
                source=SOURCE_COLLECTOR_PROCESS,
                feed=FEED,
                instrument_id=BTC,
                interval="1m",
                start_ns=BASE_NS + MINUTE_NS,
                end_ns=BASE_NS + 10 * MINUTE_NS,
                reason=GAP_HISTORY_UNSUPPORTED,
                observed_at_ns=BASE_NS,
            )
            resolved = service.reconcile_finished_job(
                _job(status=outcome),
                {
                    "state": outcome,
                    "feed": FEED,
                    "gap_ranges": [
                        [BASE_NS + 20 * MINUTE_NS, BASE_NS + 22 * MINUTE_NS]
                    ],
                },
            )
            loaded = store.get(existing.gap_id)
            rows = store.list_gaps(limit=10)
            observed = next(
                row
                for row in rows
                if row.start_ns == BASE_NS + 20 * MINUTE_NS
            )
            observed_occurrences = {
                (row.source, row.phase, row.job_id)
                for row in store.occurrences(observed.gap_id)
            }
            existing_occurrences = {
                (row.source, row.phase, row.job_id)
                for row in store.occurrences(existing.gap_id)
            }
        finally:
            store.close()

        assert resolved == ()
        assert loaded.status == "open"
        assert loaded.resolved_at_ns is None
        assert loaded.resolved_by_job_id is None
        assert loaded.related_job_ids == ("bf_one",)
        # The observed missing 1m range is recorded as a separate gap and is
        # linked to this job; the pre-existing contained gap stays open.
        assert observed.interval == "1m"
        assert observed.reason == GAP_HISTORY_MISSING
        assert observed.related_job_ids == ("bf_one",)
        assert (
            SOURCE_BACKFILL_JOB,
            "discovered",
            "bf_one",
        ) in observed_occurrences
        # Linking must not fabricate a discovery occurrence on the old gap.
        assert existing_occurrences == {
            (SOURCE_COLLECTOR_PROCESS, "discovered", None)
        }


def test_a_successful_job_does_not_resolve_a_gap_it_itself_reported_missing(
    tmp_path: Path,
) -> None:
    """Defensive consistency: reported holes reopen/resolve only by evidence."""
    service, store, _clock = _service(tmp_path)
    try:
        reported = [BASE_NS + 30 * MINUTE_NS, BASE_NS + 31 * MINUTE_NS]
        existing = store.upsert_discovery(
            source=SOURCE_COLLECTOR_PROCESS,
            feed=FEED,
            instrument_id=BTC,
            interval="1m",
            start_ns=reported[0],
            end_ns=reported[1],
            reason=GAP_HISTORY_UNSUPPORTED,
            observed_at_ns=BASE_NS,
        )
        resolved = service.reconcile_finished_job(
            _job(status=SUCCEEDED),
            {"state": SUCCEEDED, "feed": FEED, "gap_ranges": [reported]},
        )
        rows = store.list_gaps(limit=10)
        loaded = store.get(existing.gap_id)
    finally:
        store.close()

    assert resolved == ()
    # The pre-existing row stays open; the job's contradictory observation is
    # recorded under the additive ``history_missing`` reason.
    assert len(rows) == 2
    assert loaded is not None
    assert loaded.status == "open"
    assert loaded.resolved_by_job_id is None
    assert loaded.occurrence_count == 1
    observed = next(row for row in rows if row.reason == GAP_HISTORY_MISSING)
    assert observed.start_ns == reported[0]
    assert observed.end_ns == reported[1]


def test_feed_falls_back_to_the_instrument_venue_when_absent(
    tmp_path: Path,
) -> None:
    service, store, _clock = _service(tmp_path)
    try:
        service.reconcile_finished_job(
            _job(status=PARTIALLY_SUCCEEDED),
            {"state": PARTIALLY_SUCCEEDED, "gap_ranges": []},
        )
        # No ranges: nothing is invented just because the outcome is known.
        assert store.list_gaps(limit=10) == []

        service.reconcile_finished_job(
            _job(status=PARTIALLY_SUCCEEDED),
            {
                "state": PARTIALLY_SUCCEEDED,
                "gap_ranges": [[BASE_NS, BASE_NS + MINUTE_NS]],
            },
        )
        rows = store.list_gaps(limit=10)
    finally:
        store.close()

    assert rows[0].feed == "binance"
    assert rows[0].reason == GAP_HISTORY_MISSING
