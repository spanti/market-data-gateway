# ruff: file-ignore[implicit-namespace-package]
"""Persistent Gap store: natural-key upsert, append-only history, resolution.

Only public ``GapStore`` methods are exercised; the admin migration runner
remains the only schema owner. Repeated discoveries must update one row (never
duplicate natural keys), successful jobs resolve only fully contained ranges,
and reappearance after resolution reopens the same row while keeping history.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mdgw.admin.db import AdminStore
from mdgw.admin.gaps.models import (
    GAP_HISTORY_UNSUPPORTED,
    SOURCE_BACKFILL_JOB,
    SOURCE_COLLECTOR_PROCESS,
)
from mdgw.admin.gaps.store import GapStore

if TYPE_CHECKING:
    from pathlib import Path

SECOND = 1_000_000_000
BASE_NS = 1_700_000_000 * SECOND
MINUTE_NS = 60 * SECOND
HOUR_NS = 60 * MINUTE_NS
FEED = "binance_spot"
BTC = "binance:BTC/USDT"


def _open(tmp_path: Path) -> GapStore:
    path = tmp_path / "admin" / "admin.db"
    AdminStore(path).close()
    return GapStore(path)


def _discover(  # ruff: ignore[too-many-arguments] - explicit observation
    store: GapStore,
    *,
    observed_at_ns: int,
    start_ns: int | None = BASE_NS,
    end_ns: int | None = BASE_NS + HOUR_NS,
    reason: str = GAP_HISTORY_UNSUPPORTED,
    instrument_id: str = BTC,
    interval: str = "1m",
    job_id: str | None = None,
    source: str = SOURCE_COLLECTOR_PROCESS,
):
    return store.upsert_discovery(
        source=source,
        feed=FEED,
        instrument_id=instrument_id,
        interval=interval,
        start_ns=start_ns,
        end_ns=end_ns,
        reason=reason,
        observed_at_ns=observed_at_ns,
        job_id=job_id,
    )


def test_repeated_discovery_updates_first_last_and_count_without_duplicates(
    tmp_path: Path,
) -> None:
    store = _open(tmp_path)
    try:
        first = _discover(store, observed_at_ns=BASE_NS)
        second = _discover(store, observed_at_ns=BASE_NS + MINUTE_NS)
        rows = store.list_gaps(limit=10)
        occurrences = store.occurrences(first.gap_id)
    finally:
        store.close()

    assert len(rows) == 1
    assert first.gap_id == second.gap_id
    assert second.status == "open"
    assert second.first_discovered_at_ns == BASE_NS
    assert second.last_discovered_at_ns == BASE_NS + MINUTE_NS
    assert second.occurrence_count == 2
    assert second.created_at_ns == BASE_NS
    assert second.related_job_ids == ()
    assert [
        (row.source, row.phase, row.observed_at_ns, row.job_id)
        for row in occurrences
    ] == [
        (SOURCE_COLLECTOR_PROCESS, "discovered", BASE_NS, None),
        (SOURCE_COLLECTOR_PROCESS, "discovered", BASE_NS + MINUTE_NS, None),
    ]


def test_polling_the_same_observation_is_idempotent(tmp_path: Path) -> None:
    store = _open(tmp_path)
    try:
        first = _discover(store, observed_at_ns=BASE_NS)
        second = _discover(store, observed_at_ns=BASE_NS)
        occurrences = store.occurrences(first.gap_id)
    finally:
        store.close()

    assert first == second
    assert second.occurrence_count == 1
    assert len(occurrences) == 1


def test_a_null_range_is_still_one_natural_key(tmp_path: Path) -> None:
    store = _open(tmp_path)
    try:
        first = _discover(
            store, observed_at_ns=BASE_NS, start_ns=None, end_ns=None
        )
        second = _discover(
            store, observed_at_ns=BASE_NS + MINUTE_NS, start_ns=None, end_ns=None
        )
        rows = store.list_gaps(limit=10)
    finally:
        store.close()

    assert len(rows) == 1
    assert first.gap_id == second.gap_id
    assert rows[0].start_ns is None
    assert rows[0].end_ns is None
    assert rows[0].occurrence_count == 2


def test_a_successful_job_resolves_only_fully_contained_open_gaps(
    tmp_path: Path,
) -> None:
    store = _open(tmp_path)
    try:
        contained = _discover(
            store,
            observed_at_ns=BASE_NS,
            start_ns=BASE_NS + MINUTE_NS,
            end_ns=BASE_NS + 10 * MINUTE_NS,
        )
        touching = _discover(
            store,
            observed_at_ns=BASE_NS,
            start_ns=BASE_NS - MINUTE_NS,
            end_ns=BASE_NS + MINUTE_NS,
        )
        other_instrument = _discover(
            store,
            observed_at_ns=BASE_NS,
            start_ns=BASE_NS + MINUTE_NS,
            end_ns=BASE_NS + 2 * MINUTE_NS,
            instrument_id="binance:ETH/USDT",
        )
        unknown_range = _discover(
            store, observed_at_ns=BASE_NS, start_ns=None, end_ns=None
        )
        resolved = store.resolve_contained(
            feed=FEED,
            instrument_id=BTC,
            interval="1m",
            start_ns=BASE_NS,
            end_ns=BASE_NS + HOUR_NS,
            job_id="bf_success",
            observed_at_ns=BASE_NS + 2 * HOUR_NS,
        )
        loaded = {
            gap_id: store.get(gap_id)
            for gap_id in (
                contained.gap_id,
                touching.gap_id,
                other_instrument.gap_id,
                unknown_range.gap_id,
            )
        }
        occurrences = store.occurrences(contained.gap_id)
    finally:
        store.close()

    assert resolved == (contained.gap_id,)
    assert loaded[contained.gap_id].status == "resolved"
    assert loaded[contained.gap_id].resolved_at_ns == BASE_NS + 2 * HOUR_NS
    assert loaded[contained.gap_id].resolved_by_job_id == "bf_success"
    assert loaded[contained.gap_id].related_job_ids == ("bf_success",)
    assert loaded[touching.gap_id].status == "open"
    assert loaded[other_instrument.gap_id].status == "open"
    # A gap without a precise range can never be proven resolved.
    assert loaded[unknown_range.gap_id].status == "open"
    assert [
        (row.phase, row.source, row.job_id, row.start_ns, row.end_ns)
        for row in occurrences
    ][-1] == (
        "resolved",
        SOURCE_BACKFILL_JOB,
        "bf_success",
        BASE_NS + MINUTE_NS,
        BASE_NS + 10 * MINUTE_NS,
    )


def test_the_same_stale_observation_does_not_undo_a_resolution(
    tmp_path: Path,
) -> None:
    """A still-listed Collector event predates the successful job."""
    store = _open(tmp_path)
    try:
        gap = _discover(store, observed_at_ns=BASE_NS)
        store.resolve_contained(
            feed=FEED,
            instrument_id=BTC,
            interval="1m",
            start_ns=BASE_NS,
            end_ns=BASE_NS + HOUR_NS,
            job_id="bf_success",
            observed_at_ns=BASE_NS + MINUTE_NS,
        )
        # Every later poll returns the same recorded_at: it is the same
        # observation, not a new discovery.
        polled_again = _discover(store, observed_at_ns=BASE_NS)
        loaded = store.get(gap.gap_id)
    finally:
        store.close()

    assert polled_again.status == "resolved"
    assert loaded is not None
    assert loaded.status == "resolved"
    assert loaded.resolved_by_job_id == "bf_success"
    assert loaded.occurrence_count == 1


def test_reappearance_after_resolution_reopens_the_same_row_keeping_history(
    tmp_path: Path,
) -> None:
    store = _open(tmp_path)
    try:
        gap = _discover(store, observed_at_ns=BASE_NS)
        store.resolve_contained(
            feed=FEED,
            instrument_id=BTC,
            interval="1m",
            start_ns=BASE_NS,
            end_ns=BASE_NS + HOUR_NS,
            job_id="bf_success",
            observed_at_ns=BASE_NS + MINUTE_NS,
        )
        reopened = _discover(
            store,
            observed_at_ns=BASE_NS + 2 * HOUR_NS,
            job_id="bf_followup",
            source=SOURCE_BACKFILL_JOB,
        )
        occurrences = store.occurrences(gap.gap_id)
        rows = store.list_gaps(limit=10)
    finally:
        store.close()

    assert len(rows) == 1
    assert reopened.gap_id == gap.gap_id
    assert reopened.status == "open"
    assert reopened.first_discovered_at_ns == BASE_NS
    assert reopened.last_discovered_at_ns == BASE_NS + 2 * HOUR_NS
    assert reopened.occurrence_count == 2
    assert reopened.resolved_at_ns is None
    assert reopened.resolved_by_job_id is None
    assert reopened.related_job_ids == ("bf_success", "bf_followup")
    assert [(row.phase, row.source, row.job_id) for row in occurrences] == [
        ("discovered", SOURCE_COLLECTOR_PROCESS, None),
        ("resolved", SOURCE_BACKFILL_JOB, "bf_success"),
        ("discovered", SOURCE_BACKFILL_JOB, "bf_followup"),
    ]
