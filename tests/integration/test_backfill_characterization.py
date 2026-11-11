# ruff: file-ignore[implicit-namespace-package]
"""Characterization of Backfill persistence, reporting and failure ordering.

These tests freeze the CURRENT automatic and Backfill Job behaviour before the
shared execution module is introduced. There is no natural failing-first red
step for a behaviour-preserving refactor; each assertion is an independent
literal and the ticket report records deliberate mutation checks that make
these tests fail on drift.

Covered here:

* the full ``BackfillReport`` field mapping (raw fetched, parent-inclusive
  written, merge skipped, planning-only gap events, per-interval writes);
* Dataset-write failure injection for both entries (HWM and success counters
  do not advance, earlier chunks stay durable);
* the automatic entry's lack of Job-only Core counter / per-Bar health /
  expected-key side effects, and the Job's preservation of them;
* the Job's sink HWM cap over a Bar dropped by the buffered write queue.
"""

from __future__ import annotations

import time
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from mdgw.aggregate import BarAggregator
from mdgw.backfill import run_backfill
from mdgw.core.time import datetime_to_ns
from mdgw.ingest.fake import FakeFeedHandler
from mdgw.runtime import GatewayRuntime
from mdgw.service.backfill_jobs import BackfillJobRunner
from mdgw.service.bar_sink import BufferedBarSink
from mdgw.service.collector_core import CollectorCore
from mdgw.state.health import HealthTracker
from mdgw.state.high_water_mark import HighWaterMarkStore
from mdgw.store.parquet_dataset import ParquetBarStore
from tests.characterization.backfill_support import (
    BASE_INTERVALS,
    FEED,
    INSTRUMENT,
    OTHER_INSTRUMENT,
    StaticHistoryFeed,
    at,
    bar_input,
    config,
    count_rows,
    iso_at,
    open_dataset,
    raw_bar,
    rows_by_open,
)
from tests.characterization.dataset_builder import make_bar

if TYPE_CHECKING:
    from pathlib import Path


def _aggregator() -> BarAggregator:
    settings = config().aggregation
    return BarAggregator(
        intervals=settings.intervals,
        base_interval=settings.base_interval,
        retention_parent_periods=settings.retention_parent_periods,
    )


def _run(  # ruff: ignore[too-many-arguments]
    store: ParquetBarStore,
    hwm: HighWaterMarkStore,
    feed,
    *,
    now=None,
    backfill_hours: float = 24,
    health: HealthTracker | None = None,
):
    return run_backfill(
        feed_handler=feed,
        store=store,
        hwm=hwm,
        aggregator=_aggregator(),
        health=health if health is not None else HealthTracker(),
        asset_class_for=lambda _instrument_id: "crypto",
        now=at(10) if now is None else now,
        backfill_hours=backfill_hours,
    )


def _wait(runner: BackfillJobRunner, job_id: str) -> dict:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        snapshot = runner.status(job_id)
        if snapshot["state"] in {
            "succeeded",
            "partially_succeeded",
            "failed",
            "cancelled",
        }:
            return snapshot
        time.sleep(0.01)
    msg = f"job did not finish: {runner.status(job_id)}"
    raise AssertionError(msg)


def _start_job(
    runner: BackfillJobRunner,
    job_id: str,
    *,
    start_ns: int,
    end_ns: int,
) -> dict:
    accepted = runner.start_job(
        job_id=job_id,
        instrument_id=INSTRUMENT,
        interval="1m",
        start_ns=start_ns,
        end_ns=end_ns,
        mode="native_interval",
    )
    assert accepted["accepted"] is True, accepted
    return _wait(runner, job_id)


class RecordingHealth(HealthTracker):
    """A real HealthTracker that also records every per-Bar event."""

    def __init__(self) -> None:
        super().__init__()
        self.recorded: list = []

    def record_bar(self, bar) -> None:
        self.recorded.append(bar)
        super().record_bar(bar)


class FailingBarStore(ParquetBarStore):
    """A real Store whose selected writes fail before touching the Dataset."""

    def __init__(self, root: Path, *, predicate, message: str) -> None:
        super().__init__(root)
        self._predicate = predicate
        self._message = message
        self.failures = 0

    def write_bars(
        self,
        bars,
        *,
        asset_class: str,
        rule_versions=None,
    ):
        if self._predicate(list(bars)):
            self.failures += 1
            raise RuntimeError(self._message)
        return super().write_bars(
            bars, asset_class=asset_class, rule_versions=rule_versions
        )


# -- BackfillReport mapping -------------------------------------------------


def test_backfill_report_maps_every_field(tmp_path: Path) -> None:
    forming = at(9) + timedelta(seconds=30)
    raws = [
        raw_bar(at(5)),
        raw_bar(at(6)),
        raw_bar(at(7)),
        raw_bar(at(8)),
        raw_bar(at(9)),
        raw_bar(at(9)),  # duplicate collapsed after Normalization
        raw_bar(at(4)),  # before the inclusive HWM start
        raw_bar(at(10)),  # not before `now`
        raw_bar(at(7), interval="5m"),  # wrong source interval
        raw_bar(forming),  # still forming at `now`
    ]
    with open_dataset(tmp_path) as (store, hwm):
        hwm.set(INSTRUMENT, "1m", datetime_to_ns(at(5)))
        report = _run(
            store,
            hwm,
            StaticHistoryFeed(history=raws, filter_by_range=False),
        )

        assert report.history_supported is True
        assert report.instruments == 1
        # Raw rows returned by the Feed Handler, before any filtering.
        assert report.fetched_bars == 10
        # Five accepted base Bars plus their one complete 5m parent.
        assert report.written_bars == 6
        assert report.skipped_bars == 0
        # Planning produced no history-unsupported/backfill-limit evidence.
        assert report.gap_events == 0
        assert report.written_by_interval == {"1m": 5, "5m": 1}

        assert count_rows(store, "1m") == 5
        assert count_rows(store, "5m") == 1
        assert hwm.get(INSTRUMENT, "1m") == datetime_to_ns(at(9))
        assert hwm.get(INSTRUMENT, "5m") == datetime_to_ns(at(5))


def test_backfill_report_counts_merge_skips_for_identical_content(
    tmp_path: Path,
) -> None:
    base_bars = [
        make_bar(
            instrument_id=INSTRUMENT,
            interval="1m",
            open_ts=at(minute),
            close="100",
            volume="1",
            trade_count=1,
            feed=FEED,
            aggregation_source="external",
        )
        for minute in range(5, 10)
    ]
    parent = make_bar(
        instrument_id=INSTRUMENT,
        interval="5m",
        open_ts=at(5),
        close="100",
        volume="5",
        trade_count=5,
        feed=FEED,
        aggregation_source="internal",
    )
    with open_dataset(tmp_path) as (store, hwm):
        store.write_bars([*base_bars, parent], asset_class="crypto")
        hwm.set(INSTRUMENT, "1m", datetime_to_ns(at(5)))
        hwm.set(INSTRUMENT, "5m", datetime_to_ns(at(5)))

        report = _run(
            store,
            hwm,
            StaticHistoryFeed(history=[raw_bar(at(minute)) for minute in range(5, 10)]),
        )

        assert report.fetched_bars == 5
        assert report.written_bars == 0
        # Five identical base Bars plus the identical rebuilt 5m parent.
        assert report.skipped_bars == 6
        assert report.written_by_interval == {}
        assert count_rows(store, "1m") == 5
        assert count_rows(store, "5m") == 1
        assert hwm.get(INSTRUMENT, "1m") == datetime_to_ns(at(5))


def test_backfill_report_counts_only_planning_gaps(tmp_path: Path) -> None:
    health = HealthTracker()
    with open_dataset(tmp_path) as (store, hwm):
        hwm.set(INSTRUMENT, "1m", datetime_to_ns(at(0)))
        report = _run(
            store,
            hwm,
            StaticHistoryFeed(history=()),
            now=at(0, hour=2),
            backfill_hours=1,
            health=health,
        )

        assert report.history_supported is True
        assert report.instruments == 1
        assert report.fetched_bars == 0
        assert report.written_bars == 0
        assert report.skipped_bars == 0
        assert report.gap_events == 1
        assert report.written_by_interval == {}

        events = health.gap_events()
        assert len(events) == 1
        assert events[0].reason == "backfill_limit_exceeded"
        assert events[0].feed == FEED
        assert events[0].from_ts == at(0)
        assert events[0].to_ts == at(0, hour=1)


def test_backfill_report_counts_history_unsupported_per_instrument(
    tmp_path: Path,
) -> None:
    health = HealthTracker()
    with open_dataset(tmp_path) as (store, hwm):
        report = _run(
            store,
            hwm,
            StaticHistoryFeed(
                supports_history=False,
                instrument_ids=(INSTRUMENT, OTHER_INSTRUMENT),
            ),
            health=health,
        )

        assert report.history_supported is False
        assert report.instruments == 2
        assert report.fetched_bars == 0
        assert report.written_bars == 0
        assert report.skipped_bars == 0
        assert report.gap_events == 2
        assert report.written_by_interval == {}

        events = health.gap_events()
        assert {event.reason for event in events} == {"history_unsupported"}
        assert {event.instrument_id for event in events} == {
            INSTRUMENT,
            OTHER_INSTRUMENT,
        }


# -- Dataset write failure injection ---------------------------------------


def test_automatic_write_failure_leaves_hwm_unadvanced_and_earlier_writes(
    tmp_path: Path,
) -> None:
    store = FailingBarStore(
        tmp_path / "bars",
        predicate=lambda bars: any(
            bar.instrument_id == OTHER_INSTRUMENT for bar in bars
        ),
        message="injected automatic Dataset write failure",
    )
    hwm = HighWaterMarkStore(tmp_path / "bars" / "state.db")
    history = {
        INSTRUMENT: [raw_bar(at(minute)) for minute in range(5)],
        OTHER_INSTRUMENT: [
            raw_bar(at(minute), instrument_id=OTHER_INSTRUMENT) for minute in range(5)
        ],
    }
    feed = StaticHistoryFeed(
        history=history,
        instrument_ids=(INSTRUMENT, OTHER_INSTRUMENT),
    )
    try:
        with pytest.raises(RuntimeError, match="injected automatic"):
            _run(store, hwm, feed)

        assert store.failures == 1
        # The instrument completed before the failure is not rolled back.
        assert count_rows(store, "1m") == 5
        assert count_rows(store, "5m") == 1
        assert hwm.get(INSTRUMENT, "1m") == datetime_to_ns(at(4))
        assert hwm.get(INSTRUMENT, "5m") == datetime_to_ns(at(0))
        # The failed instrument wrote nothing and its HWM stayed unset.
        assert count_rows(store, "1m", instrument_id=OTHER_INSTRUMENT) == 0
        assert count_rows(store, "5m", instrument_id=OTHER_INSTRUMENT) == 0
        for interval in BASE_INTERVALS:
            assert hwm.get(OTHER_INSTRUMENT, interval) is None
    finally:
        hwm.close()
        store.close()


def test_job_write_failure_keeps_earlier_chunk_and_success_counters(
    tmp_path: Path,
) -> None:
    store = FailingBarStore(
        tmp_path / "bars",
        predicate=lambda bars: any(
            datetime_to_ns(bar.open_ts) >= datetime_to_ns(at(0, day=2)) for bar in bars
        ),
        message="injected Job Dataset write failure",
    )
    hwm = HighWaterMarkStore(tmp_path / "bars" / "state.db")
    feed = StaticHistoryFeed(
        history=[raw_bar(at(minute, day=day)) for day in (1, 2) for minute in range(60)]
    )
    core = CollectorCore(
        feed_handler=feed,
        config=config(),
        store=store,
        asset_class_for=lambda _instrument_id: "crypto",
    )
    core.hwm = hwm
    runner = BackfillJobRunner(
        core=core,
        config=config(),
        clock=lambda: datetime_to_ns(at(10, day=3)),
    )
    runner.start()
    try:
        snapshot = _start_job(
            runner,
            "bf_write_failure",
            start_ns=datetime_to_ns(at(0, day=1)),
            end_ns=datetime_to_ns(at(0, day=3)),
        )

        assert snapshot["state"] == "failed"
        assert snapshot["error"]["code"] == "backfill_failed"
        assert snapshot["counters"] == {
            "scanned_bars": 2880,
            "gap_bars": 2760,
            "backfilled_bars": 120,
            "written_bars": 77,
            "rejected_bars": 0,
        }
        assert store.failures == 1
        # Day one is durable; day two was never written or counted.
        assert count_rows(store, "1m") == 60
        assert count_rows(store, "5m") == 12
        assert count_rows(store, "15m") == 4
        assert count_rows(store, "1h") == 1
        assert hwm.get(INSTRUMENT, "1m") == datetime_to_ns(at(59, day=1))
        assert hwm.get(INSTRUMENT, "5m") == datetime_to_ns(at(55, day=1))
        assert hwm.get(INSTRUMENT, "15m") == datetime_to_ns(at(45, day=1))
        assert hwm.get(INSTRUMENT, "1h") == datetime_to_ns(at(0, day=1))
    finally:
        runner.close()
        hwm.close()
        store.close()


# -- persistence side effects ----------------------------------------------


def test_automatic_backfill_has_no_job_only_persistence_side_effects(
    tmp_path: Path,
) -> None:
    raws = [raw_bar(at(minute)) for minute in range(10)]
    feed = StaticHistoryFeed(history=raws, name=FEED, supports_history=False)
    runtime = GatewayRuntime(
        feed_handler=feed,
        config=config(backfill_hours=24),
        data_root=tmp_path,
    )
    runtime.start()
    try:
        feed.supports_history = True
        report = runtime.backfill(now=at(10))

        # The data really was written...
        assert report.written_bars == 12
        assert runtime.high_water_mark(INSTRUMENT, "1m") == datetime_to_ns(at(9))
        assert runtime.high_water_mark(INSTRUMENT, "5m") == datetime_to_ns(at(5))
        # ...but automatic persistence never runs the Job's Core callback.
        assert runtime._core.bars_written == 0
        tracked = runtime.health(now=at(10))["tracked"]
        assert {
            (entry["feed"], entry["instrument_id"], entry["interval"])
            for entry in tracked
        } == {(FEED, INSTRUMENT, interval) for interval in BASE_INTERVALS}
        assert all(entry["last_event_ts"] is None for entry in tracked)
        # Automatic rejection is fail-fast, so no Quarantine record exists.
        assert runtime.quarantine.read_records().num_rows == 0
    finally:
        runtime.stop()


def test_job_persistence_keeps_core_counters_health_and_expected_keys(
    tmp_path: Path,
) -> None:
    with open_dataset(tmp_path) as (store, hwm):
        feed = StaticHistoryFeed(history=[raw_bar(at(minute)) for minute in range(60)])
        health = RecordingHealth()
        core = CollectorCore(
            feed_handler=feed,
            config=config(),
            store=store,
            health=health,
            asset_class_for=lambda _instrument_id: "crypto",
        )
        core.hwm = hwm
        runner = BackfillJobRunner(
            core=core,
            config=config(),
            clock=lambda: datetime_to_ns(at(10, hour=1)),
        )
        runner.start()
        try:
            snapshot = _start_job(
                runner,
                "bf_side_effects",
                start_ns=datetime_to_ns(at(0)),
                end_ns=datetime_to_ns(at(0, hour=1)),
            )
        finally:
            runner.close()

        assert snapshot["state"] == "succeeded"
        assert snapshot["counters"]["written_bars"] == 77
        # Job persistence delegates to the Core callback: counters, per-Bar
        # health and expected-key registration all advance.
        assert core.bars_written == 77
        assert len(health.recorded) == 77
        assert sum(1 for bar in health.recorded if bar.interval == "1m") == 60
        tracked = health.snapshot(now=at(0, hour=2))["tracked"]
        assert {
            (entry["feed"], entry["instrument_id"], entry["interval"])
            for entry in tracked
        } == {(FEED, INSTRUMENT, interval) for interval in BASE_INTERVALS}
        assert {entry["interval"]: entry["last_event_ts"] for entry in tracked} == {
            interval: iso_at(0, hour=1) for interval in BASE_INTERVALS
        }
        assert hwm.get(INSTRUMENT, "1m") == datetime_to_ns(at(59))
        assert hwm.get(INSTRUMENT, "5m") == datetime_to_ns(at(55))
        assert hwm.get(INSTRUMENT, "15m") == datetime_to_ns(at(45))
        assert hwm.get(INSTRUMENT, "1h") == datetime_to_ns(at(0))
        assert hwm.get(INSTRUMENT, "1d") is None


def test_job_hwm_is_capped_below_a_bar_dropped_by_the_write_queue(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path / "bars")
    hwm = HighWaterMarkStore(tmp_path / "bars" / "state.db")
    # A queue of two that never flushes: ingesting five 1m Bars drops the
    # oldest ones (minutes 10-13) and records them as dropped identities.
    sink = BufferedBarSink(
        store,
        queue_size=2,
        flush_max_bytes=1_000_000_000,
        flush_max_age_s=1_000_000,
    )
    feed = FakeFeedHandler(
        instrument_id=INSTRUMENT,
        bars=[bar_input(minute) for minute in range(10, 15)],
        history=[bar_input(minute) for minute in range(30)],
        name=FEED,
        supports_history=True,
    )
    core = CollectorCore(
        feed_handler=feed,
        config=config(),
        store=store,
        asset_class_for=lambda _instrument_id: "crypto",
        sink=sink,
    )
    core.hwm = hwm
    runner = BackfillJobRunner(
        core=core,
        config=config(),
        clock=lambda: datetime_to_ns(at(30)),
    )
    runner.start()
    try:
        core.ingest()
        assert sink.stats()["dropped_bars"] >= 1
        assert hwm.get(INSTRUMENT, "1m") is None

        snapshot = _start_job(
            runner,
            "bf_hwm_cap",
            start_ns=datetime_to_ns(at(0)),
            end_ns=datetime_to_ns(at(30)),
        )

        assert snapshot["state"] == "succeeded"
        assert snapshot["counters"]["written_bars"] == 38
        # The Job wrote through minute 29, but the earliest queue-dropped
        # identity was minute 10, so the HWM must stay at minute 9.
        assert datetime_to_ns(at(29)) in rows_by_open(store, "1m")
        assert hwm.get(INSTRUMENT, "1m") == datetime_to_ns(at(9))
        assert hwm.get(INSTRUMENT, "1m") < datetime_to_ns(at(10))
        assert sink.stats()["dropped_bars"] >= 1
    finally:
        runner.close()
        core.close_state()
        store.close()
