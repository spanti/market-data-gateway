# ruff: file-ignore[implicit-namespace-package]
"""Cross-entry Backfill differential and rejection-policy characterization.

Ticket 01 freezes the CURRENT implementation before it is refactored, so there
is no natural red step: these tests must pass against the old code. Drift
detection is instead demonstrated with deliberate local mutations of the
automatic and Job orchestration (recorded in the ticket report); every
expectation below is an independent literal rather than a recomputation.

The differential uses the same InstrumentId, the same history, a fixed clock
and equivalent initial Datasets. It compares only logical Bars (all revisions,
full content minus `ingest_ts`), parent Bars and the final HWM. It deliberately
does not compare Data File names, UUIDs, physical file counts, BackfillReport
fields, Job counters, health, gap types or terminal state.
"""

from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

from mdgw.core.time import datetime_to_ns
from mdgw.normalize.batch import BatchNormalizationError
from mdgw.runtime import GatewayRuntime
from mdgw.service.backfill_jobs import BackfillJobRunner
from mdgw.service.collector_core import CollectorCore
from mdgw.state.high_water_mark import HighWaterMarkStore
from mdgw.store.parquet_dataset import ParquetBarStore
from tests.characterization.backfill_support import (
    BASE_INTERVALS,
    FEED,
    INSTRUMENT,
    TERMINAL_STATES,
    StaticHistoryFeed,
    at,
    config,
    count_rows,
    logical_bars,
    raw_bar,
    rows_by_open,
)

if TYPE_CHECKING:
    from pathlib import Path


def _history(*, corrected_minute: int | None = None) -> list:
    raws = []
    for minute in range(10):
        if minute == corrected_minute:
            raws.append(raw_bar(at(minute), high="200", close="200"))
        else:
            raws.append(raw_bar(at(minute)))
    return raws


def _wait(runner: BackfillJobRunner, job_id: str) -> dict:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        snapshot = runner.status(job_id)
        if snapshot["state"] in TERMINAL_STATES:
            return snapshot
        time.sleep(0.01)
    msg = f"job did not finish: {runner.status(job_id)}"
    raise AssertionError(msg)


class _JobEntry:
    """A real Backfill Job entry over its own temporary Dataset."""

    def __init__(self, root: Path, history: list) -> None:
        self.feed = StaticHistoryFeed(history=history, name=FEED)
        self.store = ParquetBarStore(root / "bars")
        self.hwm = HighWaterMarkStore(root / "bars" / "state.db")
        self.core = CollectorCore(
            feed_handler=self.feed,
            config=config(),
            store=self.store,
            asset_class_for=lambda _instrument_id: "crypto",
        )
        self.core.hwm = self.hwm
        self._start_ns = datetime_to_ns(at(0))
        self._end_ns = datetime_to_ns(at(10))
        self.runner = BackfillJobRunner(
            core=self.core,
            config=config(),
            clock=lambda: datetime_to_ns(at(10)),
        )
        self.runner.start()

    def run(self, job_id: str) -> dict:
        accepted = self.runner.start_job(
            job_id=job_id,
            instrument_id=INSTRUMENT,
            interval="1m",
            start_ns=self._start_ns,
            end_ns=self._end_ns,
            mode="native_interval",
        )
        assert accepted["accepted"] is True, accepted
        return _wait(self.runner, job_id)

    def set_history(self, history: list) -> None:
        self.feed.set_history(history)

    def close(self) -> None:
        self.runner.close()
        self.hwm.close()
        self.store.close()


def _latest(rows: list[dict]) -> dict:
    return max(rows, key=lambda row: int(row["revision"]))


def test_automatic_and_job_entries_produce_the_same_logical_dataset(  # ruff: ignore[too-many-statements,too-many-locals]
    tmp_path: Path,
) -> None:
    raws = _history()
    auto_feed = StaticHistoryFeed(history=raws, name=FEED, supports_history=False)
    runtime = GatewayRuntime(
        feed_handler=auto_feed,
        config=config(backfill_hours=24),
        data_root=tmp_path / "auto",
    )
    runtime.start()
    job = _JobEntry(tmp_path / "job", raws)
    try:
        # Startup intentionally ran with history unsupported so the fixed
        # clock below owns the automatic window.
        auto_feed.supports_history = True

        first_report = runtime.backfill(now=at(10))
        first_job = job.run("bf_diff_first")

        # Independent literals for the first write: 10 base Bars and the two
        # complete 5m parents, with parents included in the report count.
        assert first_report.written_bars == 12
        assert count_rows(runtime.store, "1m") == 10
        assert count_rows(runtime.store, "5m") == 2
        assert first_job["state"] == "succeeded"
        assert first_job["counters"]["written_bars"] == 12
        assert logical_bars(runtime.store) == logical_bars(job.store)
        assert runtime.high_water_marks() == job.hwm.all()
        expected_hwm = {
            (INSTRUMENT, "1m"): datetime_to_ns(at(9)),
            (INSTRUMENT, "5m"): datetime_to_ns(at(5)),
        }
        assert runtime.high_water_marks() == expected_hwm

        # Identical-content rerun: logical rows, Revisions and HWM unchanged.
        before = logical_bars(runtime.store)
        rerun_report = runtime.backfill(now=at(10))
        rerun_job = job.run("bf_diff_rerun")
        assert rerun_report.written_bars == 0
        assert rerun_job["state"] == "succeeded"
        assert rerun_job["counters"]["written_bars"] == 0
        assert logical_bars(runtime.store) == before
        assert logical_bars(job.store) == before
        assert runtime.high_water_marks() == expected_hwm
        assert job.hwm.all() == expected_hwm

        # Source correction of the last (HWM) Bar: both entries append base
        # and parent revision 1; everything else stays identical.
        corrected = _history(corrected_minute=9)
        auto_feed.set_history(corrected)
        job.set_history(corrected)
        runtime.backfill(now=at(10))
        corrected_job = job.run("bf_diff_corrected")
        assert corrected_job["state"] == "succeeded"
        assert logical_bars(runtime.store) == logical_bars(job.store)
        assert runtime.high_water_marks() == job.hwm.all()
        assert runtime.high_water_marks() == expected_hwm

        for store in (runtime.store, job.store):
            base_rows = rows_by_open(store, "1m")
            base_key = datetime_to_ns(at(9))
            assert sorted(int(row["revision"]) for row in base_rows[base_key]) == [0, 1]
            assert _latest(base_rows[base_key])["close"] == Decimal(200)

            parent_rows = rows_by_open(store, "5m")
            parent_key = datetime_to_ns(at(5))
            assert sorted(int(row["revision"]) for row in parent_rows[parent_key]) == [
                0,
                1,
            ]
            parent = _latest(parent_rows[parent_key])
            assert parent["close"] == Decimal(200)
            assert parent["high"] == Decimal(200)

            # No other interval gained a Bar in this 10-minute window.
            for interval in ("15m", "1h", "1d"):
                assert count_rows(store, interval) == 0
    finally:
        job.close()
        runtime.stop()


def test_automatic_backfill_fails_the_whole_batch_on_a_rejection(
    tmp_path: Path,
) -> None:
    raws = [
        raw_bar(at(0)),
        # A float price is rejected by batch Normalization.
        raw_bar(at(1), close=100.0),
    ]
    feed = StaticHistoryFeed(history=raws, name=FEED, supports_history=False)
    runtime = GatewayRuntime(
        feed_handler=feed,
        config=config(backfill_hours=24),
        data_root=tmp_path,
    )
    runtime.start()
    try:
        feed.supports_history = True
        with pytest.raises(BatchNormalizationError) as raised:
            runtime.backfill(now=at(10))

        assert raised.value.reason_codes == ("float_value",)
        # The whole instrument batch is atomic: neither the valid Bar nor any
        # parent was written, the HWM did not move, and no Quarantine record
        # was fabricated.
        for interval in BASE_INTERVALS:
            assert count_rows(runtime.store, interval) == 0
        assert runtime.high_water_marks() == {}
        assert runtime.quarantine.read_records().num_rows == 0
        assert runtime._core.bars_written == 0
    finally:
        runtime.stop()


def test_job_rejections_are_counted_and_accepted_bars_are_still_written(
    tmp_path: Path,
) -> None:
    missing = {10, 11}
    raws = []
    for minute in range(30):
        if minute in missing:
            continue
        if minute == 20:
            # One Validation rejection inside an otherwise complete minute.
            raws.append(raw_bar(at(minute), close=100.0))
        else:
            raws.append(raw_bar(at(minute)))

    store = ParquetBarStore(tmp_path / "bars")
    hwm = HighWaterMarkStore(tmp_path / "bars" / "state.db")
    feed = StaticHistoryFeed(history=raws, name=FEED)
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
        clock=lambda: datetime_to_ns(at(30)),
    )
    runner.start()
    try:
        accepted = runner.start_job(
            job_id="bf_partial_characterization",
            instrument_id=INSTRUMENT,
            interval="1m",
            start_ns=datetime_to_ns(at(0)),
            end_ns=datetime_to_ns(at(30)),
            mode="native_interval",
        )
        assert accepted["accepted"] is True
        snapshot = _wait(runner, "bf_partial_characterization")

        assert snapshot["state"] == "partially_succeeded"
        assert snapshot["counters"] == {
            "scanned_bars": 30,
            "gap_bars": 2,
            "backfilled_bars": 27,
            "written_bars": 31,
            "rejected_bars": 1,
        }
        # Accepted Bars (and the four complete 5m parents) are durable; the
        # two missing minutes and the rejected minute are absent.
        assert count_rows(store, "1m") == 27
        assert count_rows(store, "5m") == 4
        opens = set(rows_by_open(store, "1m"))
        assert datetime_to_ns(at(10)) not in opens
        assert datetime_to_ns(at(11)) not in opens
        assert datetime_to_ns(at(20)) not in opens
        # Partial outcome never writes Quarantine.
        assert core.quarantine.read_records().num_rows == 0
    finally:
        runner.close()
        hwm.close()
        store.close()
