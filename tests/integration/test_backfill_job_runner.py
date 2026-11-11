# ruff: file-ignore[implicit-namespace-package,no-self-use,unused-method-argument]
"""Collector Backfill Job runner over a real temporary Dataset.

The Collector is the only Dataset writer: these tests build a real
``ParquetBarStore`` and ``HighWaterMarkStore`` under ``tmp_path``, feed the
runner through the existing Feed Handler history seam, and observe only the
store and the runner's public status document. The Admin API and its SQLite
store are not involved here.
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from mdgw.config import load_config
from mdgw.core.bar import RawBar
from mdgw.core.time import datetime_to_ns
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.service.backfill_jobs import BackfillJobRunner
from mdgw.service.collector_core import CollectorCore
from mdgw.state.high_water_mark import HighWaterMarkStore
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INSTRUMENT = "binance:BTC/USDT"
DAY = datetime(2024, 1, 1, tzinfo=UTC)
TERMINAL = {
    "succeeded",
    "partially_succeeded",
    "failed",
    "cancelled",
}


def _config():
    return load_config(PROJECT_ROOT / "config" / "gateway.toml")


def _at(minute: int = 0, hour: int = 0, day: int = 1) -> datetime:
    return datetime(2024, 1, day, hour, minute, tzinfo=UTC)


def _bar_input(
    minute: int = 0,
    *,
    hour: int = 0,
    close: str = "100",
    revision: int = 0,
) -> BarInput:
    return BarInput(
        open_ts=_at(minute, hour),
        open="100",
        high="101",
        low="99",
        close=close,
        volume="1",
        trade_count=1,
        revision=revision,
    )


class RecordingHistoryFeed(FakeFeedHandler):
    """Fake handler that records every requested interval.

    It fails the test if a Backfill Job ever asks for a native high-interval
    history: the plan must always fetch 1m and aggregate internally.
    """

    def __init__(self, *, history: Sequence[BarInput]) -> None:
        super().__init__(instrument_id=INSTRUMENT, bars=(), history=history)
        self.requested: list[tuple[str, int, int]] = []

    def fetch_history(
        self,
        instrument_id: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> Iterable[RawBar]:
        self.requested.append(
            (interval, datetime_to_ns(start), datetime_to_ns(end))
        )
        if interval != "1m":
            msg = f"Backfill Job asked for a native {interval} history"
            raise AssertionError(msg)
        return super().fetch_history(instrument_id, interval, start, end)


class FailingHistoryFeed:
    """A history-capable Feed Handler whose fetch raises on demand."""

    name = "failing"
    instrument_ids = (INSTRUMENT,)
    supports_history = True

    def __init__(self) -> None:
        self.error: Exception = RuntimeError("venue history unavailable")

    def iter_bars(self) -> Iterator[RawBar]:
        return iter(())

    def feed_status(self):
        return ()

    def fetch_history(
        self,
        instrument_id: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> Iterable[RawBar]:
        raise self.error


class ScriptedHistoryFeed:
    """A history-only Feed Handler that can block between chunks."""

    name = "scripted"
    instrument_ids = (INSTRUMENT,)
    supports_history = True

    def __init__(
        self,
        history: Sequence[RawBar] | None = None,
        *,
        factory=None,
    ) -> None:
        self._history = tuple(history or ())
        self._factory = factory
        self.requested: list[tuple[str, int, int]] = []
        self.active = 0
        self.max_active = 0
        self.fetch_calls = 0
        self.block_from_ns: int | None = None
        self.release = None

    def iter_bars(self) -> Iterator[RawBar]:
        return iter(())

    def feed_status(self):
        return ()

    def fetch_history(
        self,
        instrument_id: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> Iterable[RawBar]:
        start_ns = datetime_to_ns(start)
        end_ns = datetime_to_ns(end)
        self.requested.append((interval, start_ns, end_ns))
        self.fetch_calls += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if (
                self.block_from_ns is not None
                and start_ns >= self.block_from_ns
                and self.release is not None
            ):
                self.release.wait(timeout=10)
            if self._factory is not None:
                return tuple(self._factory(start_ns, end_ns))
            return tuple(
                bar
                for bar in self._history
                if start_ns <= datetime_to_ns(bar.open_ts) < end_ns
            )
        finally:
            self.active -= 1


@pytest.fixture
def dataset(tmp_path: Path):
    store = ParquetBarStore(tmp_path / "bars")
    hwm = HighWaterMarkStore(tmp_path / "bars" / "state.db")
    try:
        yield store, hwm
    finally:
        hwm.close()
        store.close()


def _core(feed, dataset) -> CollectorCore:
    store, hwm = dataset
    core = CollectorCore(
        feed_handler=feed,
        config=_config(),
        store=store,
        asset_class_for=lambda _instrument_id: "crypto",
    )
    core.hwm = hwm
    return core


def _start_runner(core) -> BackfillJobRunner:
    runner = BackfillJobRunner(core=core, config=_config())
    runner.start()
    return runner


def _wait(runner: BackfillJobRunner, job_id: str) -> dict:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        snapshot = runner.status(job_id)
        if snapshot["state"] in TERMINAL:
            return snapshot
        time.sleep(0.01)
    msg = f"job did not finish: {runner.status(job_id)}"
    raise AssertionError(msg)


def _rows(store: ParquetBarStore, interval: str) -> list[dict]:
    return store.read_bars(
        instrument_id=INSTRUMENT, interval=interval
    ).to_pylist()


def _by_open(store: ParquetBarStore, interval: str = "1m") -> dict[int, list]:
    grouped: dict[int, list] = {}
    for row in _rows(store, interval):
        grouped.setdefault(
            datetime_to_ns(row["open_ts"]), []
        ).append(row)
    return grouped


def test_one_minute_job_writes_bars_and_internal_parents(dataset) -> None:
    store, hwm = dataset
    feed = RecordingHistoryFeed(
        history=[_bar_input(minute) for minute in range(60)]
    )
    runner = _start_runner(_core(feed, dataset))
    try:
        runner.start_job(
            job_id="bf_native_1m",
            instrument_id=INSTRUMENT,
            interval="1m",
            start_ns=datetime_to_ns(_at(0)),
            end_ns=datetime_to_ns(_at(0, hour=1)),
            mode="native_interval",
        )
        snapshot = _wait(runner, "bf_native_1m")
    finally:
        runner.close()

    assert snapshot["state"] == "succeeded"
    assert snapshot["counters"] == {
        "scanned_bars": 60,
        "gap_bars": 0,
        "backfilled_bars": 60,
        "written_bars": 77,
        "rejected_bars": 0,
    }
    assert {interval for interval, _s, _e in feed.requested} == {"1m"}

    assert len(_rows(store, "1m")) == 60
    assert len(_rows(store, "5m")) == 12
    assert len(_rows(store, "15m")) == 4
    assert len(_rows(store, "1h")) == 1
    assert _rows(store, "1d") == []

    # HWM never lowers and advances only after the durable write.
    assert hwm.get(INSTRUMENT, "1m") == datetime_to_ns(_at(59))
    assert hwm.get(INSTRUMENT, "5m") == datetime_to_ns(_at(55))
    assert hwm.get(INSTRUMENT, "15m") == datetime_to_ns(_at(45))
    assert hwm.get(INSTRUMENT, "1h") == datetime_to_ns(_at(0))


def test_higher_interval_job_fetches_only_one_minute_and_aggregates(dataset) -> None:
    store, hwm = dataset
    feed = RecordingHistoryFeed(
        history=[_bar_input(minute) for minute in range(60)]
    )
    runner = _start_runner(_core(feed, dataset))
    try:
        runner.start_job(
            job_id="bf_aggregate_1h",
            instrument_id=INSTRUMENT,
            interval="1h",
            start_ns=datetime_to_ns(_at(0)),
            end_ns=datetime_to_ns(_at(0, hour=1)),
            mode="one_minute_plus_aggregation",
        )
        snapshot = _wait(runner, "bf_aggregate_1h")
    finally:
        runner.close()

    assert snapshot["state"] == "succeeded"
    assert {interval for interval, _s, _e in feed.requested} == {"1m"}
    assert snapshot["counters"]["written_bars"] == 77
    assert len(_rows(store, "1m")) == 60
    assert len(_rows(store, "1h")) == 1
    assert hwm.get(INSTRUMENT, "1h") == datetime_to_ns(_at(0))


def test_repeated_job_is_idempotent_and_leaves_data_files_unchanged(dataset) -> None:
    store, _hwm = dataset
    feed = RecordingHistoryFeed(
        history=[_bar_input(minute) for minute in range(60)]
    )
    runner = _start_runner(_core(feed, dataset))
    try:
        runner.start_job(
            job_id="bf_first",
            instrument_id=INSTRUMENT,
            interval="1m",
            start_ns=datetime_to_ns(_at(0)),
            end_ns=datetime_to_ns(_at(0, hour=1)),
            mode="native_interval",
        )
        assert _wait(runner, "bf_first")["state"] == "succeeded"
        data_files = sorted((store.root / "bars").rglob("*.parquet"))
        before = {path: path.read_bytes() for path in data_files}

        runner.start_job(
            job_id="bf_second",
            instrument_id=INSTRUMENT,
            interval="1m",
            start_ns=datetime_to_ns(_at(0)),
            end_ns=datetime_to_ns(_at(0, hour=1)),
            mode="native_interval",
        )
        snapshot = _wait(runner, "bf_second")
    finally:
        runner.close()

    observed = {
        path: path.read_bytes()
        for path in sorted((store.root / "bars").rglob("*.parquet"))
    }
    assert snapshot["state"] == "succeeded"
    assert snapshot["counters"]["backfilled_bars"] == 60
    assert snapshot["counters"]["written_bars"] == 0
    assert observed == before


def test_a_job_never_lowers_an_existing_high_water_mark(dataset) -> None:
    store, hwm = dataset
    later = datetime_to_ns(_at(0, hour=10))
    hwm.set(INSTRUMENT, "1m", later)
    feed = RecordingHistoryFeed(
        history=[_bar_input(minute) for minute in range(60)]
    )
    runner = _start_runner(_core(feed, dataset))
    try:
        runner.start_job(
            job_id="bf_hwm",
            instrument_id=INSTRUMENT,
            interval="1m",
            start_ns=datetime_to_ns(_at(0)),
            end_ns=datetime_to_ns(_at(0, hour=1)),
            mode="native_interval",
        )
        snapshot = _wait(runner, "bf_hwm")
    finally:
        runner.close()

    assert snapshot["state"] == "succeeded"
    # The HWM is monotonic: an older Backfill range is still written, but it
    # must never move the water mark backwards.
    assert hwm.get(INSTRUMENT, "1m") == later
    assert len(_rows(store, "1m")) == 60


def test_a_corrected_source_bar_creates_a_higher_revision(dataset) -> None:
    store, hwm = dataset
    feed = RecordingHistoryFeed(
        history=[_bar_input(minute) for minute in range(60)]
    )
    runner = _start_runner(_core(feed, dataset))
    try:
        runner.start_job(
            job_id="bf_original",
            instrument_id=INSTRUMENT,
            interval="1m",
            start_ns=datetime_to_ns(_at(0)),
            end_ns=datetime_to_ns(_at(0, hour=1)),
            mode="native_interval",
        )
        assert _wait(runner, "bf_original")["state"] == "succeeded"
        corrected = RecordingHistoryFeed(
            history=[
                BarInput(
                    open_ts=_at(minute),
                    open="100",
                    high="300" if minute == 5 else "101",
                    low="99",
                    close="200" if minute == 5 else "100",
                    volume="1",
                    trade_count=1,
                    revision=0,
                )
                for minute in range(60)
            ]
        )
        runner2 = _start_runner(_core(corrected, dataset))
        try:
            runner2.start_job(
                job_id="bf_corrected",
                instrument_id=INSTRUMENT,
                interval="1m",
                start_ns=datetime_to_ns(_at(0)),
                end_ns=datetime_to_ns(_at(0, hour=1)),
                mode="native_interval",
            )
            snapshot = _wait(runner2, "bf_corrected")
        finally:
            runner2.close()
    finally:
        runner.close()

    assert snapshot["state"] == "succeeded"
    by_open = _by_open(store, "1m")
    corrected_rows = by_open[datetime_to_ns(_at(5))]
    assert sorted(int(row["revision"]) for row in corrected_rows) == [0, 1]
    latest_revision = max(
        corrected_rows, key=lambda row: int(row["revision"])
    )
    assert latest_revision["close"] == Decimal(200)
    assert len(_rows(store, "1m")) == 61
    # The 5m parent window containing the corrected Bar is rebuilt too.
    assert sorted(int(row["revision"]) for row in _rows(store, "5m")) == [
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
    ]
    assert hwm.get(INSTRUMENT, "1m") == datetime_to_ns(_at(59))


def test_missing_and_rejected_rows_make_the_job_partially_succeed(dataset) -> None:
    store, _hwm = dataset
    missing = {10, 11}

    def history(_start_ns: int, _end_ns: int) -> list[RawBar]:
        bars: list[RawBar] = []
        for minute in range(60):
            if minute in missing:
                continue
            close: object = 100.0 if minute == 20 else Decimal(100)
            bars.append(
                RawBar(
                    instrument_id=INSTRUMENT,
                    interval="1m",
                    open_ts=_at(minute),
                    open=Decimal(100),
                    high=Decimal(101),
                    low=Decimal(99),
                    close=close,  # float is rejected by Normalization
                    volume=Decimal(1),
                    trade_count=1,
                    revision=0,
                )
            )
        return bars

    feed = ScriptedHistoryFeed(factory=history)
    runner = _start_runner(_core(feed, dataset))
    try:
        runner.start_job(
            job_id="bf_partial",
            instrument_id=INSTRUMENT,
            interval="1m",
            start_ns=datetime_to_ns(_at(0)),
            end_ns=datetime_to_ns(_at(0, hour=1)),
            mode="native_interval",
        )
        snapshot = _wait(runner, "bf_partial")
    finally:
        runner.close()

    assert snapshot["state"] == "partially_succeeded"
    assert snapshot["counters"]["scanned_bars"] == 60
    assert snapshot["counters"]["gap_bars"] == 2
    assert snapshot["counters"]["rejected_bars"] == 1
    assert snapshot["counters"]["backfilled_bars"] == 57
    assert len(_rows(store, "1m")) == 57


def test_terminal_snapshot_exposes_feed_and_observed_one_minute_gap_ranges(
    dataset,
) -> None:
    missing = {10, 11, 20}

    def history(_start_ns: int, _end_ns: int) -> list[RawBar]:
        return [
            RawBar(
                instrument_id=INSTRUMENT,
                interval="1m",
                open_ts=_at(minute),
                open=Decimal(100),
                high=Decimal(101),
                low=Decimal(99),
                close=Decimal(100),
                volume=Decimal(1),
                trade_count=1,
                revision=0,
            )
            for minute in range(60)
            if minute not in missing
        ]

    feed = ScriptedHistoryFeed(factory=history)
    runner = _start_runner(_core(feed, dataset))
    try:
        runner.start_job(
            job_id="bf_gap_ranges",
            instrument_id=INSTRUMENT,
            interval="1m",
            start_ns=datetime_to_ns(_at(0)),
            end_ns=datetime_to_ns(_at(0, hour=1)),
            mode="native_interval",
        )
        snapshot = _wait(runner, "bf_gap_ranges")
    finally:
        runner.close()

    assert snapshot["state"] == "partially_succeeded"
    # The terminal snapshot must carry the real Feed name and the observed
    # missing 1m ranges as half-open [start_ns, end_ns]. The Runner never
    # resolves Gaps itself; Admin reconciliation is the only producer of ids.
    assert snapshot["feed"] == "scripted"
    assert snapshot["resolved_gap_ids"] == []
    assert snapshot["gap_ranges"] == [
        [datetime_to_ns(_at(10)), datetime_to_ns(_at(12))],
        [datetime_to_ns(_at(20)), datetime_to_ns(_at(21))],
    ]


def test_history_unsupported_fails_without_faking_success(dataset) -> None:
    store, _hwm = dataset
    feed = FakeFeedHandler(
        instrument_id=INSTRUMENT,
        bars=(),
        supports_history=False,
    )
    runner = _start_runner(_core(feed, dataset))
    try:
        runner.start_job(
            job_id="bf_unsupported",
            instrument_id=INSTRUMENT,
            interval="1h",
            start_ns=datetime_to_ns(_at(0)),
            end_ns=datetime_to_ns(_at(0, hour=1)),
            mode="one_minute_plus_aggregation",
        )
        snapshot = _wait(runner, "bf_unsupported")
    finally:
        runner.close()

    assert snapshot["state"] == "failed"
    assert snapshot["error"]["code"] == "history_unsupported"
    assert snapshot["counters"]["written_bars"] == 0
    assert _rows(store, "1m") == []


def test_history_fetch_failures_keep_the_stable_error_document(dataset) -> None:
    feed = FailingHistoryFeed()
    runner = _start_runner(_core(feed, dataset))
    try:
        runner.start_job(
            job_id="bf_fetch_failed",
            instrument_id=INSTRUMENT,
            interval="1m",
            start_ns=datetime_to_ns(_at(0)),
            end_ns=datetime_to_ns(_at(0, hour=1)),
            mode="native_interval",
        )
        failed = _wait(runner, "bf_fetch_failed")
        assert failed["state"] == "failed"
        assert failed["error"] == {
            "code": "backfill_failed",
            "message": "the Backfill history fetch failed",
            "details": {},
        }

        feed.error = NotImplementedError("no history source")
        runner.start_job(
            job_id="bf_fetch_unsupported",
            instrument_id=INSTRUMENT,
            interval="1h",
            start_ns=datetime_to_ns(_at(0)),
            end_ns=datetime_to_ns(_at(0, hour=1)),
            mode="one_minute_plus_aggregation",
        )
        unsupported = _wait(runner, "bf_fetch_unsupported")
        assert unsupported["state"] == "failed"
        assert unsupported["error"] == {
            "code": "history_unsupported",
            "message": "the Feed Handler cannot serve Backfill history",
            "details": {},
        }
    finally:
        runner.close()


def test_cancellation_between_chunks_keeps_written_revisions(dataset) -> None:
    store, _hwm = dataset
    release = threading.Event()
    day_two = datetime_to_ns(_at(0, day=2))
    feed = ScriptedHistoryFeed(history=[])
    feed.block_from_ns = day_two
    feed.release = release
    feed._factory = None
    feed._history = tuple(
        RawBar(
            instrument_id=INSTRUMENT,
            interval="1m",
            open_ts=_at(minute, day=day),
            open=Decimal(100),
            high=Decimal(101),
            low=Decimal(99),
            close=Decimal(100),
            volume=Decimal(1),
            trade_count=1,
            revision=0,
        )
        for day in (1, 2)
        for minute in range(60)
    )
    runner = _start_runner(_core(feed, dataset))
    try:
        runner.start_job(
            job_id="bf_cancel",
            instrument_id=INSTRUMENT,
            interval="1m",
            start_ns=datetime_to_ns(_at(0, day=1)),
            end_ns=datetime_to_ns(_at(0, day=3)),
            mode="native_interval",
        )
        deadline = time.monotonic() + 15
        while feed.fetch_calls < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert feed.fetch_calls >= 2

        cancelled = runner.cancel("bf_cancel")
        release.set()
        snapshot = _wait(runner, "bf_cancel")
    finally:
        release.set()
        runner.close()

    assert cancelled["state"] == "cancelling"
    assert snapshot["state"] == "cancelled"
    # Day 1 was already durably written and is never retracted.
    assert len(_rows(store, "1m")) == 60


def test_the_same_instrument_and_interval_never_runs_twice_at_once(dataset) -> None:
    release = threading.Event()
    feed = ScriptedHistoryFeed(history=[])
    feed.block_from_ns = 0
    feed.release = release
    runner = _start_runner(_core(feed, dataset))
    try:
        runner.start_job(
            job_id="bf_concurrent_a",
            instrument_id=INSTRUMENT,
            interval="1m",
            start_ns=datetime_to_ns(_at(0)),
            end_ns=datetime_to_ns(_at(0, hour=1)),
            mode="native_interval",
        )
        deadline = time.monotonic() + 15
        while feed.fetch_calls < 1 and time.monotonic() < deadline:
            time.sleep(0.01)
        second = runner.start_job(
            job_id="bf_concurrent_b",
            instrument_id=INSTRUMENT,
            interval="1m",
            start_ns=datetime_to_ns(_at(0)),
            end_ns=datetime_to_ns(_at(0, hour=1)),
            mode="native_interval",
        )
        release.set()
        _wait(runner, "bf_concurrent_a")
    finally:
        release.set()
        runner.close()

    assert second["accepted"] is False
    assert second["reason"] == "active_job"
    assert second["active_job_id"] == "bf_concurrent_a"
    assert feed.max_active == 1


def test_status_for_an_unknown_job_is_unknown(dataset) -> None:
    runner = _start_runner(_core(ScriptedHistoryFeed(), dataset))
    try:
        assert runner.status("bf_missing")["state"] == "unknown"
        assert runner.cancel("bf_missing")["state"] == "unknown"
    finally:
        runner.close()
