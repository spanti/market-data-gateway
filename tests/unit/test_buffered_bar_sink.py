"""Buffered writer: size/age/stop flush, queue overflow, and HWM ordering."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mdgw.config import load_config
from mdgw.core.bar import Bar
from mdgw.core.time import datetime_to_ns
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.service.bar_sink import BufferedBarSink
from mdgw.service.collector_core import CollectorCore
from mdgw.store.parquet_dataset import ParquetBarStore

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc
INSTRUMENT = "binance:BTC/USDT"
START = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)


def _bar(minute: int, *, close: str = "100") -> Bar:
    open_ts = START + timedelta(minutes=minute)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=INSTRUMENT,
        interval="1m",
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=None,
        aggregation_source="external",
        open=Decimal("100"),
        high=Decimal(close),
        low=Decimal("99"),
        close=Decimal(close),
        volume=Decimal("1"),
        trade_count=1,
        revision=0,
    )


def _bar_input(minute: int) -> BarInput:
    open_ts = START + timedelta(minutes=minute)
    return BarInput(
        open_ts=open_ts,
        open="100",
        high="101",
        low="99",
        close="100",
        volume="1",
        trade_count=1,
    )


def _wait_until(predicate, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def _sink(
    store: ParquetBarStore,
    *,
    queue_size: int = 100,
    flush_max_bytes: int = 100_000,
    flush_max_age_s: float = 3_600,
    persisted: list[Bar] | None = None,
) -> BufferedBarSink:
    sink = BufferedBarSink(
        store,
        queue_size=queue_size,
        flush_max_bytes=flush_max_bytes,
        flush_max_age_s=flush_max_age_s,
    )
    sink.bind(
        on_persisted=(persisted.extend if persisted is not None else lambda bars: None),
        asset_class_for=lambda instrument_id: "crypto",
    )
    return sink


def _core(tmp_path: Path, minutes: tuple[int, ...]) -> CollectorCore:
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    store = ParquetBarStore(tmp_path)
    sink = _sink(store, flush_max_bytes=1_000_000_000, flush_max_age_s=3_600)
    core = CollectorCore(
        feed_handler=FakeFeedHandler(
            instrument_id=INSTRUMENT,
            bars=[_bar_input(minute) for minute in minutes],
        ),
        config=config,
        store=store,
        sink=sink,
    )
    core.open_state()
    return core


# -- flush triggers ---------------------------------------------------------


def test_flushes_when_the_size_threshold_is_reached(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    sink = _sink(store, flush_max_bytes=1, flush_max_age_s=3_600)
    try:
        sink.write_bars([_bar(0), _bar(1)])
        assert _wait_until(lambda: store.count_bars(interval="1m") == 2)
        assert sink.stats()["flushes"] >= 1
    finally:
        sink.close()


def test_flushes_when_the_age_threshold_is_reached(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    sink = _sink(store, flush_max_bytes=1_000_000_000, flush_max_age_s=0.2)
    try:
        sink.write_bars([_bar(0)])
        # The writer waits for the age threshold, not the size threshold.
        assert store.count_bars(interval="1m") == 0
        assert _wait_until(lambda: store.count_bars(interval="1m") == 1, timeout=3)
    finally:
        sink.close()


def test_close_flushes_everything_still_buffered(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    sink = _sink(store, flush_max_bytes=1_000_000_000, flush_max_age_s=3_600)
    sink.write_bars([_bar(0), _bar(1), _bar(2)])
    assert store.count_bars(interval="1m") == 0

    sink.close()

    assert store.count_bars(interval="1m") == 3


def test_zero_thresholds_mean_flush_immediately(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    sink = _sink(store, flush_max_bytes=0, flush_max_age_s=3_600)
    try:
        sink.write_bars([_bar(0)])
        assert _wait_until(lambda: store.count_bars(interval="1m") == 1)
    finally:
        sink.close()


# -- backpressure -----------------------------------------------------------


def test_full_queue_drops_the_oldest_bar_and_counts_lag(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    persisted: list[Bar] = []
    sink = _sink(
        store,
        queue_size=2,
        flush_max_bytes=1_000_000_000,
        flush_max_age_s=3_600,
        persisted=persisted,
    )
    try:
        sink.write_bars([_bar(0), _bar(1), _bar(2)])
        stats = sink.stats()
        assert stats["queue_depth"] == 2
        assert stats["dropped_bars"] == 1
        assert stats["lag_bars"] == 1

        sink.flush()
        assert [bar.open_ts for bar in persisted] == [
            START + timedelta(minutes=1),
            START + timedelta(minutes=2),
        ]
        assert store.count_bars(interval="1m") == 2
    finally:
        sink.close()


# -- HWM / history ordering -------------------------------------------------


def test_history_and_hwm_are_invisible_until_flush(tmp_path: Path) -> None:
    core = _core(tmp_path, (0, 1, 2, 3, 4))
    try:
        base_count, published = core.ingest()
        assert base_count == 5
        assert len(published) == 6  # five 1m Bars plus one completed 5m parent
        # Nothing buffered is durable yet: history is empty and the HWM has
        # not moved. (The data HWM is a persisted-data water mark.)
        assert core.store.count_bars(interval="1m") == 0
        assert core.high_water_mark(INSTRUMENT, "1m") is None

        core.sink.flush()

        assert core.store.count_bars(interval="1m") == 5
        assert core.store.count_bars(interval="5m") == 1
        assert core.high_water_mark(INSTRUMENT, "1m") == datetime_to_ns(
            START + timedelta(minutes=4)
        )
        assert core.high_water_mark(INSTRUMENT, "5m") == datetime_to_ns(START)
    finally:
        core.close_state()


def test_shutdown_flushes_buffered_bars(tmp_path: Path) -> None:
    core = _core(tmp_path, (0, 1, 2))
    core.ingest()
    assert core.store.count_bars(interval="1m") == 0

    core.close_state()

    assert core.store.count_bars(interval="1m") == 3


def test_dedupe_sees_bars_still_waiting_in_the_buffer(tmp_path: Path) -> None:
    core = _core(tmp_path, (0, 1, 2, 3, 4))
    try:
        _, first_written = core.ingest()
        assert len(first_written) == 6

        # The feed repeats the same Bars before anything was flushed. The
        # pending index must make this a no-op, not a duplicate append.
        base_count, second_written = core.ingest()
        assert base_count == 5
        assert second_written == []

        core.sink.flush()
        assert core.store.count_bars(interval="1m") == 5
        assert core.store.count_bars(interval="5m") == 1
        assert sorted(
            row["revision"]
            for row in core.store.read_bars(interval="1m").to_pylist()
        ) == [0] * 5
    finally:
        core.close_state()
