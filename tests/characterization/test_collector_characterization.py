"""Freeze collector failure semantics before Polars Normalization.

The primary contract: a rejected live batch must not advance Bar Aggregation,
the data HWM or freshness, so the same batch can be retried after the reference
data is fixed.

The module also records the *current* source-stage differences the migration
must preserve or deliberately change:

* CCXT history: a malformed row is skipped and the source continues (there is
  no Quarantine today; only a warning log).
* Archive import: a checksum/malformed file writes nothing for the whole file
  (whole-file atomicity, no partial rows).
* Collector write queue overflow: the oldest Bar is dropped and the HWM is
  capped below the hole; it does not become a validation failure.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import zipfile
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from mdgw.aggregate import BarAggregator
from mdgw.archive import (
    ArchiveFetcher,
    ArchiveInstrument,
    ArchivePeriod,
    import_archive,
)
from mdgw.archive.binance import archive_csv_filename, archive_filename
from mdgw.config import FeedConfig, load_config
from mdgw.core.bar import RawBar
from mdgw.core.errors import UnknownInstrumentError
from mdgw.core.time import datetime_to_ns
from mdgw.ingest.ccxt_feed import CcxtFeedHandler
from mdgw.service.bar_sink import BufferedBarSink
from mdgw.service.collector_core import CollectorCore
from mdgw.state.high_water_mark import HighWaterMarkStore
from mdgw.store.parquet_dataset import ParquetBarStore

from .support import CONFIG_PATH, INSTRUMENTS_PATH
from .dataset_builder import BTC, minute

ALL_INTERVALS = ("1m", "5m", "15m", "1h", "1d")
UNKNOWN = "binance:UNKNOWN/USDT"


def _config(*, collector: dict | None = None):
    config = load_config(CONFIG_PATH)
    config = config.model_copy(
        update={
            "instruments": config.instruments.model_copy(
                update={"path": INSTRUMENTS_PATH}
            )
        }
    )
    if collector:
        config = config.model_copy(
            update={"collector": config.collector.model_copy(update=collector)}
        )
    return config


def _raw(
    open_minute: int,
    *,
    instrument_id: str = BTC,
    interval: str = "1m",
) -> RawBar:
    open_ts = minute(open_minute)
    return RawBar(
        instrument_id=instrument_id,
        interval=interval,
        open_ts=open_ts,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1"),
        trade_count=1,
        revision=0,
    )


class _ScriptedFeed:
    """Feed Handler that returns one scripted raw-Bar batch per ingest."""

    name = "scripted"
    supports_history = False

    def __init__(self, batches: list[list[RawBar]]) -> None:
        self._batches = list(batches)

    @property
    def instrument_ids(self) -> tuple[str, ...]:
        return (BTC,)

    def iter_bars(self):
        return iter(self._batches.pop(0)) if self._batches else iter(())

    def fetch_history(self, instrument_id, interval, start, end):
        return ()


def test_rejected_batch_does_not_advance_aggregation_hwm_or_freshness(
    tmp_path: Path,
) -> None:
    valid = [_raw(index) for index in range(5)]
    # The same complete 5m period plus one Bar for an unknown Instrument. The
    # whole batch must be rejected before the aggregator sees any row.
    rejected = [*valid, _raw(5, instrument_id=UNKNOWN)]
    feed = _ScriptedFeed([rejected, valid, valid])
    core = CollectorCore(
        feed_handler=feed, config=_config(), data_root=tmp_path
    )
    core.open_state()
    try:
        with pytest.raises(UnknownInstrumentError):
            core.ingest()

        assert core.store.count_bars(interval="1m") == 0
        assert core.store.count_bars(interval="5m") == 0
        assert core.high_water_marks() == {}
        tracked = {
            (row["instrument_id"], row["interval"]): row["last_event_ts"]
            for row in core.health_snapshot()["tracked"]
        }
        assert tracked[(BTC, "1m")] is None
        assert tracked[(BTC, "5m")] is None

        # Retry after removing the bad Instrument: the untouched aggregator
        # produces exactly one 5m parent from the five 1m Bars.
        base_count, written = core.ingest()
        assert base_count == 5
        assert [bar.interval for bar in written].count("1m") == 5
        assert [bar.interval for bar in written].count("5m") == 1
        assert core.store.count_bars(interval="1m") == 5
        assert core.store.count_bars(interval="5m") == 1
        assert core.high_water_mark(BTC, "1m") == datetime_to_ns(minute(4))
        assert core.high_water_mark(BTC, "5m") == datetime_to_ns(minute(0))
        tracked = {
            (row["instrument_id"], row["interval"]): row["last_event_ts"]
            for row in core.health_snapshot()["tracked"]
        }
        assert tracked[(BTC, "1m")] == "2026-01-01T00:05:00.000000000Z"
        assert tracked[(BTC, "5m")] == "2026-01-01T00:05:00.000000000Z"

        # Replaying the accepted batch is idempotent: the aggregator did not
        # advance partially during the rejected attempt.
        base_count, written = core.ingest()
        assert base_count == 5
        assert written == []
        assert core.store.count_bars(interval="1m") == 5
        assert core.store.count_bars(interval="5m") == 1
        assert core.high_water_mark(BTC, "1m") == datetime_to_ns(minute(4))
    finally:
        core.close_state()


def test_queue_overflow_drops_the_oldest_bar_and_caps_the_hwm(
    tmp_path: Path,
) -> None:
    """Current behaviour; the migration must not turn this into Quarantine."""
    feed = _ScriptedFeed([[_raw(1), _raw(2), _raw(3)]])
    core = CollectorCore(
        feed_handler=feed,
        config=_config(collector={"write_queue_size": 2}),
        data_root=tmp_path,
        sink_factory=lambda store: BufferedBarSink(
            store,
            queue_size=2,
            flush_max_bytes=1_000_000_000,
            flush_max_age_s=3_600,
        ),
    )
    core.open_state()
    try:
        base_count, written = core.ingest()
        assert base_count == 3
        assert len(written) == 3
        core.sink.flush()

        stats = core.sink.stats()
        assert stats["dropped_bars"] == 1
        assert stats["lag_bars"] == 1
        # The dropped minute-1 Bar is absent, and the HWM is capped at the
        # preceding interval so Backfill re-fetches the hole.
        assert core.store.count_bars(interval="1m") == 2
        assert core.high_water_mark(BTC, "1m") == datetime_to_ns(minute(0))
        # The live PUB path still saw all three Bars; only history lags.
        tracked = {
            (row["instrument_id"], row["interval"]): row
            for row in core.health_snapshot()["tracked"]
        }
        assert tracked[(BTC, "1m")]["last_event_ts"] == (
            "2026-01-01T00:04:00.000000000Z"
        )
    finally:
        core.close_state()


# -- archive: whole-file atomicity -----------------------------------------


def _write_corrupt_archive(archive_dir: Path, day: date) -> None:
    period = ArchivePeriod("daily", day, day)
    csv_name = archive_csv_filename("BTCUSDT", "1m", period)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(csv_name, "garbage\n")
    data = buffer.getvalue()
    filename = archive_filename("BTCUSDT", "1m", period)
    directory = archive_dir / "spot" / "daily" / "klines" / "BTCUSDT" / "1m"
    directory.mkdir(parents=True)
    (directory / filename).write_bytes(data)
    wrong = hashlib.sha256(b"not-the-archive").hexdigest()
    (directory / f"{filename}.CHECKSUM").write_text(
        f"{wrong}  {filename}\n", encoding="utf-8"
    )


def test_archive_checksum_failure_writes_no_rows_for_the_whole_file(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    _write_corrupt_archive(archive_dir, day)
    store = ParquetBarStore(tmp_path / "data")
    hwm = HighWaterMarkStore(tmp_path / "data" / "state.db")
    aggregator = BarAggregator(
        intervals=ALL_INTERVALS,
        base_interval="1m",
        retention_parent_periods=2,
    )
    try:
        report = import_archive(
            instruments=[ArchiveInstrument(BTC, "BTCUSDT", "crypto")],
            store=store,
            hwm=hwm,
            aggregator=aggregator,
            fetcher=ArchiveFetcher(archive_dir=archive_dir),
            start=day,
            end=day,
            feed="binance_archive",
            market_type="spot",
            interval="1m",
            granularity="daily",
            now=datetime(2024, 1, 2, tzinfo=UTC),
            today=date(2024, 1, 2),
        )
        assert report.failures == 1
        assert report.files == 0
        assert report.bars_written == 0
        assert store.count_bars() == 0
        assert hwm.all() == {}
    finally:
        hwm.close()


# -- CCXT: per-row skip, no Quarantine yet ---------------------------------


class _StaticExchange:
    """Minimal ccxt.pro surface used by `fetch_history`."""

    rateLimit = 50
    enableRateLimit = False

    def __init__(self, history: dict[str, list[list]]) -> None:
        self.has = {
            "watchOHLCV": True,
            "watchOHLCVForSymbols": True,
            "fetchOHLCV": True,
        }
        self.history = history

    async def load_markets(self):
        return {}

    async def watch_ohlcv_for_symbols(self, symbols):
        await asyncio.Event().wait()

    async def watch_ohlcv(self, symbol, timeframe="1m"):
        await asyncio.Event().wait()

    async def fetch_ohlcv(
        self, symbol, timeframe="1m", since=None, limit=None, params=None
    ):
        rows = list(self.history.get(symbol, ()))
        if since is not None:
            rows = [row for row in rows if row[0] >= since]
        return rows[:limit] if limit else rows

    async def close(self):
        return None


def _ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _ccxt_row(open_ts: datetime, close: float = 100.5) -> list:
    return [_ms(open_ts), 100.0, 101.0, 99.0, close, 1.25]


def test_ccxt_history_skips_malformed_rows_and_continues() -> None:
    first = minute(0)
    rows = [
        _ccxt_row(first),
        [_ms(first + timedelta(minutes=1)), "oops", 1, 1, 1, 1],
        _ccxt_row(first + timedelta(minutes=2), close=102.0),
    ]
    handler = CcxtFeedHandler(
        config=FeedConfig(
            name="ccxt",
            kind="ccxt",
            venue="binance",
            symbols=("BTC/USDT",),
        ),
        exchange=_StaticExchange({"BTC/USDT": rows}),
    )
    try:
        bars = list(
            handler.fetch_history(
                BTC,
                "1m",
                first,
                first + timedelta(minutes=5),
            )
        )
    finally:
        handler.close()

    # The malformed middle row is logged and skipped; the source continues.
    assert [bar.open_ts for bar in bars] == [
        first,
        first + timedelta(minutes=2),
    ]
