from __future__ import annotations

import asyncio
import json
import threading
import time
import urllib.request
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from mdgw.config import FeedConfig, load_config
from mdgw.core.time import datetime_to_ns, utc_now
from mdgw.ingest.ccxt_feed import CcxtFeedHandler
from mdgw.quarantine import (
    REASON_MALFORMED_ROW,
    SOURCE_STAGE_CCXT_LIVE_ROW,
    QuarantineWriteError,
)
from mdgw.runtime import GatewayRuntime
from mdgw.service.collector import CollectorService

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc
SYMBOL = "BTC/USDT"
INSTRUMENT = "binance:BTC/USDT"


class FakeWatchExchange:
    """Minimal watch + history exchange for the runtime wiring test."""

    rateLimit = 50

    def __init__(self) -> None:
        self.has = {
            "watchOHLCV": True,
            "watchOHLCVForSymbols": True,
            "fetchOHLCV": True,
        }
        self.enableRateLimit = False
        self.close_calls = 0
        self._pending: deque[object] = deque()
        self._lock = threading.Lock()
        self._loop = None
        self._wake = None

    def push(self, payload: dict[str, list[list]]) -> None:
        with self._lock:
            self._pending.append(payload)
            wake = self._wake
            loop = self._loop
        if wake is not None and loop is not None:
            loop.call_soon_threadsafe(wake.set)

    async def watch_ohlcv_for_symbols(self, symbols, since=None, limit=None, params=None):
        self._loop = asyncio.get_running_loop()
        while True:
            with self._lock:
                self._wake = asyncio.Event()
                if self._pending:
                    return self._pending.popleft()
                wake = self._wake
            await wake.wait()

    async def fetch_ohlcv(self, symbol, timeframe="1m", since=None, limit=None, params=None):
        return []

    async def load_markets(self):
        return {}

    async def close(self) -> None:
        self.close_calls += 1


def _ms(value: datetime) -> int:
    return datetime_to_ns(value) // 1_000_000


def _row(open_ts: datetime, *, close: str, closed: bool) -> list:
    return [
        _ms(open_ts),
        float(close),
        float(close),
        float(close),
        float(close),
        1.5,
        closed,
    ]


def _ingest_until(runtime: GatewayRuntime, expected: int, timeout: float = 3.0) -> int:
    ingested = 0
    deadline = time.monotonic() + timeout
    while ingested < expected and time.monotonic() < deadline:
        ingested += runtime.ingest()
    return ingested


def _wait_for(predicate, timeout: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def _malformed_row(open_ts: datetime) -> list:
    return [_ms(open_ts), "oops", 1.0, 1.0, 1.0, 1.5, True]


def _runtime_config(broken_quarantine: Path | None = None):
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    quarantine = config.storage.quarantine
    if broken_quarantine is not None:
        quarantine = quarantine.model_copy(update={"root": broken_quarantine})
    return config.model_copy(
        update={
            "server": config.server.model_copy(update={"http_port": 0}),
            "storage": config.storage.model_copy(
                update={"quarantine": quarantine}
            ),
        }
    )


def _minute_floor(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


def test_closed_ccxt_bar_flows_through_runtime(tmp_path: Path) -> None:
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    config = config.model_copy(
        update={"server": config.server.model_copy(update={"http_port": 0})}
    )
    feed_config = FeedConfig(
        name="binance_spot", venue="binance", symbols=(SYMBOL,)
    )
    exchange = FakeWatchExchange()
    handler = CcxtFeedHandler(config=feed_config, exchange=exchange)

    forming_ts = _minute_floor(utc_now()) - timedelta(minutes=3)
    closed_ts = forming_ts + timedelta(minutes=1)

    try:
        with GatewayRuntime(
            feed_handler=handler, config=config, data_root=tmp_path
        ) as runtime:
            runtime.start()

            exchange.push({SYMBOL: [_row(forming_ts, close="100", closed=False)]})
            assert _ingest_until(runtime, 1, timeout=0.4) == 0

            exchange.push({SYMBOL: [_row(closed_ts, close="101.25", closed=True)]})
            assert _ingest_until(runtime, 1) == 1

            rows = [
                row
                for batch in runtime.store.iter_batches(
                    instrument_id=INSTRUMENT,
                    interval="1m",
                    start_ns=datetime_to_ns(forming_ts),
                    end_ns=datetime_to_ns(closed_ts + timedelta(minutes=1)),
                )
                for row in batch.to_pylist()
            ]
            with urllib.request.urlopen(
                f"{runtime.base_url}/api/v1/bars"
                f"?instrument={INSTRUMENT}&interval=1m"
            ) as response:
                body = json.load(response)
    finally:
        handler.close()

    assert exchange.close_calls == 1
    assert len(rows) == 1
    assert rows[0]["open_ts"] == closed_ts
    assert rows[0]["feed"] == "binance_spot"
    assert rows[0]["open"] == Decimal("101.25")
    assert rows[0]["aggregation_source"] == "external"

    (envelope,) = body["bars"]
    assert envelope["instrument_id"] == INSTRUMENT
    assert envelope["feed"] == "binance_spot"
    assert envelope["payload"]["open_ts"] == (
        closed_ts.strftime("%Y-%m-%dT%H:%M:%S") + ".000000000Z"
    )
    assert envelope["payload"]["open"] == "101.25"


def test_malformed_ccxt_row_is_quarantined_and_skipped(
    tmp_path: Path,
) -> None:
    config = _runtime_config()
    feed_config = FeedConfig(
        name="binance_spot", venue="binance", symbols=(SYMBOL,)
    )
    exchange = FakeWatchExchange()
    handler = CcxtFeedHandler(config=feed_config, exchange=exchange)

    forming_ts = _minute_floor(utc_now()) - timedelta(minutes=3)
    closed_ts = forming_ts + timedelta(minutes=1)

    try:
        with GatewayRuntime(
            feed_handler=handler, config=config, data_root=tmp_path
        ) as runtime:
            runtime.start()

            # The handler was built before the runtime, so the runtime must
            # attach its own Quarantine Store to it.
            exchange.push(
                {
                    SYMBOL: [
                        _malformed_row(forming_ts),
                        _row(closed_ts, close="101.25", closed=True),
                    ]
                }
            )
            assert _ingest_until(runtime, 1) == 1

            (record,) = runtime.quarantine.read_records().to_pylist()
            assert record["source_stage"] == SOURCE_STAGE_CCXT_LIVE_ROW
            assert record["reason_code"] == REASON_MALFORMED_ROW
            assert record["source_location"] == "ccxt:BTC/USDT:live:row=0"
            assert record["instrument_id"] == INSTRUMENT
            assert record["feed"] == "binance_spot"

            rows = runtime.store.read_bars(
                instrument_id=INSTRUMENT, interval="1m"
            ).to_pylist()
            assert [row["open_ts"] for row in rows] == [closed_ts]
            assert rows[0]["close"] == Decimal("101.25")
    finally:
        handler.close()


def test_ccxt_quarantine_write_failure_stops_the_runtime(
    tmp_path: Path,
) -> None:
    broken_root = tmp_path / "broken-quarantine"
    broken_root.mkdir(parents=True)
    (broken_root / "_quarantine_manifest.json").mkdir()
    config = _runtime_config(broken_quarantine=broken_root)
    feed_config = FeedConfig(
        name="binance_spot", venue="binance", symbols=(SYMBOL,)
    )
    exchange = FakeWatchExchange()
    handler = CcxtFeedHandler(config=feed_config, exchange=exchange)
    open_ts = _minute_floor(utc_now()) - timedelta(minutes=2)

    try:
        with GatewayRuntime(
            feed_handler=handler, config=config, data_root=tmp_path
        ) as runtime:
            runtime.start()
            exchange.push({SYMBOL: [_malformed_row(open_ts)]})

            error: QuarantineWriteError | None = None
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                try:
                    runtime.ingest()
                except QuarantineWriteError as exc:
                    error = exc
                    break
                time.sleep(0.02)

            assert error is not None
            # Nothing was published or persisted, and the failed record left
            # no orphan Parquet file behind.
            assert runtime.store.count_bars() == 0
            assert runtime.quarantine.stats()["write_failures"] == 1
            assert runtime.quarantine.stats()["records_written"] == 0
            assert list(broken_root.glob("year=*/month=*/*.parquet")) == []
    finally:
        handler.close()


def test_collector_service_exposes_ccxt_quarantine_counts(
    tmp_path: Path,
) -> None:
    config = _runtime_config()
    feed_config = FeedConfig(
        name="binance_spot", venue="binance", symbols=(SYMBOL,)
    )
    exchange = FakeWatchExchange()
    handler = CcxtFeedHandler(config=feed_config, exchange=exchange)
    service = CollectorService(
        config, data_root=tmp_path, feed_handler=handler
    )
    service.core.open_state()
    try:
        assert handler.quarantine is service.core.quarantine

        open_ts = _minute_floor(utc_now()) - timedelta(minutes=2)
        exchange.push({SYMBOL: [_malformed_row(open_ts)]})
        assert _wait_for(
            lambda: service.core.quarantine.stats()["records_written"] == 1
        )

        metrics = service.render_metrics()
        assert (
            'mdgw_collector_quarantine_records_total'
            '{reason_code="malformed_row",source_stage="ccxt_live_row"} 1'
        ) in metrics
        assert "mdgw_collector_quarantine_active_files 1" in metrics
        assert "mdgw_collector_quarantine_retention_days 30" in metrics

        quarantine = service.status_payload()["quarantine"]
        assert quarantine["retention_days"] == 30
        assert quarantine["active_bytes"] > 0
        assert quarantine["records_by_source_stage"] == {
            SOURCE_STAGE_CCXT_LIVE_ROW: 1
        }
        assert quarantine["records_by_reason_code"] == {
            REASON_MALFORMED_ROW: 1
        }
    finally:
        service.core.close_state()
        handler.close()
