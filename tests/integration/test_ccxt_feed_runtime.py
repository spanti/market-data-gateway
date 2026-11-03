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

from mdgw.config import FeedConfig, load_config
from mdgw.core.time import datetime_to_ns, utc_now
from mdgw.ingest.ccxt_feed import CcxtFeedHandler
from mdgw.runtime import GatewayRuntime

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
