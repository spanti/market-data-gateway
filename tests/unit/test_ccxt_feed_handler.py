from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from mdgw.config import FeedConfig
from mdgw.ingest.ccxt_feed import CcxtFeedHandler, TokenBucket, build_feed_handler
from mdgw.quarantine import (
    REASON_MALFORMED_ROW,
    SOURCE_STAGE_CCXT_HISTORY_ROW,
    SOURCE_STAGE_CCXT_LIVE_ROW,
    QuarantineStore,
    QuarantineWriteError,
)

UTC = timezone.utc
SYMBOL = "BTC/USDT"
INSTRUMENT = "binance:BTC/USDT"


def _ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _row(
    open_ts: datetime,
    *,
    open: float = 100.0,
    high: float = 101.0,
    low: float = 99.0,
    close: float = 100.5,
    volume: float = 1.25,
    closed: bool | None = None,
) -> list:
    row = [_ms(open_ts), open, high, low, close, volume]
    if closed is not None:
        row.append(closed)
    return row


class FakeExchange:
    """Fake ccxt.pro exchange; only the Feed Handler's boundary is modelled."""

    rateLimit = 50

    def __init__(
        self,
        *,
        has: dict[str, bool] | None = None,
        history: dict[str, list[list]] | None = None,
    ) -> None:
        self.has = {
            "watchOHLCV": True,
            "watchOHLCVForSymbols": True,
            "fetchOHLCV": True,
        }
        if has is not None:
            self.has.update(has)
        self.rateLimit = 50
        self.enableRateLimit = False
        self.history = {symbol: list(rows) for symbol, rows in (history or {}).items()}
        self.watch_for_symbols_calls: list[dict] = []
        self.watch_calls: list[dict] = []
        self.fetch_calls: list[dict] = []
        self.close_calls = 0
        self._pending: deque[tuple[str | None, object]] = deque()
        self._pending_lock = threading.Lock()
        self._loop = None
        self._wakes: dict[str | None, asyncio.Event] = {}

    def push_for_symbols(self, payload: dict[str, list[list]]) -> None:
        self._push(None, payload)

    def push_symbol(self, symbol: str, payload: list[list]) -> None:
        self._push(symbol, payload)

    def push_error(self, error: Exception) -> None:
        self._push(None, error)

    def _push(self, symbol: str | None, payload: object) -> None:
        with self._pending_lock:
            self._pending.append((symbol, payload))
            wake = self._wakes.get(symbol)
            loop = self._loop
        if wake is not None and loop is not None:
            loop.call_soon_threadsafe(wake.set)

    async def _next(self, symbol: str | None):
        self._loop = asyncio.get_running_loop()
        while True:
            with self._pending_lock:
                wake = asyncio.Event()
                self._wakes[symbol] = wake
                for index, (target, payload) in enumerate(self._pending):
                    if target == symbol:
                        del self._pending[index]
                        return payload
            await wake.wait()

    async def watch_ohlcv_for_symbols(
        self, symbols: list[list[str]], since=None, limit=None, params=None
    ) -> dict[str, list[list]]:
        self.watch_for_symbols_calls.append({"symbols": symbols})
        payload = await self._next(None)
        if isinstance(payload, Exception):
            raise payload
        return payload

    async def watch_ohlcv(
        self, symbol: str, timeframe="1m", since=None, limit=None, params=None
    ) -> list[list]:
        self.watch_calls.append({"symbol": symbol, "timeframe": timeframe})
        payload = await self._next(symbol)
        if isinstance(payload, Exception):
            raise payload
        return payload

    async def fetch_ohlcv(
        self, symbol: str, timeframe="1m", since=None, limit=None, params=None
    ) -> list[list]:
        self.fetch_calls.append(
            {"symbol": symbol, "timeframe": timeframe, "since": since, "limit": limit}
        )
        rows = self.history.get(symbol, [])
        if since is not None:
            rows = [row for row in rows if row[0] >= since]
        if limit is not None:
            rows = rows[:limit]
        return list(rows)

    async def load_markets(self):
        return {}

    async def close(self) -> None:
        self.close_calls += 1


def _config(symbols: tuple[str, ...] = (SYMBOL,)) -> FeedConfig:
    return FeedConfig(name="binance_spot", venue="binance", symbols=symbols)


def _collect(handler: CcxtFeedHandler, count: int, timeout: float = 2.0):
    bars = []
    deadline = time.monotonic() + timeout
    while len(bars) < count and time.monotonic() < deadline:
        bars.extend(handler.iter_bars())
        if len(bars) < count:
            time.sleep(0.01)
    return bars


def _quarantine_store(tmp_path: Path) -> QuarantineStore:
    return QuarantineStore(tmp_path / "quarantine")


def _broken_quarantine_store(tmp_path: Path) -> QuarantineStore:
    root = tmp_path / "broken-quarantine"
    root.mkdir(parents=True)
    # A directory at the Manifest path makes every append fail after the
    # Parquet file has already been written.
    (root / "_quarantine_manifest.json").mkdir()
    return QuarantineStore(root)


def _wait_for(predicate, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def test_handler_exposes_instruments_and_history_capability() -> None:
    exchange = FakeExchange()
    handler = CcxtFeedHandler(config=_config(), exchange=exchange)
    try:
        assert handler.name == "binance_spot"
        assert handler.instrument_ids == (INSTRUMENT,)
        assert handler.supports_history is True
    finally:
        handler.close()


def test_iter_bars_returns_promptly_when_no_bar_is_available() -> None:
    exchange = FakeExchange()
    handler = CcxtFeedHandler(config=_config(), exchange=exchange)
    try:
        started = time.monotonic()
        assert list(handler.iter_bars()) == []
        assert time.monotonic() - started < 1.0
    finally:
        handler.close()


def test_forming_bar_is_not_emitted_until_the_next_bar_arrives() -> None:
    exchange = FakeExchange()
    handler = CcxtFeedHandler(config=_config(), exchange=exchange)
    base = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    try:
        exchange.push_for_symbols({SYMBOL: [_row(base, close=100.5)]})
        assert _collect(handler, 1, timeout=0.4) == []

        exchange.push_for_symbols({SYMBOL: [_row(base + timedelta(minutes=1), close=101.5)]})
        bars = _collect(handler, 1)
    finally:
        handler.close()

    assert len(bars) == 1
    bar = bars[0]
    assert bar.instrument_id == INSTRUMENT
    assert bar.interval == "1m"
    assert bar.open_ts == base
    assert str(bar.close) == "100.5"
    assert bar.revision == 0


def test_explicitly_closed_bar_is_emitted_once() -> None:
    exchange = FakeExchange()
    handler = CcxtFeedHandler(config=_config(), exchange=exchange)
    base = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    closed_row = _row(base, closed=True)
    try:
        exchange.push_for_symbols({SYMBOL: [closed_row]})
        assert len(_collect(handler, 1)) == 1

        exchange.push_for_symbols({SYMBOL: [closed_row]})
        assert _collect(handler, 1, timeout=0.4) == []

        exchange.push_for_symbols(
            {SYMBOL: [_row(base + timedelta(minutes=1), closed=True)]}
        )
        bars = _collect(handler, 1)
    finally:
        handler.close()

    assert [bar.open_ts for bar in bars] == [base + timedelta(minutes=1)]


def test_closed_bar_correction_at_the_same_open_ts_is_emitted() -> None:
    exchange = FakeExchange()
    handler = CcxtFeedHandler(config=_config(), exchange=exchange)
    base = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    try:
        exchange.push_for_symbols(
            {SYMBOL: [_row(base, close=100.5, closed=True)]}
        )
        first = _collect(handler, 1)

        # The Venue corrects an already-closed Bar. It must not be swallowed
        # by the per-symbol high-water mark.
        exchange.push_for_symbols(
            {SYMBOL: [_row(base, close=101.5, closed=True)]}
        )
        corrected = _collect(handler, 1)

        # An unchanged repeat of the correction is still deduplicated.
        exchange.push_for_symbols(
            {SYMBOL: [_row(base, close=101.5, closed=True)]}
        )
        assert _collect(handler, 1, timeout=0.4) == []
    finally:
        handler.close()

    assert [bar.close for bar in first] == [Decimal("100.5")]
    assert [bar.open_ts for bar in corrected] == [base]
    assert corrected[0].close == Decimal("101.5")


def test_explicitly_forming_bar_is_never_emitted() -> None:
    exchange = FakeExchange()
    handler = CcxtFeedHandler(config=_config(), exchange=exchange)
    base = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    try:
        exchange.push_for_symbols({SYMBOL: [_row(base, closed=False)]})
        exchange.push_for_symbols(
            {SYMBOL: [_row(base + timedelta(minutes=1), closed=False)]}
        )
        assert _collect(handler, 1, timeout=0.4) == []

        exchange.push_for_symbols(
            {SYMBOL: [_row(base + timedelta(minutes=2), closed=True)]}
        )
        bars = _collect(handler, 1)
    finally:
        handler.close()

    assert [bar.open_ts for bar in bars] == [base + timedelta(minutes=2)]


def test_ccxt_float_values_become_exact_decimals() -> None:
    exchange = FakeExchange()
    handler = CcxtFeedHandler(config=_config(), exchange=exchange)
    base = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    try:
        exchange.push_for_symbols(
            {
                SYMBOL: [
                    _row(
                        base,
                        open=65000.1,
                        high=65010.25,
                        low=64990.0,
                        close=65005.5,
                        volume=12.34,
                        closed=True,
                    )
                ]
            }
        )
        bars = _collect(handler, 1)
    finally:
        handler.close()

    (bar,) = bars
    assert bar.open == Decimal("65000.1")
    assert bar.high == Decimal("65010.25")
    assert bar.low == Decimal("64990.0")
    assert bar.close == Decimal("65005.5")
    assert bar.volume == Decimal("12.34")
    assert isinstance(bar.open, Decimal)
    assert bar.trade_count is None
    assert bar.revision == 0


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), None])
def test_non_finite_values_are_discarded(tmp_path: Path, bad) -> None:
    exchange = FakeExchange()
    quarantine = _quarantine_store(tmp_path)
    handler = CcxtFeedHandler(
        config=_config(), exchange=exchange, quarantine=quarantine
    )
    base = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    try:
        exchange.push_for_symbols(
            {SYMBOL: [_row(base, close=bad, closed=True)]}
        )
        exchange.push_for_symbols(
            {SYMBOL: [_row(base + timedelta(minutes=1), close=101.0, closed=True)]}
        )
        bars = _collect(handler, 1)
    finally:
        handler.close()

    assert [bar.open_ts for bar in bars] == [base + timedelta(minutes=1)]
    assert bars[0].close == Decimal("101.0")

    (record,) = quarantine.read_records().to_pylist()
    assert record["source_stage"] == SOURCE_STAGE_CCXT_LIVE_ROW
    assert record["reason_code"] == REASON_MALFORMED_ROW
    assert record["source_location"] == "ccxt:BTC/USDT:live:row=0"


def test_mapping_row_uses_x_flag_and_decimals() -> None:
    exchange = FakeExchange()
    handler = CcxtFeedHandler(config=_config(), exchange=exchange)
    base = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    try:
        exchange.push_for_symbols(
            {
                SYMBOL: [
                    {
                        "timestamp": _ms(base),
                        "open": 1,
                        "high": 2,
                        "low": 0.5,
                        "close": 1.5,
                        "volume": 10,
                        "x": True,
                    }
                ]
            }
        )
        bars = _collect(handler, 1)
    finally:
        handler.close()

    (bar,) = bars
    assert bar.open_ts == base
    assert bar.close == Decimal("1.5")
    assert bar.volume == Decimal("10")


def _minute_floor(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


def test_fetch_history_paginates_and_filters_closed_bars() -> None:
    now = datetime.now(tz=UTC)
    latest_closed = _minute_floor(now) - timedelta(minutes=1)
    first = latest_closed - timedelta(minutes=2_499)
    rows = [
        _row(first + timedelta(minutes=index), close=100 + index)
        for index in range(2_500)
    ]
    # The forming candle and a future candle must never be returned.
    rows.append(_row(latest_closed + timedelta(minutes=1), close=999))
    rows.append(_row(latest_closed + timedelta(minutes=2), close=999))

    exchange = FakeExchange(history={SYMBOL: rows})
    handler = CcxtFeedHandler(config=_config(), exchange=exchange)
    try:
        bars = list(
            handler.fetch_history(
                INSTRUMENT,
                "1m",
                first,
                latest_closed + timedelta(minutes=3),
            )
        )
    finally:
        handler.close()

    assert len(bars) == 2_500
    assert bars[0].open_ts == first
    assert bars[-1].open_ts == latest_closed
    assert all(bar.interval == "1m" for bar in bars)
    assert all(bar.open_ts + timedelta(minutes=1) <= now for bar in bars)
    assert all(not isinstance(bar.open, float) for bar in bars)

    assert len(exchange.fetch_calls) == 3
    assert all(call["limit"] == 1_000 for call in exchange.fetch_calls)
    assert all(call["timeframe"] == "1m" for call in exchange.fetch_calls)
    assert [call["since"] for call in exchange.fetch_calls] == sorted(
        call["since"] for call in exchange.fetch_calls
    )


def test_fetch_history_is_empty_for_unknown_instrument_or_interval() -> None:
    exchange = FakeExchange(history={SYMBOL: [_row(datetime(2024, 1, 1, tzinfo=UTC))]})
    handler = CcxtFeedHandler(config=_config(), exchange=exchange)
    try:
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 2, tzinfo=UTC)
        assert list(handler.fetch_history("binance:ETH/USDT", "1m", start, end)) == []
        assert list(handler.fetch_history(INSTRUMENT, "5m", start, end)) == []
    finally:
        handler.close()

    assert exchange.fetch_calls == []


def test_fetch_history_requires_fetch_ohlcv_capability() -> None:
    exchange = FakeExchange(has={"fetchOHLCV": False})
    handler = CcxtFeedHandler(config=_config(), exchange=exchange)
    try:
        assert handler.supports_history is False
        with pytest.raises(NotImplementedError):
            list(
                handler.fetch_history(
                    INSTRUMENT,
                    "1m",
                    datetime(2024, 1, 1, tzinfo=UTC),
                    datetime(2024, 1, 2, tzinfo=UTC),
                )
            )
    finally:
        handler.close()


def test_reconnect_backfills_the_gap_from_the_last_emitted_bar() -> None:
    now = datetime.now(tz=UTC)
    latest_closed = _minute_floor(now) - timedelta(minutes=1)
    emitted = latest_closed - timedelta(minutes=1)
    missed = latest_closed

    exchange = FakeExchange(
        history={
            SYMBOL: [
                _row(emitted, close=100.0),
                _row(missed, close=102.0),
            ]
        }
    )
    handler = CcxtFeedHandler(
        config=_config(),
        exchange=exchange,
        reconnect_backoff_s=0.01,
        reconnect_backoff_max_s=0.01,
    )
    try:
        exchange.push_for_symbols({SYMBOL: [_row(emitted, close=100.0)]})
        exchange.push_for_symbols({SYMBOL: [_row(missed, close=101.0)]})
        first = _collect(handler, 1)
        assert [bar.open_ts for bar in first] == [emitted]

        exchange.push_error(ConnectionError("dropped"))
        bars = _collect(handler, 1, timeout=3.0)
    finally:
        handler.close()

    assert [bar.open_ts for bar in bars] == [missed]
    assert bars[0].close == Decimal("102.0")
    assert exchange.fetch_calls
    assert exchange.fetch_calls[0]["since"] == _ms(emitted)


def test_reconnect_skips_backfill_when_history_is_unsupported() -> None:
    exchange = FakeExchange(has={"fetchOHLCV": False})
    handler = CcxtFeedHandler(
        config=_config(),
        exchange=exchange,
        reconnect_backoff_s=0.01,
        reconnect_backoff_max_s=0.01,
    )
    try:
        assert list(handler.iter_bars()) == []
        exchange.push_error(ConnectionError("dropped"))
        time.sleep(0.2)
    finally:
        handler.close()

    assert exchange.fetch_calls == []


def test_close_stops_the_worker_and_is_idempotent() -> None:
    exchange = FakeExchange()
    handler = CcxtFeedHandler(config=_config(), exchange=exchange)
    assert list(handler.iter_bars()) == []

    handler.close()
    handler.close()

    assert exchange.close_calls == 1
    assert list(handler.iter_bars()) == []
    with pytest.raises(RuntimeError, match="closed"):
        handler.fetch_history(
            INSTRUMENT,
            "1m",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 2, tzinfo=UTC),
        )


def test_falls_back_to_per_symbol_watch_ohlcv_when_needed() -> None:
    exchange = FakeExchange(has={"watchOHLCVForSymbols": False})
    handler = CcxtFeedHandler(
        config=_config(symbols=("BTC/USDT", "ETH/USDT")), exchange=exchange
    )
    base = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    try:
        exchange.push_symbol("BTC/USDT", [_row(base, closed=True)])
        exchange.push_symbol("ETH/USDT", [_row(base, closed=True)])
        bars = _collect(handler, 2)
    finally:
        handler.close()

    assert {bar.instrument_id for bar in bars} == {
        "binance:BTC/USDT",
        "binance:ETH/USDT",
    }
    assert exchange.watch_for_symbols_calls == []
    assert {call["symbol"] for call in exchange.watch_calls} == {
        "BTC/USDT",
        "ETH/USDT",
    }
    assert all(call["timeframe"] == "1m" for call in exchange.watch_calls)


def test_handler_rejects_an_exchange_without_ohlcv_watch_capability() -> None:
    exchange = FakeExchange(
        has={"watchOHLCV": False, "watchOHLCVForSymbols": False}
    )
    with pytest.raises(ValueError, match="watchOHLCV"):
        CcxtFeedHandler(config=_config(), exchange=exchange)


def test_build_feed_handler_accepts_an_injected_exchange() -> None:
    exchange = FakeExchange()
    handler = build_feed_handler(_config(), exchange=exchange)
    try:
        assert isinstance(handler, CcxtFeedHandler)
        assert handler.instrument_ids == (INSTRUMENT,)
    finally:
        handler.close()


def test_token_bucket_throttles_requests() -> None:
    clock = [0.0]
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock[0] += seconds

    bucket = TokenBucket(2.0, capacity=1.0, clock=lambda: clock[0], sleep=sleep)
    bucket.acquire()
    assert sleeps == []

    bucket.acquire()
    bucket.acquire()

    assert sleeps == [0.5, 0.5]


def test_token_bucket_rejects_non_positive_parameters() -> None:
    with pytest.raises(ValueError):
        TokenBucket(0)
    with pytest.raises(ValueError):
        TokenBucket(1, capacity=0)


def test_history_acquires_the_rate_limiter_once_per_page() -> None:
    class CountingLimiter:
        def __init__(self) -> None:
            self.count = 0

        def acquire(self, tokens: float = 1.0) -> None:
            self.count += 1

    now = datetime.now(tz=UTC)
    latest_closed = _minute_floor(now) - timedelta(minutes=1)
    first = latest_closed - timedelta(minutes=2_499)
    rows = [
        _row(first + timedelta(minutes=index)) for index in range(2_500)
    ]
    limiter = CountingLimiter()
    exchange = FakeExchange(history={SYMBOL: rows})
    handler = CcxtFeedHandler(
        config=_config(), exchange=exchange, rate_limiter=limiter
    )
    try:
        bars = list(
            handler.fetch_history(
                INSTRUMENT, "1m", first, latest_closed + timedelta(minutes=1)
            )
        )
    finally:
        handler.close()

    assert len(bars) == 2_500
    assert limiter.count == 3


def test_history_skips_malformed_rows_instead_of_failing(
    tmp_path: Path,
) -> None:
    now = datetime.now(tz=UTC)
    latest_closed = _minute_floor(now) - timedelta(minutes=1)
    first = latest_closed - timedelta(minutes=4)
    rows = [
        _row(first, close=100.0),
        [_ms(first + timedelta(minutes=1)), "oops", 1, 1, 1, 1],
        _row(first + timedelta(minutes=2), close=102.0),
        _row(first + timedelta(minutes=3), close=float("nan")),
        _row(first + timedelta(minutes=4), close=104.0),
    ]

    quarantine = _quarantine_store(tmp_path)
    exchange = FakeExchange(history={SYMBOL: rows})
    handler = CcxtFeedHandler(
        config=_config(), exchange=exchange, quarantine=quarantine
    )
    try:
        bars = list(
            handler.fetch_history(
                INSTRUMENT, "1m", first, latest_closed + timedelta(minutes=1)
            )
        )
    finally:
        handler.close()

    assert [bar.open_ts for bar in bars] == [
        first,
        first + timedelta(minutes=2),
        first + timedelta(minutes=4),
    ]
    records = quarantine.read_records().to_pylist()
    assert [
        (record["source_location"], record["reason_code"])
        for record in records
    ] == [
        ("ccxt:BTC/USDT:history:row=1", REASON_MALFORMED_ROW),
        ("ccxt:BTC/USDT:history:row=3", REASON_MALFORMED_ROW),
    ]
    assert all(
        record["source_stage"] == SOURCE_STAGE_CCXT_HISTORY_ROW
        for record in records
    )


# -- Quarantine of malformed CCXT rows --------------------------------------


def test_malformed_live_row_is_quarantined_then_skipped(
    tmp_path: Path,
) -> None:
    exchange = FakeExchange()
    quarantine = _quarantine_store(tmp_path)
    handler = CcxtFeedHandler(
        config=_config(), exchange=exchange, quarantine=quarantine
    )
    base = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    try:
        exchange.push_for_symbols(
            {
                SYMBOL: [
                    _row(base, close="oops", closed=True),
                    _row(
                        base + timedelta(minutes=1),
                        close=101.0,
                        closed=True,
                    ),
                ]
            }
        )
        bars = _collect(handler, 1)
    finally:
        handler.close()

    assert [bar.open_ts for bar in bars] == [base + timedelta(minutes=1)]
    (record,) = quarantine.read_records().to_pylist()
    assert record["source_stage"] == SOURCE_STAGE_CCXT_LIVE_ROW
    assert record["reason_code"] == REASON_MALFORMED_ROW
    assert record["source_location"] == "ccxt:BTC/USDT:live:row=0"
    assert record["feed"] == "binance_spot"
    assert record["instrument_id"] == INSTRUMENT
    assert record["interval"] == "1m"
    assert record["rule_version"] == "1"
    assert record["open_ts"] == base
    assert "oops" in record["raw_record"]


def test_malformed_live_row_stops_the_source_when_quarantine_write_fails(
    tmp_path: Path,
) -> None:
    exchange = FakeExchange()
    quarantine = _broken_quarantine_store(tmp_path)
    handler = CcxtFeedHandler(
        config=_config(), exchange=exchange, quarantine=quarantine
    )
    base = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    try:
        # Start the background watcher first; it processes pushed payloads.
        assert list(handler.iter_bars()) == []
        exchange.push_for_symbols(
            {SYMBOL: [_row(base, close="oops", closed=True)]}
        )
        assert _wait_for(lambda: quarantine.stats()["write_failures"] == 1)
        with pytest.raises(QuarantineWriteError):
            list(handler.iter_bars())

        # The source cannot advance past the failed record: a later valid row
        # is not silently consumed either.
        exchange.push_for_symbols(
            {SYMBOL: [_row(base + timedelta(minutes=1), close=101.0, closed=True)]}
        )
        time.sleep(0.1)
        with pytest.raises(QuarantineWriteError):
            list(handler.iter_bars())
    finally:
        handler.close()

    assert quarantine.stats()["write_failures"] == 1
    assert quarantine.stats()["records_written"] == 0
    assert list(quarantine.root.glob("year=*/month=*/*.parquet")) == []


def test_malformed_history_row_write_failure_stops_history(
    tmp_path: Path,
) -> None:
    now = datetime.now(tz=UTC)
    latest_closed = _minute_floor(now) - timedelta(minutes=1)
    first = latest_closed - timedelta(minutes=2)
    rows = [
        _row(first, close=100.0),
        [_ms(first + timedelta(minutes=1)), "oops", 1, 1, 1, 1],
        _row(first + timedelta(minutes=2), close=102.0),
    ]
    quarantine = _broken_quarantine_store(tmp_path)
    exchange = FakeExchange(history={SYMBOL: rows})
    handler = CcxtFeedHandler(
        config=_config(), exchange=exchange, quarantine=quarantine
    )
    try:
        with pytest.raises(QuarantineWriteError):
            list(
                handler.fetch_history(
                    INSTRUMENT,
                    "1m",
                    first,
                    latest_closed + timedelta(minutes=1),
                )
            )
    finally:
        handler.close()

    assert quarantine.stats()["write_failures"] == 1
    assert quarantine.stats()["records_written"] == 0


def test_queue_overflow_is_not_quarantined(tmp_path: Path) -> None:
    exchange = FakeExchange()
    quarantine = _quarantine_store(tmp_path)
    handler = CcxtFeedHandler(
        config=_config(),
        exchange=exchange,
        quarantine=quarantine,
        queue_maxsize=1,
    )
    base = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    try:
        assert list(handler.iter_bars()) == []
        for index in range(3):
            exchange.push_for_symbols(
                {
                    SYMBOL: [
                        _row(
                            base + timedelta(minutes=index),
                            close=100.0 + index,
                            closed=True,
                        )
                    ]
                }
            )
        assert _wait_for(lambda: handler.dropped_bars >= 1)
        bars = _collect(handler, 5, timeout=0.5)
    finally:
        handler.close()

    assert handler.dropped_bars == 2
    assert len(bars) == 1
    assert bars[0].open_ts == base + timedelta(minutes=2)
    assert quarantine.stats()["records_written"] == 0
    assert quarantine.read_records().num_rows == 0
