"""ccxt.pro Feed Handler.

Connects one Venue, watches native 1m Bars, and yields only closed Bars as
`RawBar`s (Decimal, never float). The background asyncio thread owns the
exchange connection; `iter_bars()` never blocks forever, so the collector loop
can call `GatewayRuntime.ingest()` repeatedly.

Closed-Bar rule:
- when the source exposes `x` (is-closed), only `x=true` is accepted;
- when `x` is absent, the arrival of a newer Bar's `open_ts` closes the
  previous Bar. The Bar currently forming is never emitted.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import queue
import threading
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import NamedTuple

from mdgw.config import FeedConfig
from mdgw.core.bar import RawBar
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import INTERVALS
from mdgw.core.time import UTC, datetime_to_ns, to_utc, utc_now

logger = logging.getLogger(__name__)

BASE_INTERVAL = "1m"
DEFAULT_FIRST_WAIT_S = 0.1
DEFAULT_QUEUE_MAXSIZE = 10_000
HISTORY_PAGE_LIMIT = 1_000
DEFAULT_RECONNECT_BACKOFF_S = 1.0
DEFAULT_RECONNECT_BACKOFF_MAX_S = 60.0
FETCH_TIMEOUT_S = 120.0
SHUTDOWN_TIMEOUT_S = 10.0

# A single malformed row is skipped rather than failing the feed.
_MALFORMED_ROW_ERRORS = (TypeError, ValueError, InvalidOperation, IndexError)


# -- rate limiting -----------------------------------------------------------


class TokenBucket:
    """Per-Venue request budget layered on top of ccxt's own rate limiter."""

    def __init__(
        self,
        rate_per_s: float,
        *,
        capacity: float = 1.0,
        clock=time.monotonic,
        sleep=time.sleep,
    ) -> None:
        if rate_per_s <= 0:
            raise ValueError("rate_per_s must be positive")
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.rate_per_s = float(rate_per_s)
        self.capacity = float(capacity)
        self._tokens = self.capacity
        self._clock = clock
        self._sleep = sleep
        self._updated = clock()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> None:
        if tokens <= 0:
            raise ValueError("tokens must be positive")
        if tokens > self.capacity:
            tokens = self.capacity
        while True:
            with self._lock:
                now = self._clock()
                elapsed = max(0.0, now - self._updated)
                self._tokens = min(
                    self.capacity, self._tokens + elapsed * self.rate_per_s
                )
                self._updated = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                wait = (tokens - self._tokens) / self.rate_per_s
            # Sleep outside the lock so other callers can refill and proceed.
            self._sleep(wait)


_VENUE_BUCKETS: dict[tuple[str, float | None], TokenBucket] = {}
_VENUE_BUCKETS_LOCK = threading.Lock()


def _venue_token_bucket(venue: str, rate_limit_ms: float | None) -> TokenBucket:
    """One shared token bucket per Venue, layered on ccxt's own limiter."""
    key = (venue, rate_limit_ms)
    with _VENUE_BUCKETS_LOCK:
        bucket = _VENUE_BUCKETS.get(key)
        if bucket is None:
            rate = (
                1_000.0 / rate_limit_ms
                if rate_limit_ms and rate_limit_ms > 0
                else 10.0
            )
            bucket = TokenBucket(rate, capacity=max(1.0, rate))
            _VENUE_BUCKETS[key] = bucket
        return bucket


# -- raw ccxt rows -----------------------------------------------------------

_BAR_VALUE_NAMES = ("open", "high", "low", "close", "volume")


class _BarValues(NamedTuple):
    """The five numeric columns shared by every ccxt Bar row shape."""

    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


@dataclass(frozen=True, slots=True)
class _RawRow:
    """One validated ccxt row: timestamp, values, and optional closed flag."""

    timestamp_ms: int
    open_ts: datetime
    values: _BarValues
    closed: bool | None

    @classmethod
    def from_row(cls, row) -> _RawRow:
        timestamp_ms = _row_timestamp_ms(row)
        return cls(
            timestamp_ms=timestamp_ms,
            open_ts=_ms_to_datetime(timestamp_ms),
            values=_row_values(row),
            closed=_row_closed_flag(row),
        )


def _iter_raw_rows(rows) -> Iterable:
    """Normalize the row shapes ccxt uses into an iterable of single rows."""
    if rows is None:
        return ()
    if isinstance(rows, Mapping):
        return (rows,)
    if isinstance(rows, Sequence):
        if not rows:
            return ()
        if not isinstance(rows[0], (Sequence, Mapping)):
            return (rows,)
    return rows


def _row_timestamp_ms(row) -> int:
    if isinstance(row, Mapping):
        value = row.get("timestamp", row.get("ts"))
    else:
        value = row[0]
    if isinstance(value, bool) or not isinstance(value, int):
        if isinstance(value, float) and value.is_integer():
            return int(value)
        raise TypeError(f"ccxt Bar timestamp must be integer milliseconds: {value!r}")
    return value


def _timestamp_ms_or_none(row) -> int | None:
    try:
        return _row_timestamp_ms(row)
    except (TypeError, ValueError, IndexError):
        return None


def _ms_to_datetime(value: int) -> datetime:
    seconds, millis = divmod(value, 1_000)
    return datetime.fromtimestamp(seconds, tz=UTC) + timedelta(milliseconds=millis)


def _row_values(row) -> _BarValues:
    if isinstance(row, Mapping):
        raw_values = [row.get(name) for name in _BAR_VALUE_NAMES]
    else:
        raw_values = list(row[1:6])
    values = [
        _to_decimal(value, name)
        for value, name in zip(raw_values, _BAR_VALUE_NAMES, strict=True)
    ]
    return _BarValues(*values)


def _to_decimal(value, field: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise TypeError(f"ccxt Bar {field} must be numeric: {value!r}")
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError(f"ccxt Bar {field} must be finite: {value!r}")
    return result


def _row_closed_flag(row) -> bool | None:
    if isinstance(row, Mapping):
        for key in ("x", "closed", "is_closed"):
            if key in row:
                return _coerce_bool(row[key])
        info = row.get("info")
        if isinstance(info, Mapping) and "x" in info:
            return _coerce_bool(info["x"])
        return None
    if isinstance(row, Sequence) and len(row) >= 7:
        return _coerce_bool(row[6])
    return None


def _coerce_bool(value) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1"}:
            return True
        if lowered in {"false", "0"}:
            return False
    return None


def _interval_ns(interval: str) -> int:
    delta = INTERVALS[interval]
    return (
        delta.days * 86_400 * 1_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


# -- closed-Bar rule ---------------------------------------------------------


class _ClosedBarRule:
    """Applies the closed-Bar rule per symbol and keeps output monotonic.

    Owns the per-symbol state: the last Bar handed to the queue, and, when the
    source exposes no close flag, the Bar still forming.
    """

    def __init__(self) -> None:
        self._pending: dict[str, _RawRow] = {}
        self._last_open_ts: dict[str, datetime] = {}
        self._last_values: dict[str, tuple] = {}

    def accept(self, symbol: str, row: _RawRow) -> _RawRow | None:
        """Return the row once it is closed, or None while it is still forming."""
        if row.closed is True:
            self._pending.pop(symbol, None)
            return self._newer(symbol, row)
        if row.closed is False:
            self._pending.pop(symbol, None)
            return None

        pending = self._pending.get(symbol)
        self._pending[symbol] = row
        if pending is None or pending.open_ts >= row.open_ts:
            return None
        return self._newer(symbol, pending)

    def remember(
        self, symbol: str, open_ts: datetime, values: tuple | None = None
    ) -> bool:
        """Return True when this Bar should be emitted.

        A strictly newer `open_ts` always passes. A repeat of the last
        `open_ts` is dropped only when its values are unchanged; a source
        correction with the same `open_ts` must still reach the pipeline so the
        revision merge can persist it.
        """
        last = self._last_open_ts.get(symbol)
        if last is None or open_ts > last:
            self._last_open_ts[symbol] = open_ts
            if values is not None:
                self._last_values[symbol] = values
            return True
        if open_ts == last and values is not None:
            previous = self._last_values.get(symbol)
            if previous is None or values != previous:
                self._last_values[symbol] = values
                return True
        return False

    def last_open_ts(self, symbol: str) -> datetime | None:
        return self._last_open_ts.get(symbol)

    def _newer(self, symbol: str, row: _RawRow) -> _RawRow | None:
        return row if self.remember(symbol, row.open_ts, row.values) else None


# -- REST history window -----------------------------------------------------


@dataclass(frozen=True, slots=True)
class _HistoryWindow:
    """Half-open REST range plus the closed-Bar rule applied to history."""

    start_ns: int
    end_ns: int
    now_ns: int
    interval_ns: int

    @classmethod
    def build(
        cls, start: datetime, end: datetime, now: datetime | None = None
    ) -> _HistoryWindow:
        return cls(
            start_ns=datetime_to_ns(start),
            end_ns=datetime_to_ns(end),
            now_ns=datetime_to_ns(now if now is not None else utc_now()),
            interval_ns=_interval_ns(BASE_INTERVAL),
        )

    def needs_page(self, since_ms: int) -> bool:
        return since_ms * 1_000_000 < self.end_ns

    def contains_closed(self, open_ts: datetime) -> bool:
        open_ns = datetime_to_ns(open_ts)
        return (
            self.start_ns <= open_ns < self.end_ns
            and open_ns + self.interval_ns <= self.now_ns
        )


# -- Feed Handler ------------------------------------------------------------


class CcxtFeedHandler:
    """Feed Handler backed by a ccxt.pro exchange."""

    def __init__(
        self,
        *,
        config: FeedConfig,
        exchange,
        rate_limiter: TokenBucket | None = None,
        first_wait_s: float = DEFAULT_FIRST_WAIT_S,
        queue_maxsize: int = DEFAULT_QUEUE_MAXSIZE,
        reconnect_backoff_s: float = DEFAULT_RECONNECT_BACKOFF_S,
        reconnect_backoff_max_s: float = DEFAULT_RECONNECT_BACKOFF_MAX_S,
    ) -> None:
        self.config = config
        self.name = config.name
        self.venue = config.venue
        self.interval = BASE_INTERVAL
        self.instrument_ids = config.instrument_ids
        self.supports_history = bool(self._capability(exchange, "fetchOHLCV"))

        self._exchange = exchange
        self._first_wait_s = first_wait_s
        self._queue: queue.Queue[RawBar] = queue.Queue(maxsize=queue_maxsize)
        self._reconnect_backoff_s = reconnect_backoff_s
        self._reconnect_backoff_max_s = reconnect_backoff_max_s

        rate_limit_ms = getattr(exchange, "rateLimit", None)
        self._rate_limiter = rate_limiter or _venue_token_bucket(
            config.venue, rate_limit_ms
        )

        self._symbol_to_instrument = {
            symbol: InstrumentId(venue=config.venue, symbol=symbol).value
            for symbol in config.symbols
        }
        self._instrument_to_symbol = {
            instrument_id: symbol
            for symbol, instrument_id in self._symbol_to_instrument.items()
        }

        # Watch state, owned by the background loop thread.
        self._closed_bar_rule = _ClosedBarRule()
        self.dropped_bars = 0

        self._start_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._watch_task: asyncio.Task | None = None
        self._ready = threading.Event()
        self._closed = False
        self._exchange_closed = False

        self._watch_for_symbols = self._capability(exchange, "watchOHLCVForSymbols")
        self._watch_one = self._capability(exchange, "watchOHLCV")
        if not (self._watch_for_symbols or self._watch_one):
            raise ValueError(
                f"exchange {config.venue!r} supports neither watchOHLCV nor "
                "watchOHLCVForSymbols"
            )

    # -- FeedHandler Protocol ------------------------------------------------

    def iter_bars(self) -> Iterator[RawBar]:
        """Drain the currently queued closed Bars, waiting briefly for one."""
        if self._closed:
            return iter(())
        self._ensure_worker()
        try:
            first = self._queue.get(timeout=self._first_wait_s)
        except queue.Empty:
            return iter(())
        bars = [first]
        while True:
            try:
                bars.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return iter(bars)

    def fetch_history(
        self,
        instrument_id: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> Iterable[RawBar]:
        """Return closed Bars with `start <= open_ts < end` from REST history."""
        parsed = InstrumentId.parse(instrument_id).value
        if parsed not in self._instrument_to_symbol or interval != BASE_INTERVAL:
            return ()
        if not self.supports_history:
            raise NotImplementedError(
                f"Feed Handler {self.name!r} does not support history"
            )
        if self._closed:
            raise RuntimeError("Feed Handler is closed")
        self._ensure_worker()
        loop = self._loop
        if loop is None:
            raise RuntimeError(f"Feed Handler {self.name!r} worker is not running")
        future = asyncio.run_coroutine_threadsafe(
            self._fetch_history_async(
                self._instrument_to_symbol[parsed], to_utc(start), to_utc(end)
            ),
            loop,
        )
        return tuple(future.result(timeout=FETCH_TIMEOUT_S))

    # -- lifecycle -----------------------------------------------------------

    def close(self) -> None:
        """Stop the watcher, close the exchange, and let `iter_bars()` exit."""
        with self._start_lock:
            if self._closed:
                return
            self._closed = True
            thread = self._thread
            loop = self._loop

        if thread is not None and thread.is_alive() and loop is not None:
            future = asyncio.run_coroutine_threadsafe(self._shutdown(), loop)
            try:
                future.result(timeout=SHUTDOWN_TIMEOUT_S)
            except Exception:  # pragma: no cover - defensive shutdown path
                logger.exception("error while shutting down ccxt Feed Handler")
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=SHUTDOWN_TIMEOUT_S)
            return

        self._close_exchange_sync()

    async def _shutdown(self) -> None:
        task = self._watch_task
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        await self._close_exchange()

    async def _close_exchange(self) -> None:
        if self._exchange_closed:
            return
        self._exchange_closed = True
        close = getattr(self._exchange, "close", None)
        if close is None:
            return
        result = close()
        if inspect.isawaitable(result):
            await result

    def _close_exchange_sync(self) -> None:
        if self._exchange_closed:
            return
        self._exchange_closed = True
        try:
            result = self._exchange.close()
            if inspect.isawaitable(result):
                asyncio.run(result)
        except Exception:  # pragma: no cover - defensive shutdown path
            logger.exception("error while closing ccxt exchange")

    # -- background worker ---------------------------------------------------

    def _ensure_worker(self) -> None:
        with self._start_lock:
            if self._closed or self._thread is not None:
                return
            thread = threading.Thread(
                target=self._thread_main,
                name=f"mdgw-ccxt-{self.name}",
                daemon=True,
            )
            self._thread = thread
            thread.start()
        if not self._ready.wait(timeout=5.0):
            raise RuntimeError(f"ccxt Feed Handler {self.name!r} failed to start")

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._watch_task = loop.create_task(self._watch_forever())
        self._ready.set()
        try:
            loop.run_forever()
        finally:
            pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            loop.close()

    async def _watch_forever(self) -> None:
        await self._load_markets()
        backoff = self._reconnect_backoff_s
        while not self._closed:
            try:
                await self._watch_once()
                backoff = self._reconnect_backoff_s
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._closed:
                    return
                logger.warning(
                    "ccxt watch failed for %s: %s; reconnecting in %.1fs",
                    self.name,
                    exc,
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._reconnect_backoff_max_s)
                try:
                    await self._backfill_gap()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "ccxt reconnect backfill failed for %s", self.name
                    )

    async def _load_markets(self) -> None:
        loader = getattr(self._exchange, "load_markets", None)
        if callable(loader):
            result = loader()
            if inspect.isawaitable(result):
                await result

    async def _watch_once(self) -> None:
        if self._watch_for_symbols:
            payload = await self._exchange.watch_ohlcv_for_symbols(
                [[symbol, BASE_INTERVAL] for symbol in self.config.symbols]
            )
            self._handle_payload(payload)
            return

        tasks = [
            asyncio.create_task(self._watch_symbol(symbol))
            for symbol in self.config.symbols
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _watch_symbol(self, symbol: str) -> None:
        while not self._closed:
            rows = await self._exchange.watch_ohlcv(symbol, BASE_INTERVAL)
            self._handle_rows(symbol, rows)

    # -- live rows -----------------------------------------------------------

    def _handle_payload(self, payload) -> None:
        if not isinstance(payload, Mapping):
            logger.warning("unexpected watch_ohlcv_for_symbols payload: %r", payload)
            return
        for symbol, rows in payload.items():
            self._handle_rows(symbol, rows)

    def _handle_rows(self, symbol: str, rows) -> None:
        if symbol not in self._symbol_to_instrument:
            return
        for row in _iter_raw_rows(rows):
            try:
                parsed = _RawRow.from_row(row)
            except _MALFORMED_ROW_ERRORS as exc:
                logger.warning(
                    "discarding malformed ccxt Bar for %s:%s: %s",
                    self.venue,
                    symbol,
                    exc,
                )
                continue

            closed = self._closed_bar_rule.accept(symbol, parsed)
            if closed is not None:
                self._enqueue(self._raw_bar(symbol, closed))

    def _raw_bar(self, symbol: str, row: _RawRow) -> RawBar:
        values = row.values
        return RawBar(
            instrument_id=self._symbol_to_instrument[symbol],
            interval=BASE_INTERVAL,
            open_ts=row.open_ts,
            open=values.open,
            high=values.high,
            low=values.low,
            close=values.close,
            volume=values.volume,
            trade_count=None,
            revision=0,
        )

    def _enqueue(self, bar: RawBar) -> None:
        try:
            self._queue.put_nowait(bar)
        except queue.Full:
            try:
                self._queue.get_nowait()
                self.dropped_bars += 1
            except queue.Empty:  # pragma: no cover - concurrent drain
                pass
            try:
                self._queue.put_nowait(bar)
            except queue.Full:  # pragma: no cover - concurrent producer
                self.dropped_bars += 1

    # -- REST history --------------------------------------------------------

    async def _fetch_history_async(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[RawBar]:
        window = _HistoryWindow.build(start, end)
        bars: dict[int, RawBar] = {}

        since_ms = window.start_ns // 1_000_000
        while window.needs_page(since_ms):
            rows = await self._fetch_history_page(symbol, since_ms)
            if not rows:
                break

            timestamps_ms = self._collect_history_page(symbol, rows, window, bars)
            if not timestamps_ms:
                break
            last_ms = max(timestamps_ms)
            if last_ms < since_ms:
                break
            since_ms = last_ms + 1
            if len(rows) < HISTORY_PAGE_LIMIT:
                break

        return [bars[open_ns] for open_ns in sorted(bars)]

    async def _fetch_history_page(self, symbol: str, since_ms: int) -> Sequence:
        await self._acquire_rate_limit()
        return await self._exchange.fetch_ohlcv(
            symbol, BASE_INTERVAL, since=since_ms, limit=HISTORY_PAGE_LIMIT
        )

    def _collect_history_page(
        self,
        symbol: str,
        rows: Sequence,
        window: _HistoryWindow,
        bars: dict[int, RawBar],
    ) -> list[int]:
        """Add closed rows to `bars`; return every readable row timestamp."""
        timestamps_ms: list[int] = []
        for row in rows:
            try:
                parsed = _RawRow.from_row(row)
            except _MALFORMED_ROW_ERRORS as exc:
                logger.warning(
                    "discarding malformed ccxt history Bar for %s:%s: %s",
                    self.venue,
                    symbol,
                    exc,
                )
                # A bad value must not stall pagination when the timestamp is
                # still readable.
                timestamp_ms = _timestamp_ms_or_none(row)
                if timestamp_ms is not None:
                    timestamps_ms.append(timestamp_ms)
                continue

            timestamps_ms.append(parsed.timestamp_ms)
            if window.contains_closed(parsed.open_ts):
                bars[datetime_to_ns(parsed.open_ts)] = self._raw_bar(symbol, parsed)
        return timestamps_ms

    async def _acquire_rate_limit(self) -> None:
        await asyncio.to_thread(self._rate_limiter.acquire)

    async def _backfill_gap(self) -> None:
        """Fetch missing closed Bars after a reconnect and queue them."""
        if not self.supports_history:
            return
        now = utc_now()
        for symbol in self.config.symbols:
            last = self._closed_bar_rule.last_open_ts(symbol)
            start = (
                last
                if last is not None
                else now - timedelta(hours=self.config.backfill_hours)
            )
            bars = await self._fetch_history_async(symbol, start, now)
            for bar in bars:
                values = (
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                )
                if self._closed_bar_rule.remember(
                    symbol, bar.open_ts, values
                ):
                    self._enqueue(bar)

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _capability(exchange, name: str) -> bool:
        has = getattr(exchange, "has", None) or {}
        try:
            return bool(has.get(name))
        except AttributeError:  # pragma: no cover - exotic exchange object
            return False


def build_feed_handler(
    feed_config: FeedConfig,
    *,
    exchange=None,
) -> FeedHandler:
    """Build the Feed Handler for one `[[feeds]]` entry.

    `kind = "fake"` selects the deterministic, network-free generator used by
    development and the multi-process tests. `exchange` is injectable so tests
    can supply a preconfigured ccxt.pro client.
    """
    if feed_config.kind == "fake":
        # Imported lazily: the fake factory itself imports the composite
        # adapter, which must not create an import cycle at module load.
        from mdgw.ingest.fake import build_fake_feed_handler

        return build_fake_feed_handler(feed_config)
    if feed_config.kind != "ccxt":
        raise ValueError(f"unsupported Feed Handler kind: {feed_config.kind!r}")
    if exchange is None:
        exchange = _create_exchange(feed_config)
    return CcxtFeedHandler(config=feed_config, exchange=exchange)


def _create_exchange(feed_config: FeedConfig):
    try:
        import ccxt.pro as ccxt_pro
    except ImportError as exc:  # pragma: no cover - dependency environment
        raise RuntimeError(
            "ccxt is required for kind='ccxt' Feed Handlers"
        ) from exc

    exchange_class = getattr(ccxt_pro, feed_config.venue, None)
    if exchange_class is None:
        raise ValueError(f"unknown ccxt Venue: {feed_config.venue!r}")
    return exchange_class(
        {
            "enableRateLimit": feed_config.enable_rate_limit,
            "options": {"defaultType": feed_config.market_type},
        }
    )
