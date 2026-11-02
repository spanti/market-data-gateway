"""In-process fake Feed Handler used by behavior tests and the walking skeleton."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from mdgw.config import FeedConfig
from mdgw.core.bar import RawBar
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import INTERVALS
from mdgw.core.time import iso_to_ns, ns_to_datetime, to_utc, utc_now


@dataclass(frozen=True, slots=True)
class BarInput:
    open_ts: datetime
    open: Decimal | str
    high: Decimal | str
    low: Decimal | str
    close: Decimal | str
    volume: Decimal | str
    trade_count: int | None = None
    revision: int = 0


class FakeFeedHandler:
    """A Feed Handler that emits a configured sequence of closed Bars.

    `history` is an optional replayable source for Backfill tests. Supplying it
    makes `supports_history` true unless explicitly overridden.
    """

    def __init__(
        self,
        *,
        instrument_id: str,
        bars: Sequence[BarInput],
        name: str = "fake",
        interval: str = "1m",
        history: Sequence[BarInput] | None = None,
        supports_history: bool | None = None,
    ) -> None:
        self.instrument_id = InstrumentId.parse(instrument_id).value
        self._bars = tuple(bars)
        self._history = tuple(history or ())
        self.name = name
        self.interval = interval
        self.supports_history = (
            bool(supports_history) if supports_history is not None else history is not None
        )

    @property
    def instrument_ids(self) -> tuple[str, ...]:
        return (self.instrument_id,)

    def iter_bars(self) -> Iterator[RawBar]:
        for bar in self._bars:
            yield self._raw_bar(bar)

    def fetch_history(
        self,
        instrument_id: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> Iterable[RawBar]:
        if not self.supports_history:
            raise NotImplementedError(
                f"Feed Handler {self.name!r} does not support history"
            )
        if InstrumentId.parse(instrument_id).value != self.instrument_id:
            return ()
        if interval != self.interval:
            return ()

        start = to_utc(start)
        end = to_utc(end)
        return tuple(
            self._raw_bar(bar)
            for bar in self._history
            if start <= to_utc(bar.open_ts) < end
            and to_utc(bar.open_ts) + INTERVALS[self.interval] <= end
        )

    def _raw_bar(self, bar: BarInput) -> RawBar:
        return RawBar(
            instrument_id=self.instrument_id,
            interval=self.interval,
            open_ts=bar.open_ts,
            open=_decimal(bar.open),
            high=_decimal(bar.high),
            low=_decimal(bar.low),
            close=_decimal(bar.close),
            volume=_decimal(bar.volume),
            trade_count=bar.trade_count,
            revision=bar.revision,
        )


def _decimal(value: Decimal | str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, str):
        return Decimal(value)
    raise TypeError(f"fake Bar values must be Decimal or str, got {type(value)!r}")


class OneShotFakeFeedHandler(FakeFeedHandler):
    """A `FakeFeedHandler` that drains its Bars only once.

    Live Service Role loops call ``iter_bars()`` repeatedly. A one-shot
    generator avoids re-ingesting and re-publishing the same fixed Bar run on
    every collector poll.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._drained = False

    def iter_bars(self) -> Iterator[RawBar]:
        if self._drained:
            return
        self._drained = True
        yield from super().iter_bars()


def build_fake_feed_handler(feed_config: FeedConfig) -> FakeFeedHandler:
    """Build a deterministic, network-free Feed Handler from `FeedConfig`.

    Each configured symbol receives the same run of consecutive closed 1m
    Bars. Without `fake_start_ts` the run ends at the current whole minute, so
    every Bar is closed when the collector starts.
    """
    from mdgw.ingest.composite import CompositeFeedHandler

    bar_inputs = _generated_bar_inputs(feed_config)
    handlers: list[FakeFeedHandler] = []
    for symbol in feed_config.symbols:
        instrument_id = InstrumentId(
            venue=feed_config.venue, symbol=symbol
        ).value
        handlers.append(
            OneShotFakeFeedHandler(
                instrument_id=instrument_id,
                bars=bar_inputs,
                name=feed_config.name,
                interval="1m",
                supports_history=False,
            )
        )
    if len(handlers) == 1:
        return handlers[0]
    return CompositeFeedHandler(handlers, name=feed_config.name)


def _generated_bar_inputs(feed_config: FeedConfig) -> list[BarInput]:
    count = int(feed_config.fake_bars)
    if count <= 0:
        return []
    if feed_config.fake_start_ts is not None:
        start = ns_to_datetime(iso_to_ns(feed_config.fake_start_ts))
    else:
        # End at the current whole minute so the last Bar has already closed.
        end = utc_now().replace(second=0, microsecond=0)
        start = end - timedelta(minutes=count)

    price = Decimal(feed_config.fake_price)
    return [
        BarInput(
            open_ts=start + timedelta(minutes=index),
            open=price,
            high=price,
            low=price,
            close=price,
            volume=Decimal("1"),
            trade_count=1,
        )
        for index in range(count)
    ]


__all__ = [
    "BarInput",
    "FakeFeedHandler",
    "OneShotFakeFeedHandler",
    "build_fake_feed_handler",
]
