"""In-process fake Feed Handler used by behavior tests and the walking skeleton."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from mdgw.core.bar import RawBar
from mdgw.core.instrument import InstrumentId


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
    """A Feed Handler that emits a configured sequence of closed 1m Bars."""

    def __init__(
        self,
        *,
        instrument_id: str,
        bars: Sequence[BarInput],
        name: str = "fake",
        interval: str = "1m",
    ) -> None:
        self.instrument_id = InstrumentId.parse(instrument_id).value
        self._bars = tuple(bars)
        self.name = name
        self.interval = interval

    def iter_bars(self) -> Iterator[RawBar]:
        for bar in self._bars:
            yield RawBar(
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
