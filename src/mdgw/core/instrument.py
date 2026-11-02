"""Core value types shared by every Service Role."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from urllib.parse import quote


@dataclass(frozen=True, slots=True)
class Instrument:
    """An Instrument Reference Data record as stored in an Instrument Snapshot.

    Timestamps are UTC nanoseconds so the value never has to pass through a
    float or lose precision on the way to the Arrow contract.
    """

    instrument_id: str
    venue: str
    symbol: str
    asset_class: str
    instrument_type: str
    base: str
    quote: str
    tick_size: Decimal
    lot_size: Decimal
    multiplier: Decimal
    timezone: str
    listing_ts_ns: int
    delisted_ts_ns: int | None = None

    @property
    def is_delisted(self) -> bool:
        return self.delisted_ts_ns is not None


@dataclass(frozen=True, slots=True)
class InstrumentId:
    """`<venue>:<symbol>` identity used everywhere inside the Gateway."""

    venue: str
    symbol: str

    @classmethod
    def parse(cls, value: str) -> "InstrumentId":
        venue, separator, symbol = value.partition(":")
        if not separator or not venue or not symbol:
            raise ValueError(f"invalid InstrumentId: {value!r}")
        return cls(venue=venue, symbol=symbol)

    @property
    def value(self) -> str:
        return f"{self.venue}:{self.symbol}"

    @property
    def file_symbol(self) -> str:
        """Symbol encoded for use as a single Dataset path segment."""
        return quote(self.symbol, safe="")

    def __str__(self) -> str:
        return self.value
