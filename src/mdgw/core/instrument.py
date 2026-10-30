"""Core value types shared by every Service Role."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote


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
