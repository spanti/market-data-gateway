"""Feed Handler seam.

A Feed Handler connects one Venue, parses its messages into canonical input,
and yields only closed Bars. History/backfill is added by the backfill ticket.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol

from mdgw.core.bar import RawBar


class FeedHandler(Protocol):
    name: str

    def iter_bars(self) -> Iterator[RawBar]:
        """Yield closed Bars in source order."""
        ...
