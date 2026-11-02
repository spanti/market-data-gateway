"""Feed Handler seam.

A Feed Handler connects one Venue, parses its messages into canonical input,
and yields only closed Bars. It also declares the Instruments it carries and
whether it can serve history for Backfill.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import datetime
from typing import Protocol

from mdgw.core.bar import RawBar


class FeedHandler(Protocol):
    name: str
    instrument_ids: tuple[str, ...]
    supports_history: bool

    def iter_bars(self) -> Iterator[RawBar]:
        """Yield closed Bars in source order."""
        ...

    def fetch_history(
        self,
        instrument_id: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> Iterable[RawBar]:
        """Return closed Bars with `start <= open_ts < end` (synchronous).

        `start` is inclusive and `end` exclusive. Implementations must not
        return a Bar whose `close_ts` is after `end`; the caller re-checks the
        closed-Bar rule before writing.
        """
        ...
