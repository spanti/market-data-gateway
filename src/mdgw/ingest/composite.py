"""Thin composite Feed Handler for configurations with several `[[feeds]]`."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from datetime import datetime

from mdgw.core.bar import RawBar
from mdgw.core.instrument import InstrumentId


class CompositeFeedHandler:
    """Expose several Feed Handlers as one `FeedHandler`.

    This is only an adapter: normalization, aggregation, HWM and Backfill stay
    in their existing modules and operate on the combined stream.
    """

    def __init__(
        self,
        handlers: Sequence,
        *,
        name: str | None = None,
    ) -> None:
        if not handlers:
            raise ValueError("CompositeFeedHandler requires at least one handler")
        self._handlers = tuple(handlers)
        self.name = name or "+".join(handler.name for handler in self._handlers)
        self.instrument_ids = tuple(
            instrument_id
            for handler in self._handlers
            for instrument_id in handler.instrument_ids
        )
        self.supports_history = any(
            handler.supports_history for handler in self._handlers
        )

    def iter_bars(self) -> Iterator[RawBar]:
        for handler in self._handlers:
            yield from handler.iter_bars()

    def fetch_history(
        self,
        instrument_id: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> Iterable[RawBar]:
        parsed = InstrumentId.parse(instrument_id).value
        for handler in self._handlers:
            if parsed in handler.instrument_ids:
                return handler.fetch_history(parsed, interval, start, end)
        return ()

    def close(self) -> None:
        for handler in self._handlers:
            close = getattr(handler, "close", None)
            if close is not None:
                close()


def build_configured_feed_handler(feeds: Sequence) -> object:
    """Build one Feed Handler for every `[[feeds]]` entry.

    A single entry returns that handler unchanged; several entries are joined
    by the thin `CompositeFeedHandler`.
    """
    if not feeds:
        raise ValueError("at least one [[feeds]] entry is required")
    from mdgw.ingest.ccxt_feed import build_feed_handler

    handlers = [build_feed_handler(feed) for feed in feeds]
    if len(handlers) == 1:
        return handlers[0]
    return CompositeFeedHandler(handlers)


__all__ = [
    "CompositeFeedHandler",
    "build_configured_feed_handler",
]
