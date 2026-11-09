"""Thin composite Feed Handler for configurations with several `[[feeds]]`."""

from __future__ import annotations

from typing import TYPE_CHECKING

from mdgw.core.instrument import InstrumentId
from mdgw.ingest.ccxt_feed import build_feed_handler
from mdgw.ingest.feed_status import (
    FeedConnectionStatus,
    handler_feed_statuses,
)
from mdgw.normalize.version import NORMALIZATION_RULE_VERSION

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence
    from datetime import datetime

    from mdgw.core.bar import RawBar


class CompositeFeedHandler:
    """Expose several Feed Handlers as one Feed Handler.

    This is a thin composite Feed Handler: normalization, aggregation, HWM and
    Backfill stay in their existing modules and operate on the combined stream.
    """

    def __init__(
        self,
        handlers: Sequence,
        *,
        name: str | None = None,
    ) -> None:
        if not handlers:
            msg = "CompositeFeedHandler requires at least one handler"
            raise ValueError(msg)
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

    def feed_status(self) -> tuple[FeedConnectionStatus, ...]:
        """One real status per configured child Feed Handler.

        The composite never aggregates children into a single synthetic state:
        each Venue/Feed keeps its own truthful lifecycle in ``/status`` and in
        the bounded-label metrics.
        """
        return tuple(
            status
            for handler in self._handlers
            for status in handler_feed_statuses(handler)
        )

    def close(self) -> None:
        for handler in self._handlers:
            close = getattr(handler, "close", None)
            if close is not None:
                close()


def attach_quarantine(
    handler,
    quarantine,
    *,
    rule_version: str | None = None,
) -> None:
    """Attach the Collector-owned Quarantine Store to a built Feed Handler.

    An injected (test or embedding) handler is constructed before the
    Collector exists, so the Collector attaches its Store afterward. Composite
    handlers recurse into their children; handlers without the seam are left
    untouched.
    """
    attach = getattr(handler, "attach_quarantine", None)
    if callable(attach):
        attach(quarantine, rule_version=rule_version)
        return
    for child in getattr(handler, "_handlers", ()):
        attach_quarantine(child, quarantine, rule_version=rule_version)


def build_configured_feed_handler(
    feeds: Sequence,
    *,
    quarantine=None,
    rule_version: str = NORMALIZATION_RULE_VERSION,
) -> object:
    """Build one Feed Handler for every `[[feeds]]` entry.

    A single entry returns that handler unchanged; several entries are joined
    by the thin `CompositeFeedHandler`. Every ccxt handler receives the same
    Collector-owned Quarantine Store.
    """
    if not feeds:
        msg = "at least one [[feeds]] entry is required"
        raise ValueError(msg)

    handlers = [
        build_feed_handler(
            feed, quarantine=quarantine, rule_version=rule_version
        )
        for feed in feeds
    ]
    if len(handlers) == 1:
        return handlers[0]
    return CompositeFeedHandler(handlers)


__all__ = [
    "CompositeFeedHandler",
    "attach_quarantine",
    "build_configured_feed_handler",
]
