"""Truthful Feed Handler connection status.

A Feed Handler owns a real connection lifecycle: it starts, connects, may lose
the Venue connection, reconnects with backoff, and can fail closed. This module
is the public seam that exposes those transitions (plus the last event receive
time and a sanitized last error) to the Collector Service Role and its
bounded-label metrics.

It deliberately does not read Feed Freshness (``HealthTracker``): a feed can be
"fresh" because Bars are still buffered or backfilled while its upstream
connection is down, and a connected feed can be legitimately quiet. Connection
health and data freshness are different concepts (GLOSSARY.md).

Only bounded enums leave this module: :data:`FEED_STATES` for the lifecycle
state and :data:`FEED_ERROR_CODES` for the sanitized error category. Raw
exception text, URLs, credentials and request ids never become status fields
or metric labels.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mdgw.core.instrument import InstrumentId
from mdgw.core.time import datetime_to_ns, ns_to_iso, to_utc

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import datetime

#: Bounded lifecycle vocabulary. ``unknown`` is only for handlers that do not
#: implement the status seam; it is never inferred from data freshness.
FEED_STATES: tuple[str, ...] = (
    "stopped",
    "connecting",
    "connected",
    "reconnecting",
    "failed",
    "unknown",
)

#: Bounded sanitized error categories. Raw exception text is never exposed.
FEED_ERROR_CODES: tuple[str, ...] = (
    "none",
    "connection_error",
    "timeout",
    "authentication_error",
    "quarantine_write_error",
    "other",
)


@dataclass(frozen=True, slots=True)
class FeedConnectionStatus:
    """One configured Feed Handler's truthfully observed connection state."""

    feed: str
    venue: str
    state: str = "unknown"
    state_since: datetime | None = None
    #: Arrival time of the last live watch event. It is explicitly not a
    #: protocol heartbeat: ccxt.pro exposes no heartbeat timestamp.
    last_event_ts: datetime | None = None
    #: Only a real protocol heartbeat may populate this field.
    last_heartbeat_ts: datetime | None = None
    heartbeat_supported: bool = False
    reconnect_count: int = 0
    last_error_code: str | None = None
    last_error_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.state not in FEED_STATES:
            msg = f"invalid Feed connection state: {self.state!r}"
            raise ValueError(msg)
        if (
            self.last_error_code is not None
            and self.last_error_code not in FEED_ERROR_CODES
        ):
            msg = f"invalid sanitized Feed error code: {self.last_error_code!r}"
            raise ValueError(msg)
        if self.last_error_code is None and self.last_error_at is not None:
            msg = "last_error_at requires a last_error_code"
            raise ValueError(msg)

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe wire view used by the Collector ``/status`` payload."""
        last_error = None
        if self.last_error_code is not None:
            last_error = {
                "code": self.last_error_code,
                "at": _iso(self.last_error_at),
            }
        return {
            "feed": self.feed,
            "venue": self.venue,
            "state": self.state,
            "state_since": _iso(self.state_since),
            "last_event_ts": _iso(self.last_event_ts),
            "last_heartbeat_ts": _iso(self.last_heartbeat_ts),
            "heartbeat_supported": self.heartbeat_supported,
            "reconnect_count": int(self.reconnect_count),
            "last_error": last_error,
        }


def classify_feed_error(error: BaseException) -> str:
    """Map one exception to a bounded, sanitized error category."""
    name = type(error).__name__.lower()
    if isinstance(error, TimeoutError) or "timeout" in name:
        return "timeout"
    if "quarantine" in name:
        return "quarantine_write_error"
    if "auth" in name or "permission" in name or "forbidden" in name:
        return "authentication_error"
    if (
        isinstance(error, (ConnectionError, OSError))
        or "connection" in name
        or "network" in name
        or "socket" in name
    ):
        return "connection_error"
    return "other"


def handler_feed_statuses(handler: Any) -> tuple[FeedConnectionStatus, ...]:
    """Return the statuses one Feed Handler (or a composite) truly exposes.

    A handler that predates the seam reports ``unknown``; this module never
    substitutes Feed Freshness or Bar timestamps for connection state.
    """
    status_method = getattr(handler, "feed_status", None)
    if callable(status_method):
        statuses = status_method()
        if isinstance(statuses, FeedConnectionStatus):
            return (statuses,)
        return tuple(statuses)
    return (unknown_handler_status(handler),)


def unknown_handler_status(handler: Any) -> FeedConnectionStatus:
    """Status for a handler without the seam (or before it exposes one)."""
    name = getattr(handler, "name", None)
    feed = name if isinstance(name, str) and name else type(handler).__name__
    venue = getattr(handler, "venue", None)
    if not (isinstance(venue, str) and venue):
        venue = _venue_from_instruments(handler.instrument_ids)
    return FeedConnectionStatus(
        feed=feed,
        venue=venue,
        state="unknown",
    )


def _venue_from_instruments(instrument_ids: Iterable[str]) -> str:
    for instrument_id in instrument_ids or ():
        try:
            return InstrumentId.parse(str(instrument_id)).venue
        except ValueError:
            continue
    return ""


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return ns_to_iso(datetime_to_ns(to_utc(value)))


__all__ = [
    "FEED_ERROR_CODES",
    "FEED_STATES",
    "FeedConnectionStatus",
    "classify_feed_error",
    "handler_feed_statuses",
    "unknown_handler_status",
]
