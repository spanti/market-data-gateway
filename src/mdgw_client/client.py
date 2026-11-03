"""``GatewayClient``: the one entry point most users need.

History queries use the standard library (``urllib.request``), live
subscriptions use pyzmq or the optional ``websockets`` package, and Replay
control uses ZMQ REQ/REP. No gateway internals are imported.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Self
from urllib.parse import urlsplit

from mdgw_client.errors import (
    CONFIGURATION_ERROR,
    REPLAY_UNAVAILABLE,
    ClientError,
)
from mdgw_client.history import build_bars_url, fetch_arrow
from mdgw_client.live import LiveSubscription
from mdgw_client.replay import ReplayClient, ReplaySession
from mdgw_client.schema import BAR_SCHEMA

if TYPE_CHECKING:
    from collections.abc import Sequence
    from types import TracebackType

    import pyarrow as pa

    from mdgw_client.time import Timestamp


def derive_ws_url(base_url: str, ws_path: str = "/ws") -> str:
    """Turn ``http://host:port[/prefix]`` into ``ws://host:port[/prefix]/ws``."""
    parts = urlsplit(base_url)
    scheme = "wss" if parts.scheme == "https" else "ws"
    prefix = parts.path.rstrip("/")
    path = f"{prefix}{ws_path}" if prefix else ws_path
    return f"{scheme}://{parts.netloc}{path}"


class GatewayClient:
    """Thin, framework-neutral client for history, live and replay."""

    def __init__(  # ruff: ignore[too-many-arguments] - keyword-only client configuration
        self,
        base_url: str,
        *,
        stream_endpoint: str | None = None,
        replay_publish_endpoint: str | None = None,
        replay_control_endpoint: str | None = None,
        ws_url: str | None = None,
        timeout_s: float = 10.0,
        reconnect_backoff_s: tuple[float, ...] = (0.1, 0.2, 0.5, 1.0, 2.0, 5.0),
    ) -> None:
        if not base_url:
            msg = "base_url is required"
            raise ValueError(msg)
        self.base_url = base_url.rstrip("/")
        self.stream_endpoint = stream_endpoint
        self.replay_publish_endpoint = replay_publish_endpoint
        self.replay_control_endpoint = replay_control_endpoint
        self.ws_url = ws_url or derive_ws_url(self.base_url)
        self.timeout_s = timeout_s
        self.reconnect_backoff_s = tuple(reconnect_backoff_s)

        self._subscriptions: list[LiveSubscription] = []
        self._replay: ReplayClient | None = None
        self._closed = False

    # -- history -----------------------------------------------------------

    def history(
        self,
        interval: str,
        *,
        instrument: str | None = None,
        start: Timestamp | None = None,
        end: Timestamp | None = None,
        limit: int | None = None,
    ) -> pa.Table:
        """Query ``GET /api/v1/bars?format=arrow`` and return an Arrow Table.

        ``start``/``end`` accept a ``datetime`` (naive means UTC), UTC
        nanoseconds or an ISO-8601 string. Call ``table.to_pandas()`` for a
        DataFrame: decimal columns become ``object`` dtype holding
        :class:`decimal.Decimal`, and timestamps stay exact in Arrow (pandas 3
        may expose them at microsecond resolution).
        """
        url = build_bars_url(
            self.base_url,
            interval,
            instrument=instrument,
            start=start,
            end=end,
            limit=limit,
        )
        return fetch_arrow(url, timeout_s=self.timeout_s)

    # -- live --------------------------------------------------------------

    def subscribe(
        self,
        topics: Sequence[str],
        *,
        transport: str = "auto",
        queue_size: int = 1000,
    ) -> LiveSubscription:
        """Subscribe to live topics with prefix semantics.

        ``transport="auto"`` chooses ZMQ when ``stream_endpoint`` is set and
        WebSocket otherwise. A topic such as ``market.bar.1m.binance:`` is a
        prefix: it matches every instrument under that prefix.
        """
        if isinstance(topics, str):
            topics = [topics]
        normalized = list(topics)
        if not normalized:
            msg = "at least one topic is required"
            raise ValueError(msg)

        resolved = transport
        if resolved == "auto":
            resolved = "zmq" if self.stream_endpoint else "ws"
        if resolved == "zmq" and not self.stream_endpoint:
            raise ClientError(
                CONFIGURATION_ERROR,
                "transport='zmq' requires stream_endpoint",
            )
        if resolved == "ws" and not self.ws_url:
            raise ClientError(
                CONFIGURATION_ERROR, "transport='ws' requires ws_url"
            )

        subscription = LiveSubscription(
            transport=resolved,
            topics=normalized,
            stream_endpoint=self.stream_endpoint,
            ws_url=self.ws_url,
            queue_size=queue_size,
            timeout_s=self.timeout_s,
            reconnect_backoff_s=self.reconnect_backoff_s,
        )
        self._subscriptions.append(subscription)
        return subscription

    # -- replay ------------------------------------------------------------

    @property
    def replay(self) -> ReplayClient:
        """The Replay control client; requires both replay endpoints."""
        if not self.replay_publish_endpoint or not self.replay_control_endpoint:
            raise ClientError(
                REPLAY_UNAVAILABLE,
                "Replay requires replay_publish_endpoint and "
                "replay_control_endpoint",
            )
        if self._replay is None:
            self._replay = ReplayClient(
                publish_endpoint=self.replay_publish_endpoint,
                control_endpoint=self.replay_control_endpoint,
                timeout_s=self.timeout_s,
                reconnect_backoff_s=self.reconnect_backoff_s,
            )
        return self._replay

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for subscription in self._subscriptions:
            subscription.close()
        self._subscriptions.clear()
        if self._replay is not None:
            self._replay.close()
            self._replay = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


__all__ = ["BAR_SCHEMA", "GatewayClient", "ReplaySession", "derive_ws_url"]
