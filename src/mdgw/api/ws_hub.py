"""WebSocket fan-out hub for the Streaming Interface.

The hub lives on the aiohttp event loop. ZMQ subscriber threads never touch
asyncio objects directly: they hand already-encoded messages to the hub with
`loop.call_soon_threadsafe`.

Each session owns one bounded queue. A producer that outpaces the socket drops
the oldest queued message, accumulates `dropped`, and the session reports a
throttled `lag_notice` on its next write. The connection is never closed for
being slow.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress
from typing import Any
from uuid import uuid4

from aiohttp import web

from mdgw.core.time import datetime_to_ns, ns_to_iso, utc_now

logger = logging.getLogger(__name__)

EventMessage = tuple[str, dict[str, Any]]


def valid_topics(value: Any) -> list[str]:
    """Keep only usable topic strings from a client `topics` value.

    Empty/whitespace strings and non-string entries are ignored instead of
    failing the whole subscription (or the connection).
    """
    if not isinstance(value, (list, tuple)):
        return []
    return [topic for topic in value if isinstance(topic, str) and topic.strip()]


class WebSocketSession:
    """One client connection with a bounded outbound queue."""

    def __init__(
        self,
        ws: web.WebSocketResponse,
        *,
        queue_size: int,
        lag_notice_interval_s: float,
    ) -> None:
        self.session_id = uuid4().hex
        self._ws = ws
        self._queue: asyncio.Queue[EventMessage] = asyncio.Queue(maxsize=queue_size)
        self._subscriptions: set[str] = set()
        self._loop = asyncio.get_running_loop()
        self._lag_notice_interval_s = lag_notice_interval_s

        self._dropped = 0
        self.dropped_total = 0
        self._lag_pending = False
        self._lag_since: str | None = None
        self._last_notice_at = 0.0
        self._lag_handle: asyncio.TimerHandle | None = None
        self._background: set[asyncio.Task] = set()

        self._send_lock = asyncio.Lock()
        self._closed = False
        self._writer_task = self._loop.create_task(
            self._write_loop(), name="mdgw-ws-writer"
        )

    # -- subscriptions -----------------------------------------------------

    def subscribe(self, topics: list[str]) -> None:
        self._subscriptions.update(topics)

    def unsubscribe(self, topics: list[str]) -> None:
        self._subscriptions.difference_update(topics)

    @property
    def subscriptions(self) -> frozenset[str]:
        return frozenset(self._subscriptions)

    def matches(self, topic: str) -> bool:
        # Exact and prefix subscriptions share one rule: a topic is delivered
        # when it starts with a subscribed pattern. Prefixes such as
        # `market.bar.1m.binance:` and `market.bar.` therefore work naturally.
        return any(topic.startswith(pattern) for pattern in self._subscriptions)

    # -- producer side (event loop only) -----------------------------------

    def enqueue(self, topic: str, data: dict[str, Any]) -> None:
        if self._closed:
            return
        if self._queue.full():
            with suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()  # drop the oldest message
            self._dropped += 1
            self.dropped_total += 1
            self._note_lag()
        self._queue.put_nowait((topic, data))

    def _note_lag(self) -> None:
        if self._lag_pending:
            return
        self._lag_pending = True
        self._lag_since = ns_to_iso(datetime_to_ns(utc_now()))
        self._schedule_lag_notice()

    def _schedule_lag_notice(self) -> None:
        if self._closed or self._lag_handle is not None or not self._lag_pending:
            return
        delay = 0.0
        if self._last_notice_at:
            elapsed = self._loop.time() - self._last_notice_at
            delay = max(0.0, self._lag_notice_interval_s - elapsed)
        self._lag_handle = self._loop.call_later(delay, self._flush_lag_notice)

    def _flush_lag_notice(self) -> None:
        self._lag_handle = None
        if self._closed or not self._lag_pending:
            return
        task = self._loop.create_task(self._send_lag_notice())
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _send_lag_notice(self) -> None:
        if self._closed or not self._lag_pending:
            return
        now = self._loop.time()
        if (
            self._last_notice_at
            and now - self._last_notice_at < self._lag_notice_interval_s
        ):
            self._schedule_lag_notice()
            return

        dropped = self._dropped
        since = self._lag_since
        self._dropped = 0
        self._lag_pending = False
        self._lag_since = None
        self._last_notice_at = now
        await self.send({"op": "lag_notice", "dropped": dropped, "since": since})

    # -- consumer side -----------------------------------------------------

    async def send(self, payload: dict[str, Any]) -> None:
        if self._closed:
            return
        async with self._send_lock:
            if self._closed:
                return
            await self._ws.send_json(payload)

    async def _write_loop(self) -> None:
        try:
            while not self._closed:
                topic, data = await self._queue.get()
                if self._lag_pending:
                    await self._send_lag_notice()
                await self.send({"op": "event", "topic": topic, "data": data})
        except asyncio.CancelledError:
            raise
        except Exception:  # connection closed by the peer mid-write
            logger.debug("WebSocket writer stopped", exc_info=True)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._lag_handle is not None:
            self._lag_handle.cancel()
            self._lag_handle = None
        self._writer_task.cancel()
        for task in list(self._background):
            task.cancel()
        await asyncio.gather(
            self._writer_task, *self._background, return_exceptions=True
        )
        self._background.clear()
        with suppress(Exception):
            await self._ws.close()


class WebSocketHub:
    """Registry of WebSocket sessions and the fan-out entry point."""

    def __init__(
        self,
        *,
        queue_size: int = 1000,
        lag_notice_interval_ms: int = 1000,
    ) -> None:
        self._queue_size = queue_size
        self._lag_notice_interval_s = lag_notice_interval_ms / 1000.0
        self._sessions: set[WebSocketSession] = set()
        self._closed = False

    @property
    def session_count(self) -> int:
        return len(self._sessions)

    @property
    def dropped_total(self) -> int:
        """Total messages dropped by all sessions for backpressure."""
        return sum(session.dropped_total for session in self._sessions)

    def register(self, ws: web.WebSocketResponse) -> WebSocketSession:
        if self._closed:
            raise RuntimeError("WebSocketHub is closed")
        session = WebSocketSession(
            ws,
            queue_size=self._queue_size,
            lag_notice_interval_s=self._lag_notice_interval_s,
        )
        self._sessions.add(session)
        logger.info(
            "websocket session opened",
            extra={
                "event": "api.ws_session_opened",
                "session_id": session.session_id,
            },
        )
        return session

    async def unregister(self, session: WebSocketSession) -> None:
        self._sessions.discard(session)
        logger.info(
            "websocket session closed",
            extra={
                "event": "api.ws_session_closed",
                "session_id": session.session_id,
            },
        )
        await session.close()

    def publish(self, topic: str, messages: list[dict[str, Any]]) -> None:
        """Fan out pre-encoded Message Envelopes. Must run on the event loop."""
        if self._closed or not messages:
            return
        for session in tuple(self._sessions):
            if not session.matches(topic):
                continue
            for message in messages:
                session.enqueue(topic, message)

    async def handle_client_message(
        self, session: WebSocketSession, text: str
    ) -> None:
        try:
            payload = json.loads(text)
        except (TypeError, ValueError):
            return
        if not isinstance(payload, dict):
            return

        op = payload.get("op")
        if op == "subscribe":
            topics = valid_topics(payload.get("topics"))
            session.subscribe(topics)
            logger.info(
                "websocket subscribe",
                extra={
                    "event": "api.ws_subscribe",
                    "session_id": session.session_id,
                    "topics": topics,
                },
            )
        elif op == "unsubscribe":
            topics = valid_topics(payload.get("topics"))
            session.unsubscribe(topics)
            logger.info(
                "websocket unsubscribe",
                extra={
                    "event": "api.ws_unsubscribe",
                    "session_id": session.session_id,
                    "topics": topics,
                },
            )
        elif op == "ping":
            await session.send({"op": "pong"})

    async def close(self) -> None:
        self._closed = True
        sessions = tuple(self._sessions)
        self._sessions.clear()
        await asyncio.gather(
            *(session.close() for session in sessions), return_exceptions=True
        )
