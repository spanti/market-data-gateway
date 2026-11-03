"""Live Streaming Interface subscriptions over ZMQ or WebSocket.

Both transports deliver the same logical type, :class:`StreamEvent`. ZMQ uses
the canonical two-frame ``[topic][Arrow IPC stream]`` message; WebSocket uses
JSON Message Envelopes decoded with the SDK's own schema copy.

Topics are prefix subscriptions in *both* transports: subscribing to
``market.bar.1m.binance:`` receives every instrument with that prefix, and
``market.bar.`` receives every Bar topic.

Received events go through a bounded :class:`queue.Queue`. A slow consumer
therefore applies backpressure to the receiver thread instead of silently
losing data.
"""

from __future__ import annotations

import contextlib
import json
import queue
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self

import zmq

from mdgw_client.errors import (
    CONFIGURATION_ERROR,
    CONNECTION_ERROR,
    TIMEOUT,
    WS_UNAVAILABLE,
    ClientError,
)
from mdgw_client.schema import ipc_to_table, messages_to_table

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from types import TracebackType

    import pyarrow as pa

READY_TOPIC = "_internal.ready"


@dataclass(frozen=True, slots=True)
class StreamEvent:
    """One message received from the Streaming Interface."""

    topic: str
    table: pa.Table | None = None
    kind: str = "bar"
    data: dict[str, Any] | None = None
    lag_notice: dict[str, Any] | None = None


def _import_ws_connect() -> Callable[..., Any]:
    """Import ``websockets.sync.client.connect`` lazily.

    ``websockets`` is optional: the base install only needs pyarrow and pyzmq.
    """
    try:
        # ruff: ignore[import-outside-top-level] - optional dependency
        from websockets.sync.client import connect
    except ImportError as exc:  # pragma: no cover - exercised when not installed
        raise ClientError(
            WS_UNAVAILABLE,
            "the WebSocket transport requires the optional 'websockets' "
            "dependency; install it with: pip install websockets",
        ) from exc
    return connect


class LiveSubscription:
    """A live subscription that can be iterated for :class:`StreamEvent`."""

    def __init__(  # ruff: ignore[too-many-arguments] - public constructor
        self,
        *,
        transport: str,
        topics: Sequence[str],
        stream_endpoint: str | None = None,
        ws_url: str | None = None,
        queue_size: int = 1000,
        timeout_s: float = 10.0,
        reconnect_backoff_s: tuple[float, ...] = (0.1, 0.2, 0.5, 1.0, 2.0, 5.0),
    ) -> None:
        if transport not in {"zmq", "ws"}:
            msg = f"unsupported transport {transport!r}; use 'zmq', 'ws' or 'auto'"
            raise ClientError(CONFIGURATION_ERROR, msg)
        if not topics:
            msg = "at least one topic is required"
            raise ValueError(msg)
        if queue_size <= 0:
            msg = "queue_size must be positive"
            raise ValueError(msg)
        if transport == "zmq" and not stream_endpoint:
            raise ClientError(
                CONFIGURATION_ERROR,
                "transport='zmq' requires a stream_endpoint",
            )
        if transport == "ws" and not ws_url:
            raise ClientError(
                CONFIGURATION_ERROR, "transport='ws' requires a ws_url"
            )

        self.transport = transport
        self.topics = tuple(topics)
        self._queue: queue.Queue[StreamEvent] = queue.Queue(maxsize=queue_size)
        self._timeout_s = timeout_s
        self._backoff = tuple(reconnect_backoff_s) or (0.0,)

        self._closed = threading.Event()
        self._stop = threading.Event()
        self._reconnect_requested = threading.Event()
        self._ready = threading.Event()
        self._state_lock = threading.Lock()
        self._reconnect_count = 0
        self._last_error: BaseException | None = None
        self._ws_conn: Any = None
        self._thread: threading.Thread | None = None

        if transport == "zmq":
            self._stream_endpoint = stream_endpoint
            self._thread = threading.Thread(
                target=self._run_zmq,
                name="mdgw-client-zmq-sub",
                daemon=True,
            )
            self._thread.start()
        else:
            self._ws_url = ws_url
            # Fail synchronously and clearly when the optional dependency is
            # missing, rather than in the receiver thread.
            self._ws_connect = _import_ws_connect()
            self._thread = threading.Thread(
                target=self._run_ws,
                name="mdgw-client-ws-sub",
                daemon=True,
            )
            self._thread.start()
            if not self._ready.wait(timeout_s + 1.0):
                self.close()
                raise ClientError(
                    TIMEOUT,
                    f"WebSocket subscription to {ws_url} was not confirmed in time",
                )

    # -- public surface ----------------------------------------------------

    @property
    def reconnect_count(self) -> int:
        with self._state_lock:
            return self._reconnect_count

    @property
    def last_error(self) -> BaseException | None:
        with self._state_lock:
            return self._last_error

    def reconnect(self) -> None:
        """Force the receiver to rebuild its transport connection."""
        if self._closed.is_set():
            raise ClientError(CONNECTION_ERROR, "subscription is closed")
        self._reconnect_requested.set()
        conn, _ = self._ws_snapshot()
        if conn is not None:
            self._close_quietly(conn)

    def next(self, timeout: float | None = None) -> StreamEvent:
        """Return the next event, or raise ``ClientError('timeout')``."""
        if timeout is not None and timeout < 0:
            msg = "timeout must be non-negative or None"
            raise ValueError(msg)

        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self._closed.is_set() and self._queue.empty():
                raise StopIteration
            poll = 0.1
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ClientError(
                        TIMEOUT,
                        f"no StreamEvent arrived within {timeout} seconds",
                    )
                poll = min(poll, remaining)
            try:
                return self._queue.get(timeout=poll)
            except queue.Empty:
                continue

    def __iter__(self) -> Iterator[StreamEvent]:
        while True:
            try:
                yield self.next()
            except StopIteration:
                return

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        self._stop.set()
        self._reconnect_requested.set()
        conn, _ = self._ws_snapshot()
        if conn is not None:
            self._close_quietly(conn)
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    # -- shared helpers ----------------------------------------------------

    def _bump_reconnect(self) -> None:
        with self._state_lock:
            self._reconnect_count += 1

    def _set_error(self, exc: BaseException) -> None:
        with self._state_lock:
            self._last_error = exc

    def _reset_blocked(self) -> None:
        self._reconnect_requested.clear()

    def _wait_backoff(self, attempt: int) -> None:
        delay = self._backoff[min(attempt, len(self._backoff) - 1)]
        if delay > 0:
            self._stop.wait(delay)

    def _put(self, event: StreamEvent) -> bool:
        """Block until queued (backpressure) unless the subscription closes."""
        while not self._stop.is_set():
            try:
                self._queue.put(event, timeout=0.1)
            except queue.Full:
                continue
            else:
                return True
        return False

    def _ws_snapshot(self) -> tuple[Any, bool]:
        with self._state_lock:
            return self._ws_conn, self._closed.is_set()

    @staticmethod
    def _close_quietly(conn: Any) -> None:
        # Closing a transport may fail if the peer already went away; shutdown
        # must stay best-effort.
        with contextlib.suppress(Exception):
            conn.close()

    # -- ZMQ transport -----------------------------------------------------

    def _make_zmq_socket(self) -> zmq.Socket:
        socket = zmq.Context.instance().socket(zmq.SUB)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.RCVHWM, 10_000)
        for topic in self.topics:
            socket.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))
        # The ready gesture is consumed and skipped; subscribing to it keeps
        # the wire format honest even though the live bus only pings at start.
        socket.setsockopt(zmq.SUBSCRIBE, READY_TOPIC.encode("utf-8"))
        socket.connect(self._stream_endpoint)
        return socket

    def _run_zmq(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            socket = self._make_zmq_socket()
            poller = zmq.Poller()
            poller.register(socket, zmq.POLLIN)
            try:
                attempt = self._receive_zmq(socket, poller, attempt)
            except Exception as exc:  # ruff: ignore[blind-except] - reconnection boundary
                if self._stop.is_set():
                    break
                self._set_error(exc)
                self._bump_reconnect()
                self._wait_backoff(attempt)
                attempt += 1
            finally:
                poller.unregister(socket)
                socket.close()

            if self._stop.is_set():
                break
            if self._reconnect_requested.is_set():
                self._reset_blocked()
                self._bump_reconnect()
                attempt = 0

    def _receive_zmq(
        self, socket: zmq.Socket, poller: zmq.Poller, attempt: int
    ) -> int:
        """Consume frames until the socket fails or a reconnect is requested."""
        while not self._stop.is_set():
            if self._reconnect_requested.is_set():
                return attempt
            if dict(poller.poll(50)).get(socket) != zmq.POLLIN:
                continue
            frames = socket.recv_multipart()
            if len(frames) < 2:
                continue
            attempt = self._handle_zmq_frame(frames, attempt)
        return attempt

    def _handle_zmq_frame(self, frames: list[bytes], attempt: int) -> int:
        """Decode one frame, keeping the backoff counter across bad frames."""
        # One malformed frame (bad UTF-8 topic, non-Arrow payload) must not
        # kill the receiver thread: record it, skip it, and keep consuming
        # later frames on the same socket.
        try:
            topic = frames[0].decode("utf-8")
            if topic == READY_TOPIC:
                self._ready.set()
                return 0
            event = StreamEvent(
                topic=topic,
                table=ipc_to_table(frames[1]),
                kind="bar",
            )
        except Exception as exc:  # ruff: ignore[blind-except] - bad frame
            if not self._stop.is_set():
                self._set_error(exc)
            return attempt
        # A decoded message proves the connection is healthy, so do not linger
        # at the maximum reconnect backoff.
        return 0 if self._put(event) else attempt

    # -- WebSocket transport ----------------------------------------------

    def _run_ws(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            try:
                connection = self._ws_connect(
                    self._ws_url,
                    open_timeout=self._timeout_s,
                    close_timeout=1.0,
                )
            except Exception as exc:  # ruff: ignore[blind-except] - reconnection boundary
                self._set_error(exc)
                if self._stop.is_set():
                    break
                self._wait_backoff(attempt)
                attempt += 1
                continue

            attempt = self._serve_ws_connection(connection, attempt)

            if self._stop.is_set():
                break
            if self._reconnect_requested.is_set():
                self._reset_blocked()
            self._bump_reconnect()
            self._wait_backoff(attempt)
            attempt += 1

    def _serve_ws_connection(self, connection: Any, attempt: int) -> int:
        """Serve one connection, returning the updated backoff attempt."""
        try:
            # Using connect() as a context manager is the API contract in
            # websockets >= 14 and is also supported by 13.
            with connection as conn:
                attempt = self._pump_ws(conn, attempt)
        except ClientError as exc:
            self._set_error(exc)
        except Exception as exc:  # ruff: ignore[blind-except] - reconnection boundary
            self._set_error(exc)
        return attempt

    def _pump_ws(self, conn: Any, attempt: int) -> int:
        """Handshake and receive until the connection ends."""
        with self._state_lock:
            self._ws_conn = conn
        try:
            self._ws_handshake(conn)
            self._ready.set()
            attempt = 0
            self._receive_ws(conn)
        except ClientError as exc:
            self._set_error(exc)
        except Exception as exc:  # ruff: ignore[blind-except] - reconnection boundary
            self._set_error(exc)
        finally:
            with self._state_lock:
                if self._ws_conn is conn:
                    self._ws_conn = None
        return attempt

    def _receive_ws(self, conn: Any) -> None:
        while not self._stop.is_set():
            if self._reconnect_requested.is_set():
                return
            try:
                raw = conn.recv(timeout=0.2)
            except TimeoutError:
                continue
            if raw is None:
                return
            self._handle_ws_message(raw)

    def _ws_handshake(self, conn: Any) -> None:
        conn.send(
            json.dumps(
                {"op": "subscribe", "topics": list(self.topics)},
                ensure_ascii=False,
            )
        )
        conn.send(json.dumps({"op": "ping"}))
        deadline = time.monotonic() + self._timeout_s
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ClientError(
                    TIMEOUT, "WebSocket subscribe/ping handshake timed out"
                )
            raw = conn.recv(timeout=min(0.2, remaining))
            if raw is None:
                raise ClientError(
                    CONNECTION_ERROR, "WebSocket closed during the handshake"
                )
            try:
                payload = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if not isinstance(payload, dict):
                continue
            if payload.get("op") == "pong":
                return
            # Events may already be queued behind our own subscribe; decode
            # them rather than dropping them while waiting for the pong.
            self._handle_ws_message(raw)

    def _handle_ws_message(self, raw: Any) -> None:
        """Decode one message; record and skip anything malformed.

        A single bad JSON document or envelope must not escape the receive
        loop, where it would be treated as a connection failure and force a
        reconnect. The error is still exposed through :attr:`last_error`.
        """
        try:
            self._process_ws_message(raw)
        except Exception as exc:  # ruff: ignore[blind-except] - one bad message
            self._set_error(exc)

    def _process_ws_message(self, raw: Any) -> None:
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError) as exc:
            self._set_error(exc)
            return
        if not isinstance(payload, dict):
            self._set_error(
                ValueError("WebSocket message envelope is not an object")
            )
            return
        op = payload.get("op")
        if op == "event":
            topic = payload.get("topic")
            data = payload.get("data")
            if not isinstance(topic, str):
                return
            if isinstance(data, dict):
                table = messages_to_table([data])
            elif isinstance(data, list):
                table = messages_to_table(data)
            else:
                return
            self._put(
                StreamEvent(
                    topic=topic,
                    table=table,
                    kind="bar",
                    data=data if isinstance(data, dict) else None,
                )
            )
        elif op == "lag_notice":
            self._put(
                StreamEvent(
                    topic="",
                    table=None,
                    kind="lag_notice",
                    data=payload,
                    lag_notice=payload,
                )
            )


__all__ = ["LiveSubscription", "StreamEvent"]
