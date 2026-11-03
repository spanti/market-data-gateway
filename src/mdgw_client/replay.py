"""Replay Session control over ZMQ REQ/REP, consuming events from PUB.

The Replay Protocol uses one REQ socket for serialized JSON control requests
and a SUB socket for ``replay.<session_id>.market.*`` bars plus a
``replay.<session_id>.control`` End of Session.

The SUB socket is created and its ``_internal.ready`` handshake awaited
*before* ``create_session`` is sent. That ordering matters: an ``asap``
session starts publishing immediately, and without the handshake its first
messages could be lost to the PUB/SUB slow-joiner race.
"""

from __future__ import annotations

import contextlib
import json
import queue
import threading
import time
from collections import deque
from collections.abc import Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Self

import zmq

from mdgw_client.errors import (
    CONFIGURATION_ERROR,
    CONNECTION_ERROR,
    PROTOCOL_ERROR,
    TIMEOUT,
    ClientError,
    error_from_control_response,
)
from mdgw_client.live import READY_TOPIC, StreamEvent
from mdgw_client.schema import ipc_to_table
from mdgw_client.time import Timestamp, ns_to_iso, to_ns

if TYPE_CHECKING:
    import types

REPLAY_PREFIX = "replay."

# At most this many not-yet-registered sessions are buffered. Other clients on
# the same PUB keep sending, so unknown session ids must never accumulate
# without bound.
MAX_PENDING_SESSIONS = 32

_CLOSED = object()


class _SessionChannel:
    """One bounded queue owned by a single :class:`ReplaySession`.

    The receiver thread is the only producer and it must never block: when the
    queue is full the oldest message is sacrificed and counted in ``dropped``.
    The End of Session is the exception: an incoming control message always
    makes room for itself and an End of Session already queued is never
    displaced by an ordinary market message.
    """

    def __init__(self, queue_size: int) -> None:
        self._queue: queue.Queue[tuple[str, bytes] | object] = queue.Queue(
            maxsize=queue_size
        )
        self._state_lock = threading.Lock()
        self._dropped = 0
        self._closed = threading.Event()
        self._eos_queued = False

    @property
    def dropped(self) -> int:
        with self._state_lock:
            return self._dropped

    def add_dropped(self, count: int) -> None:
        if count <= 0:
            return
        with self._state_lock:
            self._dropped += count

    def offer(
        self, message: tuple[str, bytes], *, critical: bool = False
    ) -> None:
        """Enqueue without blocking; the receiver thread calls this."""
        if self._closed.is_set():
            return
        if critical:
            self._eos_queued = True
        try:
            self._queue.put_nowait(message)
        except queue.Full:
            pass
        else:
            return

        # The queue is full. An ordinary message yields to an End of Session
        # that is already waiting; otherwise the oldest message is dropped to
        # make room for the new one (the control message always gets in).
        if self._eos_queued and not critical:
            self._bump_dropped()
            return
        try:
            self._queue.get_nowait()
            self._bump_dropped()
        except queue.Empty:  # pragma: no cover - only the producer fills it
            pass
        try:
            self._queue.put_nowait(message)
        except queue.Full:  # pragma: no cover - defensive single producer
            self._bump_dropped()

    def _bump_dropped(self) -> None:
        with self._state_lock:
            self._dropped += 1

    def next_message(
        self, timeout: float | None = None
    ) -> tuple[str, bytes] | None:
        """Return ``(topic, payload)``, None on timeout; StopIteration on close."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self._closed.is_set() and self._queue.empty():
                raise StopIteration
            poll = 0.1
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                poll = min(poll, remaining)
            try:
                item = self._queue.get(timeout=poll)
            except queue.Empty:
                if deadline is not None and time.monotonic() >= deadline:
                    return None
                continue
            if item is _CLOSED:
                raise StopIteration
            assert isinstance(item, tuple)
            return item

    def close(self) -> None:
        """Wake a blocked consumer with a sentinel; idempotent."""
        with self._state_lock:
            if self._closed.is_set():
                return
            self._closed.set()
        with contextlib.suppress(queue.Full):
            self._queue.put_nowait(_CLOSED)
        with contextlib.suppress(queue.Empty):  # pragma: no cover - defensive
            self._queue.get_nowait()
        with contextlib.suppress(queue.Full):  # pragma: no cover - defensive
            self._queue.put_nowait(_CLOSED)


class _ReplayReceiver:
    """One SUB socket shared by the sessions of a :class:`ReplayClient`.

    Messages are routed to a queue per Replay Session. Routing happens on the
    receiver thread and uses only non-blocking puts, so one slow consumer can
    never starve another session.
    """

    def __init__(
        self,
        publish_endpoint: str,
        *,
        timeout_s: float,
        reconnect_backoff_s: tuple[float, ...],
        queue_size: int = 1000,
    ) -> None:
        self._publish_endpoint = publish_endpoint
        self._timeout_s = timeout_s
        self._backoff = tuple(reconnect_backoff_s) or (0.0,)
        if queue_size <= 0:
            msg = "queue_size must be positive"
            raise ValueError(msg)
        self._queue_size = queue_size
        self._routing_lock = threading.Lock()
        self._channels: dict[str, _SessionChannel] = {}
        # Early events can arrive before ``create_session`` returns the id.
        # They are buffered per session and migrated on registration.
        self._pending: dict[str, deque[tuple[str, bytes]]] = {}
        self._pending_dropped: dict[str, int] = {}
        self._ready = threading.Event()
        self._closed = threading.Event()
        self._stop = threading.Event()
        self._reconnect_requested = threading.Event()
        self._state_lock = threading.Lock()
        self._last_error: BaseException | None = None
        self._thread = threading.Thread(
            target=self._run, name="mdgw-client-replay-sub", daemon=True
        )
        self._thread.start()

    # -- lifecycle ---------------------------------------------------------

    @property
    def closed(self) -> bool:
        return self._closed.is_set()

    @property
    def last_error(self) -> BaseException | None:
        with self._state_lock:
            return self._last_error

    def wait_ready(self, timeout: float | None = None) -> None:
        if self._ready.wait(timeout):
            return
        error = self.last_error
        self.close()
        if isinstance(error, ClientError):
            raise error
        raise ClientError(
            TIMEOUT,
            f"could not establish the replay subscription at "
            f"{self._publish_endpoint}",
        )

    # -- session routing ---------------------------------------------------

    def register_session(self, session_id: str) -> _SessionChannel:
        """Return the queue for ``session_id``, migrating buffered events.

        A Replay Server may publish an ``asap`` session before
        ``create_session`` has returned its id, so those messages wait in
        ``_pending``. Registration moves them atomically into the new queue,
        carrying over the drops that happened while they were buffered.
        """
        with self._routing_lock:
            if self._closed.is_set():
                raise ClientError(
                    CONNECTION_ERROR, "replay subscription is closed"
                )
            channel = self._channels.get(session_id)
            if channel is not None:
                return channel
            channel = _SessionChannel(self._queue_size)
            channel.add_dropped(self._pending_dropped.pop(session_id, 0))
            pending = self._pending.pop(session_id, None)
            if pending:
                control_topic = f"{REPLAY_PREFIX}{session_id}.control"
                for message in pending:
                    channel.offer(
                        message, critical=message[0] == control_topic
                    )
            self._channels[session_id] = channel
            return channel

    def unregister_session(self, session_id: str) -> None:
        with self._routing_lock:
            channel = self._channels.pop(session_id, None)
        if channel is not None:
            channel.close()

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        self._stop.set()
        self._reconnect_requested.set()
        with self._routing_lock:
            channels = list(self._channels.values())
            self._channels.clear()
            self._pending.clear()
            self._pending_dropped.clear()
        # Wake consumers blocked in next_message before joining the receiver.
        for channel in channels:
            channel.close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    # -- receiver thread ---------------------------------------------------

    def _make_socket(self) -> zmq.Socket:
        socket = zmq.Context.instance().socket(zmq.SUB)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.RCVHWM, 10_000)
        socket.setsockopt(zmq.SUBSCRIBE, REPLAY_PREFIX.encode("utf-8"))
        socket.setsockopt(zmq.SUBSCRIBE, READY_TOPIC.encode("utf-8"))
        socket.connect(self._publish_endpoint)
        return socket

    @staticmethod
    def _session_id(topic: str) -> str | None:
        """Extract the id from ``replay.<session_id>.<...>``.

        Replay Session ids are server-generated ``rpl-<hex>`` strings and do
        not contain a dot, so the first dot after the prefix is the boundary.
        """
        rest = topic[len(REPLAY_PREFIX) :]
        session_id, sep, _ = rest.partition(".")
        return session_id if sep and session_id else None

    def _route(self, topic: str, payload: bytes) -> None:
        """Deliver one frame to its session queue without ever blocking."""
        session_id = self._session_id(topic)
        if session_id is None:
            return
        message = (topic, payload)
        critical = topic == f"{REPLAY_PREFIX}{session_id}.control"
        with self._routing_lock:
            if self._closed.is_set():
                return
            channel = self._channels.get(session_id)
            if channel is not None:
                channel.offer(message, critical=critical)
                return
            self._buffer_pending_locked(session_id, message)

    def _buffer_pending_locked(
        self, session_id: str, message: tuple[str, bytes]
    ) -> None:
        pending = self._pending.get(session_id)
        if pending is None:
            if len(self._pending) >= MAX_PENDING_SESSIONS:
                # Dicts preserve insertion order: evict the oldest unknown
                # session. Its events have no owner and are discarded.
                oldest = next(iter(self._pending))
                del self._pending[oldest]
                self._pending_dropped.pop(oldest, None)
            pending = deque()
            self._pending[session_id] = pending
        pending.append(message)
        if len(pending) > self._queue_size:
            pending.popleft()
            self._pending_dropped[session_id] = (
                self._pending_dropped.get(session_id, 0) + 1
            )

    def _run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            socket = self._make_socket()
            poller = zmq.Poller()
            poller.register(socket, zmq.POLLIN)
            try:
                attempt = self._pump_socket(socket, poller, attempt)
            except Exception as exc:  # ruff: ignore[blind-except] - reconnection boundary
                if self._stop.is_set():
                    break
                self._record_error(exc)
                self._wait_backoff(attempt)
                attempt += 1
            finally:
                poller.unregister(socket)
                socket.close()

            if self._stop.is_set():
                break
            if self._reconnect_requested.is_set():
                self._reconnect_requested.clear()
                attempt = 0
            elif not self._ready.is_set():
                # A socket that failed before proving readiness needs a
                # fresh connect; a live socket simply keeps listening.
                self._wait_backoff(attempt)
                attempt += 1

    def _pump_socket(
        self, socket: zmq.Socket, poller: zmq.Poller, attempt: int
    ) -> int:
        """Pump one SUB socket until it breaks or a reconnect is requested."""
        while not self._stop.is_set():
            if self._reconnect_requested.is_set():
                break
            if dict(poller.poll(50)).get(socket) != zmq.POLLIN:
                continue
            frames = socket.recv_multipart()
            if len(frames) < 2:
                continue
            attempt = self._handle_frame(frames, attempt)
        return attempt

    def _handle_frame(self, frames: Sequence[bytes], attempt: int) -> int:
        """Route one frame; a malformed frame is recorded, never fatal."""
        try:
            healthy = self._route_frame(frames)
        except Exception as exc:  # ruff: ignore[blind-except] - bad frame
            self._record_error(exc)
            return attempt
        return 0 if healthy else attempt

    def _route_frame(self, frames: Sequence[bytes]) -> bool:
        """Route one frame; return True when it proves the socket is healthy."""
        topic = frames[0].decode("utf-8")
        if topic == READY_TOPIC:
            # The Replay PUB repeats this every 50 ms, so it proves the
            # subscription was forwarded.
            self._ready.set()
            return True
        if not topic.startswith(REPLAY_PREFIX):
            return False
        self._route(topic, frames[1])
        # A routed message proves the socket is healthy again.
        return True

    def _record_error(self, exc: Exception) -> None:
        """Record the last transport error unless the receiver is stopping."""
        if not self._stop.is_set():
            with self._state_lock:
                self._last_error = exc

    def _wait_backoff(self, attempt: int) -> None:
        delay = self._backoff[min(attempt, len(self._backoff) - 1)]
        if delay > 0 and not self._stop.is_set():
            self._stop.wait(delay)


class ReplayClient:
    """Creates and controls Replay Sessions against one Replay Server."""

    def __init__(
        self,
        *,
        publish_endpoint: str,
        control_endpoint: str,
        timeout_s: float = 10.0,
        reconnect_backoff_s: tuple[float, ...] = (0.1, 0.2, 0.5, 1.0, 2.0, 5.0),
        queue_size: int = 1000,
    ) -> None:
        if not publish_endpoint or not control_endpoint:
            msg = (
                "both replay_publish_endpoint and replay_control_endpoint "
                "are required"
            )
            raise ClientError(CONFIGURATION_ERROR, msg)
        if queue_size <= 0:
            msg = "queue_size must be positive"
            raise ValueError(msg)
        self._publish_endpoint = publish_endpoint
        self._control_endpoint = control_endpoint
        self._timeout_s = timeout_s
        self._backoff = tuple(reconnect_backoff_s)
        self._queue_size = queue_size

        self._context = zmq.Context()
        self._req: zmq.Socket | None = None
        self._request_lock = threading.Lock()

        self._receiver: _ReplayReceiver | None = None
        self._sessions: set[ReplaySession] = set()
        self._state_lock = threading.Lock()
        self._closed = False

    # -- sessions ----------------------------------------------------------

    def create_session(
        self,
        subscriptions: Sequence[Mapping[str, Any]],
        start_ts: Timestamp,
        end_ts: Timestamp,
        *,
        replay_mode: str = "asap",
        speed: float = 1.0,
    ) -> ReplaySession:
        """Create a session. The SUB handshake is completed first."""
        if not subscriptions:
            msg = "subscriptions must not be empty"
            raise ValueError(msg)
        normalized: list[dict[str, Any]] = []
        for item in subscriptions:
            if not isinstance(item, Mapping):
                # ValueError is the documented input-error type; keep it.
                msg = "each subscription must be a mapping"
                raise ValueError(msg)  # ruff: ignore[type-check-without-type-error]
            subscription = dict(item)
            subscription.setdefault("data_type", "bar")
            normalized.append(subscription)

        receiver = self._ensure_receiver()
        response = self._request(
            {
                "op": "create_session",
                "subscriptions": normalized,
                "start_ts": ns_to_iso(to_ns(start_ts)),
                "end_ts": ns_to_iso(to_ns(end_ts)),
                "replay_mode": replay_mode,
                "speed": speed,
            }
        )
        session_id = response.get("session_id")
        if not isinstance(session_id, str) or not session_id:
            raise ClientError(
                PROTOCOL_ERROR,
                "create_session response did not contain a session_id",
            )
        publish_endpoint = response.get("publish_endpoint")
        if not isinstance(publish_endpoint, str) or not publish_endpoint:
            publish_endpoint = self._publish_endpoint

        # Register immediately: an ``asap`` session may already have published
        # before this response arrived, and those frames are waiting in
        # ``receiver``'s pending buffer.
        channel = receiver.register_session(session_id)
        session = ReplaySession(
            client=self,
            session_id=session_id,
            publish_endpoint=publish_endpoint,
            channel=channel,
        )
        with self._state_lock:
            self._sessions.add(session)
        return session

    def _release_session(self, session: ReplaySession) -> None:
        with self._state_lock:
            self._sessions.discard(session)
            receiver = self._receiver
            close_receiver = receiver is not None and not self._sessions
            if close_receiver:
                self._receiver = None
        if receiver is not None:
            receiver.unregister_session(session.session_id)
            if close_receiver:
                receiver.close()

    # -- transport ---------------------------------------------------------

    def _ensure_receiver(self) -> _ReplayReceiver:
        with self._state_lock:
            if self._closed:
                raise ClientError(CONNECTION_ERROR, "ReplayClient is closed")
            receiver = self._receiver
            if receiver is None:
                receiver = _ReplayReceiver(
                    self._publish_endpoint,
                    timeout_s=self._timeout_s,
                    reconnect_backoff_s=self._backoff,
                    queue_size=self._queue_size,
                )
                self._receiver = receiver
        # Wait outside the lock; the receiver thread does not need it.
        try:
            receiver.wait_ready(self._timeout_s)
        except ClientError:
            with self._state_lock:
                if self._receiver is receiver:
                    self._receiver = None
            raise
        return receiver

    def _ensure_req_locked(self, *, allow_closed: bool = False) -> zmq.Socket:
        # ``allow_closed`` is reserved for ``close()``: it must still be able
        # to send ``stop`` after marking the client closed so new sessions
        # cannot be created. Every user-facing call keeps the hard check.
        if self._closed and not allow_closed:
            raise ClientError(CONNECTION_ERROR, "ReplayClient is closed")
        if self._req is None:
            socket = self._context.socket(zmq.REQ)
            socket.setsockopt(zmq.LINGER, 0)
            timeout_ms = max(1, int(self._timeout_s * 1000))
            socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
            socket.setsockopt(zmq.SNDTIMEO, timeout_ms)
            socket.connect(self._control_endpoint)
            self._req = socket
        return self._req

    def _reset_req_locked(self) -> None:
        if self._req is not None:
            self._req.close()
            self._req = None

    def _request(
        self,
        payload: Mapping[str, Any],
        *,
        allow_closed: bool = False,
    ) -> dict[str, Any]:
        with self._request_lock:
            socket = self._ensure_req_locked(allow_closed=allow_closed)
            try:
                socket.send_string(json.dumps(payload, ensure_ascii=False))
                raw = socket.recv()
            except zmq.Again as exc:
                self._reset_req_locked()
                raise ClientError(
                    TIMEOUT, "Replay control request timed out"
                ) from exc
            except zmq.ZMQError as exc:
                self._reset_req_locked()
                raise ClientError(
                    CONNECTION_ERROR, f"Replay control transport failed: {exc}"
                ) from exc

        try:
            response = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise ClientError(
                PROTOCOL_ERROR, "Replay control returned invalid JSON"
            ) from exc
        if not isinstance(response, dict):
            raise ClientError(
                PROTOCOL_ERROR, "Replay control returned a non-object response"
            )
        if response.get("ok") is False:
            raise error_from_control_response(response)
        return response

    def close(self) -> None:
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            sessions = list(self._sessions)
            self._sessions.clear()
            receiver = self._receiver
            self._receiver = None

        # The client is already marked closed so no new session can appear,
        # but the already-captured sessions still need a ``stop`` on the wire.
        # ``allow_closed`` opens exactly that narrow path; a transport failure
        # is swallowed so local resources are always released.
        for session in sessions:
            with contextlib.suppress(ClientError):
                session._control("stop", allow_closed=True)
            session._ended.set()
            # Wake any consumer blocked in ``next_event`` for this session.
            session._channel.close()

        if receiver is not None:
            receiver.close()
        with self._request_lock:
            self._reset_req_locked()
        self._context.term()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        self.close()


class ReplaySession:
    """One Replay Session: event iteration plus the Replay Protocol controls."""

    def __init__(
        self,
        *,
        client: ReplayClient,
        session_id: str,
        publish_endpoint: str,
        channel: _SessionChannel,
    ) -> None:
        self.session_id = session_id
        self.publish_endpoint = publish_endpoint
        self._client = client
        self._channel = channel
        self._end_of_session: dict[str, Any] | None = None
        self._ended = threading.Event()
        self._released = threading.Event()
        self._control_lock = threading.Lock()

    # -- state -------------------------------------------------------------

    @property
    def dropped(self) -> int:
        """Events dropped for this session because its queue was full/pending."""
        return self._channel.dropped

    @property
    def end_of_session(self) -> dict[str, Any] | None:
        with self._control_lock:
            return None if self._end_of_session is None else dict(
                self._end_of_session
            )

    @property
    def ended(self) -> bool:
        return self._ended.is_set()

    # -- controls ----------------------------------------------------------

    def _control(
        self, op: str, *, allow_closed: bool = False, **fields: Any
    ) -> dict[str, Any]:
        return self._client._request(
            {"op": op, "session_id": self.session_id, **fields},
            allow_closed=allow_closed,
        )

    def pause(self) -> None:
        self._control("pause")

    def resume(self) -> None:
        self._control("resume")

    def seek(self, ts: Timestamp) -> None:
        self._control("seek", ts=ns_to_iso(to_ns(ts)))

    def set_speed(self, speed: float) -> None:
        self._control("set_speed", speed=speed)

    # `credits` is the Replay Protocol field name; renaming breaks callers.
    def step(
        self, credits: int = 1  # ruff: ignore[builtin-argument-shadowing]
    ) -> None:
        if isinstance(credits, bool) or not isinstance(credits, int) or credits <= 0:
            msg = "credits must be a positive integer"
            raise ValueError(msg)
        self._control("step", credits=credits)

    def stop(self) -> None:
        self._control("stop")

    # -- events ------------------------------------------------------------

    @property
    def _topic_prefix(self) -> str:
        return f"replay.{self.session_id}."

    @property
    def _control_topic(self) -> str:
        return f"replay.{self.session_id}.control"

    def next_event(self, timeout: float | None = None) -> StreamEvent:
        """Return the next event for this session, ending at End of Session."""
        if timeout is not None and timeout < 0:
            msg = "timeout must be non-negative or None"
            raise ValueError(msg)
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            if self.ended and self.end_of_session is not None:
                raise StopIteration
            poll: float | None = None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ClientError(
                        TIMEOUT,
                        f"no Replay event for session {self.session_id!r} "
                        f"within {timeout} seconds",
                    )
                poll = min(remaining, 0.2)

            try:
                message = self._channel.next_message(poll)
            except StopIteration:
                raise StopIteration from None
            if message is None:
                continue

            topic, payload = message
            if not topic.startswith(self._topic_prefix):
                # Routing is per session; this guard only protects against a
                # malformed topic that happens to share this channel.
                continue
            if topic == self._control_topic:
                try:
                    control = json.loads(payload.decode("utf-8"))
                except (UnicodeDecodeError, ValueError):
                    continue
                if (
                    isinstance(control, dict)
                    and control.get("op") == "end_of_session"
                ):
                    with self._control_lock:
                        self._end_of_session = control
                    self._ended.set()
                    raise StopIteration
                continue
            if ".market." in topic:
                return StreamEvent(
                    topic=topic, table=ipc_to_table(payload), kind="bar"
                )

    def __iter__(self) -> Iterator[StreamEvent]:
        while True:
            try:
                yield self.next_event()
            except StopIteration:
                return

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        if self._released.is_set():
            return
        if not self._ended.is_set():
            with contextlib.suppress(ClientError):
                self._control("stop")
            self._ended.set()
        self._released.set()
        # Wake a consumer blocked in next_event before unregistering.
        self._channel.close()
        self._client._release_session(self)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        with contextlib.suppress(ClientError):
            self.stop()
        self.close()


__all__ = ["ReplayClient", "ReplaySession"]
