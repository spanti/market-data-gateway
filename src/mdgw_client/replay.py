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

import json
import queue
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import pyarrow as pa
import zmq

from mdgw_client.errors import (
    CONNECTION_ERROR,
    PROTOCOL_ERROR,
    TIMEOUT,
    ClientError,
    error_from_control_response,
)
from mdgw_client.live import READY_TOPIC, StreamEvent
from mdgw_client.schema import ipc_to_table
from mdgw_client.time import Timestamp, ns_to_iso, to_ns

REPLAY_PREFIX = "replay."


class _ReplayReceiver:
    """One SUB socket shared by the sessions of a :class:`ReplayClient`."""

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
        self._queue: queue.Queue[tuple[str, bytes]] = queue.Queue(
            maxsize=queue_size
        )
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

    def next_message(
        self, timeout: float | None = None
    ) -> tuple[str, bytes] | None:
        """Return ``(topic, payload)``, None on timeout, or StopIteration."""
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
                return self._queue.get(timeout=poll)
            except queue.Empty:
                if deadline is not None and time.monotonic() >= deadline:
                    return None

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        self._stop.set()
        self._reconnect_requested.set()
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

    def _put(self, message: tuple[str, bytes]) -> bool:
        while not self._stop.is_set():
            try:
                self._queue.put(message, timeout=0.1)
                return True
            except queue.Full:
                continue
        return False

    def _run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            socket = self._make_socket()
            poller = zmq.Poller()
            poller.register(socket, zmq.POLLIN)
            try:
                while not self._stop.is_set():
                    if self._reconnect_requested.is_set():
                        break
                    if dict(poller.poll(50)).get(socket) != zmq.POLLIN:
                        continue
                    frames = socket.recv_multipart()
                    if len(frames) < 2:
                        continue
                    topic = frames[0].decode("utf-8")
                    if topic == READY_TOPIC:
                        # The Replay PUB repeats this every 50 ms, so it is a
                        # reliable proof the subscription was forwarded.
                        self._ready.set()
                        continue
                    if not topic.startswith(REPLAY_PREFIX):
                        continue
                    if not self._put((topic, frames[1])):
                        break
            except zmq.ZMQError as exc:
                if self._stop.is_set():
                    break
                with self._state_lock:
                    self._last_error = exc
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
    ) -> None:
        if not publish_endpoint or not control_endpoint:
            raise ClientError(
                "configuration_error",
                "both replay_publish_endpoint and replay_control_endpoint are required",
            )
        self._publish_endpoint = publish_endpoint
        self._control_endpoint = control_endpoint
        self._timeout_s = timeout_s
        self._backoff = tuple(reconnect_backoff_s)

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
    ) -> "ReplaySession":
        """Create a session. The SUB handshake is completed first."""
        if not subscriptions:
            raise ValueError("subscriptions must not be empty")
        normalized: list[dict[str, Any]] = []
        for item in subscriptions:
            if not isinstance(item, Mapping):
                raise ValueError("each subscription must be a mapping")
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

        session = ReplaySession(
            client=self,
            session_id=session_id,
            publish_endpoint=publish_endpoint,
            receiver=receiver,
        )
        with self._state_lock:
            self._sessions.add(session)
        return session

    def _release_session(self, session: "ReplaySession") -> None:
        with self._state_lock:
            self._sessions.discard(session)
            receiver = self._receiver
            close_receiver = receiver is not None and not self._sessions
            if close_receiver:
                self._receiver = None
        if close_receiver:
            assert receiver is not None
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
            try:
                session._control("stop", allow_closed=True)
            except ClientError:
                pass
            session._ended.set()

        if receiver is not None:
            receiver.close()
        with self._request_lock:
            self._reset_req_locked()
        self._context.term()

    def __enter__(self) -> "ReplayClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()


class ReplaySession:
    """One Replay Session: event iteration plus the Replay Protocol controls."""

    def __init__(
        self,
        *,
        client: ReplayClient,
        session_id: str,
        publish_endpoint: str,
        receiver: _ReplayReceiver,
    ) -> None:
        self.session_id = session_id
        self.publish_endpoint = publish_endpoint
        self._client = client
        self._receiver = receiver
        self._end_of_session: dict[str, Any] | None = None
        self._ended = threading.Event()
        self._released = threading.Event()
        self._control_lock = threading.Lock()

    # -- state -------------------------------------------------------------

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

    def step(self, credits: int = 1) -> None:
        if isinstance(credits, bool) or not isinstance(credits, int) or credits <= 0:
            raise ValueError("credits must be a positive integer")
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
            raise ValueError("timeout must be non-negative or None")
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
                message = self._receiver.next_message(poll)
            except StopIteration:
                raise StopIteration from None
            if message is None:
                continue

            topic, payload = message
            if not topic.startswith(self._topic_prefix):
                # Events from other Replay Sessions are not ours.
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
            try:
                self._control("stop")
            except ClientError:
                pass
            self._ended.set()
        self._released.set()
        self._client._release_session(self)

    def __enter__(self) -> "ReplaySession":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        try:
            self.stop()
        except ClientError:
            pass
        self.close()


__all__ = ["ReplayClient", "ReplaySession"]
