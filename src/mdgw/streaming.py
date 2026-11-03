"""The Streaming Interface: topic syntax, Arrow IPC codec, PUB/SUB, READY.

This module is the single owner of everything that travels over the streaming
wire:

* Bar topic syntax, including the optional ``namespace`` prefix used by Replay;
* the two-frame ``[topic][Arrow IPC stream]`` codec;
* one ZeroMQ context with a PUB socket and (when connecting) a SUB socket and
  subscription thread;
* the READY handshake and the ``_internal.status`` diagnostic topic;
* the Replay-only ``{namespace}.control`` event topic;
* forwarding and the streaming counters.

Public surface (and nothing else): :class:`StreamingInterface`,
:func:`bar_topic`, :func:`bar_topic_prefix`, :func:`table_to_ipc`,
:func:`ipc_to_table` and :data:`READY_TOPIC`.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import zmq

from mdgw.core.schema import bars_to_logical_table

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from mdgw.core.bar import Bar

logger = logging.getLogger(__name__)

#: READY handshake topic. Always global, never namespaced.
READY_TOPIC = "_internal.ready"

#: Diagnostic status topic. Always global, never namespaced.
_STATUS_TOPIC = "_internal.status"

#: Bar topics are ``market.bar.<interval>.<instrument_id>``; namespaces go in
#: front as whole segments.
_BAR_TOPIC_PREFIX = "market.bar."

#: The READY heartbeat period required by the streaming contract.
_READY_INTERVAL_S = 0.05

#: How long `subscribe_bars(await_ready=True)` waits for its own SUB socket.
_AWAIT_READY_TIMEOUT_S = 2.0

#: Subscriber poll interval. Short enough to notice shutdown promptly.
_POLL_INTERVAL_MS = 20

__all__ = [
    "READY_TOPIC",
    "StreamingInterface",
    "bar_topic",
    "bar_topic_prefix",
    "ipc_to_table",
    "table_to_ipc",
]


def _namespaced(namespace: str, topic: str) -> str:
    """Prefix ``topic`` with ``namespace`` as whole dot-separated segments."""
    namespace = namespace.strip(".")
    if not namespace:
        return topic
    return f"{namespace}.{topic}"


def bar_topic(bar: Bar, *, namespace: str = "") -> str:
    """Return the wire topic for one Bar."""
    return _namespaced(
        namespace, f"{_BAR_TOPIC_PREFIX}{bar.interval}.{bar.instrument_id}"
    )


def bar_topic_prefix(
    *,
    interval: str | None = None,
    instrument_id: str | None = None,
    namespace: str = "",
) -> str:
    """Return a Bar topic prefix for use with raw prefix subscriptions.

    ``bar_topic_prefix()`` is ``"market.bar."``. Providing ``interval`` adds
    ``"<interval>."``; providing ``instrument_id`` appends it verbatim, so the
    pair reproduces :func:`bar_topic`. A namespace is applied as in
    :func:`bar_topic`.
    """
    prefix = _BAR_TOPIC_PREFIX
    if interval is not None:
        prefix = f"{prefix}{interval}."
    if instrument_id is not None:
        prefix = f"{prefix}{instrument_id}"
    return _namespaced(namespace, prefix)


def table_to_ipc(table: pa.Table) -> bytes:
    """Encode a table as an Arrow IPC stream."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def ipc_to_table(payload: bytes) -> pa.Table:
    """Decode an Arrow IPC stream into a table."""
    return pa.ipc.open_stream(pa.BufferReader(payload)).read_all()


@dataclass(frozen=True, slots=True)
class _BarSubscription:
    handler: Callable[[str, pa.Table], None]
    forward: bool


class _State(StrEnum):
    NEW = "new"
    STARTED = "started"
    STOPPED = "stopped"


class StreamingInterface:
    """One process-local owner of the Streaming Interface wire contract.

    The instance is single-use: :meth:`start` may be called repeatedly while
    running, but once :meth:`stop` has run a new instance is required.

    A SUB socket (and its thread) exists only when ``connect`` is provided; a
    publish-only instance such as the collector therefore does not spend a
    socket on receiving. Consequently every ``subscribe_*`` call requires a
    ``connect`` endpoint and raises :class:`RuntimeError` without one.
    """

    def __init__(
        self,
        *,
        bind: str | Iterable[str],
        connect: str | None = None,
        namespace: str = "",
        zmq_hwm: int = 10_000,
    ) -> None:
        endpoints = [bind] if isinstance(bind, str) else list(bind)
        if not endpoints:
            msg = "at least one bind endpoint is required"
            raise ValueError(msg)

        self._bind = tuple(endpoints)
        self._connect = connect
        self._namespace = namespace
        self._zmq_hwm = zmq_hwm

        # NEW -> STARTED -> STOPPED; guarded by _lifecycle_lock when
        # mutated, read without it where only a benign snapshot is needed.
        self._state = _State.NEW
        self._lifecycle_lock = threading.Lock()
        self._publisher: zmq.Socket | None = None
        self._context: zmq.Context | None = None
        self._resolved_endpoints: tuple[str, ...] = ()

        self._sub_stop: threading.Event | None = None
        self._sub_thread: threading.Thread | None = None
        self._sub_ready = threading.Event()
        self._peer_ready = threading.Event()

        self._ready_stop: threading.Event | None = None
        self._ready_thread: threading.Thread | None = None

        self._send_lock = threading.Lock()
        self._subscriptions_lock = threading.Lock()
        self._bar_subscription: _BarSubscription | None = None
        self._status_handler: Callable[[dict[str, Any]], None] | None = None

        self._counter_lock = threading.Lock()
        self._published_frames = 0
        self._received_frames = 0
        self._decode_errors = 0
        self._handler_errors = 0

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        """Create the context, bind the PUB, and connect the SUB if requested."""
        with self._lifecycle_lock:
            if self._state == _State.STARTED:
                return
            if self._state == _State.STOPPED:
                msg = "StreamingInterface instances are single-use"
                raise RuntimeError(msg)

            context = zmq.Context()
            self._context = context
            try:
                self._open(context)
            except BaseException:
                self._discard_start(context)
                raise

    def _open(self, context: zmq.Context) -> None:
        publisher = context.socket(zmq.PUB)
        self._publisher = publisher
        publisher.setsockopt(zmq.SNDHWM, self._zmq_hwm)
        publisher.setsockopt(zmq.LINGER, 0)
        resolved: list[str] = []
        for endpoint in self._bind:
            publisher.bind(endpoint)
            resolved.append(publisher.getsockopt_string(zmq.LAST_ENDPOINT))

        # Commit the running state before the subscriber thread starts: a peer
        # may already be publishing, and a forwarded frame must see a fully
        # usable interface rather than a half-built one.
        self._resolved_endpoints = tuple(resolved)
        self._state = _State.STARTED

        if self._connect is not None:
            sub_stop = threading.Event()
            sub_thread = threading.Thread(
                target=self._run_subscriber,
                args=(context, sub_stop),
                name="mdgw-streaming-subscriber",
                daemon=True,
            )
            self._sub_stop = sub_stop
            self._sub_thread = sub_thread
            sub_thread.start()

    def _discard_start(self, context: zmq.Context) -> None:
        self._state = _State.NEW
        self._resolved_endpoints = ()
        sub_stop, self._sub_stop = self._sub_stop, None
        sub_thread, self._sub_thread = self._sub_thread, None
        publisher, self._publisher = self._publisher, None
        self._context = None
        if sub_stop is not None:
            sub_stop.set()
        if sub_thread is not None:
            sub_thread.join(timeout=2.0)
        if publisher is not None:
            publisher.close(linger=0)
        context.term()

    def stop(self) -> None:
        """Stop the READY heartbeat and subscriber, close sockets, term context."""
        with self._lifecycle_lock:
            if self._state != _State.STARTED:
                return
            self._state = _State.STOPPED
            ready_stop = self._ready_stop
            ready_thread = self._ready_thread
            sub_stop = self._sub_stop
            sub_thread = self._sub_thread
            publisher = self._publisher
            context = self._context
            self._ready_stop = None
            self._ready_thread = None
            self._sub_stop = None
            self._sub_thread = None
            self._publisher = None
            self._context = None

        if ready_stop is not None:
            ready_stop.set()
        with self._send_lock:
            if publisher is not None:
                publisher.close(linger=0)
        if sub_stop is not None:
            sub_stop.set()
        if sub_thread is not None:
            sub_thread.join(timeout=2.0)
        if ready_thread is not None:
            ready_thread.join(timeout=2.0)
        if context is not None:
            context.term()

    @property
    def endpoints(self) -> tuple[str, ...]:
        """Resolved bind endpoints, in bind order (empty before start)."""
        return self._resolved_endpoints

    # -- counters ----------------------------------------------------------

    @property
    def published_frames(self) -> int:
        """Bar frames sent, including forwarded frames."""
        with self._counter_lock:
            return self._published_frames

    @property
    def received_frames(self) -> int:
        """Decoded Bar frames received."""
        with self._counter_lock:
            return self._received_frames

    @property
    def decode_errors(self) -> int:
        """Malformed frames or payloads dropped by this instance."""
        with self._counter_lock:
            return self._decode_errors

    @property
    def handler_errors(self) -> int:
        """Handler exceptions logged by this instance."""
        with self._counter_lock:
            return self._handler_errors

    # -- publishing --------------------------------------------------------

    def publish_bars(
        self, bars: Sequence[Bar], *, namespace: str | None = None
    ) -> None:
        """Group Bars by ``(interval, instrument_id)`` and send one frame each.

        ``namespace`` overrides the instance namespace for this call only;
        ``None`` keeps the instance namespace. Replay uses it because one PUB
        multiplexes several Sessions under ``replay.<session_id>`` topics.

        Callers on multiple threads may invoke this concurrently; sends are
        serialized by the module's publish lock.
        """
        if not self._publish_allowed():
            return
        effective_namespace = (
            self._namespace if namespace is None else namespace
        )
        groups: dict[tuple[str, str], list[Bar]] = {}
        for bar in bars:
            groups.setdefault((bar.interval, bar.instrument_id), []).append(bar)

        for group in groups.values():
            topic = bar_topic(group[0], namespace=effective_namespace)
            payload = table_to_ipc(bars_to_logical_table(group))
            self._send(topic, payload, bar=True)

    def publish_status(self, status: Mapping[str, Any]) -> None:
        """Publish one JSON status document on ``_internal.status``."""
        if not self._publish_allowed():
            return
        payload = _encode_json(status)
        self._send(_STATUS_TOPIC, payload, bar=False)

    def publish_event(
        self, event: Mapping[str, Any], *, namespace: str | None = None
    ) -> None:
        """Publish one JSON event on ``{namespace}.control`` (Replay only).

        ``namespace`` overrides the instance namespace for this call only;
        ``None`` keeps the instance namespace. An empty effective namespace is
        rejected because Replay control topics must name their Session.
        """
        if not self._publish_allowed():
            return
        effective_namespace = (
            self._namespace if namespace is None else namespace
        )
        if not effective_namespace.strip("."):
            msg = "publish_event requires a non-empty namespace"
            raise ValueError(msg)
        topic = _namespaced(effective_namespace, "control")
        payload = _encode_json(event)
        self._send(topic, payload, bar=False)

    def _publish_allowed(self) -> bool:
        """Raise before start; return False (no-op) after stop."""
        if self._state == _State.STOPPED:
            return False
        if self._state != _State.STARTED:
            msg = "StreamingInterface is not started"
            raise RuntimeError(msg)
        return True

    def _send(self, topic: str, payload: bytes, *, bar: bool) -> bool:
        with self._send_lock:
            if self._state == _State.STOPPED:
                return False
            if self._state != _State.STARTED or self._publisher is None:
                msg = "StreamingInterface is not started"
                raise RuntimeError(msg)
            self._publisher.send_multipart([topic.encode("utf-8"), payload])
            if bar:
                with self._counter_lock:
                    self._published_frames += 1
            return True

    # -- subscriptions -----------------------------------------------------

    def subscribe_bars(
        self,
        handler: Callable[[str, pa.Table], None],
        *,
        await_ready: bool = False,
        forward: bool = False,
    ) -> None:
        """Install the single active Bar handler.

        The handler runs on the subscription thread and receives every Bar
        topic, including namespaced Replay topics. With ``forward=True`` the
        original frame bytes are sent on this instance's PUB before the
        handler runs. With ``await_ready=True`` the call blocks until a READY
        ping proves this instance's own SUB socket is connected.
        """
        self._require_subscribable()
        subscription = _BarSubscription(handler=handler, forward=forward)
        with self._subscriptions_lock:
            if self._bar_subscription is not None:
                msg = "a Bar subscription is already active"
                raise RuntimeError(msg)
            self._bar_subscription = subscription

        if not await_ready:
            return
        try:
            self._await_own_subscription()
        except TimeoutError:
            with self._subscriptions_lock:
                if self._bar_subscription is subscription:
                    self._bar_subscription = None
            raise

    def unsubscribe_bars(self) -> bool:
        """Remove the active Bar handler, leaving the SUB socket connected."""
        with self._subscriptions_lock:
            if self._bar_subscription is None:
                return False
            self._bar_subscription = None
            return True

    def subscribe_status(
        self, handler: Callable[[dict[str, Any]], None]
    ) -> None:
        """Install the single active status handler (``_internal.status``)."""
        self._require_subscribable()
        with self._subscriptions_lock:
            if self._status_handler is not None:
                msg = "a status subscription is already active"
                raise RuntimeError(msg)
            self._status_handler = handler

    def _require_subscribable(self) -> None:
        if self._state != _State.STARTED:
            msg = "StreamingInterface is not started"
            raise RuntimeError(msg)
        if self._connect is None:
            msg = "subscribe_* requires a connect endpoint"
            raise RuntimeError(msg)

    def _await_own_subscription(self) -> None:
        """Publish READY pings until this instance's own SUB receives one."""
        self._sub_ready.clear()
        deadline = time.monotonic() + _AWAIT_READY_TIMEOUT_S
        while not self._sub_ready.is_set():
            if self._state != _State.STARTED:
                msg = "StreamingInterface stopped while awaiting readiness"
                raise RuntimeError(msg)
            if time.monotonic() >= deadline:
                msg = "own ZMQ subscriber did not become ready in time"
                raise TimeoutError(msg)
            self._send(READY_TOPIC, b"", bar=False)
            time.sleep(0.01)

    # -- readiness ---------------------------------------------------------

    def mark_ready(self) -> None:
        """Start the 50 ms READY heartbeat; idempotent until :meth:`stop`."""
        with self._lifecycle_lock:
            if self._state == _State.STOPPED:
                return
            if self._state != _State.STARTED:
                msg = "StreamingInterface is not started"
                raise RuntimeError(msg)
            if self._ready_thread is not None:
                return
            stop_event = threading.Event()
            thread = threading.Thread(
                target=self._run_ready_loop,
                args=(stop_event,),
                name="mdgw-streaming-ready",
                daemon=True,
            )
            self._ready_stop = stop_event
            self._ready_thread = thread
            thread.start()

    @property
    def peer_ready(self) -> bool:
        """True once a READY was received over ``connect``.

        Any READY frame received on the SUB socket counts, including the ping
        this instance sends to prove its own subscription is wired during
        ``subscribe_bars(await_ready=True)``.
        """
        return self._state == _State.STARTED and self._peer_ready.is_set()

    def wait_until_peer_ready(self, timeout: float | None = None) -> bool:
        """Wait for a peer READY; False on timeout or without ``connect``."""
        if self._state == _State.NEW:
            msg = "StreamingInterface is not started"
            raise RuntimeError(msg)
        if self._state == _State.STOPPED or self._connect is None:
            return False
        return self._peer_ready.wait(timeout)

    def _run_ready_loop(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            try:
                self._send(READY_TOPIC, b"", bar=False)
            except RuntimeError:
                return
            except zmq.ZMQError:
                logger.debug("READY ping failed during shutdown", exc_info=True)
            stop_event.wait(_READY_INTERVAL_S)

    # -- subscriber thread -------------------------------------------------

    def _run_subscriber(
        self, context: zmq.Context, stop_event: threading.Event
    ) -> None:
        socket = context.socket(zmq.SUB)
        socket.setsockopt(zmq.SUBSCRIBE, b"")
        socket.setsockopt(zmq.RCVHWM, self._zmq_hwm)
        socket.setsockopt(zmq.LINGER, 0)
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        try:
            socket.connect(self._connect)
            self._pump_subscriber(socket, poller, stop_event)
        except Exception:
            if not stop_event.is_set():
                logger.exception("streaming subscriber thread failed")
        finally:
            poller.unregister(socket)
            socket.close()

    def _pump_subscriber(
        self,
        socket: zmq.Socket,
        poller: zmq.Poller,
        stop_event: threading.Event,
    ) -> None:
        while not stop_event.is_set():
            ready = dict(poller.poll(_POLL_INTERVAL_MS))
            if ready.get(socket) != zmq.POLLIN:
                continue
            try:
                frames = socket.recv_multipart()
            except zmq.ZMQError:
                if stop_event.is_set():
                    return
                logger.exception("failed to receive a streaming frame")
                continue
            self._dispatch_frame(frames)

    def _dispatch_frame(self, frames: list[bytes]) -> None:
        if len(frames) != 2:
            logger.warning(
                "dropping streaming frame with %d parts", len(frames)
            )
            self._bump_decode_error()
            return
        try:
            topic = frames[0].decode("utf-8")
        except UnicodeDecodeError:
            logger.warning("dropping streaming frame with a non-UTF-8 topic")
            self._bump_decode_error()
            return

        if topic == READY_TOPIC:
            self._consume_ready()
        elif topic == _STATUS_TOPIC:
            self._handle_status_frame(frames[1])
        elif _BAR_TOPIC_PREFIX in topic:
            self._handle_bar_frame(topic, frames[1])
        # Other topics are outside the Streaming Interface; ignore them.

    def _consume_ready(self) -> None:
        self._sub_ready.set()
        self._peer_ready.set()

    def _handle_bar_frame(self, topic: str, payload: bytes) -> None:
        with self._subscriptions_lock:
            subscription = self._bar_subscription
        if subscription is None:
            return

        try:
            table = ipc_to_table(payload)
        except Exception:
            logger.exception("dropping malformed Arrow frame on %s", topic)
            self._bump_decode_error()
            return

        with self._counter_lock:
            self._received_frames += 1

        if subscription.forward:
            try:
                self._send(topic, payload, bar=True)
            except zmq.ZMQError:
                # Only the shutdown race is safe to suppress: the handler must
                # still run with the frame it already decoded.
                logger.debug(
                    "forwarding Bar frame failed during shutdown",
                    exc_info=True,
                )

        try:
            subscription.handler(topic, table)
        except Exception:
            logger.exception("Bar handler failed for topic %s", topic)
            self._bump_handler_error()

    def _handle_status_frame(self, payload: bytes) -> None:
        with self._subscriptions_lock:
            handler = self._status_handler
        if handler is None:
            return

        try:
            status = json.loads(payload)
        except (TypeError, ValueError):
            logger.exception("dropping malformed status JSON")
            self._bump_decode_error()
            return
        if not isinstance(status, dict):
            logger.warning("dropping non-object status document")
            self._bump_decode_error()
            return

        try:
            handler(status)
        except Exception:
            logger.exception("status handler failed")
            self._bump_handler_error()

    # -- counter helpers ---------------------------------------------------

    def _bump_decode_error(self) -> None:
        with self._counter_lock:
            self._decode_errors += 1

    def _bump_handler_error(self) -> None:
        with self._counter_lock:
            self._handler_errors += 1


def _encode_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, default=str).encode("utf-8")
