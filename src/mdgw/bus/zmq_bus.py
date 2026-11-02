"""ZMQ in-process bus and Arrow IPC wire codec.

Every published message is a two-frame multipart message: `[topic][Arrow IPC
stream]`, matching the Streaming Interface. The ready topic exists only to
solve the PUB/SUB slow-joiner problem described in DESIGN §2.2.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterable

import pyarrow as pa
import zmq

READY_TOPIC = "_internal.ready"

import logging
logger = logging.getLogger(__name__)


def table_to_ipc(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def ipc_to_table(payload: bytes) -> pa.Table:
    return pa.ipc.open_stream(pa.BufferReader(payload)).read_all()


class ZMQBus:
    """One process-local ZeroMQ context with a PUB socket and SUB threads."""

    def __init__(self, *, zmq_hwm: int = 10_000) -> None:
        self._context = zmq.Context()
        self._zmq_hwm = zmq_hwm

        self._publisher: zmq.Socket | None = None
        self._pub_lock = threading.Lock()
        self._bound_endpoints: tuple[str, ...] = ()

        self._subscribers: list[tuple[threading.Thread, threading.Event, threading.Event]] = []
        self._sub_lock = threading.Lock()

        self._closed = False

    def bind_publisher(self, endpoint: str | Iterable[str]) -> tuple[str, ...]:
        """Bind the PUB socket to one or more endpoints.

        Returns the resolved endpoints, in binding order. A request such as
        `tcp://127.0.0.1:0` is resolved by ZeroMQ, so callers can advertise the
        actual port. One PUB socket serves both the internal `inproc://` bus and
        the external `tcp://` Streaming Interface.
        """
        endpoints = [endpoint] if isinstance(endpoint, str) else list(endpoint)
        if not endpoints:
            raise ValueError("at least one publisher endpoint is required")

        with self._pub_lock:
            if self._publisher is not None:
                raise RuntimeError("Publisher is already bound")

            publisher = self._context.socket(zmq.PUB)
            publisher.setsockopt(zmq.SNDHWM, self._zmq_hwm)
            # 进程内通信时，设置 Linger=0 确保关闭时不阻塞
            publisher.setsockopt(zmq.LINGER, 0)
            resolved: list[str] = []
            try:
                for value in endpoints:
                    publisher.bind(value)
                    resolved.append(
                        publisher.getsockopt_string(zmq.LAST_ENDPOINT)
                    )
            except Exception:
                publisher.close()
                raise

            self._publisher = publisher
            self._bound_endpoints = tuple(resolved)
            return self._bound_endpoints

    @property
    def endpoints(self) -> tuple[str, ...]:
        """Resolved endpoints the PUB socket is bound to."""
        return self._bound_endpoints

    def subscribe(
        self,
        endpoint: str,
        *,
        topic_prefix: str,
        handler: Callable[[str, bytes], None],
    ) -> threading.Event:
        ready_event = threading.Event()
        stop_event = threading.Event()

        sub_id = f"{endpoint}-{topic_prefix}"

        thread = threading.Thread(
            target=self._run_subscriber,
            args=(endpoint, topic_prefix, handler, stop_event, ready_event, sub_id),
            name="mdgw-zmq-subscriber",
            daemon=True,
        )

        with self._sub_lock:
            if self._closed:
                raise RuntimeError("Bus is closed")
            self._subscribers.append((thread, stop_event, ready_event))
            thread.start()
        return ready_event

    def publish(self, topic: str, payload: bytes) -> None:
        with self._pub_lock:
            if self._publisher is None:
                raise RuntimeError("Publisher is not bound")
            if self._closed:
                return
            self._publisher.send_multipart([topic.encode("utf-8"), payload])

    def wait_for_subscriber(self, ready_events: list[threading.Event] | threading.Event, timeout: float = 2.0) -> None:
        """Publish readiness pings until specified SUB socket(s) are actually receiving."""
        events = [ready_events] if isinstance(ready_events, threading.Event) else ready_events
        if not events:
            return
        deadline = time.monotonic() + timeout

        while not all(event.is_set() for event in events):
            if time.monotonic() >= deadline:
                raise TimeoutError("no ZMQ subscriber became ready")
            try:
                self.publish(READY_TOPIC, b"")
            except RuntimeError:
                break
            time.sleep(0.01)

    def _run_subscriber(
        self,
        endpoint: str,
        topic_prefix: str,
        handler: Callable[[str, bytes], None],
        stop_event: threading.Event,
        ready_event: threading.Event,
        sub_id: str,
    ) -> None:
        socket = self._context.socket(zmq.SUB)
        socket.setsockopt(zmq.SUBSCRIBE, topic_prefix.encode("utf-8"))
        socket.setsockopt(zmq.SUBSCRIBE, READY_TOPIC.encode("utf-8"))
        socket.setsockopt(zmq.RCVHWM, self._zmq_hwm)
        socket.setsockopt(zmq.LINGER, 0)
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        try:
            socket.connect(endpoint)
            while not stop_event.is_set():
                socks = dict(poller.poll(20))
                if socket not in socks or socks[socket] != zmq.POLLIN:
                    continue

                topic_bytes, payload = socket.recv_multipart()
                topic = topic_bytes.decode("utf-8")
                if topic == READY_TOPIC:
                    # Receiving a ping proves subscription forwarding finished.
                    ready_event.set()
                    continue
                try:
                    handler(topic, payload)
                except Exception as e:
                    logger.error(f"[Sub-{sub_id}] Error in message handler: {e}", exc_info=True)
        finally:
            poller.unregister(socket)
            socket.close()

    def close(self) -> None:
        with self._sub_lock, self._pub_lock:
            if self._closed:
                return
            self._closed = True
            
            for _, stop_event, _ in self._subscribers:
                stop_event.set()
                
            subscribers_to_join = list(self._subscribers)
            self._subscribers.clear()

            if self._publisher is not None:
                self._publisher.close()
                self._publisher = None
            self._bound_endpoints = ()

        for thread, _, _ in subscribers_to_join:
            thread.join(timeout=2.0)

        self._context.term()
