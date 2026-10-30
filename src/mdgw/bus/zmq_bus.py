"""ZMQ in-process bus and Arrow IPC wire codec.

Every published message is a two-frame multipart message: `[topic][Arrow IPC
stream]`, matching the Streaming Interface. The ready topic exists only to
solve the PUB/SUB slow-joiner problem described in DESIGN §2.2.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

import pyarrow as pa
import zmq

READY_TOPIC = "_internal.ready"


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
        self._subscribers: list[tuple[threading.Thread, threading.Event]] = []
        self._subscriber_ready = threading.Event()
        self._closed = False

    def bind_publisher(self, endpoint: str) -> None:
        publisher = self._context.socket(zmq.PUB)
        publisher.setsockopt(zmq.SNDHWM, self._zmq_hwm)
        publisher.bind(endpoint)
        self._publisher = publisher

    def subscribe(
        self,
        endpoint: str,
        *,
        topic_prefix: str,
        handler: Callable[[str, bytes], None],
    ) -> None:
        stop = threading.Event()
        thread = threading.Thread(
            target=self._run_subscriber,
            args=(endpoint, topic_prefix, handler, stop),
            name="mdgw-zmq-subscriber",
            daemon=True,
        )
        self._subscribers.append((thread, stop))
        thread.start()

    def publish(self, topic: str, payload: bytes) -> None:
        if self._publisher is None:
            raise RuntimeError("publisher is not bound")
        self._publisher.send_multipart([topic.encode("utf-8"), payload])

    def wait_for_subscriber(self, timeout: float = 2.0) -> None:
        """Publish readiness pings until a SUB socket is actually receiving."""
        deadline = time.monotonic() + timeout
        while not self._subscriber_ready.wait(0.01):
            if time.monotonic() >= deadline:
                raise TimeoutError("no ZMQ subscriber became ready")
            self.publish(READY_TOPIC, b"")

    def _run_subscriber(
        self,
        endpoint: str,
        topic_prefix: str,
        handler: Callable[[str, bytes], None],
        stop: threading.Event,
    ) -> None:
        socket = self._context.socket(zmq.SUB)
        socket.setsockopt(zmq.SUBSCRIBE, topic_prefix.encode("utf-8"))
        socket.setsockopt(zmq.SUBSCRIBE, READY_TOPIC.encode("utf-8"))
        socket.setsockopt(zmq.RCVHWM, self._zmq_hwm)
        socket.connect(endpoint)
        try:
            poller = zmq.Poller()
            poller.register(socket, zmq.POLLIN)
            while not stop.is_set():
                if not poller.poll(50):
                    continue
                topic_bytes, payload = socket.recv_multipart()
                topic = topic_bytes.decode("utf-8")
                if topic == READY_TOPIC:
                    # Receiving a ping proves subscription forwarding finished.
                    self._subscriber_ready.set()
                    continue
                handler(topic, payload)
        finally:
            socket.close(linger=0)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for thread, stop in self._subscribers:
            stop.set()
        for thread, _ in self._subscribers:
            thread.join(timeout=2.0)
        if self._publisher is not None:
            self._publisher.close(linger=0)
        self._context.term()
