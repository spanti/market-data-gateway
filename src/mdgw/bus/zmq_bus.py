"""ZMQ in-process bus retained for not-yet-migrated callers.

Every published message is a two-frame multipart message: `[topic][Arrow IPC
stream]`, matching the Streaming Interface. The ready topic exists only to
solve the PUB/SUB slow-joiner problem described in DESIGN §2.2.

The Arrow IPC codec and `READY_TOPIC` are owned by `mdgw.streaming` and only
re-exported here, so there is exactly one implementation. `ZMQBus` itself is
scheduled for removal once every caller uses `StreamingInterface`.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import zmq

from mdgw.streaming import READY_TOPIC, ipc_to_table, table_to_ipc

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

__all__ = ["READY_TOPIC", "ZMQBus", "ipc_to_table", "table_to_ipc"]

logger = logging.getLogger(__name__)


@dataclass
class _Subscription:
    endpoint: str
    topic_prefixes: tuple[str, ...]
    handler: Callable[[str, bytes], None]
    stop_event: threading.Event
    ready_event: threading.Event
    sub_id: str
    thread: threading.Thread | None = None


class ZMQBus:
    """One process-local ZeroMQ context with a PUB socket and SUB threads."""

    def __init__(self, *, zmq_hwm: int = 10_000) -> None:
        self._context = zmq.Context()
        self._zmq_hwm = zmq_hwm

        self._publisher: zmq.Socket | None = None
        self._pub_lock = threading.Lock()
        self._bound_endpoints: tuple[str, ...] = ()

        self._subscribers: list[_Subscription] = []
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
            msg = "at least one publisher endpoint is required"
            raise ValueError(msg)

        with self._pub_lock:
            if self._publisher is not None:
                msg = "Publisher is already bound"
                raise RuntimeError(msg)

            publisher = self._context.socket(zmq.PUB)
            publisher.setsockopt(zmq.SNDHWM, self._zmq_hwm)
            # 进程内通信时, 设置 Linger=0 确保关闭时不阻塞
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
        extra_prefixes: Iterable[str] = (),
    ) -> threading.Event:
        ready_event = threading.Event()
        stop_event = threading.Event()

        prefixes = (topic_prefix, *tuple(extra_prefixes))
        sub_id = f"{endpoint}-{','.join(prefixes)}"

        subscription = _Subscription(
            endpoint=endpoint,
            topic_prefixes=prefixes,
            handler=handler,
            stop_event=stop_event,
            ready_event=ready_event,
            sub_id=sub_id,
        )
        subscription.thread = threading.Thread(
            target=self._run_subscriber,
            args=(subscription,),
            name="mdgw-zmq-subscriber",
            daemon=True,
        )

        with self._sub_lock:
            if self._closed:
                msg = "Bus is closed"
                raise RuntimeError(msg)
            self._subscribers.append(subscription)
            subscription.thread.start()
        return ready_event

    def publish(self, topic: str, payload: bytes) -> None:
        with self._pub_lock:
            if self._publisher is None:
                msg = "Publisher is not bound"
                raise RuntimeError(msg)
            if self._closed:
                return
            self._publisher.send_multipart([topic.encode("utf-8"), payload])

    def wait_for_subscriber(
        self,
        ready_events: list[threading.Event] | threading.Event,
        timeout: float = 2.0,
    ) -> None:
        """Publish readiness pings until given SUB socket(s) are receiving."""
        events = (
            [ready_events]
            if isinstance(ready_events, threading.Event)
            else ready_events
        )
        if not events:
            return
        deadline = time.monotonic() + timeout

        while not all(event.is_set() for event in events):
            if time.monotonic() >= deadline:
                msg = "no ZMQ subscriber became ready"
                raise TimeoutError(msg)
            try:
                self.publish(READY_TOPIC, b"")
            except RuntimeError:
                break
            time.sleep(0.01)

    def _run_subscriber(self, subscription: _Subscription) -> None:
        socket = self._context.socket(zmq.SUB)
        for prefix in subscription.topic_prefixes:
            socket.setsockopt(zmq.SUBSCRIBE, prefix.encode("utf-8"))
        socket.setsockopt(zmq.SUBSCRIBE, READY_TOPIC.encode("utf-8"))
        socket.setsockopt(zmq.RCVHWM, self._zmq_hwm)
        socket.setsockopt(zmq.LINGER, 0)
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        try:
            socket.connect(subscription.endpoint)
            while not subscription.stop_event.is_set():
                socks = dict(poller.poll(20))
                if socket not in socks or socks[socket] != zmq.POLLIN:
                    continue

                topic_bytes, payload = socket.recv_multipart()
                topic = topic_bytes.decode("utf-8")
                if topic == READY_TOPIC:
                    # Receiving a ping proves subscription forwarding finished.
                    subscription.ready_event.set()
                    continue
                try:
                    subscription.handler(topic, payload)
                except Exception:
                    logger.exception(
                        "[Sub-%s] Error in message handler",
                        subscription.sub_id,
                    )
        finally:
            poller.unregister(socket)
            socket.close()

    def close(self) -> None:
        with self._sub_lock, self._pub_lock:
            if self._closed:
                return
            self._closed = True
            
            for subscription in self._subscribers:
                subscription.stop_event.set()

            subscribers_to_join = list(self._subscribers)
            self._subscribers.clear()

            if self._publisher is not None:
                self._publisher.close()
                self._publisher = None
            self._bound_endpoints = ()

        for subscription in subscribers_to_join:
            if subscription.thread is not None:
                subscription.thread.join(timeout=2.0)

        self._context.term()
