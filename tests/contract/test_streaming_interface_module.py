"""Seam tests for the Streaming Interface module (`mdgw.streaming`).

Every test drives the public surface with real ZMQ sockets. Loopback TCP with
port 0 is used where two independent contexts must talk; `inproc://` is used
where one instance publishes and subscribes through its own context (the same
shape `GatewayRuntime` uses).

No HTTP or aiohttp objects are involved here.
"""

from __future__ import annotations

import ast
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pytest
import zmq

import mdgw.streaming
from mdgw.core.bar import Bar

UTC = timezone.utc
BAR_TOPIC = "market.bar.1m.binance:BTC/USDT"


def _bar(
    *,
    interval: str = "1m",
    instrument_id: str = "binance:BTC/USDT",
    open_ts: datetime | None = None,
) -> Bar:
    start = open_ts or datetime(2024, 1, 1, tzinfo=UTC)
    close = start + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=instrument_id,
        interval=interval,
        open_ts=start,
        close_ts=close,
        event_ts=close,
        ingest_ts=close,
        feed="binance_spot",
        seq=123456,
        aggregation_source="external",
        open=Decimal("65000.1"),
        high=Decimal("65010.0"),
        low=Decimal("64990.0"),
        close=Decimal("65005.5"),
        volume=Decimal("12.34"),
        trade_count=128,
        revision=0,
    )


def _inproc_endpoint() -> str:
    return f"inproc://mdgw-streaming-{uuid4().hex}"


# -- topic syntax and exports -------------------------------------------------


def test_bar_topic_builds_the_wire_topic_from_a_bar() -> None:
    from mdgw.streaming import bar_topic

    assert bar_topic(_bar()) == BAR_TOPIC


def test_bar_topic_prefixes_namespace_as_whole_segments() -> None:
    from mdgw.streaming import bar_topic

    assert (
        bar_topic(_bar(), namespace="replay.rpl-abc")
        == f"replay.rpl-abc.{BAR_TOPIC}"
    )


def test_bar_topic_prefix_supports_optional_parts_and_namespace() -> None:
    from mdgw.streaming import bar_topic_prefix

    assert bar_topic_prefix() == "market.bar."
    assert bar_topic_prefix(interval="1m") == "market.bar.1m."
    assert (
        bar_topic_prefix(interval="1m", instrument_id="binance:BTC/USDT")
        == BAR_TOPIC
    )
    assert (
        bar_topic_prefix(namespace="replay.rpl-abc")
        == "replay.rpl-abc.market.bar."
    )
    assert (
        bar_topic_prefix(
            interval="1m",
            instrument_id="binance:BTC/USDT",
            namespace="replay.rpl-abc",
        )
        == f"replay.rpl-abc.{BAR_TOPIC}"
    )


# -- exports and lifecycle ----------------------------------------------------


def test_public_exports_are_exactly_the_documented_seams() -> None:
    import mdgw.streaming as streaming

    assert set(streaming.__all__) == {
        "StreamingInterface",
        "bar_topic",
        "bar_topic_prefix",
        "table_to_ipc",
        "ipc_to_table",
        "READY_TOPIC",
    }
    for name in streaming.__all__:
        assert hasattr(streaming, name)
    assert streaming.READY_TOPIC == "_internal.ready"


def test_module_imports_only_the_allowed_layers() -> None:
    tree = ast.parse(
        Path(mdgw.streaming.__file__).read_text(encoding="utf-8")
    )
    roots: set[str] = set()
    first_party: set[str] = set()
    for node in ast.walk(tree):
        modules: list[str] = []
        if isinstance(node, ast.ImportFrom) and node.module and not node.level:
            modules.append(node.module)
        elif isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        for module in modules:
            roots.add(module.split(".")[0])
            if module == "mdgw" or module.startswith("mdgw."):
                first_party.add(module)

    assert roots <= {
        "__future__",
        "collections",
        "dataclasses",
        "json",
        "logging",
        "threading",
        "time",
        "typing",
        "pyarrow",
        "zmq",
        "mdgw",
    }
    assert all(
        module == "mdgw.core" or module.startswith("mdgw.core.")
        for module in first_party
    )


def test_start_resolves_endpoints_in_bind_order() -> None:
    from mdgw.streaming import StreamingInterface

    inproc = _inproc_endpoint()
    interface = StreamingInterface(
        bind=["tcp://127.0.0.1:0", inproc],
    )
    assert interface.endpoints == ()
    try:
        interface.start()
        interface.start()  # idempotent

        endpoints = interface.endpoints
        assert len(endpoints) == 2
        assert endpoints[0].startswith("tcp://127.0.0.1:")
        assert not endpoints[0].endswith(":0")
        assert endpoints[1] == inproc
    finally:
        interface.stop()
        interface.stop()  # idempotent


def test_start_after_stop_is_rejected_single_use() -> None:
    from mdgw.streaming import StreamingInterface

    interface = StreamingInterface(bind=_inproc_endpoint())
    interface.start()
    interface.stop()

    with pytest.raises(RuntimeError):
        interface.start()


def test_publish_and_ready_calls_raise_before_start() -> None:
    from mdgw.streaming import StreamingInterface

    endpoint = _inproc_endpoint()
    interface = StreamingInterface(
        bind=endpoint, connect=endpoint, namespace="replay.unit"
    )

    with pytest.raises(RuntimeError):
        interface.publish_bars([])
    with pytest.raises(RuntimeError):
        interface.publish_bars([_bar()])
    with pytest.raises(RuntimeError):
        interface.publish_status({})
    with pytest.raises(RuntimeError):
        interface.publish_event({})
    with pytest.raises(RuntimeError):
        interface.mark_ready()
    with pytest.raises(RuntimeError):
        interface.wait_until_peer_ready(0.0)
    with pytest.raises(RuntimeError):
        interface.subscribe_bars(lambda _topic, _table: None)
    with pytest.raises(RuntimeError):
        interface.subscribe_status(lambda _status: None)

    assert interface.peer_ready is False


def test_publishing_after_stop_is_silent_and_peer_is_not_ready() -> None:
    from mdgw.streaming import StreamingInterface

    interface = StreamingInterface(
        bind=_inproc_endpoint(), namespace="replay.unit"
    )
    interface.start()
    interface.stop()

    interface.publish_bars([_bar()])
    interface.publish_status({"ok": True})
    interface.publish_event({"event": "end_of_session"})

    assert interface.peer_ready is False


def test_stop_releases_the_bound_port() -> None:
    from mdgw.streaming import StreamingInterface

    first = StreamingInterface(bind="tcp://127.0.0.1:0")
    first.start()
    endpoint = first.endpoints[0]
    first.stop()

    second = StreamingInterface(bind=endpoint)
    try:
        second.start()
    finally:
        second.stop()


def test_start_failure_cleans_up_and_reraises() -> None:
    from mdgw.streaming import StreamingInterface

    interface = StreamingInterface(
        bind=["tcp://127.0.0.1:0", "invalid://not-a-transport"]
    )

    with pytest.raises(zmq.ZMQError):
        interface.start()

    assert interface.endpoints == ()
    with pytest.raises(RuntimeError):
        interface.publish_bars([])
    assert interface.peer_ready is False


# -- Bar publishing and subscription -----------------------------------------


def _wait_until(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met before the timeout")


def _recv_matching(
    socket: zmq.Socket,
    predicate: Callable[[list[bytes]], bool],
    timeout: float = 2.0,
) -> list[bytes]:
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))
            events = dict(poller.poll(min(remaining_ms, 100)))
            if events.get(socket) != zmq.POLLIN:
                continue
            frames = socket.recv_multipart()
            if predicate(frames):
                return frames
    finally:
        poller.unregister(socket)
    raise AssertionError("matching frame was not received before the timeout")


def test_publish_bars_roundtrip_over_inproc_arrow_ipc() -> None:
    from mdgw.core.schema import BAR_SCHEMA
    from mdgw.streaming import StreamingInterface

    endpoint = _inproc_endpoint()
    received: list[tuple[str, pa.Table]] = []
    interface = StreamingInterface(bind=endpoint, connect=endpoint)
    interface.start()
    try:
        interface.subscribe_bars(
            lambda topic, table: received.append((topic, table)),
            await_ready=True,
        )
        interface.publish_bars([_bar()])
        _wait_until(lambda: len(received) == 1)
    finally:
        interface.stop()

    topic, table = received[0]
    assert topic == BAR_TOPIC
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 1
    assert table.column("open").to_pylist() == [Decimal("65000.1")]
    assert interface.published_frames == 1
    assert interface.received_frames == 1


def test_publish_bars_groups_by_interval_and_instrument() -> None:
    from mdgw.streaming import StreamingInterface

    endpoint = _inproc_endpoint()
    received: list[tuple[str, pa.Table]] = []
    interface = StreamingInterface(bind=endpoint, connect=endpoint)
    interface.start()
    try:
        interface.subscribe_bars(
            lambda topic, table: received.append((topic, table)),
            await_ready=True,
        )
        interface.publish_bars(
            [
                _bar(),
                _bar(open_ts=datetime(2024, 1, 1, 0, 1, tzinfo=UTC)),
                _bar(
                    interval="5m",
                    instrument_id="binance:ETH/USDT",
                    open_ts=datetime(2024, 1, 1, tzinfo=UTC),
                ),
            ]
        )
        _wait_until(lambda: len(received) == 2)
    finally:
        interface.stop()

    rows_by_topic = {topic: table.num_rows for topic, table in received}
    assert rows_by_topic == {
        BAR_TOPIC: 2,
        "market.bar.5m.binance:ETH/USDT": 1,
    }
    assert interface.published_frames == 2


def test_subscribe_bars_requires_a_connect_endpoint() -> None:
    from mdgw.streaming import StreamingInterface

    interface = StreamingInterface(bind=_inproc_endpoint())
    interface.start()
    try:
        assert interface.unsubscribe_bars() is False
        with pytest.raises(RuntimeError):
            interface.subscribe_bars(lambda _topic, _table: None)
        with pytest.raises(RuntimeError):
            interface.subscribe_status(lambda _status: None)
    finally:
        interface.stop()


def test_duplicate_subscriptions_are_rejected() -> None:
    from mdgw.streaming import StreamingInterface

    endpoint = _inproc_endpoint()
    interface = StreamingInterface(bind=endpoint, connect=endpoint)
    interface.start()
    try:
        interface.subscribe_bars(lambda _topic, _table: None)
        with pytest.raises(RuntimeError):
            interface.subscribe_bars(lambda _topic, _table: None)

        interface.subscribe_status(lambda _status: None)
        with pytest.raises(RuntimeError):
            interface.subscribe_status(lambda _status: None)
    finally:
        interface.stop()


def test_unsubscribe_bars_keeps_the_subscription_reusable() -> None:
    from mdgw.streaming import StreamingInterface

    endpoint = _inproc_endpoint()
    received: list[str] = []
    interface = StreamingInterface(bind=endpoint, connect=endpoint)
    interface.start()
    try:
        assert interface.unsubscribe_bars() is False
        interface.subscribe_bars(
            lambda topic, _table: received.append(topic), await_ready=True
        )
        assert interface.unsubscribe_bars() is True
        assert interface.unsubscribe_bars() is False

        # The SUB socket is still connected, so a second subscription needs no
        # new readiness handshake.
        interface.subscribe_bars(
            lambda topic, _table: received.append(topic)
        )
        interface.publish_bars([_bar()])
        _wait_until(lambda: len(received) == 1)
    finally:
        interface.stop()

    assert received == [BAR_TOPIC]


def test_status_roundtrip_uses_json_and_does_not_count_as_a_bar() -> None:
    from mdgw.streaming import StreamingInterface

    endpoint = _inproc_endpoint()
    received: list[dict[str, object]] = []
    interface = StreamingInterface(bind=endpoint, connect=endpoint)
    interface.start()
    try:
        interface.subscribe_bars(
            lambda _topic, _table: None, await_ready=True
        )
        interface.subscribe_status(received.append)
        interface.publish_status(
            {"role": "collector", "label": "行情", "value": Decimal("1.5")}
        )
        _wait_until(lambda: len(received) == 1)
    finally:
        interface.stop()

    assert received == [
        {"role": "collector", "label": "行情", "value": "1.5"}
    ]
    assert interface.published_frames == 0
    assert interface.received_frames == 0


def test_forward_true_replays_the_original_frame_bytes() -> None:
    from mdgw.streaming import StreamingInterface

    bar = _bar()
    publisher = StreamingInterface(bind="tcp://127.0.0.1:0")
    publisher.start()
    forwarder = StreamingInterface(
        bind="tcp://127.0.0.1:0", connect=publisher.endpoints[0]
    )
    forwarder.start()
    context = zmq.Context()
    raw_from_publisher = context.socket(zmq.SUB)
    raw_from_forwarder = context.socket(zmq.SUB)
    for socket in (raw_from_publisher, raw_from_forwarder):
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.SUBSCRIBE, b"market.bar.")
        socket.setsockopt(zmq.SUBSCRIBE, b"_internal.ready")
    raw_from_publisher.connect(publisher.endpoints[0])
    raw_from_forwarder.connect(forwarder.endpoints[0])
    forwarded: list[tuple[str, pa.Table]] = []
    try:
        forwarder.subscribe_bars(
            lambda topic, table: forwarded.append((topic, table)),
            forward=True,
        )
        publisher.mark_ready()
        forwarder.mark_ready()
        _recv_matching(
            raw_from_publisher,
            lambda frames: frames[0] == b"_internal.ready",
        )
        _recv_matching(
            raw_from_forwarder,
            lambda frames: frames[0] == b"_internal.ready",
        )
        assert forwarder.wait_until_peer_ready(2.0) is True

        publisher.publish_bars([bar])
        original = _recv_matching(
            raw_from_publisher, lambda frames: frames[0] == BAR_TOPIC.encode()
        )
        forwarded_frame = _recv_matching(
            raw_from_forwarder, lambda frames: frames[0] == BAR_TOPIC.encode()
        )
        _wait_until(lambda: len(forwarded) == 1)
    finally:
        raw_from_publisher.close()
        raw_from_forwarder.close()
        context.term()
        forwarder.stop()
        publisher.stop()

    assert forwarded_frame[1] == original[1]
    topic, table = forwarded[0]
    assert topic == BAR_TOPIC
    assert table.num_rows == 1
    assert forwarder.received_frames == 1
    assert forwarder.published_frames == 1


def test_malformed_bar_frames_are_dropped_and_counted() -> None:
    from mdgw.streaming import StreamingInterface

    bar = _bar()
    context = zmq.Context()
    raw_publisher = context.socket(zmq.PUB)
    raw_publisher.setsockopt(zmq.LINGER, 0)
    raw_publisher.bind("tcp://127.0.0.1:0")
    endpoint = raw_publisher.getsockopt_string(zmq.LAST_ENDPOINT)

    received: list[str] = []
    interface = StreamingInterface(
        bind="tcp://127.0.0.1:0", connect=endpoint
    )
    interface.start()
    try:
        interface.subscribe_bars(
            lambda topic, _table: received.append(topic)
        )
        # READY proves the SUB socket is connected, so the malformed frame
        # cannot be lost to the PUB/SUB slow joiner.
        deadline = time.monotonic() + 2.0
        while not interface.peer_ready:
            assert time.monotonic() < deadline
            raw_publisher.send_multipart([b"_internal.ready", b""])
            time.sleep(0.01)

        raw_publisher.send_multipart([BAR_TOPIC.encode(), b"not-arrow"])
        _wait_until(lambda: interface.decode_errors == 1)
        assert received == []
        assert interface.received_frames == 0

        from mdgw.core.schema import bars_to_logical_table
        from mdgw.streaming import table_to_ipc

        raw_publisher.send_multipart(
            [
                BAR_TOPIC.encode(),
                table_to_ipc(bars_to_logical_table([bar])),
            ]
        )
        _wait_until(lambda: len(received) == 1)
    finally:
        interface.stop()
        raw_publisher.close()
        context.term()

    assert received == [BAR_TOPIC]
    assert interface.received_frames == 1


def test_malformed_status_json_is_dropped_and_counted() -> None:
    from mdgw.streaming import StreamingInterface

    context = zmq.Context()
    raw_publisher = context.socket(zmq.PUB)
    raw_publisher.setsockopt(zmq.LINGER, 0)
    raw_publisher.bind("tcp://127.0.0.1:0")
    endpoint = raw_publisher.getsockopt_string(zmq.LAST_ENDPOINT)

    received: list[dict[str, object]] = []
    interface = StreamingInterface(
        bind="tcp://127.0.0.1:0", connect=endpoint
    )
    interface.start()
    try:
        interface.subscribe_status(received.append)
        deadline = time.monotonic() + 2.0
        while not interface.peer_ready:
            assert time.monotonic() < deadline
            raw_publisher.send_multipart([b"_internal.ready", b""])
            time.sleep(0.01)

        raw_publisher.send_multipart([b"_internal.status", b"{not json"])
        _wait_until(lambda: interface.decode_errors == 1)

        raw_publisher.send_multipart(
            [b"_internal.status", b'{"role": "collector"}']
        )
        _wait_until(lambda: len(received) == 1)
    finally:
        interface.stop()
        raw_publisher.close()
        context.term()

    assert received == [{"role": "collector"}]
    assert interface.received_frames == 0


def test_handler_exceptions_do_not_kill_the_subscription_thread() -> None:
    from mdgw.streaming import StreamingInterface

    calls: list[str] = []

    def failing_handler(topic: str, _table: pa.Table) -> None:
        calls.append(topic)
        raise ValueError("boom")

    endpoint = _inproc_endpoint()
    interface = StreamingInterface(bind=endpoint, connect=endpoint)
    interface.start()
    try:
        interface.subscribe_bars(failing_handler, await_ready=True)
        interface.publish_bars(
            [
                _bar(),
                _bar(instrument_id="binance:ETH/USDT"),
            ]
        )
        _wait_until(lambda: len(calls) == 2)
    finally:
        interface.stop()

    assert interface.handler_errors == 2
    assert interface.received_frames == 2


# -- READY handshake and events ----------------------------------------------


def test_mark_ready_makes_the_peer_ready_and_satisfies_wait() -> None:
    from mdgw.streaming import StreamingInterface

    publisher = StreamingInterface(bind="tcp://127.0.0.1:0")
    publisher.start()
    subscriber = StreamingInterface(
        connect=publisher.endpoints[0], bind=_inproc_endpoint()
    )
    subscriber.start()
    try:
        assert subscriber.peer_ready is False
        assert subscriber.wait_until_peer_ready(0.01) is False

        publisher.mark_ready()
        publisher.mark_ready()  # idempotent

        assert subscriber.wait_until_peer_ready(2.0) is True
        assert subscriber.peer_ready is True
        assert subscriber.published_frames == 0
        assert subscriber.received_frames == 0
    finally:
        subscriber.stop()
        publisher.stop()

    assert subscriber.peer_ready is False


def test_wait_until_peer_ready_returns_false_without_a_peer() -> None:
    from mdgw.streaming import StreamingInterface

    interface = StreamingInterface(
        bind=_inproc_endpoint(), connect=_inproc_endpoint()
    )
    interface.start()
    try:
        assert interface.wait_until_peer_ready(0.01) is False
    finally:
        interface.stop()


def test_publish_event_uses_the_namespaced_control_topic() -> None:
    from mdgw.streaming import StreamingInterface

    interface = StreamingInterface(
        bind="tcp://127.0.0.1:0", namespace="replay.rpl-abc"
    )
    interface.start()
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"replay.rpl-abc.control")
    subscriber.setsockopt(zmq.SUBSCRIBE, b"_internal.ready")
    subscriber.connect(interface.endpoints[0])
    try:
        interface.mark_ready()
        _recv_matching(
            subscriber, lambda frames: frames[0] == b"_internal.ready"
        )
        interface.publish_event({"event": "end_of_session"})
        frames = _recv_matching(
            subscriber, lambda frame: frame[0] != b"_internal.ready"
        )
    finally:
        subscriber.close()
        context.term()
        interface.stop()

    assert frames[0] == b"replay.rpl-abc.control"
    assert frames[1] == b'{"event": "end_of_session"}'
    assert interface.published_frames == 0


def test_publish_event_requires_a_namespace() -> None:
    from mdgw.streaming import StreamingInterface

    interface = StreamingInterface(bind=_inproc_endpoint())
    interface.start()
    try:
        with pytest.raises(ValueError):
            interface.publish_event({"event": "end_of_session"})
    finally:
        interface.stop()

