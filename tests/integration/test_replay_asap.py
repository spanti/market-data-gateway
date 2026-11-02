"""Behavior tests for the asap Replay Protocol.

Seams under test:
- `ReplayService` + real Parquet/Manifest: one Arrow IPC message per Bar in
  `close_ts` order, followed by a JSON End of Session.
- Determinism: two runs of the same spec produce byte-identical events after
  stripping the session id.
- `ReplayServer`: REQ/REP JSON control and PUB market messages, with the
  `_internal.ready` handshake used to avoid the slow-joiner race.
- Replay Session heartbeat locks stay alive for the run and are released.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import zmq

from mdgw.bus.zmq_bus import READY_TOPIC, ipc_to_table
from mdgw.core.bar import Bar
from mdgw.core.errors import (
    INVALID_INSTRUMENT,
    INVALID_INTERVAL,
    INVALID_PARAMETER,
    INVALID_RANGE,
    SESSION_NOT_FOUND,
    InvalidParameterError,
    SessionNotFoundError,
)
from mdgw.core.schema import BAR_SCHEMA, INTERVALS
from mdgw.replay.server import ReplayServer
from mdgw.replay.service import ReplayService
from mdgw.replay.session import ReplaySessionSpec
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import active_session_locks, lock_path

UTC = timezone.utc
BASE = datetime(2026, 10, 30, 0, 0, tzinfo=UTC)
BTC = "binance:BTC/USDT"
ETH = "binance:ETH/USDT"

START = "2026-10-30T00:00:00Z"
END = "2026-10-30T00:10:00Z"


def _bar(
    *,
    instrument_id: str = BTC,
    interval: str = "1m",
    open_minute: int = 0,
    seq: int | None = None,
) -> Bar:
    open_ts = BASE + timedelta(minutes=open_minute)
    close_ts = open_ts + INTERVALS[interval]
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=instrument_id,
        interval=interval,
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=seq,
        aggregation_source="external",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1"),
        trade_count=1,
        revision=0,
    )


def _populate(store: ParquetBarStore) -> None:
    store.write_bars(
        [
            _bar(instrument_id=BTC, interval="1m", open_minute=0, seq=2),
            _bar(instrument_id=ETH, interval="1m", open_minute=0, seq=1),
            _bar(instrument_id=BTC, interval="5m", open_minute=0),
            _bar(instrument_id=BTC, interval="1m", open_minute=4),
        ],
        asset_class="crypto",
    )


def _spec() -> ReplaySessionSpec:
    return ReplaySessionSpec.from_mapping(
        {
            "op": "create_session",
            "subscriptions": [
                {"data_type": "bar", "instrument_id": BTC, "interval": "1m"},
                {"data_type": "bar", "instrument_id": ETH, "interval": "1m"},
                {"data_type": "bar", "instrument_id": BTC, "interval": "5m"},
            ],
            "start_ts": START,
            "end_ts": END,
            "replay_mode": "asap",
        }
    )


class _Collector:
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes]] = []
        self._lock = threading.Lock()

    def __call__(self, topic: str, payload: bytes) -> None:
        with self._lock:
            self.messages.append((topic, payload))


class _BlockingPublisher:
    """Publish once into a gate, so tests can inspect a live Session."""

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()
        self.messages: list[tuple[str, bytes]] = []
        self._lock = threading.Lock()

    def __call__(self, topic: str, payload: bytes) -> None:
        if not self.entered.is_set():
            self.entered.set()
            assert self.release.wait(5), "test never released the first publish"
        with self._lock:
            self.messages.append((topic, payload))


class _BlockingReadStore:
    """Proxy whose Parquet read waits for a test-controlled gate."""

    def __init__(self, inner: ParquetBarStore, gate: threading.Event) -> None:
        self._inner = inner
        self._gate = gate

    @property
    def dataset_root(self) -> Path:
        return self._inner.dataset_root

    def iter_batches(self, **kwargs):
        assert self._gate.wait(5), "test never released the read gate"
        yield from self._inner.iter_batches(**kwargs)


def _end_of_session(messages: list[tuple[str, bytes]]) -> dict[str, Any]:
    topic, payload = messages[-1]
    assert topic.endswith(".control"), topic
    return json.loads(payload.decode("utf-8"))


def _market_messages(
    messages: list[tuple[str, bytes]], session_id: str
) -> list[tuple[str, bytes]]:
    return [
        (topic, payload)
        for topic, payload in messages
        if topic.startswith(f"replay.{session_id}.market.")
    ]


# -- ReplayService ---------------------------------------------------------


def test_asap_session_streams_ordered_bars_then_end_of_session(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate(store)
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-test"
    )

    session_id = service.create_session(_spec())
    assert session_id == "rpl-test"
    assert service.wait_session(session_id, timeout=10)

    expected_topics = [
        f"replay.{session_id}.market.bar.1m.{BTC}",
        f"replay.{session_id}.market.bar.1m.{ETH}",
        f"replay.{session_id}.market.bar.1m.{BTC}",
        f"replay.{session_id}.market.bar.5m.{BTC}",
    ]
    market = _market_messages(collector.messages, session_id)
    assert [topic for topic, _ in market] == expected_topics

    tables = [ipc_to_table(payload) for _, payload in market]
    for table in tables:
        assert table.schema == BAR_SCHEMA
        assert table.num_rows == 1
    assert [table.column("seq").to_pylist()[0] for table in tables] == [
        2,
        1,
        None,
        None,
    ]
    assert [table.column("interval").to_pylist()[0] for table in tables] == [
        "1m",
        "1m",
        "1m",
        "5m",
    ]
    assert [table.column("close_ts").to_pylist()[0] for table in tables] == [
        BASE + timedelta(minutes=1),
        BASE + timedelta(minutes=1),
        BASE + timedelta(minutes=5),
        BASE + timedelta(minutes=5),
    ]

    end = _end_of_session(collector.messages)
    assert end == {
        "op": "end_of_session",
        "session_id": session_id,
        "reason": "exhausted",
        "bars_sent": 4,
    }
    assert service.session_status(session_id) == "finished"
    assert service.active_session_ids() == []


def _run_once(
    store: ParquetBarStore, session_id: str
) -> list[tuple[str, bytes]]:
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: session_id
    )
    created = service.create_session(_spec())
    assert created == session_id
    assert service.wait_session(created, timeout=10)
    service.close()
    return collector.messages


def test_same_spec_twice_produces_byte_identical_events(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate(store)

    first = _run_once(store, "rpl-run-a")
    second = _run_once(store, "rpl-run-b")

    assert len(first) == len(second) == 5
    for (topic_a, payload_a), (topic_b, payload_b) in zip(first, second):
        assert topic_a.replace("rpl-run-a", "<session>") == topic_b.replace(
            "rpl-run-b", "<session>"
        )
        if topic_a.endswith(".control"):
            # The session id is the only allowed difference in End of Session.
            assert payload_a.replace(b"rpl-run-a", b"<session>") == payload_b.replace(
                b"rpl-run-b", b"<session>"
            )
            left = json.loads(payload_a)
            right = json.loads(payload_b)
            left.pop("session_id")
            right.pop("session_id")
            assert left == right
        else:
            assert payload_a == payload_b


def test_stop_during_publish_sends_stopped_end_of_session(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate(store)
    publisher = _BlockingPublisher()
    service = ReplayService(
        store, publish=publisher, session_id_factory=lambda: "rpl-stop"
    )

    session_id = service.create_session(_spec())
    assert publisher.entered.wait(5), "the Session never started publishing"
    assert service.stop_session(session_id) is True
    publisher.release.set()
    assert service.wait_session(session_id, timeout=10)

    end = _end_of_session(publisher.messages)
    assert end["reason"] == "stopped"
    assert end["bars_sent"] == 1
    assert service.session_status(session_id) == "stopped"
    # Stopping an already terminal Session is still `ok` and sends no second EOS.
    before = len(publisher.messages)
    assert service.stop_session(session_id) is True
    assert len(publisher.messages) == before


def test_unknown_session_raises_session_not_found(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate(store)
    service = ReplayService(store, publish=lambda topic, payload: None)

    with pytest.raises(SessionNotFoundError) as excinfo:
        service.stop_session("rpl-missing")
    assert excinfo.value.code == SESSION_NOT_FOUND

    with pytest.raises(SessionNotFoundError):
        service.session_status("rpl-missing")


def test_session_lock_lives_for_the_run_and_is_released(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate(store)
    publisher = _BlockingPublisher()
    service = ReplayService(
        store, publish=publisher, session_id_factory=lambda: "rpl-lock"
    )

    session_id = service.create_session(_spec())
    assert publisher.entered.wait(5)

    locks = active_session_locks(store.dataset_root)
    assert [lock.session_id for lock in locks] == [session_id]
    assert locks[0].subscriptions == (
        {"data_type": "bar", "instrument_id": BTC, "interval": "1m"},
        {"data_type": "bar", "instrument_id": ETH, "interval": "1m"},
        {"data_type": "bar", "instrument_id": BTC, "interval": "5m"},
    )
    lock_file = lock_path(store.dataset_root, session_id)
    assert lock_file.exists()

    publisher.release.set()
    assert service.wait_session(session_id, timeout=10)

    assert not lock_file.exists()
    assert active_session_locks(store.dataset_root) == []


def test_max_sessions_rejects_an_extra_session(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate(store)
    publisher = _BlockingPublisher()
    service = ReplayService(
        store,
        max_sessions=1,
        publish=publisher,
        session_id_factory=lambda: "rpl-only",
    )

    session_id = service.create_session(_spec())
    assert publisher.entered.wait(5)
    try:
        with pytest.raises(InvalidParameterError) as excinfo:
            service.create_session(_spec())
        assert excinfo.value.code == INVALID_PARAMETER
    finally:
        service.stop_session(session_id)
        publisher.release.set()
        assert service.wait_session(session_id, timeout=10)
        service.close()


# -- ReplayServer (real ZMQ) ----------------------------------------------


def _wait_ready(subscriber: zmq.Socket, *, timeout_s: float = 5.0) -> None:
    poller = zmq.Poller()
    poller.register(subscriber, zmq.POLLIN)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))
        if dict(poller.poll(min(100, remaining_ms))).get(subscriber) != zmq.POLLIN:
            continue
        topic, *_ = subscriber.recv_multipart()
        if topic.decode("utf-8") == READY_TOPIC:
            return
    raise AssertionError("Replay PUB never sent the ready handshake")


def _collect_until_control(
    subscriber: zmq.Socket,
    session_id: str,
    *,
    timeout_s: float = 10.0,
) -> list[tuple[str, bytes]]:
    poller = zmq.Poller()
    poller.register(subscriber, zmq.POLLIN)
    prefix = f"replay.{session_id}."
    control_topic = f"replay.{session_id}.control"
    received: list[tuple[str, bytes]] = []
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))
        if dict(poller.poll(min(100, remaining_ms))).get(subscriber) != zmq.POLLIN:
            continue
        frames = subscriber.recv_multipart()
        topic = frames[0].decode("utf-8")
        if topic == READY_TOPIC:
            continue
        if topic.startswith(prefix):
            received.append((topic, frames[1]))
            if topic == control_topic:
                return received
    raise AssertionError(f"no End of Session for {session_id!r}: {received!r}")


def _request(socket: zmq.Socket, payload: dict[str, Any]) -> dict[str, Any]:
    socket.send_string(json.dumps(payload))
    return json.loads(socket.recv().decode("utf-8"))


def _create_request() -> dict[str, Any]:
    return {
        "op": "create_session",
        "subscriptions": [
            {"data_type": "bar", "instrument_id": BTC, "interval": "1m"},
            {"data_type": "bar", "instrument_id": ETH, "interval": "1m"},
            {"data_type": "bar", "instrument_id": BTC, "interval": "5m"},
        ],
        "start_ts": START,
        "end_ts": END,
        "replay_mode": "asap",
    }


def test_zmq_control_creates_a_session_and_streams_to_pub(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate(store)
    service = ReplayService(store, session_id_factory=lambda: "rpl-zmq")
    server = ReplayServer(
        service,
        control_bind_host="127.0.0.1",
        control_port=0,
        publish_bind_host="127.0.0.1",
        publish_port=0,
    )
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    requester = context.socket(zmq.REQ)
    requester.setsockopt(zmq.LINGER, 0)
    requester.setsockopt(zmq.RCVTIMEO, 5000)
    try:
        server.start()
        assert server.publish_endpoint.startswith("tcp://127.0.0.1:")
        assert server.control_endpoint.startswith("tcp://127.0.0.1:")
        subscriber.connect(server.publish_endpoint)
        requester.connect(server.control_endpoint)

        _wait_ready(subscriber)
        response = _request(requester, _create_request())
        assert response["ok"] is True
        assert response["session_id"] == "rpl-zmq"
        assert response["publish_endpoint"] == server.publish_endpoint

        messages = _collect_until_control(subscriber, "rpl-zmq")
        market = [item for item in messages if ".market." in item[0]]
        assert len(market) == 4
        assert [ipc_to_table(payload).column("seq").to_pylist()[0] for _, payload in market] == [
            2,
            1,
            None,
            None,
        ]
        end = _end_of_session(messages)
        assert end["reason"] == "exhausted"
        assert end["bars_sent"] == 4

        # A finished Session still answers stop with ok.
        assert _request(requester, {"op": "stop", "session_id": "rpl-zmq"}) == {
            "ok": True,
            "session_id": "rpl-zmq",
        }
        assert service.wait_session("rpl-zmq", timeout=5)
        assert service.session_status("rpl-zmq") == "finished"
    finally:
        subscriber.close()
        requester.close()
        server.stop()
        context.term()


def test_zmq_stop_of_a_blocked_session_sends_stopped(tmp_path: Path) -> None:
    inner = ParquetBarStore(tmp_path / "data")
    _populate(inner)
    gate = threading.Event()
    store = _BlockingReadStore(inner, gate)
    service = ReplayService(store, session_id_factory=lambda: "rpl-blocked")
    server = ReplayServer(service)
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    requester = context.socket(zmq.REQ)
    requester.setsockopt(zmq.LINGER, 0)
    requester.setsockopt(zmq.RCVTIMEO, 5000)
    try:
        server.start()
        subscriber.connect(server.publish_endpoint)
        requester.connect(server.control_endpoint)
        _wait_ready(subscriber)

        response = _request(requester, _create_request())
        assert response["ok"] is True
        # The worker is blocked inside the Parquet read, so the Session cannot
        # finish before this stop arrives.
        assert _request(
            requester, {"op": "stop", "session_id": "rpl-blocked"}
        ) == {"ok": True, "session_id": "rpl-blocked"}
        gate.set()

        messages = _collect_until_control(subscriber, "rpl-blocked")
        end = _end_of_session(messages)
        assert end["reason"] == "stopped"
        assert end["bars_sent"] == 0
        assert service.wait_session("rpl-blocked", timeout=5)
        assert service.session_status("rpl-blocked") == "stopped"
    finally:
        subscriber.close()
        requester.close()
        server.stop()
        context.term()


@pytest.mark.parametrize(
    ("raw", "expected_code"),
    [
        (b"{not json", INVALID_PARAMETER),
        (b"[]", INVALID_PARAMETER),
        (json.dumps({"op": "unknown"}).encode(), INVALID_PARAMETER),
        (json.dumps({"op": "stop"}).encode(), INVALID_PARAMETER),
        (
            json.dumps(
                {
                    **_create_request(),
                    "replay_mode": "realtime",
                }
            ).encode(),
            INVALID_PARAMETER,
        ),
        (
            json.dumps(
                {
                    **_create_request(),
                    "start_ts": "2026-10-31T00:00:00Z",
                    "end_ts": "2026-10-30T00:00:00Z",
                }
            ).encode(),
            INVALID_RANGE,
        ),
        (
            json.dumps(
                {
                    **_create_request(),
                    "subscriptions": [
                        {
                            "data_type": "bar",
                            "instrument_id": BTC,
                            "interval": "2m",
                        }
                    ],
                }
            ).encode(),
            INVALID_INTERVAL,
        ),
        (
            json.dumps(
                {
                    **_create_request(),
                    "subscriptions": [
                        {
                            "data_type": "bar",
                            "instrument_id": "BTCUSDT",
                            "interval": "1m",
                        }
                    ],
                }
            ).encode(),
            INVALID_INSTRUMENT,
        ),
        (
            json.dumps({"op": "stop", "session_id": "rpl-missing"}).encode(),
            SESSION_NOT_FOUND,
        ),
    ],
)
def test_zmq_control_errors_are_json_responses(
    tmp_path: Path, raw: bytes, expected_code: str
) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate(store)
    service = ReplayService(store)
    server = ReplayServer(service)
    context = zmq.Context()
    requester = context.socket(zmq.REQ)
    requester.setsockopt(zmq.LINGER, 0)
    requester.setsockopt(zmq.RCVTIMEO, 5000)
    try:
        server.start()
        requester.connect(server.control_endpoint)
        requester.send(raw)
        response = json.loads(requester.recv().decode("utf-8"))
        assert response["ok"] is False
        assert response["error"]["code"] == expected_code
        # The REP socket stayed in a valid state for the next request.
        assert _request(requester, {"op": "unknown"})["error"]["code"] == (
            INVALID_PARAMETER
        )
    finally:
        requester.close()
        server.stop()
        context.term()
