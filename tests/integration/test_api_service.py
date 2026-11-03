# ruff: file-ignore[implicit-namespace-package]
"""In-process integration test for the ApiService HTTP lifecycle.

The api can serve historical REST without a live collector: `collector_required`
defaults to false, so `/readyz` is 200 while `collector_ready` is false. This
exercises the api's public start/stop/base_url seam directly instead of spawning
a subprocess.
"""

from __future__ import annotations

import json
import socket
import threading
import time
import urllib.request
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
import zmq

from mdgw.config import load_config
from mdgw.core.bar import Bar
from mdgw.core.schema import bars_to_logical_table
from mdgw.service.api import ApiService
from mdgw.streaming import READY_TOPIC, table_to_ipc

PROJECT_ROOT = Path(__file__).resolve().parents[2]
HTTP_THREAD_NAME = "mdgw-api-http"
WAIT_TIMEOUT_S = 15.0
HOST = "127.0.0.1"
NOT_STARTED_MESSAGE = r"ApiService\.start\(\) must be called first"

COLLECTOR_STATUS = {"service": "collector", "ready": True}
BAR_TOPIC = "market.bar.1m.binance:BTC/USDT"
BAR = Bar(
    schema_version=1,
    data_type="bar",
    instrument_id="binance:BTC/USDT",
    interval="1m",
    open_ts=datetime(2026, 10, 30, tzinfo=UTC),
    close_ts=datetime(2026, 10, 30, 0, 1, tzinfo=UTC),
    event_ts=datetime(2026, 10, 30, 0, 1, tzinfo=UTC),
    ingest_ts=datetime(2026, 10, 30, 0, 1, tzinfo=UTC),
    feed="fake",
    seq=None,
    aggregation_source="external",
    open=Decimal(100),
    high=Decimal(101),
    low=Decimal(99),
    close=Decimal("100.5"),
    volume=Decimal(1),
    trade_count=1,
    revision=0,
)
BAR_PAYLOAD = table_to_ipc(bars_to_logical_table([BAR]))


def _config():
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(
        update={"http_port": 0, "host": HOST}
    )
    bus = config.bus.model_copy(
        update={"api_pub_bind": "tcp://127.0.0.1:0"}
    )
    return config.model_copy(update={"server": server, "bus": bus})


def _get_json(url: str) -> tuple[int, dict]:
    with urllib.request.urlopen(  # ruff: ignore[suspicious-url-open-usage]
        url, timeout=WAIT_TIMEOUT_S
    ) as response:
        return response.status, json.loads(response.read())


def _threads_named(name: str) -> list[threading.Thread]:
    return [thread for thread in threading.enumerate() if thread.name == name]


def test_api_service_serves_operational_endpoints_without_a_collector(
    tmp_path: Path,
) -> None:
    service = ApiService(_config(), data_root=tmp_path)
    try:
        service.start()
        port = service.http_port
        base_url = service.base_url

        assert port > 0
        assert base_url == f"http://{HOST}:{port}"

        health_status, health = _get_json(f"{base_url}/healthz")
        assert health_status == 200
        assert health["status"] == "ok"
        assert health["service"] == "api"

        ready_status, ready = _get_json(f"{base_url}/readyz")
        assert ready_status == 200
        assert ready["ready"] is True
        assert ready["collector_ready"] is False

        dataset_status, dataset = _get_json(f"{base_url}/api/v1/status")
        assert dataset_status == 200
        assert dataset["dataset"] == "bars"

        status_code, status = _get_json(f"{base_url}/status")
        assert status_code == 200
        assert status["service"] == "api"
        assert status["collector"]["ready"] is False
    finally:
        service.stop()

    assert _threads_named(HTTP_THREAD_NAME) == []
    # No lingering listener: the port is free for immediate reuse.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind((HOST, port))


def test_base_url_and_http_port_require_start(tmp_path: Path) -> None:
    service = ApiService(_config(), data_root=tmp_path)

    with pytest.raises(RuntimeError, match=NOT_STARTED_MESSAGE):
        _ = service.base_url
    with pytest.raises(RuntimeError, match=NOT_STARTED_MESSAGE):
        _ = service.http_port

    try:
        service.start()
        assert service.http_port > 0
    finally:
        service.stop()

    with pytest.raises(RuntimeError, match=NOT_STARTED_MESSAGE):
        _ = service.base_url
    with pytest.raises(RuntimeError, match=NOT_STARTED_MESSAGE):
        _ = service.http_port


def _wait_until(predicate, *, timeout: float = WAIT_TIMEOUT_S) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def _recv_matching(
    subscriber: zmq.Socket, predicate, *, timeout: float = WAIT_TIMEOUT_S
) -> list[bytes]:
    poller = zmq.Poller()
    poller.register(subscriber, zmq.POLLIN)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if dict(poller.poll(100)).get(subscriber) != zmq.POLLIN:
            continue
        frames = subscriber.recv_multipart()
        if predicate(frames):
            return frames
    msg = "expected frame did not arrive on the api PUB"
    raise AssertionError(msg)


def test_api_marks_ready_and_forwards_collector_frames(tmp_path: Path) -> None:
    """The api handshake follows the collector's READY and forwards its Bars.

    This pins the behavior the multicast handshake depends on: a collector
    that waits for the api must see READY, and external consumers must receive
    the collector's original Bar bytes from the api PUB.
    """
    context = zmq.Context()
    collector_pub = context.socket(zmq.PUB)
    collector_pub.setsockopt(zmq.LINGER, 0)
    collector_pub.bind("tcp://127.0.0.1:0")
    collector_endpoint = collector_pub.getsockopt_string(zmq.LAST_ENDPOINT)

    config = _config()
    bus = config.bus.model_copy(
        update={
            "api_pub_bind": "tcp://127.0.0.1:0",
            "collector_pub_connect": collector_endpoint,
        }
    )
    service_config = config.service.model_copy(
        update={"collector_required": True}
    )
    service = ApiService(
        config.model_copy(update={"bus": bus, "service": service_config}),
        data_root=tmp_path,
    )

    external = context.socket(zmq.SUB)
    external.setsockopt(zmq.LINGER, 0)
    external.setsockopt(zmq.SUBSCRIBE, b"")
    stop_publishing = threading.Event()

    def publish_collector_traffic() -> None:
        status_payload = json.dumps(COLLECTOR_STATUS).encode("utf-8")
        while not stop_publishing.is_set():
            collector_pub.send_multipart([READY_TOPIC.encode("utf-8"), b""])
            collector_pub.send_multipart([b"_internal.status", status_payload])
            collector_pub.send_multipart([BAR_TOPIC.encode("utf-8"), BAR_PAYLOAD])
            stop_publishing.wait(0.02)

    publisher = threading.Thread(
        target=publish_collector_traffic,
        name="test-collector-publisher",
        daemon=True,
    )
    try:
        service.start()
        external.connect(service.publish_endpoint)

        # collector_required: not ready until the collector handshake lands.
        assert service.ready_payload()["ready"] is False

        publisher.start()

        assert _wait_until(
            lambda: service.ready_payload()["ready"] is True
        ), "api never became ready after the collector handshake"
        collector = service.status_payload()["collector"]
        assert collector["ready"] is True
        assert collector["status"] == COLLECTOR_STATUS

        ready_frame = _recv_matching(
            external,
            lambda frames: frames[0] == READY_TOPIC.encode("utf-8"),
        )
        assert ready_frame == [READY_TOPIC.encode("utf-8"), b""]

        topic_frame, bar_frame = _recv_matching(
            external,
            lambda frames: frames[0] == BAR_TOPIC.encode("utf-8"),
        )
        assert topic_frame == BAR_TOPIC.encode("utf-8")
        assert bar_frame == BAR_PAYLOAD
        assert pa.ipc.open_stream(pa.BufferReader(bar_frame)).read_all().num_rows == 1

        assert _wait_until(
            lambda: service.status_payload()["messages_received"] >= 1
        )
        assert service.status_payload()["messages_forwarded"] >= 1
        metrics = service.render_metrics()
        assert "mdgw_api_messages_forwarded_total" in metrics
        assert "mdgw_api_messages_received_total" in metrics
    finally:
        stop_publishing.set()
        publisher.join(timeout=5)
        service.stop()
        external.close()
        collector_pub.close()
        context.term()
