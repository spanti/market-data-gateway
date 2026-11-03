"""CollectorService startup handshake and streaming ownership (no 30s waits)."""

from __future__ import annotations

import logging
import socket
import threading
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import zmq

from mdgw.config import GatewayConfig
from mdgw.core.bar import Bar
from mdgw.ingest.fake import FakeFeedHandler
from mdgw.service.collector import CollectorService
from mdgw.streaming import READY_TOPIC


def _free_endpoint() -> str:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"tcp://127.0.0.1:{sock.getsockname()[1]}"


def _config(
    *,
    wait_for_api_ready: bool,
    timeout_s: float = 0.05,
    api_pub_connect: str | None = None,
) -> GatewayConfig:
    return GatewayConfig.model_validate(
        {
            "service": {
                "wait_for_api_ready": wait_for_api_ready,
                "api_ready_timeout_s": timeout_s,
                "startup_delay_s": 0.0,
                "collector_health_port": 0,
            },
            "bus": {
                "collector_pub_bind": "tcp://127.0.0.1:0",
                "api_pub_connect": api_pub_connect or _free_endpoint(),
            },
        }
    )


def _service(tmp_path: Path, config: GatewayConfig, **kwargs) -> CollectorService:
    feed = FakeFeedHandler(instrument_id="binance:BTC/USDT", bars=())
    return CollectorService(
        config,
        data_root=tmp_path,
        feed_handler=feed,
        **kwargs,
    )


def _bar(
    *,
    instrument_id: str = "binance:BTC/USDT",
    open_ts: datetime | None = None,
) -> Bar:
    start = open_ts or datetime(2024, 1, 1, tzinfo=UTC)
    close = start + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=instrument_id,
        interval="1m",
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


def test_wait_for_api_timeout_is_bounded_and_logged(
    tmp_path: Path, caplog
) -> None:
    service = _service(tmp_path, _config(wait_for_api_ready=True))
    service.start()
    try:
        with caplog.at_level(
            logging.WARNING, logger="mdgw.service.collector"
        ):
            started = time.monotonic()
            assert service._wait_for_api_ready() is False
            elapsed = time.monotonic() - started
    finally:
        service.stop()

    assert elapsed >= 0.04
    assert elapsed < 5.0
    timeouts = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "collector.api_ready_timeout"
    ]
    assert timeouts
    assert all(
        record.timeout_s == pytest.approx(0.05) for record in timeouts
    )


def test_wait_for_api_returns_true_once_a_peer_ready_is_observed(
    tmp_path: Path,
) -> None:
    context = zmq.Context()
    publisher = context.socket(zmq.PUB)
    publisher.setsockopt(zmq.LINGER, 0)
    publisher.bind("tcp://127.0.0.1:0")
    endpoint = publisher.getsockopt_string(zmq.LAST_ENDPOINT)

    stop = threading.Event()

    def publish_ready() -> None:
        while not stop.is_set():
            publisher.send_multipart([READY_TOPIC.encode("utf-8"), b""])
            stop.wait(0.01)

    heartbeat = threading.Thread(target=publish_ready, daemon=True)
    heartbeat.start()
    service = _service(
        tmp_path,
        _config(
            wait_for_api_ready=True,
            timeout_s=5.0,
            api_pub_connect=endpoint,
        ),
    )
    try:
        service.start()
        started = time.monotonic()
        assert service._wait_for_api_ready() is True
        assert time.monotonic() - started < 4.0
    finally:
        service.stop()
        stop.set()
        heartbeat.join(timeout=2.0)
        publisher.close(linger=0)
        context.term()


def test_wait_for_api_stops_promptly_when_shutdown_is_requested(
    tmp_path: Path,
) -> None:
    service = _service(
        tmp_path, _config(wait_for_api_ready=True, timeout_s=30.0)
    )
    service.start()
    try:
        service._stop.set()
        started = time.monotonic()
        assert service._wait_for_api_ready() is False
        assert time.monotonic() - started < 1.0
    finally:
        service.stop()


def test_wait_for_api_defaults_to_config_and_can_be_overridden(
    tmp_path: Path,
) -> None:
    from_config = _service(tmp_path, _config(wait_for_api_ready=True))
    assert from_config._wait_for_api is True

    overridden = _service(
        tmp_path, _config(wait_for_api_ready=True), wait_for_api=False
    )
    assert overridden._wait_for_api is False
    started = time.monotonic()
    assert overridden._wait_for_api_ready() is True
    assert time.monotonic() - started < 1.0


def test_publish_endpoint_requires_start(tmp_path: Path) -> None:
    service = _service(tmp_path, _config(wait_for_api_ready=False))

    with pytest.raises(RuntimeError):
        _ = service.publish_endpoint


def test_publish_endpoint_is_the_resolved_streaming_endpoint(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path, _config(wait_for_api_ready=False))
    service.start()
    try:
        endpoint = service.publish_endpoint
    finally:
        service.stop()

    assert endpoint.startswith("tcp://127.0.0.1:")
    assert not endpoint.endswith(":0")


def test_start_publishes_the_ready_heartbeat(tmp_path: Path) -> None:
    service = _service(tmp_path, _config(wait_for_api_ready=False))
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.SUBSCRIBE, READY_TOPIC.encode("utf-8"))
    poller = zmq.Poller()
    poller.register(subscriber, zmq.POLLIN)
    try:
        service.start()
        subscriber.connect(service.publish_endpoint)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if dict(poller.poll(100)).get(subscriber) != zmq.POLLIN:
                continue
            frames = subscriber.recv_multipart()
            if frames[0].decode("utf-8") == READY_TOPIC:
                return
        pytest.fail("collector never published a READY heartbeat")
    finally:
        service.stop()
        subscriber.close(linger=0)
        context.term()


def test_one_publish_metric_per_published_bar_frame(tmp_path: Path) -> None:
    service = _service(tmp_path, _config(wait_for_api_ready=False))
    service.start()
    try:
        first = _bar(open_ts=datetime(2024, 1, 1, tzinfo=UTC))
        second = _bar(open_ts=datetime(2024, 1, 1, 0, 1, tzinfo=UTC))
        other = _bar(
            instrument_id="binance:ETH/USDT",
            open_ts=datetime(2024, 1, 1, tzinfo=UTC),
        )

        # Two (interval, instrument) groups -> two Bar frames on the wire.
        service._publish_bars([first, second, other])
        text = service.render_metrics()
        payload = service.status_payload()
    finally:
        service.stop()

    assert "mdgw_collector_zmq_publishes_total 2" in text
    assert payload["last_bar_ts"] == "2024-01-01T00:02:00.000000000Z"


def test_status_and_metrics_expose_the_buffered_writer(tmp_path: Path) -> None:
    service = _service(tmp_path, _config(wait_for_api_ready=False))

    payload = service.status_payload()
    assert payload["write_queue_depth"] == 0
    assert payload["write_dropped_bars"] == 0
    assert payload["write_lag_bars"] == 0
    assert payload["write_last_flush_ts"] is None
    assert payload["write_sink"]["mode"] == "buffered"
    assert payload["write_sink"]["queue_capacity"] == 10_000

    text = service.render_metrics()
    for line in (
        "mdgw_collector_write_queue_depth 0",
        "mdgw_collector_write_dropped_total 0",
        "mdgw_collector_write_lag_total 0",
        "mdgw_collector_last_flush_ts_seconds 0",
    ):
        assert line in text
