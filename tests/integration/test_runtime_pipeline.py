from __future__ import annotations

import json
import threading
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from mdgw.bus.zmq_bus import ipc_to_table
from mdgw.config import load_config
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.replay.session import ReplaySessionSpec
from mdgw.runtime import GatewayRuntime

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class _ClosingFeedHandler(FakeFeedHandler):
    """Fake Feed Handler that records whether `close()` was called."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _config():
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    return config.model_copy(update={"server": server})


def test_closed_fake_bar_roundtrips_to_rest(tmp_path: Path) -> None:
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    config = config.model_copy(update={"server": server})
    feed = FakeFeedHandler(
        instrument_id="binance:BTC/USDT",
        bars=[
            BarInput(
                open_ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
                open="65000.1",
                high="65010.0",
                low="64990.0",
                close="65005.5",
                volume="12.34",
                trade_count=128,
            )
        ],
    )

    with GatewayRuntime(feed_handler=feed, config=config, data_root=tmp_path) as runtime:
        runtime.start()
        runtime.ingest()
        assert runtime.received_bar_count == 1

        with urllib.request.urlopen(
            f"{runtime.base_url}/api/v1/bars?instrument=binance:BTC/USDT&interval=1m"
        ) as response:
            body = json.load(response)

    assert len(body["bars"]) == 1
    envelope = body["bars"][0]
    assert envelope["schema_version"] == 1
    assert envelope["data_type"] == "bar"
    assert envelope["instrument_id"] == "binance:BTC/USDT"
    assert envelope["event_ts"] == "2024-01-01T00:01:00.000000000Z"
    assert envelope["feed"] == "fake"
    assert envelope["seq"] is None

    payload = envelope["payload"]
    assert payload["interval"] == "1m"
    assert payload["open_ts"] == "2024-01-01T00:00:00.000000000Z"
    assert payload["close_ts"] == "2024-01-01T00:01:00.000000000Z"
    assert payload["aggregation_source"] == "external"
    assert payload["open"] == "65000.1"
    assert payload["high"] == "65010.0"
    assert payload["low"] == "64990.0"
    assert payload["close"] == "65005.5"
    assert Decimal(payload["volume"]) == Decimal("12.34")
    assert payload["trade_count"] == 128
    assert payload["revision"] == 0


def test_runtime_stop_closes_the_feed_handler(tmp_path: Path) -> None:
    feed = _ClosingFeedHandler(instrument_id="binance:BTC/USDT", bars=())
    runtime = GatewayRuntime(
        feed_handler=feed, config=_config(), data_root=tmp_path
    )

    runtime.start()
    runtime.stop()

    assert feed.closed is True


def test_runtime_start_failure_closes_the_feed_handler(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    feed = _ClosingFeedHandler(instrument_id="binance:BTC/USDT", bars=())
    runtime = GatewayRuntime(
        feed_handler=feed, config=_config(), data_root=tmp_path
    )

    def _fail_start_http() -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(runtime, "_start_http", _fail_start_http)

    with pytest.raises(RuntimeError, match="boom"):
        runtime.start()

    assert feed.closed is True
    assert runtime._started is False


# -- replay seam -----------------------------------------------------------


def _three_closed_bars() -> list[BarInput]:
    start = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    return [
        BarInput(
            open_ts=start + timedelta(minutes=offset),
            open="100",
            high="101",
            low="99",
            close="100.5",
            volume="1",
            trade_count=1,
        )
        for offset in range(3)
    ]


def _replay_spec(*, replay_mode: str = "asap") -> ReplaySessionSpec:
    return ReplaySessionSpec.from_mapping(
        {
            "op": "create_session",
            "subscriptions": [
                {
                    "data_type": "bar",
                    "instrument_id": "binance:BTC/USDT",
                    "interval": "1m",
                }
            ],
            "start_ts": "2024-01-01T00:00:00Z",
            "end_ts": "2024-01-01T00:10:00Z",
            "replay_mode": replay_mode,
        }
    )


def test_runtime_replay_seam_streams_collected_bars_in_close_order(
    tmp_path: Path,
) -> None:
    feed = FakeFeedHandler(
        instrument_id="binance:BTC/USDT", bars=_three_closed_bars()
    )
    events: list[tuple[str, bytes]] = []
    lock = threading.Lock()

    def _publish(topic: str, payload: bytes) -> None:
        with lock:
            events.append((topic, payload))

    with GatewayRuntime(
        feed_handler=feed, config=_config(), data_root=tmp_path
    ) as runtime:
        runtime.start()
        runtime.ingest()
        assert runtime.received_bar_count == 3

        session_id = runtime.create_replay_session(
            _replay_spec(), publish=_publish
        )
        assert runtime.replay_service.wait_session(session_id, timeout=10)

    with lock:
        collected = list(events)

    market = [
        (topic, payload)
        for topic, payload in collected
        if topic.startswith(f"replay.{session_id}.market.")
    ]
    assert [topic for topic, _ in market] == [
        f"replay.{session_id}.market.bar.1m.binance:BTC/USDT"
    ] * 3

    close_ts = [
        ipc_to_table(payload).column("close_ts").to_pylist()[0]
        for _, payload in market
    ]
    start = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert close_ts == [
        start + timedelta(minutes=1),
        start + timedelta(minutes=2),
        start + timedelta(minutes=3),
    ]

    control_topic, control_payload = collected[-1]
    assert control_topic == f"replay.{session_id}.control"
    end = json.loads(control_payload)
    assert end["reason"] == "exhausted"
    assert end["bars_sent"] == 3


def test_runtime_stop_closes_active_replay_sessions(tmp_path: Path) -> None:
    feed = FakeFeedHandler(
        instrument_id="binance:BTC/USDT", bars=_three_closed_bars()
    )
    events: list[tuple[str, bytes]] = []
    lock = threading.Lock()

    def _publish(topic: str, payload: bytes) -> None:
        with lock:
            events.append((topic, payload))

    runtime = GatewayRuntime(
        feed_handler=feed, config=_config(), data_root=tmp_path
    )
    runtime.start()
    try:
        runtime.ingest()
        # A step Session stays alive until it receives credits.
        session_id = runtime.create_replay_session(
            _replay_spec(replay_mode="step"), publish=_publish
        )
        service = runtime.replay_service
    finally:
        runtime.stop()

    assert service.session_status(session_id) == "stopped"
    assert runtime._replay_service is None
    with lock:
        control_topic, control_payload = events[-1]
    assert control_topic == f"replay.{session_id}.control"
    assert json.loads(control_payload)["reason"] == "stopped"
