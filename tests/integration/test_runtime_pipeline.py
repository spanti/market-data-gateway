from __future__ import annotations

import json
import threading
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from mdgw.config import load_config
from mdgw.core.bar import Bar
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.replay.session import ReplaySessionSpec
from mdgw.runtime import GatewayRuntime
from mdgw.streaming import bar_topic

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

    def _fail_start_http(*_args, **_kwargs) -> None:
        msg = "boom"
        raise RuntimeError(msg)

    monkeypatch.setattr(runtime._http, "start", _fail_start_http)

    with pytest.raises(RuntimeError, match="boom"):
        runtime.start()

    assert feed.closed is True
    assert runtime._started is False
    assert runtime._hwm is None
    assert runtime._bus is None


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
    bars: list[Bar] = []
    events: list[dict[str, Any]] = []
    lock = threading.Lock()

    def _publish_bar(bar: Bar) -> None:
        with lock:
            bars.append(bar)

    def _publish_event(event: dict[str, Any]) -> None:
        with lock:
            events.append(event)

    with GatewayRuntime(
        feed_handler=feed, config=_config(), data_root=tmp_path
    ) as runtime:
        runtime.start()
        runtime.ingest()
        assert runtime.received_bar_count == 3

        session_id = runtime.create_replay_session(
            _replay_spec(),
            publish_bar=_publish_bar,
            publish_event=_publish_event,
        )
        assert runtime.replay_service.wait_session(session_id, timeout=10)

    with lock:
        collected_bars = list(bars)
        collected_events = list(events)

    assert [
        bar_topic(bar, namespace=f"replay.{session_id}")
        for bar in collected_bars
    ] == [f"replay.{session_id}.market.bar.1m.binance:BTC/USDT"] * 3

    start = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert [bar.close_ts for bar in collected_bars] == [
        start + timedelta(minutes=1),
        start + timedelta(minutes=2),
        start + timedelta(minutes=3),
    ]

    end = collected_events[-1]
    assert end["session_id"] == session_id
    assert end["reason"] == "exhausted"
    assert end["bars_sent"] == 3


def test_runtime_stop_closes_active_replay_sessions(tmp_path: Path) -> None:
    feed = FakeFeedHandler(
        instrument_id="binance:BTC/USDT", bars=_three_closed_bars()
    )
    events: list[dict[str, Any]] = []
    lock = threading.Lock()

    def _publish_event(event: dict[str, Any]) -> None:
        with lock:
            events.append(event)

    runtime = GatewayRuntime(
        feed_handler=feed, config=_config(), data_root=tmp_path
    )
    runtime.start()
    try:
        runtime.ingest()
        # A step Session stays alive until it receives credits.
        session_id = runtime.create_replay_session(
            _replay_spec(replay_mode="step"), publish_event=_publish_event
        )
        service = runtime.replay_service
    finally:
        runtime.stop()

    assert service.session_status(session_id) == "stopped"
    assert runtime._replay_service is None
    with lock:
        end = events[-1]
    assert end["session_id"] == session_id
    assert end["reason"] == "stopped"
