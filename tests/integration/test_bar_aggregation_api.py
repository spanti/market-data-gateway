from __future__ import annotations

import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from mdgw.config import load_config
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.runtime import GatewayRuntime

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc


def _config():
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    return config.model_copy(update={"server": server})


def _bar_input(minute: int, *, instrument_id: str = "binance:BTC/USDT") -> BarInput:
    return BarInput(
        open_ts=datetime(2024, 1, 1, 0, minute, tzinfo=UTC),
        open=str(100 + minute),
        high=str(110 + minute),
        low=str(90 + minute),
        close=str(105 + minute),
        volume="2",
        trade_count=10,
    )


def _runtime(
    tmp_path: Path,
    bars: list[BarInput],
    *,
    instrument_id: str = "binance:BTC/USDT",
) -> GatewayRuntime:
    feed = FakeFeedHandler(instrument_id=instrument_id, bars=bars)
    return GatewayRuntime(feed_handler=feed, config=_config(), data_root=tmp_path)


def _get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=15) as response:
        return json.load(response)


def test_five_closed_one_minute_bars_are_queryable_as_one_five_minute_bar(
    tmp_path: Path,
) -> None:
    with _runtime(tmp_path, [_bar_input(minute) for minute in range(5)]) as runtime:
        runtime.start()
        ingested = runtime.ingest()
        assert ingested == 5
        assert runtime.received_bar_count == 6

        five_minute = _get(
            f"{runtime.base_url}/api/v1/bars?instrument=binance:BTC/USDT&interval=5m"
        )["bars"]
        one_minute = _get(
            f"{runtime.base_url}/api/v1/bars?instrument=binance:BTC/USDT&interval=1m"
        )["bars"]

    assert len(one_minute) == 5
    assert len(five_minute) == 1

    envelope = five_minute[0]
    assert envelope["instrument_id"] == "binance:BTC/USDT"
    assert envelope["event_ts"] == "2024-01-01T00:05:00.000000000Z"
    assert envelope["feed"] == "fake"
    assert envelope["seq"] is None

    payload = envelope["payload"]
    assert payload["interval"] == "5m"
    assert payload["open_ts"] == "2024-01-01T00:00:00.000000000Z"
    assert payload["close_ts"] == "2024-01-01T00:05:00.000000000Z"
    assert payload["aggregation_source"] == "internal"
    assert payload["open"] == "100.0"
    assert payload["high"] == "114.0"
    assert payload["low"] == "90.0"
    assert payload["close"] == "109.0"
    assert payload["volume"] == "10.0"
    assert payload["trade_count"] == 50
    assert payload["revision"] == 0


def test_incomplete_period_is_not_visible_through_the_api(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(minute) for minute in range(4)]) as runtime:
        runtime.start()
        assert runtime.ingest() == 4
        assert runtime.received_bar_count == 4
        five_minute = _get(
            f"{runtime.base_url}/api/v1/bars?instrument=binance:BTC/USDT&interval=5m"
        )["bars"]

    assert five_minute == []


def test_gap_prevents_the_parent_from_appearing(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(minute) for minute in (0, 1, 2, 4, 5)]) as runtime:
        runtime.start()
        assert runtime.ingest() == 5
        assert runtime.received_bar_count == 5
        five_minute = _get(
            f"{runtime.base_url}/api/v1/bars?instrument=binance:BTC/USDT&interval=5m"
        )["bars"]

    assert five_minute == []


def test_configured_intervals_are_aggregated_together(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(minute) for minute in range(15)]) as runtime:
        runtime.start()
        assert runtime.ingest() == 15
        assert runtime.received_bar_count == 19  # 15 x 1m + 3 x 5m + 1 x 15m

        base = f"{runtime.base_url}/api/v1/bars?instrument=binance:BTC/USDT"
        five_minute = _get(f"{base}&interval=5m")["bars"]
        fifteen_minute = _get(f"{base}&interval=15m")["bars"]
        hourly = _get(f"{base}&interval=1h")["bars"]

    assert [bar["payload"]["open_ts"] for bar in five_minute] == [
        "2024-01-01T00:00:00.000000000Z",
        "2024-01-01T00:05:00.000000000Z",
        "2024-01-01T00:10:00.000000000Z",
    ]
    assert len(fifteen_minute) == 1
    assert fifteen_minute[0]["payload"]["volume"] == "30.0"
    assert fifteen_minute[0]["payload"]["trade_count"] == 150
    assert hourly == []
