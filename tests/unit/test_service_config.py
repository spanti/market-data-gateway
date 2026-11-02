from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import ValidationError

from mdgw.config import FeedConfig, GatewayConfig
from mdgw.ingest.ccxt_feed import build_feed_handler
from mdgw.ingest.composite import CompositeFeedHandler
from mdgw.ingest.fake import OneShotFakeFeedHandler


def test_service_config_defaults() -> None:
    service = GatewayConfig().service
    assert service.health_host == "127.0.0.1"
    assert service.collector_health_port == 8081
    assert service.replay_health_port == 8082
    assert service.log_level == "INFO"
    assert service.collector_required is False
    assert service.wait_for_api_ready is False
    assert service.api_ready_timeout_s == 30.0


def test_bus_multi_process_endpoint_defaults() -> None:
    bus = GatewayConfig().bus
    assert bus.collector_pub_bind == "tcp://127.0.0.1:5555"
    assert bus.collector_pub_connect == "tcp://127.0.0.1:5555"
    assert bus.api_pub_bind == "tcp://127.0.0.1:5556"
    assert bus.api_pub_connect == "tcp://127.0.0.1:5556"


@pytest.mark.parametrize(
    "field",
    ["collector_pub_bind", "collector_pub_connect", "api_pub_bind", "api_pub_connect"],
)
def test_bus_endpoints_reject_ipc(field: str) -> None:
    with pytest.raises(ValidationError, match="tcp://"):
        GatewayConfig.model_validate({"bus": {field: "ipc://mdgw"}})


@pytest.mark.parametrize(
    "value",
    ["tcp://127.0.0.1", "tcp://127.0.0.1:notaport", "tcp://127.0.0.1:70000"],
)
def test_bus_endpoints_reject_malformed_values(value: str) -> None:
    with pytest.raises(ValidationError):
        GatewayConfig.model_validate({"bus": {"api_pub_bind": value}})


def test_bus_endpoints_accept_docker_service_names() -> None:
    config = GatewayConfig.model_validate(
        {
            "bus": {
                "collector_pub_bind": "tcp://0.0.0.0:5555",
                "collector_pub_connect": "tcp://collector:5555",
                "api_pub_bind": "tcp://0.0.0.0:5556",
                "api_pub_connect": "tcp://api:5556",
            }
        }
    )
    assert config.bus.collector_pub_connect == "tcp://collector:5555"
    assert config.bus.api_pub_connect == "tcp://api:5556"


def test_fake_feed_config_defaults() -> None:
    feed = FeedConfig(
        name="fake_spot", kind="fake", venue="binance", symbols=("BTC/USDT",)
    )
    assert feed.fake_bars == 0
    assert feed.fake_start_ts is None
    assert feed.fake_price == "100"


def test_fake_feed_config_rejects_negative_bars() -> None:
    with pytest.raises(ValidationError):
        FeedConfig(
            name="fake_spot",
            kind="fake",
            venue="binance",
            symbols=("BTC/USDT",),
            fake_bars=-1,
        )


def test_build_feed_handler_fake_is_deterministic_and_one_shot() -> None:
    feed_config = FeedConfig(
        name="fake_spot",
        kind="fake",
        venue="binance",
        symbols=("BTC/USDT",),
        fake_bars=3,
        fake_start_ts="2024-01-01T00:00:00Z",
        fake_price="100",
    )

    handler = build_feed_handler(feed_config)
    assert isinstance(handler, OneShotFakeFeedHandler)
    assert handler.instrument_ids == ("binance:BTC/USDT",)
    assert handler.supports_history is False

    bars = list(handler.iter_bars())
    assert len(bars) == 3
    assert all(bar.instrument_id == "binance:BTC/USDT" for bar in bars)
    assert all(bar.close == 100 for bar in bars)
    assert [bar.open_ts for bar in bars] == [
        bars[0].open_ts,
        bars[0].open_ts + timedelta(minutes=1),
        bars[0].open_ts + timedelta(minutes=2),
    ]
    # A live collector poll must not re-ingest the fixed run forever.
    assert list(handler.iter_bars()) == []


def test_build_feed_handler_fake_combines_symbols() -> None:
    feed_config = FeedConfig(
        name="fake_spot",
        kind="fake",
        venue="binance",
        symbols=("BTC/USDT", "ETH/USDT"),
        fake_bars=2,
        fake_start_ts="2024-01-01T00:00:00Z",
    )

    handler = build_feed_handler(feed_config)
    assert isinstance(handler, CompositeFeedHandler)
    assert handler.instrument_ids == (
        "binance:BTC/USDT",
        "binance:ETH/USDT",
    )
    bars = list(handler.iter_bars())
    assert len(bars) == 4
    assert {bar.instrument_id for bar in bars} == {
        "binance:BTC/USDT",
        "binance:ETH/USDT",
    }
