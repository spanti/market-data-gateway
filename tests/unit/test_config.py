from __future__ import annotations

import tomllib
from pathlib import Path

import pytest
from pydantic import ValidationError

from mdgw.config import FeedConfig, GatewayConfig, load_config

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_load_config_reads_repository_toml() -> None:
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    assert config.server.http_port == 8080
    assert config.storage.root == Path("./data")
    assert config.bus.transport == "tcp"


def test_repository_toml_declares_aggregation() -> None:
    with (PROJECT_ROOT / "config" / "gateway.toml").open("rb") as handle:
        raw = tomllib.load(handle)
    assert raw["aggregation"]["base_interval"] == "1m"
    assert raw["aggregation"]["intervals"] == ["1m", "5m", "15m", "1h", "1d"]
    assert raw["aggregation"]["retention_parent_periods"] == 2


def test_repository_toml_declares_instruments_path() -> None:
    with (PROJECT_ROOT / "config" / "gateway.toml").open("rb") as handle:
        raw = tomllib.load(handle)
    assert raw["instruments"]["path"] == "config/instruments"

    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    assert config.instruments.path == Path("config/instruments")


def test_instruments_config_defaults() -> None:
    assert GatewayConfig().instruments.path == Path("config/instruments")


def test_aggregation_config_defaults() -> None:
    aggregation = GatewayConfig().aggregation
    assert aggregation.base_interval == "1m"
    assert aggregation.intervals == ("1m", "5m", "15m", "1h", "1d")
    assert aggregation.retention_parent_periods == 2


def test_config_is_immutable() -> None:
    config = GatewayConfig()
    with pytest.raises(ValidationError):
        config.server.host = "0.0.0.0"


def test_unknown_keys_are_ignored() -> None:
    config = GatewayConfig.model_validate({"future": {"answer": 42}})
    assert config == GatewayConfig()


def test_negative_retention_days_rejected() -> None:
    with pytest.raises(ValidationError):
        GatewayConfig.model_validate({"retention": {"bars_days": -1}})


def test_unsupported_transport_rejected() -> None:
    with pytest.raises(ValidationError, match="unsupported bus transport"):
        GatewayConfig.model_validate({"bus": {"transport": "ipc"}})


def test_tcp_transport_accepted() -> None:
    config = GatewayConfig.model_validate({"bus": {"transport": "tcp"}})
    assert config.bus.transport == "tcp"


def test_aggregation_rejects_unknown_interval() -> None:
    with pytest.raises(ValidationError, match="unsupported Bar Interval"):
        GatewayConfig.model_validate(
            {"aggregation": {"intervals": ["1m", "5m", "2m"]}}
        )


def test_aggregation_rejects_duplicate_intervals() -> None:
    with pytest.raises(ValidationError, match="duplicates"):
        GatewayConfig.model_validate(
            {"aggregation": {"intervals": ["1m", "5m", "5m"]}}
        )


def test_aggregation_requires_base_interval_in_intervals() -> None:
    with pytest.raises(ValidationError, match="contain the base interval"):
        GatewayConfig.model_validate(
            {"aggregation": {"intervals": ["5m", "15m"]}}
        )


def test_aggregation_rejects_unsupported_base_interval() -> None:
    with pytest.raises(ValidationError, match="unsupported base Bar Interval"):
        GatewayConfig.model_validate({"aggregation": {"base_interval": "2m"}})


def test_aggregation_rejects_non_positive_retention_window() -> None:
    with pytest.raises(ValidationError):
        GatewayConfig.model_validate(
            {"aggregation": {"retention_parent_periods": 0}}
        )


def test_aggregation_accepts_positive_retention_window() -> None:
    config = GatewayConfig.model_validate(
        {"aggregation": {"retention_parent_periods": 1}}
    )
    assert config.aggregation.retention_parent_periods == 1


def test_collector_config_defaults() -> None:
    collector = GatewayConfig().collector
    assert collector.backfill_hours == 24
    assert collector.stale_after_s == 120
    assert collector.state_db_name == "state.db"


def test_repository_toml_declares_collector() -> None:
    with (PROJECT_ROOT / "config" / "gateway.toml").open("rb") as handle:
        raw = tomllib.load(handle)
    assert raw["collector"]["backfill_hours"] == 24
    assert raw["collector"]["stale_after_s"] == 120
    assert raw["collector"]["state_db_name"] == "state.db"


def test_collector_rejects_negative_values() -> None:
    with pytest.raises(ValidationError):
        GatewayConfig.model_validate({"collector": {"backfill_hours": -1}})
    with pytest.raises(ValidationError):
        GatewayConfig.model_validate({"collector": {"stale_after_s": -1}})


def test_collector_rejects_empty_state_db_name() -> None:
    with pytest.raises(ValidationError):
        GatewayConfig.model_validate({"collector": {"state_db_name": ""}})


def test_feeds_default_to_empty() -> None:
    assert GatewayConfig().feeds == ()


def test_feed_config_defaults() -> None:
    feed = FeedConfig(name="binance_spot", venue="binance", symbols=("BTC/USDT",))

    assert feed.kind == "ccxt"
    assert feed.market_type == "spot"
    assert feed.enable_rate_limit is True
    assert feed.data_types == ("bar",)
    assert feed.backfill_hours == 24
    assert feed.stale_after_s == 120
    assert feed.batch_window_ms == 100
    assert feed.batch_max_events == 500


def test_repository_toml_declares_ccxt_feed() -> None:
    with (PROJECT_ROOT / "config" / "gateway.toml").open("rb") as handle:
        raw = tomllib.load(handle)

    (feed,) = raw["feeds"]
    assert feed["name"] == "binance_spot"
    assert feed["kind"] == "ccxt"
    assert feed["venue"] == "binance"
    assert feed["market_type"] == "spot"
    assert feed["enable_rate_limit"] is True
    assert feed["data_types"] == ["bar"]
    assert feed["symbols"] == ["BTC/USDT", "ETH/USDT"]

    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    assert config.feeds[0].symbols == ("BTC/USDT", "ETH/USDT")


def test_feed_config_rejects_unknown_kind() -> None:
    with pytest.raises(ValidationError, match="kind"):
        FeedConfig(name="feed", kind="websocket", venue="binance", symbols=("BTC/USDT",))


def test_feed_config_rejects_empty_name_or_venue() -> None:
    with pytest.raises(ValidationError):
        FeedConfig(name="", venue="binance", symbols=("BTC/USDT",))
    with pytest.raises(ValidationError):
        FeedConfig(name="feed", venue="", symbols=("BTC/USDT",))


def test_feed_config_rejects_empty_symbols() -> None:
    with pytest.raises(ValidationError, match="symbols"):
        FeedConfig(name="feed", venue="binance", symbols=())


def test_feed_config_rejects_duplicate_symbols() -> None:
    with pytest.raises(ValidationError, match="duplicates"):
        FeedConfig(
            name="feed",
            venue="binance",
            symbols=("BTC/USDT", "BTC/USDT"),
        )


def test_feed_config_rejects_unsupported_data_types() -> None:
    with pytest.raises(ValidationError, match="data_types"):
        FeedConfig(
            name="feed",
            venue="binance",
            symbols=("BTC/USDT",),
            data_types=("trade",),
        )


def test_feed_config_rejects_negative_batch_values() -> None:
    with pytest.raises(ValidationError):
        FeedConfig(
            name="feed",
            venue="binance",
            symbols=("BTC/USDT",),
            batch_window_ms=-1,
        )
    with pytest.raises(ValidationError):
        FeedConfig(
            name="feed",
            venue="binance",
            symbols=("BTC/USDT",),
            batch_max_events=0,
        )


def test_replay_config_defaults() -> None:
    replay = GatewayConfig().replay
    assert replay.control_bind_host == "127.0.0.1"
    assert replay.control_port == 0
    assert replay.publish_bind_host == "127.0.0.1"
    assert replay.publish_port == 0
    assert replay.max_sessions == 4
    assert replay.heartbeat_s == 1.0


def test_repository_toml_declares_replay() -> None:
    with (PROJECT_ROOT / "config" / "gateway.toml").open("rb") as handle:
        raw = tomllib.load(handle)

    assert raw["replay"]["control_bind_host"] == "127.0.0.1"
    assert raw["replay"]["control_port"] == 0
    assert raw["replay"]["publish_bind_host"] == "127.0.0.1"
    assert raw["replay"]["publish_port"] == 0
    assert raw["replay"]["max_sessions"] == 4
    assert raw["replay"]["heartbeat_s"] == 1.0

    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    assert config.replay.max_sessions == 4


@pytest.mark.parametrize(
    "payload",
    [
        {"replay": {"control_port": 70_000}},
        {"replay": {"publish_port": -1}},
        {"replay": {"max_sessions": 0}},
        {"replay": {"heartbeat_s": 0}},
    ],
)
def test_replay_config_rejects_invalid_values(payload: dict) -> None:
    with pytest.raises(ValidationError):
        GatewayConfig.model_validate(payload)

