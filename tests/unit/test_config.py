from __future__ import annotations

import tomllib
from pathlib import Path

import pytest
from pydantic import ValidationError

from mdgw.config import GatewayConfig, load_config

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_load_config_reads_repository_toml() -> None:
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    assert config.server.http_port == 8080
    assert config.storage.root == Path("./data")
    assert config.bus.transport == "inproc"


def test_repository_toml_declares_aggregation() -> None:
    with (PROJECT_ROOT / "config" / "gateway.toml").open("rb") as handle:
        raw = tomllib.load(handle)
    assert raw["aggregation"]["base_interval"] == "1m"
    assert raw["aggregation"]["intervals"] == ["1m", "5m", "15m", "1h", "1d"]
    assert raw["aggregation"]["retention_parent_periods"] == 2


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
        GatewayConfig.model_validate({"bus": {"transport": "tcp"}})


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
