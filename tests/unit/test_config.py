from __future__ import annotations

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
