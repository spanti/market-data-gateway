"""Runtime configuration (TOML) and typed defaults."""

from __future__ import annotations

import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator

SUPPORTED_BUS_TRANSPORTS = frozenset({"inproc"})


class _ConfigModel(BaseModel):
    """Immutable config model; unknown keys are ignored for forward compatibility."""

    model_config = ConfigDict(frozen=True, extra="ignore")


class ServerConfig(_ConfigModel):
    host: str = "127.0.0.1"
    http_port: int = Field(default=8080, ge=0, le=65535)
    ws_path: str = "/ws"


class BusConfig(_ConfigModel):
    transport: str = "inproc"
    zmq_hwm: int = 10_000

    @field_validator("transport")
    @classmethod
    def _supported_transport(cls, value: str) -> str:
        if value not in SUPPORTED_BUS_TRANSPORTS:
            raise ValueError(
                f"unsupported bus transport in this walking skeleton: "
                f"{value!r} (supported: {sorted(SUPPORTED_BUS_TRANSPORTS)})"
            )
        return value


class StorageConfig(_ConfigModel):
    root: Path = Path("./data")
    schema_version: int = 1
    format_version: str = "2.6"
    compression: str = "zstd"
    row_group_size: int = 1_000_000
    flush_max_bytes: int = 67_108_864
    flush_max_age_s: int = 3_600


class RetentionConfig(_ConfigModel):
    enabled: bool = False
    bars_days: int = Field(default=0, ge=0)  # 0 = keep forever


class GatewayConfig(_ConfigModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    bus: BusConfig = Field(default_factory=BusConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    retention: RetentionConfig = Field(default_factory=RetentionConfig)


def load_config(path: Path) -> GatewayConfig:
    """Parse and validate a runtime config TOML file."""
    with Path(path).open("rb") as handle:
        raw = tomllib.load(handle)
    return GatewayConfig.model_validate(raw)
