"""Runtime configuration (TOML) and typed defaults."""

from __future__ import annotations

import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from mdgw.core.schema import INTERVALS

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


class InstrumentsConfig(_ConfigModel):
    """Instrument Reference Data truth-source directory (`*.yaml` files)."""

    path: Path = Path("config/instruments")


class AggregationConfig(_ConfigModel):
    """Bar Aggregation settings: the base interval and produced parent intervals."""

    base_interval: str = "1m"
    intervals: tuple[str, ...] = ("1m", "5m", "15m", "1h", "1d")
    # Keep the newest period plus this many older parent periods in the buffer.
    retention_parent_periods: int = Field(default=2, ge=1)

    @field_validator("base_interval")
    @classmethod
    def _supported_base_interval(cls, value: str) -> str:
        if value not in INTERVALS:
            raise ValueError(f"unsupported base Bar Interval: {value!r}")
        if value != "1m":
            raise ValueError(
                "v1 Bar Aggregation only supports '1m' as the base interval"
            )
        return value

    @field_validator("intervals")
    @classmethod
    def _supported_intervals(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        unknown = [interval for interval in value if interval not in INTERVALS]
        if unknown:
            raise ValueError(f"unsupported Bar Interval(s): {unknown!r}")
        if len(set(value)) != len(value):
            raise ValueError("aggregation intervals must not contain duplicates")
        return value

    @model_validator(mode="after")
    def _base_interval_is_aggregated(self) -> "AggregationConfig":
        if self.base_interval not in self.intervals:
            raise ValueError(
                "aggregation intervals must contain the base interval "
                f"{self.base_interval!r}"
            )
        return self


class GatewayConfig(_ConfigModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    bus: BusConfig = Field(default_factory=BusConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    retention: RetentionConfig = Field(default_factory=RetentionConfig)
    aggregation: AggregationConfig = Field(default_factory=AggregationConfig)
    instruments: InstrumentsConfig = Field(default_factory=InstrumentsConfig)


def load_config(path: Path) -> GatewayConfig:
    """Parse and validate a runtime config TOML file."""
    with Path(path).open("rb") as handle:
        raw = tomllib.load(handle)
    return GatewayConfig.model_validate(raw)
