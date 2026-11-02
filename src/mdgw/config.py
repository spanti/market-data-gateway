"""Runtime configuration (TOML) and typed defaults."""

from __future__ import annotations

import tomllib
from pathlib import Path

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

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
    # External Streaming Interface PUB endpoint. Port 0 picks an ephemeral
    # port; the resolved endpoint is exposed as `runtime.stream_endpoint`.
    stream_bind_host: str = "127.0.0.1"
    stream_pub_port: int = Field(default=0, ge=0, le=65_535)

    @field_validator("transport")
    @classmethod
    def _supported_transport(cls, value: str) -> str:
        if value not in SUPPORTED_BUS_TRANSPORTS:
            raise ValueError(
                f"unsupported bus transport in this walking skeleton: "
                f"{value!r} (supported: {sorted(SUPPORTED_BUS_TRANSPORTS)})"
            )
        return value


class StreamingConfig(_ConfigModel):
    """WebSocket fan-out limits for the Streaming Interface."""

    # Per-session bounded queue. A full queue drops the oldest message and is
    # reported to the client via `lag_notice`; the connection stays open.
    queue_size: int = Field(default=1000, ge=1)
    lag_notice_interval_ms: int = Field(default=1000, ge=0)



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


class FeedConfig(_ConfigModel):
    """One Feed Handler's runtime settings (DESIGN §6 `[[feeds]]`)."""

    name: str
    kind: str = "ccxt"
    venue: str
    market_type: str = "spot"
    enable_rate_limit: bool = True
    symbols: tuple[str, ...]
    data_types: tuple[str, ...] = ("bar",)
    backfill_hours: float = Field(default=24.0, ge=0)
    stale_after_s: float = Field(default=120.0, ge=0)
    # Retained for the collector batching work in a later ticket; the ccxt
    # Feed Handler does not consume these yet.
    batch_window_ms: int = Field(default=100, ge=0)
    batch_max_events: int = Field(default=500, ge=1)

    @field_validator("name", "venue", "market_type")
    @classmethod
    def _non_empty(cls, value: str, info: ValidationInfo) -> str:
        if not value.strip():
            raise ValueError(f"{info.field_name} must be a non-empty string")
        return value.strip()

    @field_validator("kind")
    @classmethod
    def _supported_kind(cls, value: str) -> str:
        if value != "ccxt":
            raise ValueError(f"unsupported Feed Handler kind: {value!r}")
        return value

    @field_validator("symbols")
    @classmethod
    def _symbols(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("symbols must contain at least one symbol")
        cleaned = tuple(symbol.strip() for symbol in value)
        if any(not symbol for symbol in cleaned):
            raise ValueError("symbols must not contain empty values")
        if len(set(cleaned)) != len(cleaned):
            raise ValueError("symbols must not contain duplicates")
        return cleaned

    @field_validator("data_types")
    @classmethod
    def _data_types(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("data_types must contain at least one Data Type")
        unsupported = [data_type for data_type in value if data_type != "bar"]
        if unsupported:
            raise ValueError(
                f"unsupported data_types for v1: {unsupported!r} (supported: ['bar'])"
            )
        return value

    @property
    def instrument_ids(self) -> tuple[str, ...]:
        return tuple(f"{self.venue}:{symbol}" for symbol in self.symbols)


class CollectorConfig(_ConfigModel):
    """Backfill window and freshness thresholds for the collector process."""

    backfill_hours: float = Field(default=24.0, ge=0)
    stale_after_s: float = Field(default=120.0, ge=0)
    state_db_name: str = "state.db"

    @field_validator("state_db_name")
    @classmethod
    def _non_empty_state_db_name(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("state_db_name must be a non-empty file name")
        return value


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


class ReplayConfig(_ConfigModel):
    """Replay Service Role endpoints and concurrency limits (DESIGN §6)."""

    control_bind_host: str = "127.0.0.1"
    control_port: int = Field(default=0, ge=0, le=65535)
    publish_bind_host: str = "127.0.0.1"
    publish_port: int = Field(default=0, ge=0, le=65535)
    max_sessions: int = Field(default=4, ge=1)
    heartbeat_s: float = Field(default=1.0, gt=0)


class GatewayConfig(_ConfigModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    bus: BusConfig = Field(default_factory=BusConfig)
    streaming: StreamingConfig = Field(default_factory=StreamingConfig)
    replay: ReplayConfig = Field(default_factory=ReplayConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    retention: RetentionConfig = Field(default_factory=RetentionConfig)
    aggregation: AggregationConfig = Field(default_factory=AggregationConfig)
    collector: CollectorConfig = Field(default_factory=CollectorConfig)
    instruments: InstrumentsConfig = Field(default_factory=InstrumentsConfig)
    feeds: tuple[FeedConfig, ...] = ()


def load_config(path: Path) -> GatewayConfig:
    """Parse and validate a runtime config TOML file."""
    with Path(path).open("rb") as handle:
        raw = tomllib.load(handle)
    return GatewayConfig.model_validate(raw)
