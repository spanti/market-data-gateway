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

# `bus.transport` only selects the single-process `GatewayRuntime` internal
# transport. Multi-process Service Roles always use the explicit `tcp://`
# endpoints below, because Windows pyzmq builds cannot bind `ipc://`.
SUPPORTED_BUS_TRANSPORTS = frozenset({"inproc", "tcp"})
SUPPORTED_FEED_KINDS = frozenset({"ccxt", "fake"})


class _ConfigModel(BaseModel):
    """Immutable config model; unknown keys are ignored for forward compatibility."""

    model_config = ConfigDict(frozen=True, extra="ignore")


class ServerConfig(_ConfigModel):
    host: str = "127.0.0.1"
    http_port: int = Field(default=8080, ge=0, le=65535)
    ws_path: str = "/ws"


def _tcp_endpoint(value: str) -> str:
    """Validate one multi-process ZeroMQ endpoint.

    Windows pyzmq wheels disable `ipc://`, so every cross-process endpoint is
    deliberately restricted to TCP.
    """
    value = value.strip()
    if not value.startswith("tcp://"):
        raise ValueError(
            f"bus endpoint must use tcp:// (ipc:// is not supported): {value!r}"
        )
    host, separator, port = value[len("tcp://") :].rpartition(":")
    if not separator or not host:
        raise ValueError(f"bus endpoint must be tcp://HOST:PORT: {value!r}")
    if not port.isdigit() or not 0 <= int(port) <= 65_535:
        raise ValueError(f"bus endpoint has an invalid port: {value!r}")
    return value


class BusConfig(_ConfigModel):
    transport: str = "inproc"
    zmq_hwm: int = 10_000
    # External Streaming Interface PUB endpoint. Port 0 picks an ephemeral
    # port; the resolved endpoint is exposed as `runtime.stream_endpoint`.
    stream_bind_host: str = "127.0.0.1"
    stream_pub_port: int = Field(default=0, ge=0, le=65_535)
    # Multi-process Service Role endpoints. The collector publishes market
    # Bars here; the api connects to it and re-publishes on its own PUB so
    # external consumers never depend on the collector's address.
    collector_pub_bind: str = "tcp://127.0.0.1:5555"
    collector_pub_connect: str = "tcp://127.0.0.1:5555"
    api_pub_bind: str = "tcp://127.0.0.1:5556"
    # The collector connects here to observe the api PUB handshake. It only
    # blocks ingest when `[service].wait_for_api_ready` (or `--wait-for-api`)
    # is enabled.
    api_pub_connect: str = "tcp://127.0.0.1:5556"

    @field_validator("transport")
    @classmethod
    def _supported_transport(cls, value: str) -> str:
        if value not in SUPPORTED_BUS_TRANSPORTS:
            raise ValueError(
                f"unsupported bus transport: {value!r} "
                f"(supported: {sorted(SUPPORTED_BUS_TRANSPORTS)})"
            )
        return value

    @field_validator(
        "collector_pub_bind",
        "collector_pub_connect",
        "api_pub_bind",
        "api_pub_connect",
    )
    @classmethod
    def _tcp_bus_endpoint(cls, value: str) -> str:
        return _tcp_endpoint(value)


class ServiceConfig(_ConfigModel):
    """Per-Service-Role observability and supervisor settings."""

    health_host: str = "127.0.0.1"
    collector_health_port: int = Field(default=8081, ge=0, le=65_535)
    replay_health_port: int = Field(default=8082, ge=0, le=65_535)
    log_level: str = "INFO"
    # How long to wait after binding the collector PUB before the first
    # ingest, so the api SUB can finish its subscription handshake.
    startup_delay_s: float = Field(default=1.0, ge=0)
    # The api can serve historical REST without a live collector. In
    # deployments where the live stream is mandatory, set this true so
    # `/readyz` returns 503 until the collector handshake is observed.
    collector_required: bool = False
    collector_ready_timeout_s: float = Field(default=30.0, gt=0)
    # When true the collector waits for the api PUB to signal readiness (which
    # the api only does after receiving the collector READY) before ingesting.
    # Standalone collectors default to false; `mdgw up` and Docker enable it.
    wait_for_api_ready: bool = False
    api_ready_timeout_s: float = Field(default=30.0, gt=0)


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
    # Development/test Feed Handler (`kind = "fake"`). It emits a deterministic
    # run of closed 1m Bars and never reaches the network. `fake_bars = 0`
    # disables the generator.
    fake_bars: int = Field(default=0, ge=0)
    fake_start_ts: str | None = None
    fake_price: str = "100"

    @field_validator("name", "venue", "market_type")
    @classmethod
    def _non_empty(cls, value: str, info: ValidationInfo) -> str:
        if not value.strip():
            raise ValueError(f"{info.field_name} must be a non-empty string")
        return value.strip()

    @field_validator("kind")
    @classmethod
    def _supported_kind(cls, value: str) -> str:
        if value not in SUPPORTED_FEED_KINDS:
            raise ValueError(
                f"unsupported Feed Handler kind: {value!r} "
                f"(supported: {sorted(SUPPORTED_FEED_KINDS)})"
            )
        return value

    @field_validator("fake_start_ts")
    @classmethod
    def _fake_start_ts(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("fake_start_ts must be an ISO-8601 string")
        return cleaned

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
    service: ServiceConfig = Field(default_factory=ServiceConfig)
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
