"""Runtime configuration (TOML) and typed defaults."""

from __future__ import annotations

import ipaddress
import re
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from mdgw.core.schema import INTERVALS
from mdgw.normalize.version import NORMALIZATION_RULE_VERSION

if TYPE_CHECKING:
    from mdgw.store.duckdb_reader import DuckDBOptions

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
        msg = f"bus endpoint must use tcp:// (ipc:// is not supported): {value!r}"
        raise ValueError(msg)
    host, separator, port = value[len("tcp://") :].rpartition(":")
    if not separator or not host:
        msg = f"bus endpoint must be tcp://HOST:PORT: {value!r}"
        raise ValueError(msg)
    if not port.isdigit() or not 0 <= int(port) <= 65_535:
        msg = f"bus endpoint has an invalid port: {value!r}"
        raise ValueError(msg)
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
            msg = (
                f"unsupported bus transport: {value!r} "
                f"(supported: {sorted(SUPPORTED_BUS_TRANSPORTS)})"
            )
            raise ValueError(msg)
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


class RateLimitConfig(_ConfigModel):
    """Default-off per-IP / per-route HTTP and WebSocket rate limiting."""

    enabled: bool = False
    # Steady-state refill per `(source_ip, route_template)` bucket.
    requests_per_second: float = Field(default=50, gt=0)
    # Bucket capacity; 0 denies the first request.
    burst: int = Field(default=100, ge=0)
    # Per-IP concurrent WebSocket sessions; 0 means unlimited.
    max_ws_sessions_per_ip: int = Field(default=16, ge=0)



_BYTE_SIZE_RE = re.compile(r"^\d+(?:\.\d+)?\s*(?:B|KB|MB|GB|TB)$", re.IGNORECASE)


def _byte_size(value: str) -> str:
    value = value.strip()
    if _BYTE_SIZE_RE.match(value) is None:
        msg = f"invalid byte size (expected e.g. '512MB'): {value!r}"
        raise ValueError(msg)
    return value


class DuckDBConfig(_ConfigModel):
    """Explicit resources for the DuckDB Arrow history read path.

    This is not an engine switch: DuckDB is always a formal runtime
    dependency. These settings only bound the per-read connection.
    """

    memory_limit: str = "1GB"
    threads: int = Field(default=4, ge=1)
    # None resolves to `<storage.root>/.duckdb_tmp` at Store startup, so spill
    # never silently uses the process/OS default.
    temp_directory: Path | None = None
    max_temp_directory_size: str = "10GB"
    arrow_batch_size: int = Field(default=65_536, ge=1)

    @field_validator("memory_limit", "max_temp_directory_size")
    @classmethod
    def _valid_byte_size(cls, value: str) -> str:
        return _byte_size(value)

    def to_options(self) -> DuckDBOptions:
        """Build the plain Store-level options without importing at module load."""
        # Storage imports stay lazy so `mdgw.config` remains dependency-light.
        from mdgw.store.duckdb_reader import DuckDBOptions  # ruff: ignore[import-outside-top-level]

        return DuckDBOptions(
            memory_limit=self.memory_limit,
            threads=self.threads,
            temp_directory=self.temp_directory,
            max_temp_directory_size=self.max_temp_directory_size,
            arrow_batch_size=self.arrow_batch_size,
        )


class QuarantineConfig(_ConfigModel):
    """Independent Quarantine Dataset settings.

    ``root=None`` means ``<storage.root>/quarantine``. The schema version is
    independent of the Bar Data Contract. ``rule_version`` is the live
    Normalization Rule Version stamped on Quarantine Records and on new Bars
    Manifest entries; it is audit metadata, never a Schema authority.
    """

    root: Path | None = None
    schema_version: int = Field(default=1, ge=1)
    rule_version: str = NORMALIZATION_RULE_VERSION
    # Whole UTC months older than this are eligible for GC. 0 keeps them
    # forever; GC itself is always an explicit operator command (dry run by
    # default) and never a background task.
    retention_days: int = Field(default=30, ge=0)

    @field_validator("rule_version")
    @classmethod
    def _non_empty_rule_version(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            msg = "quarantine rule_version must be a non-empty string"
            raise ValueError(msg)
        return cleaned

    def resolved_root(self, storage_root: Path) -> Path:
        """Resolve the configured root against the Storage root."""
        if self.root is not None:
            return Path(self.root)
        return Path(storage_root) / "quarantine"


def ensure_quarantine_is_independent(
    bars_dataset_root: Path, quarantine_root: Path
) -> None:
    """Raise when the Quarantine Dataset is inside the Bars Dataset.

    ADR-0014 requires the two Datasets to be independent. Normalized
    comparison does not require either path to exist, so the same check also
    covers root overrides and programmatically copied configs.
    """
    bars_root = Path(bars_dataset_root).resolve()
    quarantine = Path(quarantine_root).resolve()
    if quarantine == bars_root or bars_root in quarantine.parents:
        msg = (
            "storage.quarantine.root must not be inside the Bars Dataset "
            f"({bars_root})"
        )
        raise ValueError(msg)


class StorageConfig(_ConfigModel):
    root: Path = Path("./data")
    schema_version: int = 1
    format_version: str = "2.6"
    compression: str = "zstd"
    row_group_size: int = 1_000_000
    flush_max_bytes: int = 67_108_864
    flush_max_age_s: int = 3_600
    duckdb: DuckDBConfig = Field(default_factory=DuckDBConfig)
    quarantine: QuarantineConfig = Field(default_factory=QuarantineConfig)

    @model_validator(mode="after")
    def _quarantine_root_is_independent(self) -> StorageConfig:
        """The Quarantine Dataset must never live inside the Bars Dataset."""
        if self.quarantine.root is None:
            return self
        ensure_quarantine_is_independent(
            Path(self.root) / "bars", Path(self.quarantine.root)
        )
        return self


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
    # Optional HTTP proxy shared by ccxt.pro REST and WSS (e.g. Clash).
    # This is deployment-specific; leave unset to connect directly.
    proxy: str | None = None
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
            msg = f"{info.field_name} must be a non-empty string"
            raise ValueError(msg)
        return value.strip()

    @field_validator("kind")
    @classmethod
    def _supported_kind(cls, value: str) -> str:
        if value not in SUPPORTED_FEED_KINDS:
            msg = (
                f"unsupported Feed Handler kind: {value!r} "
                f"(supported: {sorted(SUPPORTED_FEED_KINDS)})"
            )
            raise ValueError(msg)
        return value

    @field_validator("fake_start_ts")
    @classmethod
    def _fake_start_ts(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            msg = "fake_start_ts must be an ISO-8601 string"
            raise ValueError(msg)
        return cleaned

    @field_validator("proxy")
    @classmethod
    def _proxy_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            return None
        if not cleaned.startswith(("http://", "https://")):
            msg = "proxy must be an http:// or https:// URL"
            raise ValueError(msg)
        return cleaned

    @field_validator("symbols")
    @classmethod
    def _symbols(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            msg = "symbols must contain at least one symbol"
            raise ValueError(msg)
        cleaned = tuple(symbol.strip() for symbol in value)
        if any(not symbol for symbol in cleaned):
            msg = "symbols must not contain empty values"
            raise ValueError(msg)
        if len(set(cleaned)) != len(cleaned):
            msg = "symbols must not contain duplicates"
            raise ValueError(msg)
        return cleaned

    @field_validator("data_types")
    @classmethod
    def _data_types(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            msg = "data_types must contain at least one Data Type"
            raise ValueError(msg)
        unsupported = [data_type for data_type in value if data_type != "bar"]
        if unsupported:
            msg = f"unsupported data_types for v1: {unsupported!r} (supported: ['bar'])"
            raise ValueError(msg)
        return value

    @property
    def instrument_ids(self) -> tuple[str, ...]:
        return tuple(f"{self.venue}:{symbol}" for symbol in self.symbols)


class CollectorConfig(_ConfigModel):
    """Backfill window and freshness thresholds for the collector process."""

    backfill_hours: float = Field(default=24.0, ge=0)
    stale_after_s: float = Field(default=120.0, ge=0)
    state_db_name: str = "state.db"
    # Bounded live-write queue between ingest and the collector writer thread.
    # A full queue drops its oldest Bar and increments the lag/dropped metric.
    write_queue_size: int = Field(default=10_000, ge=1)

    @field_validator("state_db_name")
    @classmethod
    def _non_empty_state_db_name(cls, value: str) -> str:
        if not value.strip():
            msg = "state_db_name must be a non-empty file name"
            raise ValueError(msg)
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
            msg = f"unsupported base Bar Interval: {value!r}"
            raise ValueError(msg)
        if value != "1m":
            msg = "v1 Bar Aggregation only supports '1m' as the base interval"
            raise ValueError(msg)
        return value

    @field_validator("intervals")
    @classmethod
    def _supported_intervals(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        unknown = [interval for interval in value if interval not in INTERVALS]
        if unknown:
            msg = f"unsupported Bar Interval(s): {unknown!r}"
            raise ValueError(msg)
        if len(set(value)) != len(value):
            msg = "aggregation intervals must not contain duplicates"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _base_interval_is_aggregated(self) -> AggregationConfig:
        if self.base_interval not in self.intervals:
            msg = (
                "aggregation intervals must contain the base interval "
                f"{self.base_interval!r}"
            )
            raise ValueError(msg)
        return self


class ReplayConfig(_ConfigModel):
    """Replay Service Role endpoints, concurrency and streaming limits."""

    control_bind_host: str = "127.0.0.1"
    control_port: int = Field(default=0, ge=0, le=65535)
    publish_bind_host: str = "127.0.0.1"
    publish_port: int = Field(default=0, ge=0, le=65535)
    max_sessions: int = Field(default=4, ge=1)
    heartbeat_s: float = Field(default=1.0, gt=0)
    # asap Replay reads the ordered DuckDB query through its Arrow batch
    # reader. Both values are explicit memory bounds, not an engine switch:
    # in-flight Bars are roughly `batch_size * (prefetch_batches + 2)`.
    batch_size: int = Field(default=4_096, ge=1)
    prefetch_batches: int = Field(default=2, ge=1)


class AdminAdaptersConfig(_ConfigModel):
    """Deployment intent for adapters implemented by later admin tickets.

    Ticket 01 only ships the admin store; every flag defaults to off so a
    capability is never announced as available before its adapter exists.
    """

    oidc: bool = False
    audit_store: bool = False
    supervisor: bool = False
    collector_control: bool = False
    export_executor: bool = False
    prometheus: bool = False


#: Access Roles are fixed by the contract. Kept here (not imported from the
#: admin package) so configuration stays a foundation module.
ADMIN_ACCESS_ROLES: tuple[str, ...] = ("SuperAdmin", "Operator", "Auditor")


def _validate_relative_redirect_path(value: str, field: str) -> str:
    """Reject unsafe SPA redirect paths.

    Redirects only ever go to a same-origin relative path: no absolute URL,
    no protocol-relative ``//host``, no control characters and no backslash
    (which some browsers normalize into a separator).
    """
    if not isinstance(value, str):
        msg = f"{field} must be a string"
        # Pydantic validators signal bad input as ValueError.
        raise ValueError(msg)  # ruff: ignore[type-check-without-type-error]
    if not value.startswith("/") or value.startswith("//"):
        msg = f"{field} must be a same-origin relative path starting with '/'"
        raise ValueError(msg)
    if "\\" in value or any(
        ord(character) < 0x20 or ord(character) == 0x7F for character in value
    ):
        msg = f"{field} must not contain control characters or backslashes"
        raise ValueError(msg)
    return value


class AdminOidcConfig(_ConfigModel):
    """OIDC Authorization Code + PKCE client settings and role mapping."""

    issuer: str | None = None
    client_id: str | None = None
    # The secret itself must come from a controlled secret provider; config
    # only names the environment variable so it is never logged or audited.
    client_secret_env: str | None = None
    roles_claim: str = "groups"
    scopes: tuple[str, ...] = ("openid", "profile", "email")
    #: Ordered group -> Access Role mapping. The client can never select a
    #: role; exactly one distinct role must resolve or the login is denied.
    role_mapping: dict[str, str] = Field(default_factory=dict)
    #: Optional fixed redirect URI. When unset it is derived from the request
    #: origin (or ``admin.proxy.public_origin``), which keeps local tests and
    #: reverse-proxy deployments working without a second source of truth.
    redirect_uri: str | None = None
    default_return_to: str = "/"
    error_redirect_path: str = "/login"
    #: Bounded caches for discovery/JWKS and bounded HTTP waits.
    discovery_ttl_s: float = Field(default=300.0, gt=0)
    jwks_ttl_s: float = Field(default=300.0, gt=0)
    http_timeout_s: float = Field(default=5.0, gt=0)
    #: Login transactions are short-lived (the contract allows at most 10 min).
    transaction_ttl_s: int = Field(default=300, gt=0, le=600)
    clock_skew_s: int = Field(default=60, ge=0, le=300)

    @property
    def is_configured(self) -> bool:
        """True only when a full OIDC client can be built."""
        return bool(
            self.issuer and self.client_id and self.client_secret_env
        )

    @field_validator("issuer")
    @classmethod
    def _http_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        parts = urlsplit(cleaned)
        if parts.scheme not in {"http", "https"} or not parts.netloc:
            msg = "issuer must be an http(s) URL"
            raise ValueError(msg)
        return cleaned

    @field_validator("client_id", "client_secret_env", "roles_claim")
    @classmethod
    def _non_empty(cls, value: str | None, info: ValidationInfo) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            msg = f"{info.field_name} must be a non-empty string"
            raise ValueError(msg)
        return cleaned

    @field_validator("redirect_uri")
    @classmethod
    def _redirect_uri_is_http(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        parts = urlsplit(cleaned)
        if (
            parts.scheme not in {"http", "https"}
            or not parts.netloc
            or parts.fragment
        ):
            msg = "redirect_uri must be an absolute http(s) URL"
            raise ValueError(msg)
        return cleaned

    @field_validator("default_return_to")
    @classmethod
    def _default_return_to_is_relative(cls, value: str) -> str:
        return _validate_relative_redirect_path(value, "default_return_to")

    @field_validator("error_redirect_path")
    @classmethod
    def _error_path_is_relative(cls, value: str) -> str:
        return _validate_relative_redirect_path(value, "error_redirect_path")

    @field_validator("role_mapping", mode="before")
    @classmethod
    def _normalize_role_mapping(cls, value: object) -> dict[str, str]:
        """Accept a mapping or an ordered list of ``[group, role]`` pairs.

        Duplicate groups are rejected when they disagree: an ambiguous mapping
        could silently elevate a user and must fail configuration.
        """
        if value is None:
            return {}
        if isinstance(value, dict):
            entries: list[tuple[object, object]] = list(value.items())
        elif isinstance(value, (list, tuple)):
            entries = []
            for entry in value:
                if isinstance(entry, (list, tuple)) and len(entry) == 2:
                    entries.append((entry[0], entry[1]))
                elif isinstance(entry, dict) and set(entry) == {
                    "group",
                    "role",
                }:
                    entries.append((entry["group"], entry["role"]))
                else:
                    msg = (
                        "role_mapping entries must be [group, role] pairs or "
                        "{group, role} tables"
                    )
                    raise ValueError(msg)
        else:
            msg = "role_mapping must be a mapping or a list of pairs"
            # Pydantic validators signal bad input as ValueError.
            raise ValueError(msg)  # ruff: ignore[type-check-without-type-error]

        mapping: dict[str, str] = {}
        for raw_group, raw_role in entries:
            if not isinstance(raw_group, str) or not raw_group.strip():
                msg = "role_mapping group must be a non-empty string"
                raise ValueError(msg)
            if not isinstance(raw_role, str) or raw_role not in (
                ADMIN_ACCESS_ROLES
            ):
                msg = (
                    "role_mapping role must be one of "
                    f"{', '.join(ADMIN_ACCESS_ROLES)}"
                )
                raise ValueError(msg)
            group = raw_group.strip()
            previous = mapping.get(group)
            if previous is not None and previous != raw_role:
                msg = (
                    f"role_mapping maps group {group!r} to conflicting roles "
                    f"({previous}, {raw_role})"
                )
                raise ValueError(msg)
            mapping[group] = raw_role
        return mapping


class AdminSessionConfig(_ConfigModel):
    """Server session lifetimes required by the admin contract."""

    idle_timeout_s: int = Field(default=1800, gt=0)
    absolute_timeout_s: int = Field(default=28800, gt=0)
    expiry_warning_s: int = Field(default=300, ge=0)

    @model_validator(mode="after")
    def _timeouts_are_ordered(self) -> AdminSessionConfig:
        if self.expiry_warning_s >= self.idle_timeout_s:
            msg = "expiry_warning_s must be less than idle_timeout_s"
            raise ValueError(msg)
        if self.idle_timeout_s > self.absolute_timeout_s:
            msg = "idle_timeout_s must not exceed absolute_timeout_s"
            raise ValueError(msg)
        return self


class AdminExportConfig(_ConfigModel):
    """Placeholder Export Job limits (ticket 08 implements the executor)."""

    root: Path | None = None
    retention_s: int = Field(default=86400, gt=0)
    min_days: int = Field(default=31, ge=1)
    max_days: int = Field(default=366, ge=1)
    max_running_per_user: int = Field(default=1, ge=1)
    global_concurrency: int = Field(default=2, ge=1)
    # 0 means no configured storage cap; the executor still reports usage.
    storage_quota_bytes: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def _range_is_ordered(self) -> AdminExportConfig:
        if self.min_days > self.max_days:
            msg = "export min_days must not exceed max_days"
            raise ValueError(msg)
        return self


_CONTROL_ENV_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class AdminControlConfig(_ConfigModel):
    """Controlled Collector/Supervisor IPC channel.

    Windows pyzmq cannot bind ``ipc://``, so the Admin control plane is a
    loopback TCP listener carrying length-prefixed, token-authenticated,
    schema-validated envelopes. The token itself is never stored in config:
    ``token_env`` names the environment variable both sides read.
    """

    socket_path: Path | None = None
    host: str = "127.0.0.1"
    # 0 means "not configured": the Collector does not bind a control server
    # and the API reports the collector_control adapter as unavailable.
    port: int = Field(default=0, ge=0, le=65535)
    # This is the environment variable *name*, never the token itself.
    token_env: str = "MDGW_ADMIN_CONTROL_TOKEN"  # ruff: ignore[hardcoded-password-string]
    timeout_s: float = Field(default=5.0, gt=0)
    max_message_bytes: int = Field(default=65_536, ge=1)
    grace_s: float = Field(default=10.0, gt=0)

    @field_validator("host")
    @classmethod
    def _loopback_host(cls, value: str) -> str:
        cleaned = value.strip()
        if cleaned == "localhost":
            return cleaned
        try:
            address = ipaddress.ip_address(cleaned)
        except ValueError as exc:
            msg = "control.host must be a loopback address"
            raise ValueError(msg) from exc
        if not address.is_loopback:
            msg = "control.host must be a loopback address"
            raise ValueError(msg)
        return cleaned

    @field_validator("token_env")
    @classmethod
    def _token_env_name(cls, value: str) -> str:
        cleaned = value.strip()
        if _CONTROL_ENV_RE.match(cleaned) is None:
            msg = "control.token_env must be a valid environment variable name"
            raise ValueError(msg)
        return cleaned


class AdminLimitsConfig(_ConfigModel):
    """Documented admin list/operation limits exposed by capabilities."""

    page_default: int = Field(default=50, ge=1)
    page_max: int = Field(default=200, ge=1)
    reason_max_length: int = Field(default=1000, ge=1)
    operation_retention_s: int = Field(default=604_800, ge=1)
    backfill_max_range_days: int = Field(default=366, ge=1)

    @model_validator(mode="after")
    def _pages_are_ordered(self) -> AdminLimitsConfig:
        if self.page_default > self.page_max:
            msg = "page_default must not exceed page_max"
            raise ValueError(msg)
        return self


_HEADER_TOKEN_RE = re.compile(r"^[A-Za-z0-9!#$%&'*+.^_`|~-]+$")


class AdminProxyConfig(_ConfigModel):
    """Explicit same-origin / TLS-terminating-proxy model.

    No CORS middleware is installed anywhere: the console is same-origin, and
    forwarded client addresses are trusted only from these proxy sources.
    """

    public_origin: str | None = None
    trusted_proxies: tuple[str, ...] = ()
    forwarded_for_header: str = "X-Forwarded-For"
    max_forwarded_hops: int = Field(default=16, ge=1, le=64)

    @field_validator("public_origin")
    @classmethod
    def _valid_origin(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        parts = urlsplit(cleaned)
        valid_authority = (
            parts.scheme in {"http", "https"} and bool(parts.netloc)
        )
        valid_shape = parts.path in {"", "/"} and not (
            parts.query
            or parts.fragment
            or parts.username
            or parts.password
        )
        if not valid_authority or not valid_shape:
            msg = (
                "public_origin must be an http(s) origin such as "
                "'https://console.example.com'"
            )
            raise ValueError(msg)
        return cleaned

    @field_validator("trusted_proxies")
    @classmethod
    def _valid_trusted_proxies(
        cls, value: tuple[str, ...]
    ) -> tuple[str, ...]:
        cleaned: list[str] = []
        for entry in value:
            text = entry.strip()
            if not text:
                msg = "trusted_proxies must not contain empty values"
                raise ValueError(msg)
            try:
                ipaddress.ip_network(text, strict=False)
            except ValueError as exc:
                msg = (
                    "trusted_proxies entries must be IP addresses or CIDR "
                    f"networks: {text!r}"
                )
                raise ValueError(msg) from exc
            cleaned.append(text)
        if len(set(cleaned)) != len(cleaned):
            msg = "trusted_proxies must not contain duplicates"
            raise ValueError(msg)
        return tuple(cleaned)

    @field_validator("forwarded_for_header")
    @classmethod
    def _valid_header_name(cls, value: str) -> str:
        if _HEADER_TOKEN_RE.match(value) is None:
            msg = f"invalid forwarded_for_header name: {value!r}"
            raise ValueError(msg)
        return value


class AdminMonitoringConfig(_ConfigModel):
    """Server-side Prometheus template proxy limits (ticket 09).

    The proxy only runs fixed, server-owned PromQL templates; it never accepts
    arbitrary PromQL or label selectors. ``prometheus_url`` is the deployment
    endpoint (``admin.adapters.prometheus`` is the separate deployment-intent
    flag); when it is unset the adapter reports ``unavailable`` instead of
    silently defaulting to a local Prometheus.
    """

    prometheus_url: str | None = None
    timeout_s: float = Field(default=5.0, gt=0)
    max_range_s: int = Field(default=86_400, gt=0)
    max_points: int = Field(default=1_000, gt=0)
    min_step_s: int = Field(default=15, ge=1)

    @field_validator("prometheus_url")
    @classmethod
    def _http_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        parts = urlsplit(cleaned)
        if parts.scheme not in {"http", "https"} or not parts.netloc:
            msg = "prometheus_url must be an http(s) URL"
            raise ValueError(msg)
        if parts.query or parts.fragment or parts.username or parts.password:
            msg = "prometheus_url must not contain credentials, query or fragment"
            raise ValueError(msg)
        return cleaned.rstrip("/")


class AdminConfig(_ConfigModel):
    """Admin API foundation settings; disabled unless a deployment opts in."""

    enabled: bool = False
    # None resolves to `<storage.root>/admin/admin.db` at app construction.
    db_path: Path | None = None
    # Lease that makes the single-API-instance constraint explicit: a second
    # live instance is rejected, while a crashed instance is taken over after
    # the lease expires.
    instance_lease_s: float = Field(default=60.0, gt=0)
    proxy: AdminProxyConfig = Field(default_factory=AdminProxyConfig)
    adapters: AdminAdaptersConfig = Field(default_factory=AdminAdaptersConfig)
    oidc: AdminOidcConfig = Field(default_factory=AdminOidcConfig)
    sessions: AdminSessionConfig = Field(default_factory=AdminSessionConfig)
    export: AdminExportConfig = Field(default_factory=AdminExportConfig)
    control: AdminControlConfig = Field(default_factory=AdminControlConfig)
    limits: AdminLimitsConfig = Field(default_factory=AdminLimitsConfig)
    monitoring: AdminMonitoringConfig = Field(
        default_factory=AdminMonitoringConfig
    )

    def resolved_db_path(self, storage_root: Path) -> Path:
        """Resolve the management database path against the storage root."""
        if self.db_path is not None:
            return Path(self.db_path)
        return Path(storage_root) / "admin" / "admin.db"


def ensure_admin_store_is_independent(
    db_path: Path,
    bars_dataset_root: Path,
    quarantine_root: Path,
) -> None:
    """Raise when the management SQLite file lives in a market Dataset.

    Admin records are operational state, not market data: they must never be
    written into the Bars or Quarantine Datasets (ADR-0016 / ticket 01).
    """
    admin_path = Path(db_path).resolve()
    for label, root in (
        ("Bars Dataset", bars_dataset_root),
        ("Quarantine Dataset", quarantine_root),
    ):
        resolved = Path(root).resolve()
        if admin_path == resolved or resolved in admin_path.parents:
            msg = (
                "admin.db_path must not be inside the "
                f"{label} ({resolved})"
            )
            raise ValueError(msg)


class GatewayConfig(_ConfigModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    bus: BusConfig = Field(default_factory=BusConfig)
    service: ServiceConfig = Field(default_factory=ServiceConfig)
    streaming: StreamingConfig = Field(default_factory=StreamingConfig)
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    replay: ReplayConfig = Field(default_factory=ReplayConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    retention: RetentionConfig = Field(default_factory=RetentionConfig)
    aggregation: AggregationConfig = Field(default_factory=AggregationConfig)
    collector: CollectorConfig = Field(default_factory=CollectorConfig)
    instruments: InstrumentsConfig = Field(default_factory=InstrumentsConfig)
    admin: AdminConfig = Field(default_factory=AdminConfig)
    feeds: tuple[FeedConfig, ...] = ()

    @model_validator(mode="after")
    def _admin_store_is_independent(self) -> GatewayConfig:
        """The admin SQLite file must never live in a market Dataset."""
        if self.admin.db_path is None:
            return self
        storage_root = Path(self.storage.root)
        ensure_admin_store_is_independent(
            Path(self.admin.db_path),
            storage_root / "bars",
            self.storage.quarantine.resolved_root(storage_root),
        )
        return self

    @model_validator(mode="after")
    def _backfill_lookback_within_retention(self) -> GatewayConfig:
        """Reject a Backfill window the Retention policy would immediately erase.

        DESIGN §11: the Backfill lookback must not exceed the Data Type
        Retention, and the constraint is checked when the config is loaded.
        """
        if not self.retention.enabled or self.retention.bars_days <= 0:
            return self
        retention_hours = self.retention.bars_days * 24
        offenders: list[str] = []
        if self.collector.backfill_hours > retention_hours:
            offenders.append(
                f"collector.backfill_hours={self.collector.backfill_hours:g}h"
            )
        offenders.extend(
            f"feed {feed.name!r} backfill_hours={feed.backfill_hours:g}h"
            for feed in self.feeds
            if feed.backfill_hours > retention_hours
        )
        if offenders:
            msg = (
                "Backfill lookback must not exceed Retention "
                f"({self.retention.bars_days} day(s) = {retention_hours:g}h): "
                + ", ".join(offenders)
            )
            raise ValueError(msg)
        return self


def load_config(path: Path) -> GatewayConfig:
    """Parse and validate a runtime config TOML file."""
    with Path(path).open("rb") as handle:
        raw = tomllib.load(handle)
    return GatewayConfig.model_validate(raw)
