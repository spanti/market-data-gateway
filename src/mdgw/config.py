"""Runtime configuration (TOML) and typed defaults."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path

SUPPORTED_BUS_TRANSPORTS = frozenset({"inproc"})


@dataclass(frozen=True, slots=True)
class ServerConfig:
    host: str = "127.0.0.1"
    http_port: int = 8080
    ws_path: str = "/ws"


@dataclass(frozen=True, slots=True)
class BusConfig:
    transport: str = "inproc"
    zmq_hwm: int = 10_000


@dataclass(frozen=True, slots=True)
class StorageConfig:
    root: Path = Path("./data")
    schema_version: int = 1
    format_version: str = "2.6"
    compression: str = "zstd"
    row_group_size: int = 1_000_000
    flush_max_bytes: int = 67_108_864
    flush_max_age_s: int = 3_600


@dataclass(frozen=True, slots=True)
class RetentionConfig:
    enabled: bool = False
    bars_days: int = 0  # 0 = keep forever


@dataclass(frozen=True, slots=True)
class GatewayConfig:
    server: ServerConfig = ServerConfig()
    bus: BusConfig = BusConfig()
    storage: StorageConfig = StorageConfig()
    retention: RetentionConfig = RetentionConfig()


def load_config(path: Path) -> GatewayConfig:
    with Path(path).open("rb") as handle:
        raw = tomllib.load(handle)

    server = raw.get("server", {})
    bus = raw.get("bus", {})
    storage = raw.get("storage", {})
    retention = raw.get("retention", {})
    server_defaults = ServerConfig()
    bus_defaults = BusConfig()
    storage_defaults = StorageConfig()

    config = GatewayConfig(
        server=ServerConfig(
            host=server.get("host", server_defaults.host),
            http_port=int(server.get("http_port", server_defaults.http_port)),
            ws_path=server.get("ws_path", server_defaults.ws_path),
        ),
        bus=BusConfig(
            transport=bus.get("transport", bus_defaults.transport),
            zmq_hwm=int(bus.get("zmq_hwm", bus_defaults.zmq_hwm)),
        ),
        storage=StorageConfig(
            root=Path(storage.get("root", str(storage_defaults.root))),
            schema_version=int(storage.get("schema_version", storage_defaults.schema_version)),
            format_version=storage.get("format_version", storage_defaults.format_version),
            compression=storage.get("compression", storage_defaults.compression),
            row_group_size=int(
                storage.get("row_group_size", storage_defaults.row_group_size)
            ),
            flush_max_bytes=int(
                storage.get("flush_max_bytes", storage_defaults.flush_max_bytes)
            ),
            flush_max_age_s=int(
                storage.get("flush_max_age_s", storage_defaults.flush_max_age_s)
            ),
        ),
        retention=RetentionConfig(
            enabled=bool(retention.get("enabled", False)),
            bars_days=int(retention.get("bars_days", 0)),
        ),
    )

    if config.retention.bars_days < 0:
        raise ValueError("retention.bars_days must not be negative")
    if config.bus.transport not in SUPPORTED_BUS_TRANSPORTS:
        raise ValueError(
            f"unsupported bus transport in this walking skeleton: "
            f"{config.bus.transport!r} (supported: {sorted(SUPPORTED_BUS_TRANSPORTS)})"
        )
    return config
