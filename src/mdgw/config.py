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
class GatewayConfig:
    server: ServerConfig = ServerConfig()
    bus: BusConfig = BusConfig()
    storage: StorageConfig = StorageConfig()


def load_config(path: Path) -> GatewayConfig:
    with Path(path).open("rb") as handle:
        raw = tomllib.load(handle)

    server = raw.get("server", {})
    bus = raw.get("bus", {})
    storage = raw.get("storage", {})

    config = GatewayConfig(
        server=ServerConfig(
            host=server.get("host", ServerConfig.host),
            http_port=int(server.get("http_port", ServerConfig.http_port)),
            ws_path=server.get("ws_path", ServerConfig.ws_path),
        ),
        bus=BusConfig(
            transport=bus.get("transport", BusConfig.transport),
            zmq_hwm=int(bus.get("zmq_hwm", BusConfig.zmq_hwm)),
        ),
        storage=StorageConfig(
            root=Path(storage.get("root", str(StorageConfig.root))),
            schema_version=int(storage.get("schema_version", 1)),
            format_version=storage.get("format_version", "2.6"),
            compression=storage.get("compression", "zstd"),
            row_group_size=int(storage.get("row_group_size", 1_000_000)),
            flush_max_bytes=int(storage.get("flush_max_bytes", 67_108_864)),
            flush_max_age_s=int(storage.get("flush_max_age_s", 3_600)),
        ),
    )

    if config.bus.transport not in SUPPORTED_BUS_TRANSPORTS:
        raise ValueError(
            f"unsupported bus transport in this walking skeleton: "
            f"{config.bus.transport!r} (supported: {sorted(SUPPORTED_BUS_TRANSPORTS)})"
        )
    return config
