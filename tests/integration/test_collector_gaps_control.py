# ruff: file-ignore[implicit-namespace-package,hardcoded-password-string]
"""Collector ``gaps.snapshot`` control action over the real loopback plane.

The Collector stays the only writer of its process-local gap list; the Admin
API polls this allow-listed action. The snapshot must expose the real Feed
name and UTC nanosecond bounds, and the existing ``/status`` gap count keeps
its process-local meaning.
"""

from __future__ import annotations

import socket
from typing import TYPE_CHECKING

import yaml

from mdgw.config import GatewayConfig
from mdgw.ingest.fake import FakeFeedHandler
from mdgw.service.collector import CollectorService
from mdgw.service.control_plane import (
    GAPS_SNAPSHOT_ACTION,
    ControlPlaneClient,
    EmptyPayload,
    collector_control_registry,
)
from mdgw.state.health import GAP_HISTORY_UNSUPPORTED

if TYPE_CHECKING:
    from pathlib import Path

HOST = "127.0.0.1"
TOKEN_ENV = "MDGW_TEST_GAPS_TOKEN"
TOKEN = "gaps-control-token"
BTC = "binance:BTC/USDT"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((HOST, 0))
        return int(probe.getsockname()[1])


def _unused_endpoint() -> str:
    return f"tcp://{HOST}:{_free_port()}"


def _write_reference(tmp_path: Path) -> Path:
    venue, symbol = BTC.split(":", 1)
    reference = tmp_path / "reference"
    reference.mkdir(parents=True, exist_ok=True)
    (reference / "instruments.yaml").write_text(
        yaml.safe_dump(
            [
                {
                    "instrument_id": BTC,
                    "venue": venue,
                    "symbol": symbol,
                    "asset_class": "crypto",
                    "instrument_type": "spot",
                    "base": symbol.split("/")[0],
                    "quote": symbol.split("/")[1],
                    "tick_size": "0.01",
                    "lot_size": "0.0001",
                    "multiplier": "1",
                    "timezone": "UTC",
                    "listing_ts": "2020-01-01T00:00:00Z",
                }
            ]
        ),
        encoding="utf-8",
    )
    return reference


def _config(control_port: int, reference: Path) -> GatewayConfig:
    return GatewayConfig.model_validate(
        {
            "server": {"host": HOST, "http_port": 0},
            "bus": {
                "collector_pub_bind": f"tcp://{HOST}:0",
                "api_pub_connect": _unused_endpoint(),
            },
            "service": {
                "wait_for_api_ready": False,
                "startup_delay_s": 0.0,
                "collector_health_port": 0,
                "replay_health_port": 0,
            },
            "storage": {"flush_max_age_s": 0},
            "aggregation": {"base_interval": "1m", "intervals": ["1m"]},
            "instruments": {"path": str(reference)},
            "feeds": [
                {
                    "name": "binance_spot",
                    "kind": "fake",
                    "venue": "binance",
                    "symbols": ["BTC/USDT"],
                }
            ],
            "admin": {
                "enabled": False,
                "adapters": {"collector_control": True},
                "control": {
                    "collector_host": HOST,
                    "collector_port": control_port,
                    "token_env": TOKEN_ENV,
                    "timeout_s": 2.0,
                },
            },
        }
    )


def test_gaps_snapshot_returns_the_in_memory_events_with_feed_and_range(
    tmp_path: Path,
) -> None:
    reference = _write_reference(tmp_path)
    control_port = _free_port()
    feed = FakeFeedHandler(
        instrument_id=BTC,
        bars=(),
        name="binance_spot",
        supports_history=False,
    )
    service = CollectorService(
        _config(control_port, reference),
        data_root=tmp_path,
        feed_handler=feed,
        environ={TOKEN_ENV: TOKEN},
    )
    service.start()
    try:
        client = ControlPlaneClient(
            host=HOST,
            port=service.control_port,
            token=TOKEN,
            registry=collector_control_registry(),
        )
        result = client.call(
            GAPS_SNAPSHOT_ACTION, EmptyPayload(), timeout_ms=2000
        )

        status = service.status_payload()
        events = service.core.gap_events()
    finally:
        service.stop()

    assert result["collector_instance_id"].startswith("collector-")
    assert isinstance(result["snapshot_at_ns"], int)
    assert len(result["gaps"]) == 1
    gap = result["gaps"][0]
    assert gap["feed"] == "binance_spot"
    assert gap["instrument_id"] == BTC
    assert gap["interval"] == "1m"
    assert gap["reason"] == GAP_HISTORY_UNSUPPORTED
    assert isinstance(gap["from_ns"], int)
    assert isinstance(gap["to_ns"], int)
    assert gap["from_ns"] < gap["to_ns"]
    assert isinstance(gap["recorded_at_ns"], int)
    assert "seq" not in gap

    # The process-local /status snapshot keeps its own meaning and count; it
    # remains the old wire shape (no new keys) and is still process-local.
    assert status["gap_unrecoverable_count"] == len(events) == 1
    assert events[0].feed == "binance_spot"
    assert "feed" not in status["health"]["gap_unrecoverable"][0]
