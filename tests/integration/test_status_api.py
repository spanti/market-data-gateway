from __future__ import annotations

import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from mdgw.config import load_config
from mdgw.core.time import iso_to_ns
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.runtime import GatewayRuntime

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc


def _config():
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    return config.model_copy(update={"server": server})


def _bar_input(minute: int) -> BarInput:
    return BarInput(
        open_ts=datetime(2024, 1, 1, 0, minute, tzinfo=UTC),
        open="65000.1",
        high="65010.0",
        low="64990.0",
        close="65005.5",
        volume="12.34",
        trade_count=128,
    )


def test_status_endpoint_exposes_last_flushed_and_file_metrics(tmp_path: Path) -> None:
    feed = FakeFeedHandler(
        instrument_id="binance:BTC/USDT",
        bars=[_bar_input(0), _bar_input(1)],
    )
    with GatewayRuntime(feed_handler=feed, config=_config(), data_root=tmp_path) as runtime:
        runtime.start()
        runtime.ingest()
        with urllib.request.urlopen(
            f"{runtime.base_url}/api/v1/status", timeout=15
        ) as response:
            status = json.load(response)

    assert status["dataset"] == "bars"
    assert status["active_file_count"] == 1
    assert status["median_file_size_bytes"] > 0
    assert iso_to_ns(status["last_flushed_ts"]) > 0

    interval = status["intervals"]["1m"]
    assert interval["active_file_count"] == 1
    assert interval["median_file_size_bytes"] == status["median_file_size_bytes"]
    assert interval["last_flushed_ts"] == status["last_flushed_ts"]


def test_status_endpoint_on_empty_dataset(tmp_path: Path) -> None:
    feed = FakeFeedHandler(instrument_id="binance:BTC/USDT", bars=[])
    with GatewayRuntime(feed_handler=feed, config=_config(), data_root=tmp_path) as runtime:
        runtime.start()
        with urllib.request.urlopen(
            f"{runtime.base_url}/api/v1/status", timeout=15
        ) as response:
            status = json.load(response)

    assert status["active_file_count"] == 0
    assert status["median_file_size_bytes"] is None
    assert status["last_flushed_ts"] is None
    assert status["intervals"] == {}
