from __future__ import annotations

import json
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from mdgw.config import load_config
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.runtime import GatewayRuntime

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_closed_fake_bar_roundtrips_to_rest(tmp_path: Path) -> None:
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    config = config.model_copy(update={"server": server})
    feed = FakeFeedHandler(
        instrument_id="binance:BTC/USDT",
        bars=[
            BarInput(
                open_ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
                open="65000.1",
                high="65010.0",
                low="64990.0",
                close="65005.5",
                volume="12.34",
                trade_count=128,
            )
        ],
    )

    with GatewayRuntime(feed_handler=feed, config=config, data_root=tmp_path) as runtime:
        runtime.start()
        runtime.ingest()
        assert runtime.received_bar_count == 1

        with urllib.request.urlopen(
            f"{runtime.base_url}/api/v1/bars?instrument=binance:BTC/USDT&interval=1m"
        ) as response:
            body = json.load(response)

    assert len(body["bars"]) == 1
    envelope = body["bars"][0]
    assert envelope["schema_version"] == 1
    assert envelope["data_type"] == "bar"
    assert envelope["instrument_id"] == "binance:BTC/USDT"
    assert envelope["event_ts"] == "2024-01-01T00:01:00.000000000Z"
    assert envelope["feed"] == "fake"
    assert envelope["seq"] is None

    payload = envelope["payload"]
    assert payload["interval"] == "1m"
    assert payload["open_ts"] == "2024-01-01T00:00:00.000000000Z"
    assert payload["close_ts"] == "2024-01-01T00:01:00.000000000Z"
    assert payload["aggregation_source"] == "external"
    assert payload["open"] == "65000.1"
    assert payload["high"] == "65010.0"
    assert payload["low"] == "64990.0"
    assert payload["close"] == "65005.5"
    assert Decimal(payload["volume"]) == Decimal("12.34")
    assert payload["trade_count"] == 128
    assert payload["revision"] == 0
