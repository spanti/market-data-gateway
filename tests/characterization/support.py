"""Shared helpers for characterization tests (real runtime, real HTTP).

The REST characterization deliberately enters through `GatewayRuntime`: the
real ASGI app on an ephemeral port, the real ZMQ Streaming Interface and the
real Store wired to the Dataset built by `dataset_builder`.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from mdgw.config import load_config
from mdgw.ingest.fake import FakeFeedHandler
from mdgw.runtime import GatewayRuntime

if TYPE_CHECKING:
    from collections.abc import Iterator
    from http.client import HTTPMessage

    from mdgw.config import GatewayConfig

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "gateway.toml"
INSTRUMENTS_PATH = PROJECT_ROOT / "config" / "instruments"

BTC = "binance:BTC/USDT"
ETH = "binance:ETH/USDT"

ARROW_STREAM = "application/vnd.apache.arrow.stream"
ARROW_FILE = "application/vnd.apache.arrow.file"
PARQUET = "application/vnd.apache.parquet"
CSV = "text/csv; charset=utf-8"

NS_TIMESTAMP = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z"
)


def test_config() -> GatewayConfig:
    """The checked-in config with an ephemeral HTTP port and absolute paths."""
    config = load_config(CONFIG_PATH)
    return config.model_copy(
        update={
            "server": config.server.model_copy(update={"http_port": 0}),
            "instruments": config.instruments.model_copy(
                update={"path": INSTRUMENTS_PATH}
            ),
        }
    )


@contextmanager
def running_runtime(
    dataset_root: Path,
    *,
    feed_instrument: str = BTC,
) -> Iterator[GatewayRuntime]:
    """Start a real GatewayRuntime over an already-built Dataset root."""
    feed = FakeFeedHandler(instrument_id=feed_instrument, bars=())
    runtime = GatewayRuntime(
        feed_handler=feed,
        config=test_config(),
        data_root=dataset_root,
    )
    runtime.start()
    try:
        yield runtime
    finally:
        runtime.stop()


def http_get(
    url: str,
    *,
    headers: dict[str, str] | None = None,
) -> tuple[int, HTTPMessage, bytes]:
    request = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, response.headers, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.headers, error.read()


def get_json(url: str) -> dict:
    status, _, body = http_get(url)
    assert status == 200, body
    return json.loads(body)


def media_type(headers: HTTPMessage) -> str:
    return (headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
