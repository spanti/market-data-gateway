# ruff: file-ignore[implicit-namespace-package]
"""In-process integration test for the api HTTP latency histogram.

The api is served on a real socket with port 0 through ``ApiService``: a
normal request is made first, then ``/metrics`` is scraped and the rendered
Prometheus text is asserted. No ``httpx.ASGITransport`` is used.
"""

from __future__ import annotations

import re
import time
import urllib.error
import urllib.request
from pathlib import Path

from mdgw.config import load_config
from mdgw.service.api import ApiService

PROJECT_ROOT = Path(__file__).resolve().parents[2]
HOST = "127.0.0.1"
WAIT_TIMEOUT_S = 15.0
DURATION_METRIC = "mdgw_api_http_request_duration_seconds"
HEALTH_SERIES = 'method="GET",path="/healthz",status="200"'


def _config():
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0, "host": HOST})
    bus = config.bus.model_copy(update={"api_pub_bind": "tcp://127.0.0.1:0"})
    return config.model_copy(update={"server": server, "bus": bus})


def _get(url: str) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(  # ruff: ignore[suspicious-url-open-usage]
            url, timeout=WAIT_TIMEOUT_S
        ) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8")


def _scrape_until_observed(metrics_url: str, series: str) -> str:
    """Poll ``/metrics`` until the expected histogram series lands.

    The request middleware records the duration after the response body has
    been handed to the server, so the scrape immediately after a request can
    race it. Polling keeps the test deterministic without a fixed sleep.
    """
    deadline = time.monotonic() + WAIT_TIMEOUT_S
    while time.monotonic() < deadline:
        status, text = _get(metrics_url)
        assert status == 200
        if f"{DURATION_METRIC}_count{{{series}}}" in text:
            return text
        time.sleep(0.02)
    message = f"histogram series {series} never appeared in /metrics"
    raise AssertionError(message)


def test_metrics_exposes_http_request_duration_histogram(
    tmp_path: Path,
) -> None:
    service = ApiService(_config(), data_root=tmp_path)
    try:
        service.start()
        health_status, health_body = _get(f"{service.base_url}/healthz")
        assert health_status == 200
        text = _scrape_until_observed(
            f"{service.base_url}/metrics", HEALTH_SERIES
        )
    finally:
        service.stop()

    # Response bytes are measured on the ASGI send path, per method/route
    # template/status, and never claimed as client-received network bytes.
    assert "# TYPE mdgw_api_http_response_bytes_total counter" in text
    assert (
        "mdgw_api_http_response_bytes_total"
        f"{{{HEALTH_SERIES}}} {len(health_body.encode('utf-8'))}"
    ) in text
    assert "complete response stream" in text

    assert f"# TYPE {DURATION_METRIC} histogram" in text
    # Router template is the label value, not the raw request path.
    assert (
        f'{DURATION_METRIC}_bucket{{{HEALTH_SERIES},le="0.005"}}' in text
    )
    assert (
        f'{DURATION_METRIC}_bucket{{{HEALTH_SERIES},le="+Inf"}}' in text
    )
    assert re.search(
        rf"{DURATION_METRIC}_sum\{{{HEALTH_SERIES}\}} [0-9.eE+-]+", text
    )
    count_match = re.search(
        rf"{DURATION_METRIC}_count\{{{HEALTH_SERIES}\}} (\d+)", text
    )
    assert count_match is not None
    assert int(count_match.group(1)) >= 1

    # Existing gauges/counters are untouched.
    assert "mdgw_up 1" in text
    assert "mdgw_ready 1" in text
    assert "mdgw_api_ws_sessions" in text
    assert "mdgw_api_ws_dropped_total" in text
    assert "mdgw_dataset_active_file_count" in text
    # DuckDB spill disk and checked-out connections are observable without
    # changing the /api/v1/status JSON shape.
    assert "mdgw_duckdb_temp_bytes" in text
    assert "mdgw_duckdb_active_connections" in text


def test_histogram_labels_use_route_template_and_response_status(
    tmp_path: Path,
) -> None:
    service = ApiService(_config(), data_root=tmp_path)
    route_series = (
        'method="GET",path="/api/v1/instruments/{instrument_id:path}",'
        'status="404"'
    )
    try:
        service.start()
        status, _ = _get(
            f"{service.base_url}/api/v1/instruments/binance:MISSING/USDT"
        )
        assert status == 404
        text = _scrape_until_observed(
            f"{service.base_url}/metrics", route_series
        )
    finally:
        service.stop()

    assert (
        f'{DURATION_METRIC}_bucket{{{route_series},le="+Inf"}} 1' in text
    )
    assert (
        f"{DURATION_METRIC}_count{{{route_series}}} 1" in text
    )
