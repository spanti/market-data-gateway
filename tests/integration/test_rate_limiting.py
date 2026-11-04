# ruff: file-ignore[implicit-namespace-package]
"""End-to-end rate-limiting tests through `GatewayRuntime` and `ApiService`.

All assertions observe wire behaviour on a real socket: status codes, the
`Retry-After` header, the unified error envelope, `/metrics` text, and the
WebSocket handshake outcome. No test sleeps on the limiter clock.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import websockets
from websockets.exceptions import InvalidStatus

from mdgw.config import GatewayConfig, load_config
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.runtime import GatewayRuntime
from mdgw.service.api import ApiService

if TYPE_CHECKING:
    from collections.abc import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
HOST = "127.0.0.1"
WAIT_TIMEOUT_S = 15.0
INSTRUMENTS_PATH = "/api/v1/instruments"


def _config(**rate_limit_overrides) -> GatewayConfig:
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0, "host": HOST})
    rate_limit = config.rate_limit.model_copy(update=rate_limit_overrides)
    return config.model_copy(update={"server": server, "rate_limit": rate_limit})


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


@contextmanager
def _runtime(tmp_path: Path, **overrides) -> Generator[GatewayRuntime]:
    feed = FakeFeedHandler(
        instrument_id="binance:BTC/USDT",
        bars=[_bar_input(0)],
    )
    runtime = GatewayRuntime(
        feed_handler=feed,
        config=_config(**overrides),
        data_root=tmp_path / "data",
    )
    runtime.start()
    try:
        yield runtime
    finally:
        runtime.stop()


def _request(url: str) -> tuple[int, dict[str, str], bytes]:
    try:
        with urllib.request.urlopen(  # ruff: ignore[suspicious-url-open-usage]
            url, timeout=WAIT_TIMEOUT_S
        ) as response:
            headers = response.headers
            return response.status, _lower_headers(headers), response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, _lower_headers(exc.headers), exc.read()


def _lower_headers(headers) -> dict[str, str]:
    return {
        str(key).lower(): str(value) for key, value in dict(headers).items()
    }


def _get_json(url: str) -> tuple[int, dict[str, str], dict]:
    status, headers, body = _request(url)
    return status, headers, json.loads(body)


def _ws_url(runtime: GatewayRuntime) -> str:
    return runtime.base_url.replace("http://", "ws://", 1) + "/ws"


def test_rate_limiting_is_disabled_by_default(tmp_path: Path) -> None:
    with _runtime(tmp_path) as runtime:
        statuses = [
            _request(f"{runtime.base_url}{INSTRUMENTS_PATH}")[0]
            for _ in range(110)
        ]

    # The default burst is 100, so an enabled limiter would have denied.
    assert statuses == [200] * 110


def test_burst_zero_rejects_the_first_request_with_the_error_envelope(
    tmp_path: Path,
) -> None:
    with _runtime(
        tmp_path,
        enabled=True,
        requests_per_second=50,
        burst=0,
    ) as runtime:
        status, headers, body = _get_json(
            f"{runtime.base_url}{INSTRUMENTS_PATH}"
        )

    assert status == 429
    assert headers["retry-after"] == "1"
    assert headers["content-type"].startswith("application/json")
    assert headers["x-request-id"]
    error = body["error"]
    assert error["code"] == "rate_limited"
    assert error["details"]["retry_after_s"] == 1
    assert error["request_id"] == headers["x-request-id"]


def test_exempt_paths_are_never_rate_limited(tmp_path: Path) -> None:
    with _runtime(
        tmp_path,
        enabled=True,
        requests_per_second=50,
        burst=0,
    ) as runtime:
        results = {
            path: _request(f"{runtime.base_url}{path}")[0]
            for path in (
                "/healthz",
                "/readyz",
                "/metrics",
                "/openapi.json",
                "/docs",
            )
        }
        # A non-exempt path proves the limiter is active in this app.
        limited_status = _request(
            f"{runtime.base_url}{INSTRUMENTS_PATH}"
        )[0]

    assert limited_status == 429
    assert results == {
        "/healthz": 200,
        "/readyz": 200,
        "/metrics": 200,
        "/openapi.json": 200,
        "/docs": 200,
    }


def test_rate_limited_counter_is_rendered_on_metrics(tmp_path: Path) -> None:
    config = _config(enabled=True, requests_per_second=50, burst=0)
    bus = config.bus.model_copy(update={"api_pub_bind": "tcp://127.0.0.1:0"})
    service = ApiService(
        config.model_copy(update={"bus": bus}),
        data_root=tmp_path,
    )
    try:
        service.start()
        status, _, _ = _request(
            f"{service.base_url}{INSTRUMENTS_PATH}"
        )
        assert status == 429

        metrics_status, _, body = _request(f"{service.base_url}/metrics")
        rendered = body.decode("utf-8")
    finally:
        service.stop()

    assert metrics_status == 200
    assert (
        f'mdgw_api_rate_limited_total{{path="{INSTRUMENTS_PATH}"}} 1'
        in rendered
    )


def test_per_ip_ws_session_cap_rejects_the_second_connection(
    tmp_path: Path,
) -> None:
    with _runtime(
        tmp_path,
        enabled=True,
        requests_per_second=100,
        burst=100,
        max_ws_sessions_per_ip=1,
    ) as runtime:
        url = _ws_url(runtime)

        async def rejected_second_connection() -> None:
            with pytest.raises(InvalidStatus):
                async with websockets.connect(url):
                    pass

        async def scenario() -> None:
            async with websockets.connect(url) as first:
                await first.send(json.dumps({"op": "ping"}))
                pong = json.loads(
                    await asyncio.wait_for(first.recv(), WAIT_TIMEOUT_S)
                )
                assert pong == {"op": "pong"}
                await rejected_second_connection()

        asyncio.run(scenario())
