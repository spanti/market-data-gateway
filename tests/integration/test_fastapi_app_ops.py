# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[suspicious-url-open-usage]
"""End-to-end tests for the FastAPI application's ops and Instrument surface.

The app is served through the real `AsgiServer` lifecycle on an ephemeral
socket; requests use `urllib` so the assertions observe wire behaviour only.
Bars are written by `GatewayRuntime` (the old server stack) before the FastAPI
app is built, keeping this ticket independent of the Bars migration.
"""

from __future__ import annotations

import json
import logging
import re
import socket
import threading
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import pytest
from fastapi import FastAPI, Query

from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.api.errors import install_exception_handlers
from mdgw.api.middleware import RequestContextMiddleware
from mdgw.config import load_config
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.metrics import MetricsRegistry
from mdgw.runtime import GatewayRuntime

if TYPE_CHECKING:
    from collections.abc import Generator

    from mdgw.reference import InstrumentStore
    from mdgw.store.parquet_dataset import ParquetBarStore

PROJECT_ROOT = Path(__file__).resolve().parents[2]
HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-fastapi-ops"
BTC = "binance:BTC/USDT"
NS_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z")
GENERATED_REQUEST_ID = re.compile(r"[0-9a-f]{32}")

INSTRUMENTS_YAML = """
- instrument_id: "binance:BTC/USDT"
  venue: binance
  symbol: "BTC/USDT"
  asset_class: crypto
  instrument_type: spot
  base: BTC
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.00001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
- instrument_id: "binance:ETH/USDT"
  venue: binance
  symbol: "ETH/USDT"
  asset_class: crypto
  instrument_type: spot
  base: ETH
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.0001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
- instrument_id: "binance:OLD/USDT"
  venue: binance
  symbol: "OLD/USDT"
  asset_class: crypto
  instrument_type: spot
  base: OLD
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.0001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
  delisted_ts: "2020-01-01T00:00:00Z"
""".strip()


# -- fixtures and helpers ---------------------------------------------------


def _config(instruments_path: Path):
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    instruments = config.instruments.model_copy(update={"path": instruments_path})
    return config.model_copy(update={"server": server, "instruments": instruments})


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


@pytest.fixture
def seeded(tmp_path: Path) -> tuple[ParquetBarStore, InstrumentStore]:
    """Write two Bars through the runtime, then return the read-side stores."""
    instruments = tmp_path / "instruments"
    instruments.mkdir()
    (instruments / "instruments.yaml").write_text(
        INSTRUMENTS_YAML + "\n", encoding="utf-8"
    )
    feed = FakeFeedHandler(
        instrument_id=BTC,
        bars=[_bar_input(0), _bar_input(1)],
    )
    runtime = GatewayRuntime(
        feed_handler=feed,
        config=_config(instruments),
        data_root=tmp_path / "data",
    )
    runtime.start()
    try:
        runtime.ingest()
    finally:
        runtime.stop()

    assert runtime.instrument_store is not None
    return runtime.store, runtime.instrument_store


@contextmanager
def _serve(app: Any) -> Generator[AsgiServer]:
    server = AsgiServer()
    try:
        server.start(app, host=HOST, port=0, thread_name=THREAD_NAME)
        yield server
    finally:
        server.stop()

    assert _threads_named(THREAD_NAME) == []


def _threads_named(name: str) -> list[threading.Thread]:
    return [thread for thread in threading.enumerate() if thread.name == name]


def _request(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(
        url,
        headers=headers or {},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), error.read()


def _header(headers: dict[str, str], name: str) -> str | None:
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


def _assert_error(
    status: int,
    headers: dict[str, str],
    body: bytes,
    *,
    code: str,
    expected_status: int,
) -> dict[str, Any]:
    payload = json.loads(body)
    assert status == expected_status
    assert set(payload) == {"error"}
    error = payload["error"]
    assert set(error) == {"code", "message", "details", "request_id"}
    assert error["code"] == code
    assert isinstance(error["message"], str)
    assert error["message"]
    assert isinstance(error["details"], dict)
    assert isinstance(error["request_id"], str)
    assert error["request_id"]
    assert _header(headers, "X-Request-ID") == error["request_id"]
    return error


def _new_app(
    seeded: tuple[ParquetBarStore, InstrumentStore],
    **kwargs: Any,
):
    store, instrument_store = seeded
    return create_app(
        store,
        instrument_store=instrument_store,
        service_role="api",
        **kwargs,
    )


# -- operational endpoints --------------------------------------------------


def test_ops_endpoints_match_the_characterization(  # ruff: ignore[too-many-locals]
    seeded: tuple[ParquetBarStore, InstrumentStore],
) -> None:
    with _serve(_new_app(seeded)) as server:
        health_status, health_headers, health_body = _request(
            f"{server.base_url}/healthz"
        )
        ready_status, _, ready_body = _request(f"{server.base_url}/readyz")
        metrics_status, metrics_headers, metrics_body = _request(
            f"{server.base_url}/metrics"
        )
        status, status_headers, status_body = _request(
            f"{server.base_url}/api/v1/status"
        )
        service_status, _, service_body = _request(
            f"{server.base_url}/status"
        )

    assert health_status == 200
    assert _header(health_headers, "Content-Type") == "application/json"
    assert json.loads(health_body) == {"status": "ok", "service": "api"}
    health_request_id = _header(health_headers, "X-Request-ID")
    assert health_request_id is not None
    assert GENERATED_REQUEST_ID.fullmatch(health_request_id) is not None

    assert ready_status == 200
    assert json.loads(ready_body) == {"ready": True, "reason": "ok"}

    assert metrics_status == 200
    assert (
        _header(metrics_headers, "Content-Type")
        == "text/plain; version=0.0.4"
    )
    # No renderer was supplied, matching GatewayRuntime's wiring today.
    assert metrics_body == b""

    assert status == 200
    assert _header(status_headers, "Content-Type") == "application/json"
    payload = json.loads(status_body)
    assert payload["dataset"] == "bars"
    assert payload["schema_version"] == 1
    assert payload["active_file_count"] == 1
    assert payload["median_file_size_bytes"] > 0
    assert NS_TIMESTAMP.fullmatch(payload["last_flushed_ts"]) is not None
    assert set(payload) == {
        "dataset",
        "schema_version",
        "last_flushed_ts",
        "active_file_count",
        "median_file_size_bytes",
        "intervals",
    }
    assert set(payload["intervals"]) == {"1m"}
    assert set(payload["intervals"]["1m"]) == {
        "last_flushed_ts",
        "active_file_count",
        "median_file_size_bytes",
    }
    assert (
        payload["intervals"]["1m"]["last_flushed_ts"]
        == payload["last_flushed_ts"]
    )

    # Without a status provider `/status` is the Dataset status payload.
    assert service_status == 200
    assert json.loads(service_body) == payload


def test_status_provider_is_merged_and_readyz_can_be_503(
    seeded: tuple[ParquetBarStore, InstrumentStore],
) -> None:
    app = _new_app(
        seeded,
        status_provider=lambda: {"service": "api", "role": "api"},
        ready_provider=lambda: {"ready": False, "reason": "not warmed up"},
    )
    with _serve(app) as server:
        status, _, status_body = _request(f"{server.base_url}/status")
        ready_status, _, ready_body = _request(f"{server.base_url}/readyz")

    payload = json.loads(status_body)
    assert status == 200
    assert payload["service"] == "api"
    assert payload["role"] == "api"
    assert payload["dataset"] == "bars"

    assert ready_status == 503
    assert json.loads(ready_body) == {
        "ready": False,
        "reason": "not warmed up",
    }


def test_metrics_renderer_request_metrics_and_completion_log(
    seeded: tuple[ParquetBarStore, InstrumentStore],
    caplog: pytest.LogCaptureFixture,
) -> None:
    metrics = MetricsRegistry()
    app = _new_app(
        seeded,
        metrics=metrics,
        metrics_render=metrics.render,
    )
    with (
        caplog.at_level(logging.INFO, logger="mdgw.api.middleware"),
        _serve(app) as server,
    ):
        _request(f"{server.base_url}/healthz")
        _request(f"{server.base_url}/healthz")
        _request(f"{server.base_url}/api/v1/instruments/{BTC}")
        _request(f"{server.base_url}/does-not-exist")
        status, headers, body = _request(f"{server.base_url}/metrics")

    assert status == 200
    assert _header(headers, "Content-Type") == "text/plain; version=0.0.4"
    text = body.decode("utf-8")
    assert (
        'mdgw_api_http_requests_total{method="GET",path="/healthz",status="200"} 2'
        in text
    )
    # Dynamic routes use the matched template, unknown paths the raw path.
    assert (
        'path="/api/v1/instruments/{instrument_id:path}",status="200"'
        in text
    )
    assert 'path="/does-not-exist",status="404"' in text

    completed = [
        record
        for record in caplog.records
        if record.getMessage() == "http request completed"
    ]
    assert completed
    first = completed[0]
    assert getattr(first, "event", None) == "api.http_request"
    assert getattr(first, "request_id", None)
    assert getattr(first, "status", None) == 200


# -- instrument endpoints ---------------------------------------------------


def test_instrument_endpoints_match_the_characterization(  # ruff: ignore[too-many-locals]
    seeded: tuple[ParquetBarStore, InstrumentStore],
) -> None:
    with _serve(_new_app(seeded)) as server:
        status, headers, body = _request(
            f"{server.base_url}/api/v1/instruments"
        )
        _, _, included_body = _request(
            f"{server.base_url}/api/v1/instruments?include_delisted=true"
        )
        _, _, by_asset_body = _request(
            f"{server.base_url}/api/v1/instruments?asset_class=crypto"
        )
        by_venue_status, _, by_venue_body = _request(
            f"{server.base_url}/api/v1/instruments?venue=unknown"
        )
        raw_status, _, raw_body = _request(
            f"{server.base_url}/api/v1/instruments/{BTC}"
        )
        encoded_status, _, encoded_body = _request(
            f"{server.base_url}/api/v1/instruments/binance%3ABTC%2FUSDT"
        )
        delisted_status, _, delisted_body = _request(
            f"{server.base_url}/api/v1/instruments/binance:OLD/USDT"
        )

    assert status == 200
    assert _header(headers, "Content-Type") == "application/json"
    payload = json.loads(body)
    assert [item["instrument_id"] for item in payload["instruments"]] == [
        "binance:BTC/USDT",
        "binance:ETH/USDT",
    ]
    btc = payload["instruments"][0]
    assert btc["instrument_id"] == BTC
    assert btc["venue"] == "binance"
    assert btc["symbol"] == "BTC/USDT"
    assert btc["asset_class"] == "crypto"
    assert btc["instrument_type"] == "spot"
    assert btc["base"] == "BTC"
    assert btc["quote"] == "USDT"
    assert btc["tick_size"] == "0.01"
    assert btc["lot_size"] == "0.00001"
    assert btc["multiplier"] == "1.0"
    assert btc["timezone"] == "UTC"
    assert btc["listing_ts"] == "2017-08-17T04:00:00.000000000Z"
    assert btc["delisted_ts"] is None

    included_ids = [
        item["instrument_id"]
        for item in json.loads(included_body)["instruments"]
    ]
    assert included_ids == [
        "binance:BTC/USDT",
        "binance:ETH/USDT",
        "binance:OLD/USDT",
    ]
    assert len(json.loads(by_asset_body)["instruments"]) == 2
    assert by_venue_status == 200
    assert json.loads(by_venue_body)["instruments"] == []

    assert raw_status == 200
    instrument = json.loads(raw_body)["instrument"]
    assert instrument["instrument_id"] == BTC
    assert instrument["tick_size"] == "0.01"
    assert encoded_status == 200
    assert json.loads(encoded_body)["instrument"] == instrument

    assert delisted_status == 200
    delisted = json.loads(delisted_body)["instrument"]
    assert delisted["instrument_id"] == "binance:OLD/USDT"
    assert delisted["delisted_ts"] == "2020-01-01T00:00:00.000000000Z"


def test_unknown_and_malformed_instrument_ids_use_the_unified_envelope(
    seeded: tuple[ParquetBarStore, InstrumentStore],
) -> None:
    with _serve(_new_app(seeded)) as server:
        missing_status, missing_headers, missing_body = _request(
            f"{server.base_url}/api/v1/instruments/binance:MISSING/USDT"
        )
        malformed_status, malformed_headers, malformed_body = _request(
            f"{server.base_url}/api/v1/instruments/BTCUSDT"
        )

    missing = _assert_error(
        missing_status,
        missing_headers,
        missing_body,
        code="instrument_not_found",
        expected_status=404,
    )
    assert missing["details"] == {}
    assert "unknown Instrument" in missing["message"]

    malformed = _assert_error(
        malformed_status,
        malformed_headers,
        malformed_body,
        code="invalid_instrument",
        expected_status=400,
    )
    assert malformed["details"] == {}


def test_invalid_include_delisted_is_invalid_parameter(
    seeded: tuple[ParquetBarStore, InstrumentStore],
) -> None:
    with _serve(_new_app(seeded)) as server:
        status, headers, body = _request(
            f"{server.base_url}/api/v1/instruments?include_delisted=maybe"
        )

    error = _assert_error(
        status,
        headers,
        body,
        code="invalid_parameter",
        expected_status=400,
    )
    assert error["details"] == {}


# -- shared HTTP layer ------------------------------------------------------


def test_unknown_path_returns_not_found_envelope(
    seeded: tuple[ParquetBarStore, InstrumentStore],
) -> None:
    with _serve(_new_app(seeded)) as server:
        status, headers, body = _request(f"{server.base_url}/does-not-exist")

    _assert_error(
        status,
        headers,
        body,
        code="not_found",
        expected_status=404,
    )


def test_unknown_method_returns_method_not_allowed_envelope(
    seeded: tuple[ParquetBarStore, InstrumentStore],
) -> None:
    with _serve(_new_app(seeded)) as server:
        status, headers, body = _request(
            f"{server.base_url}/healthz",
            method="POST",
        )

    _assert_error(
        status,
        headers,
        body,
        code="method_not_allowed",
        expected_status=405,
    )


@pytest.mark.parametrize(
    ("query", "code"),
    [
        ("interval=abc", "invalid_interval"),
        ("instrument=abc", "invalid_instrument"),
        ("instrument_id=abc", "invalid_instrument"),
        ("start=abc", "invalid_range"),
        ("end=abc", "invalid_range"),
        ("limit=abc", "invalid_range"),
        ("format=abc", "invalid_format"),
        ("other=abc", "invalid_parameter"),
    ],
)
def test_validation_errors_are_mapped_without_422_or_detail(
    query: str,
    code: str,
) -> None:
    app = FastAPI(redirect_slashes=False)
    install_exception_handlers(app)
    app.add_middleware(RequestContextMiddleware)

    @app.get("/validate")
    async def validate(  # ruff: ignore[too-many-arguments,too-many-positional-arguments]
        interval: int = 1,
        instrument: int = 1,
        instrument_id: int = 1,
        start: int = 1,
        end: int = 1,
        limit: int = 1,
        output_format: Annotated[int, Query(alias="format")] = 1,
        other: int = 1,
    ) -> dict[str, bool]:
        _ = (
            interval,
            instrument,
            instrument_id,
            start,
            end,
            limit,
            output_format,
            other,
        )
        return {"ok": True}

    with _serve(app) as server:
        status, headers, body = _request(
            f"{server.base_url}/validate?{query}"
        )

    error = _assert_error(
        status,
        headers,
        body,
        code=code,
        expected_status=400,
    )
    assert error["details"] == {}
    assert "detail" not in json.loads(body)


def test_unexpected_exception_returns_internal_error_with_request_id(
    seeded: tuple[ParquetBarStore, InstrumentStore],
    caplog: pytest.LogCaptureFixture,
) -> None:
    def _explode() -> dict[str, Any]:
        msg = "provider exploded"
        raise RuntimeError(msg)

    app = _new_app(seeded, status_provider=_explode)
    with caplog.at_level(logging.ERROR), _serve(app) as server:
        status, headers, body = _request(f"{server.base_url}/status")

    error = _assert_error(
        status,
        headers,
        body,
        code="internal_error",
        expected_status=500,
    )
    assert _header(headers, "X-Request-ID") == error["request_id"]
    failures = [
        record
        for record in caplog.records
        if record.getMessage() == "http request failed"
    ]
    assert failures
    assert failures[0].exc_info is not None
    assert getattr(failures[0], "event", None) == "api.http_request_failed"
    assert getattr(failures[0], "request_id", None) == error["request_id"]


def test_request_id_is_echoed_from_the_client_and_generated_otherwise(
    seeded: tuple[ParquetBarStore, InstrumentStore],
) -> None:
    inbound = "migration-oracle-42"
    url = "/api/v1/instruments?include_delisted=maybe"
    with _serve(_new_app(seeded)) as server:
        echoed_status, echoed_headers, echoed_body = _request(
            server.base_url + url,
            headers={"X-Request-ID": inbound},
        )
        generated_status, generated_headers, generated_body = _request(
            server.base_url + url
        )

    assert echoed_status == 400
    assert _header(echoed_headers, "X-Request-ID") == inbound
    assert json.loads(echoed_body)["error"]["request_id"] == inbound

    generated = _header(generated_headers, "X-Request-ID")
    assert generated_status == 400
    assert isinstance(generated, str)
    assert generated
    assert generated != inbound
    assert GENERATED_REQUEST_ID.fullmatch(generated) is not None
    assert json.loads(generated_body)["error"]["request_id"] == generated


def test_server_thread_shuts_down_cleanly_and_releases_the_port(
    seeded: tuple[ParquetBarStore, InstrumentStore],
) -> None:
    server = AsgiServer()
    port = server.start(
        _new_app(seeded),
        host=HOST,
        port=0,
        thread_name=THREAD_NAME,
    )
    assert _threads_named(THREAD_NAME)

    server.stop()

    assert _threads_named(THREAD_NAME) == []
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind((HOST, port))
