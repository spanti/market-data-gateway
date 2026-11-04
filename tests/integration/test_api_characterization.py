# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[suspicious-url-open-usage]
"""Characterization oracle for the user-visible REST / WebSocket API.

These tests freeze the behaviour of the current aiohttp implementation before
it is replaced by FastAPI. They must keep passing after the implementation
switch, so they assert only wire semantics: status codes, media types, exact
decimal and nanosecond timestamp strings, the Message Envelope shape, the
unified error envelope, ``X-Request-ID`` and the WebSocket ops.

Everything enters through ``GatewayRuntime`` with ``FakeFeedHandler`` and a
temporary data root. There is deliberately no router-level test seam, and
Arrow responses are compared by table semantics rather than bytes.
"""

from __future__ import annotations

import asyncio
import json
import re
import urllib.error
import urllib.request
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiohttp
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mdgw.config import GatewayConfig, load_config
from mdgw.core.envelope import table_to_messages
from mdgw.core.schema import BAR_SCHEMA
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.runtime import GatewayRuntime

if TYPE_CHECKING:
    from http.client import HTTPMessage

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BTC = "binance:BTC/USDT"
TOPIC = f"market.bar.1m.{BTC}"
NS_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z")

ENVELOPE_KEYS = frozenset(
    {
        "schema_version",
        "data_type",
        "instrument_id",
        "event_ts",
        "ingest_ts",
        "feed",
        "seq",
        "payload",
    }
)
PAYLOAD_KEYS = frozenset(
    {
        "interval",
        "open_ts",
        "close_ts",
        "aggregation_source",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
        "revision",
    }
)

ARROW_STREAM = "application/vnd.apache.arrow.stream"
ARROW_FILE = "application/vnd.apache.arrow.file"
PARQUET = "application/vnd.apache.parquet"
CSV = "text/csv; charset=utf-8"


# -- fixtures and helpers ---------------------------------------------------


def _config(
    *,
    queue_size: int = 1000,
    lag_notice_interval_ms: int = 1000,
) -> GatewayConfig:
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    streaming = config.streaming.model_copy(
        update={
            "queue_size": queue_size,
            "lag_notice_interval_ms": lag_notice_interval_ms,
        }
    )
    return config.model_copy(update={"server": server, "streaming": streaming})


def _runtime(
    data_root: Path,
    bars: list[BarInput],
    *,
    queue_size: int = 1000,
    lag_notice_interval_ms: int = 1000,
) -> GatewayRuntime:
    feed = FakeFeedHandler(instrument_id=BTC, bars=bars)
    config = _config(
        queue_size=queue_size,
        lag_notice_interval_ms=lag_notice_interval_ms,
    )
    return GatewayRuntime(feed_handler=feed, config=config, data_root=data_root)


def _bar_input(minute: int, *, open_value: str = "65000.1") -> BarInput:
    return BarInput(
        open_ts=datetime(2024, 1, 1, 0, minute, tzinfo=UTC),
        open=open_value,
        high="65010.0",
        low="64990.0",
        close="65005.5",
        volume="12.34",
        trade_count=128,
    )


def _get(
    url: str,
    *,
    headers: dict[str, str] | None = None,
) -> tuple[int, HTTPMessage, bytes]:
    request = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, response.headers, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.headers, error.read()


def _media_type(headers: HTTPMessage) -> str:
    """Return the media type without optional parameters such as charset.

    aiohttp appends ``; charset=utf-8`` even to ``application/json``; the
    FastAPI migration emits the bare media type. The media type token is the
    stable part of the contract.
    """
    value = headers.get("Content-Type") or ""
    return value.split(";", 1)[0].strip().lower()


def _assert_message_envelope(
    message: dict[str, Any],
    *,
    minute: int = 0,
) -> None:
    open_iso = f"2024-01-01T00:{minute:02d}:00.000000000Z"
    close_iso = f"2024-01-01T00:{minute + 1:02d}:00.000000000Z"

    assert set(message) == ENVELOPE_KEYS
    assert set(message["payload"]) == PAYLOAD_KEYS
    assert message["schema_version"] == 1
    assert message["data_type"] == "bar"
    assert message["instrument_id"] == BTC
    assert message["feed"] == "fake"
    assert message["seq"] is None
    # The synthetic feed closes each 1m Bar at its event time.
    assert message["event_ts"] == close_iso
    assert NS_TIMESTAMP.fullmatch(message["ingest_ts"]) is not None

    payload = message["payload"]
    assert payload["interval"] == "1m"
    assert payload["open_ts"] == open_iso
    assert payload["close_ts"] == close_iso
    assert payload["aggregation_source"] == "external"
    assert payload["trade_count"] == 128
    assert payload["revision"] == 0
    for name in ("open", "high", "low", "close", "volume"):
        assert isinstance(payload[name], str)


def _assert_error(
    status: int,
    headers: HTTPMessage,
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
    assert headers.get("X-Request-ID") == error["request_id"]
    return error


def _csv_row(message: dict[str, Any]) -> list[str]:
    payload = message["payload"]
    values = {
        "schema_version": message["schema_version"],
        "data_type": message["data_type"],
        "instrument_id": message["instrument_id"],
        "interval": payload["interval"],
        "open_ts": payload["open_ts"],
        "close_ts": payload["close_ts"],
        "event_ts": message["event_ts"],
        "ingest_ts": message["ingest_ts"],
        "feed": message["feed"],
        "seq": message["seq"],
        "aggregation_source": payload["aggregation_source"],
        "open": payload["open"],
        "high": payload["high"],
        "low": payload["low"],
        "close": payload["close"],
        "volume": payload["volume"],
        "trade_count": payload["trade_count"],
        "revision": payload["revision"],
    }
    return [
        "" if values[name] is None else str(values[name])
        for name in BAR_SCHEMA.names
    ]


async def _connect_ws(
    runtime: GatewayRuntime,
) -> tuple[aiohttp.ClientSession, aiohttp.ClientWebSocketResponse]:
    session = aiohttp.ClientSession()
    url = runtime.base_url.replace("http://", "ws://", 1) + "/ws"
    ws = await session.ws_connect(url)
    return session, ws


async def _await_op(
    ws: aiohttp.ClientWebSocketResponse,
    op: str,
    *,
    timeout_s: float = 3.0,
) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            message = f"timed out waiting for WS op {op!r}"
            raise AssertionError(message)
        incoming = await asyncio.wait_for(ws.receive(), remaining)
        assert incoming.type == aiohttp.WSMsgType.TEXT, incoming
        payload = json.loads(incoming.data)
        if payload.get("op") == op:
            return payload


async def _ping(ws: aiohttp.ClientWebSocketResponse) -> None:
    await ws.send_json({"op": "ping"})
    assert await _await_op(ws, "pong") == {"op": "pong"}


# -- JSON -------------------------------------------------------------------


def test_json_bars_envelope_decimals_and_nanosecond_timestamps(
    tmp_path: Path,
) -> None:
    precise = "0.000000000000000001"
    with _runtime(
        tmp_path, [_bar_input(0, open_value=precise), _bar_input(1)]
    ) as runtime:
        runtime.start()
        runtime.ingest()
        status, headers, body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=1m"
        )

    assert status == 200
    assert _media_type(headers) == "application/json"
    payload = json.loads(body)
    assert set(payload) == {"bars"}
    messages = payload["bars"]
    assert len(messages) == 2
    for index, message in enumerate(messages):
        _assert_message_envelope(message, minute=index)

    # Decimal values stay exact decimal strings, never JSON numbers/floats.
    assert messages[0]["payload"]["open"] == precise
    assert messages[0]["payload"]["high"] == "65010.0"
    assert messages[0]["payload"]["volume"] == "12.34"
    assert messages[1]["payload"]["open"] == "65000.1"


# -- Arrow ------------------------------------------------------------------


def test_arrow_stream_is_table_semantically_equal_to_json(
    tmp_path: Path,
) -> None:
    precise = "0.000000000000000001"
    with _runtime(
        tmp_path, [_bar_input(0, open_value=precise), _bar_input(1)]
    ) as runtime:
        runtime.start()
        runtime.ingest()
        _, _, json_body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=1m"
        )
        status, headers, body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=1m&format=arrow"
        )

    assert status == 200
    assert headers.get("Content-Type") == ARROW_STREAM
    assert (
        headers.get("Content-Disposition")
        == 'inline; filename="1m-bars.arrow"'
    )

    table = pa.ipc.open_stream(pa.BufferReader(body)).read_all()
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 2
    assert table.column("open").to_pylist() == [
        Decimal(precise),
        Decimal("65000.1"),
    ]
    assert table_to_messages(table) == json.loads(json_body)["bars"]


# -- CSV --------------------------------------------------------------------


def test_csv_text_header_and_exact_decimal_rendering(tmp_path: Path) -> None:
    precise = "0.000000000000000001"
    with _runtime(
        tmp_path, [_bar_input(0, open_value=precise), _bar_input(1)]
    ) as runtime:
        runtime.start()
        runtime.ingest()
        _, _, json_body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=1m"
        )
        status, headers, body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=1m&format=csv"
        )

    assert status == 200
    assert headers.get("Content-Type") == CSV
    assert (
        headers.get("Content-Disposition")
        == 'inline; filename="1m-bars.csv"'
    )

    lines = body.decode("utf-8").splitlines()
    assert lines[0] == ",".join(BAR_SCHEMA.names)
    expected = [
        ",".join(_csv_row(message))
        for message in json.loads(json_body)["bars"]
    ]
    assert lines[1:] == expected
    open_index = BAR_SCHEMA.names.index("open")
    assert expected[0].split(",")[open_index] == precise
    assert "e-" not in expected[0].lower()


# -- export -----------------------------------------------------------------


def test_export_formats_preserve_headers_and_table_semantics(
    tmp_path: Path,
) -> None:
    with _runtime(
        tmp_path, [_bar_input(0), _bar_input(1)]
    ) as runtime:
        runtime.start()
        runtime.ingest()
        _, _, json_body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=1m"
        )
        parquet_status, parquet_headers, parquet_body = _get(
            f"{runtime.base_url}/api/v1/bars/export"
            "?interval=1m&format=parquet"
        )
        arrow_status, arrow_headers, arrow_body = _get(
            f"{runtime.base_url}/api/v1/bars/export"
            "?interval=1m&format=arrow"
        )

    messages = json.loads(json_body)["bars"]

    assert parquet_status == 200
    assert parquet_headers.get("Content-Type") == PARQUET
    assert (
        parquet_headers.get("Content-Disposition")
        == 'attachment; filename="1m-bars.parquet"'
    )
    parquet_table = pq.read_table(pa.BufferReader(parquet_body))
    assert parquet_table.schema == BAR_SCHEMA
    assert parquet_table.num_rows == 2
    assert table_to_messages(parquet_table) == messages

    assert arrow_status == 200
    assert arrow_headers.get("Content-Type") == ARROW_FILE
    assert (
        arrow_headers.get("Content-Disposition")
        == 'attachment; filename="1m-bars.arrow"'
    )
    arrow_table = pa.ipc.open_file(pa.BufferReader(arrow_body)).read_all()
    assert arrow_table.schema == BAR_SCHEMA
    assert arrow_table.num_rows == 2
    assert table_to_messages(arrow_table) == messages


# -- errors and request ids -------------------------------------------------


def test_validation_error_uses_the_unified_envelope(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(0)]) as runtime:
        runtime.start()
        runtime.ingest()
        status, headers, body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=2m"
        )

    error = _assert_error(
        status,
        headers,
        body,
        code="invalid_interval",
        expected_status=400,
    )
    assert error["details"] == {}


def test_unknown_instrument_uses_the_unified_envelope(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(0)]) as runtime:
        runtime.start()
        status, headers, body = _get(
            f"{runtime.base_url}/api/v1/instruments/binance:MISSING/USDT"
        )

    error = _assert_error(
        status,
        headers,
        body,
        code="instrument_not_found",
        expected_status=404,
    )
    assert error["details"] == {}
    assert "unknown Instrument" in error["message"]


def test_schema_version_mismatch_uses_the_unified_envelope(
    tmp_path: Path,
) -> None:
    with _runtime(tmp_path, [_bar_input(0)]) as runtime:
        runtime.start()
        runtime.ingest()

    descriptor_path = tmp_path / "bars" / "_dataset.json"
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    descriptor["schema_version"] = 2
    descriptor_path.write_text(json.dumps(descriptor), encoding="utf-8")

    with _runtime(tmp_path, []) as runtime:
        runtime.start()
        status, headers, body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=1m"
        )

    error = _assert_error(
        status,
        headers,
        body,
        code="schema_version_mismatch",
        expected_status=409,
    )
    assert error["details"] == {}


def test_request_id_is_echoed_from_the_client_and_generated_otherwise(
    tmp_path: Path,
) -> None:
    inbound = "migration-oracle-42"
    url = "/api/v1/bars?interval=2m"
    with _runtime(tmp_path, [_bar_input(0)]) as runtime:
        runtime.start()
        runtime.ingest()
        echoed_status, echoed_headers, echoed_body = _get(
            runtime.base_url + url,
            headers={"X-Request-ID": inbound},
        )
        generated_status, generated_headers, generated_body = _get(
            runtime.base_url + url
        )

    assert echoed_status == 400
    assert echoed_headers.get("X-Request-ID") == inbound
    assert json.loads(echoed_body)["error"]["request_id"] == inbound

    generated = generated_headers.get("X-Request-ID")
    assert generated_status == 400
    assert isinstance(generated, str)
    assert generated
    assert generated != inbound
    assert re.fullmatch(r"[0-9a-f]{32}", generated) is not None
    assert json.loads(generated_body)["error"]["request_id"] == generated


# -- WebSocket --------------------------------------------------------------


def test_websocket_subscribe_event_ping_and_unsubscribe(tmp_path: Path) -> None:
    bars = [_bar_input(0), _bar_input(1)]

    async def scenario() -> dict[str, Any]:
        session, ws = await _connect_ws(runtime)
        try:
            await ws.send_json({"op": "subscribe", "topics": [TOPIC]})
            # Pong proves the subscribe op was processed before ingestion.
            await _ping(ws)
            runtime.ingest()
            event = await _await_op(ws, "event")
            assert event["topic"] == TOPIC

            await ws.send_json({"op": "unsubscribe", "topics": [TOPIC]})
            await _ping(ws)
            runtime.ingest()
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(_await_op(ws, "event"), 0.5)
            # The connection is still usable after unsubscribing.
            await _ping(ws)
            return event
        finally:
            await ws.close()
            await session.close()

    with _runtime(tmp_path, bars) as runtime:
        runtime.start()
        event = asyncio.run(scenario())
        _, _, rest_body = _get(
            f"{runtime.base_url}/api/v1/bars?instrument={BTC}&interval=1m"
        )

    assert set(event) == {"op", "topic", "data"}
    assert event["op"] == "event"
    assert event["topic"] == TOPIC
    _assert_message_envelope(event["data"])
    # The WS Message Envelope is exactly the REST JSON form.
    assert event["data"] == json.loads(rest_body)["bars"][0]


def test_websocket_slow_consumer_gets_lag_notice_without_disconnect(
    tmp_path: Path,
) -> None:
    bars = [_bar_input(minute) for minute in range(30)]

    async def scenario() -> tuple[dict[str, Any], set[str]]:
        session, ws = await _connect_ws(runtime)
        try:
            await ws.send_json({"op": "subscribe", "topics": [TOPIC]})
            await _ping(ws)
            # One ingestion publishes all Bars in a single hub fan-out, so the
            # writer cannot drain a queue of size 1 in between.
            runtime.ingest()
            notice = await _await_op(ws, "lag_notice")
            seen: set[str] = set()
            while TOPIC not in seen:
                event = await _await_op(ws, "event")
                seen.add(event["topic"])
            # Still connected after the drop burst.
            await _ping(ws)
            return notice, seen
        finally:
            await ws.close()
            await session.close()

    with _runtime(
        tmp_path,
        bars,
        queue_size=1,
        lag_notice_interval_ms=0,
    ) as runtime:
        runtime.start()
        notice, seen = asyncio.run(scenario())

    assert set(notice) == {"op", "dropped", "since"}
    assert notice["op"] == "lag_notice"
    assert notice["dropped"] == len(bars) - 1
    assert NS_TIMESTAMP.fullmatch(notice["since"]) is not None
    assert seen == {TOPIC}


# -- operational endpoints --------------------------------------------------


def test_operational_endpoints_keep_their_shapes(tmp_path: Path) -> None:
    with _runtime(
        tmp_path, [_bar_input(0), _bar_input(1)]
    ) as runtime:
        runtime.start()
        runtime.ingest()
        health_status, health_headers, health_body = _get(
            f"{runtime.base_url}/healthz"
        )
        ready_status, _, ready_body = _get(f"{runtime.base_url}/readyz")
        metrics_status, metrics_headers, metrics_body = _get(
            f"{runtime.base_url}/metrics"
        )
        status, status_headers, status_body = _get(
            f"{runtime.base_url}/api/v1/status"
        )

    assert health_status == 200
    assert _media_type(health_headers) == "application/json"
    assert json.loads(health_body) == {"status": "ok", "service": "api"}

    assert ready_status == 200
    assert json.loads(ready_body) == {"ready": True, "reason": "ok"}

    assert metrics_status == 200
    assert metrics_headers.get("Content-Type") == "text/plain; version=0.0.4"
    # GatewayRuntime supplies no renderer, so the body stays empty.
    assert metrics_body == b""

    assert status == 200
    assert _media_type(status_headers) == "application/json"
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
