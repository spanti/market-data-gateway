# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[suspicious-url-open-usage]
"""End-to-end tests for the FastAPI Bar history surface (ticket 06).

The FastAPI application is served through the real `AsgiServer` lifecycle on an
ephemeral socket, and every request is a real HTTP request, so these assertions
mirror `test_api_characterization.py` at the wire level. Arrow equality is
table-semantic, never byte-wise.
"""

from __future__ import annotations

import json
import re
import tempfile
import threading
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.config import load_config
from mdgw.core.envelope import table_to_messages
from mdgw.core.schema import BAR_SCHEMA
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.runtime import GatewayRuntime
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-fastapi-bars"
BTC = "binance:BTC/USDT"
NS_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z")
GENERATED_REQUEST_ID = re.compile(r"[0-9a-f]{32}")

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


def _config():
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    return config.model_copy(update={"server": server})


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


def _seed(tmp_path: Path, bars: list[BarInput]) -> ParquetBarStore:
    """Write Bars through the runtime, then return the read-side store."""
    feed = FakeFeedHandler(instrument_id=BTC, bars=bars)
    runtime = GatewayRuntime(
        feed_handler=feed,
        config=_config(),
        data_root=tmp_path,
    )
    runtime.start()
    try:
        runtime.ingest()
    finally:
        runtime.stop()
    # `GatewayRuntime.stop()` closes the Store it owns (including its DuckDB
    # connection cache). The REST surface under test reads the same Dataset
    # through its own read-side Store, exactly like the API service role.
    return ParquetBarStore(tmp_path)


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
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(url, headers=headers or {})
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


def _media_type(headers: dict[str, str]) -> str:
    value = _header(headers, "Content-Type") or ""
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


# -- JSON -------------------------------------------------------------------


def test_json_bars_envelope_decimals_and_nanosecond_timestamps(
    tmp_path: Path,
) -> None:
    precise = "0.000000000000000001"
    store = _seed(
        tmp_path, [_bar_input(0, open_value=precise), _bar_input(1)]
    )
    with _serve(create_app(store)) as server:
        status, headers, body = _request(
            f"{server.base_url}/api/v1/bars?interval=1m"
        )

    assert status == 200
    assert _header(headers, "Content-Type") == "application/json"
    payload = json.loads(body)
    assert set(payload) == {"bars"}
    messages = payload["bars"]
    assert len(messages) == 2
    # Non-paginated JSON has no ordering contract: validate each message from
    # its own open_ts instead of relying on physical scan order.
    for message in messages:
        minute_index = int(message["payload"]["open_ts"][14:16])
        _assert_message_envelope(message, minute=minute_index)

    by_open = {message["payload"]["open"]: message for message in messages}
    assert by_open[precise]["payload"]["high"] == "65010.0"
    assert by_open[precise]["payload"]["volume"] == "12.34"
    assert by_open["65000.1"]["payload"]["open_ts"].endswith(
        "T00:01:00.000000000Z"
    )


def test_json_limit_caps_rows_and_empty_result_is_an_empty_array(
    tmp_path: Path,
) -> None:
    store = _seed(
        tmp_path, [_bar_input(0), _bar_input(1), _bar_input(2)]
    )
    with _serve(create_app(store)) as server:
        limited_status, _, limited_body = _request(
            f"{server.base_url}/api/v1/bars?interval=1m&limit=1"
        )
        empty_status, _, empty_body = _request(
            f"{server.base_url}/api/v1/bars?interval=1m"
            "&start=2025-01-01T00:00:00Z"
        )

    assert limited_status == 200
    assert len(json.loads(limited_body)["bars"]) == 1
    assert empty_status == 200
    assert json.loads(empty_body) == {"bars": []}


# -- Arrow ------------------------------------------------------------------


def test_arrow_stream_is_table_semantically_equal_to_json(
    tmp_path: Path,
) -> None:
    precise = "0.000000000000000001"
    store = _seed(
        tmp_path, [_bar_input(0, open_value=precise), _bar_input(1)]
    )
    with _serve(create_app(store)) as server:
        _, _, json_body = _request(
            f"{server.base_url}/api/v1/bars?interval=1m"
        )
        status, headers, body = _request(
            f"{server.base_url}/api/v1/bars?interval=1m&format=arrow"
        )

    assert status == 200
    assert _header(headers, "Content-Type") == ARROW_STREAM
    assert (
        _header(headers, "Content-Disposition")
        == 'inline; filename="1m-bars.arrow"'
    )

    table = pa.ipc.open_stream(pa.BufferReader(body)).read_all()
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 2
    assert sorted(table.column("open").to_pylist()) == sorted(
        [Decimal(precise), Decimal("65000.1")]
    )
    # Non-paginated responses have no ordering contract; compare exact message
    # content and multiplicity, not physical scan order.
    assert sorted(
        json.dumps(message, sort_keys=True)
        for message in table_to_messages(table)
    ) == sorted(
        json.dumps(message, sort_keys=True)
        for message in json.loads(json_body)["bars"]
    )


def test_empty_arrow_stream_is_a_valid_stream_with_the_canonical_schema(
    tmp_path: Path,
) -> None:
    store = _seed(tmp_path, [_bar_input(0)])
    with _serve(create_app(store)) as server:
        status, headers, body = _request(
            f"{server.base_url}/api/v1/bars?interval=1m&format=arrow"
            "&start=2025-01-01T00:00:00Z"
        )

    assert status == 200
    assert _header(headers, "Content-Type") == ARROW_STREAM
    table = pa.ipc.open_stream(pa.BufferReader(body)).read_all()
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 0


# -- CSV --------------------------------------------------------------------


def test_csv_text_header_and_exact_decimal_rendering(tmp_path: Path) -> None:
    precise = "0.000000000000000001"
    store = _seed(
        tmp_path, [_bar_input(0, open_value=precise), _bar_input(1)]
    )
    with _serve(create_app(store)) as server:
        _, _, json_body = _request(
            f"{server.base_url}/api/v1/bars?interval=1m"
        )
        status, headers, body = _request(
            f"{server.base_url}/api/v1/bars?interval=1m&format=csv"
        )

    assert status == 200
    assert _header(headers, "Content-Type") == CSV
    assert (
        _header(headers, "Content-Disposition")
        == 'inline; filename="1m-bars.csv"'
    )

    lines = body.decode("utf-8").splitlines()
    assert lines[0] == ",".join(BAR_SCHEMA.names)
    expected = [
        ",".join(_csv_row(message))
        for message in json.loads(json_body)["bars"]
    ]
    # Non-paginated output has no ordering contract; compare exact content and
    # multiplicity instead of physical scan order.
    assert sorted(lines[1:]) == sorted(expected)
    open_index = BAR_SCHEMA.names.index("open")
    precise_row = next(
        line
        for line in lines[1:]
        if line.split(",")[open_index] == precise
    )
    assert "e-" not in precise_row.lower()


def test_empty_csv_is_header_only(tmp_path: Path) -> None:
    store = _seed(tmp_path, [_bar_input(0)])
    with _serve(create_app(store)) as server:
        status, headers, body = _request(
            f"{server.base_url}/api/v1/bars?interval=1m&format=csv"
            "&start=2025-01-01T00:00:00Z"
        )

    assert status == 200
    assert _header(headers, "Content-Type") == CSV
    assert body.decode("utf-8").splitlines() == [",".join(BAR_SCHEMA.names)]


# -- export -----------------------------------------------------------------


def test_export_formats_preserve_headers_length_and_table_semantics(
    tmp_path: Path,
) -> None:
    store = _seed(tmp_path, [_bar_input(0), _bar_input(1)])
    with _serve(create_app(store)) as server:
        _, _, json_body = _request(
            f"{server.base_url}/api/v1/bars?interval=1m"
        )
        parquet_status, parquet_headers, parquet_body = _request(
            f"{server.base_url}/api/v1/bars/export"
            "?interval=1m&format=parquet"
        )
        arrow_status, arrow_headers, arrow_body = _request(
            f"{server.base_url}/api/v1/bars/export"
            "?interval=1m&format=arrow"
        )

    # Non-paginated exports have no ordering contract; compare exact content
    # and multiplicity, not physical scan order.
    messages = sorted(
        json.dumps(message, sort_keys=True)
        for message in json.loads(json_body)["bars"]
    )

    def canonical(messages_: list[dict]) -> list[str]:
        return sorted(
            json.dumps(message, sort_keys=True) for message in messages_
        )

    assert parquet_status == 200
    assert _header(parquet_headers, "Content-Type") == PARQUET
    assert (
        _header(parquet_headers, "Content-Disposition")
        == 'attachment; filename="1m-bars.parquet"'
    )
    assert int(_header(parquet_headers, "Content-Length") or -1) == len(
        parquet_body
    )
    parquet_table = pq.read_table(pa.BufferReader(parquet_body))
    assert parquet_table.schema == BAR_SCHEMA
    assert parquet_table.num_rows == 2
    assert canonical(table_to_messages(parquet_table)) == messages

    assert arrow_status == 200
    assert _header(arrow_headers, "Content-Type") == ARROW_FILE
    assert (
        _header(arrow_headers, "Content-Disposition")
        == 'attachment; filename="1m-bars.arrow"'
    )
    assert int(_header(arrow_headers, "Content-Length") or -1) == len(
        arrow_body
    )
    arrow_table = pa.ipc.open_file(pa.BufferReader(arrow_body)).read_all()
    assert arrow_table.schema == BAR_SCHEMA
    assert arrow_table.num_rows == 2
    assert canonical(table_to_messages(arrow_table)) == messages


def test_empty_exports_are_valid_files_with_the_canonical_schema(
    tmp_path: Path,
) -> None:
    store = _seed(tmp_path, [_bar_input(0)])
    with _serve(create_app(store)) as server:
        _, _, parquet_body = _request(
            f"{server.base_url}/api/v1/bars/export"
            "?interval=1m&format=parquet&start=2025-01-01T00:00:00Z"
        )
        _, _, arrow_body = _request(
            f"{server.base_url}/api/v1/bars/export"
            "?interval=1m&format=arrow&start=2025-01-01T00:00:00Z"
        )

    parquet_table = pq.read_table(pa.BufferReader(parquet_body))
    arrow_table = pa.ipc.open_file(pa.BufferReader(arrow_body)).read_all()
    assert parquet_table.schema == BAR_SCHEMA
    assert arrow_table.schema == BAR_SCHEMA
    assert parquet_table.num_rows == 0
    assert arrow_table.num_rows == 0


def test_export_cleans_up_its_temporary_directory(tmp_path: Path) -> None:
    temp_root = Path(tempfile.gettempdir())
    before = {entry.name for entry in temp_root.glob("mdgw-export-*")}

    store = _seed(tmp_path, [_bar_input(0)])
    with _serve(create_app(store)) as server:
        parquet_status, _, _ = _request(
            f"{server.base_url}/api/v1/bars/export"
            "?interval=1m&format=parquet"
        )
        arrow_status, _, _ = _request(
            f"{server.base_url}/api/v1/bars/export?interval=1m&format=arrow"
        )

    assert parquet_status == 200
    assert arrow_status == 200
    after = {entry.name for entry in temp_root.glob("mdgw-export-*")}
    assert after - before == set()


# -- errors and request ids -------------------------------------------------


@pytest.mark.parametrize(
    ("path", "code"),
    [
        ("/api/v1/bars?interval=2m", "invalid_interval"),
        ("/api/v1/bars", "invalid_interval"),
        (
            "/api/v1/bars?interval=1m&instrument=BTCUSDT",
            "invalid_instrument",
        ),
        ("/api/v1/bars?interval=1m&start=abc", "invalid_range"),
        ("/api/v1/bars?interval=1m&end=abc", "invalid_range"),
        ("/api/v1/bars?interval=1m&limit=abc", "invalid_range"),
        ("/api/v1/bars?interval=1m&limit=-1", "invalid_range"),
        (
            (
                "/api/v1/bars?interval=1m"
                "&start=2024-01-02T00:00:00Z&end=2024-01-01T00:00:00Z"
            ),
            "invalid_range",
        ),
        ("/api/v1/bars?interval=1m&format=xml", "invalid_format"),
        (
            "/api/v1/bars/export?interval=1m&format=csv",
            "invalid_format",
        ),
    ],
)
def test_invalid_parameters_use_stable_codes_without_422_or_detail(
    tmp_path: Path,
    path: str,
    code: str,
) -> None:
    store = _seed(tmp_path, [_bar_input(0)])
    with _serve(create_app(store)) as server:
        status, headers, body = _request(server.base_url + path)

    error = _assert_error(
        status,
        headers,
        body,
        code=code,
        expected_status=400,
    )
    assert error["details"] == {}
    assert "detail" not in json.loads(body)


def test_schema_version_mismatch_returns_409_for_bars_and_export(  # ruff: ignore[too-many-locals] - wire assertions
    tmp_path: Path,
) -> None:
    store = _seed(tmp_path, [_bar_input(0)])

    descriptor_path = tmp_path / "bars" / "_dataset.json"
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    descriptor["schema_version"] = 2
    descriptor_path.write_text(json.dumps(descriptor), encoding="utf-8")

    temp_root = Path(tempfile.gettempdir())
    before = {entry.name for entry in temp_root.glob("mdgw-export-*")}

    with _serve(create_app(store)) as server:
        status, headers, body = _request(
            f"{server.base_url}/api/v1/bars?interval=1m"
        )
        arrow_status, arrow_headers, arrow_body = _request(
            f"{server.base_url}/api/v1/bars?interval=1m&format=arrow"
        )
        export_status, export_headers, export_body = _request(
            f"{server.base_url}/api/v1/bars/export?interval=1m"
        )

    error = _assert_error(
        status,
        headers,
        body,
        code="schema_version_mismatch",
        expected_status=409,
    )
    assert error["details"] == {}
    _assert_error(
        arrow_status,
        arrow_headers,
        arrow_body,
        code="schema_version_mismatch",
        expected_status=409,
    )
    _assert_error(
        export_status,
        export_headers,
        export_body,
        code="schema_version_mismatch",
        expected_status=409,
    )

    after = {entry.name for entry in temp_root.glob("mdgw-export-*")}
    assert after - before == set()


def test_json_row_ceiling_is_bounded_by_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("mdgw.api.routes.bars.JSON_ROW_LIMIT", 1)
    store = _seed(
        tmp_path, [_bar_input(0), _bar_input(1), _bar_input(2)]
    )
    with _serve(create_app(store)) as server:
        limited_status, _, limited_body = _request(
            f"{server.base_url}/api/v1/bars?interval=1m&limit=1"
        )
        status, headers, body = _request(
            f"{server.base_url}/api/v1/bars?interval=1m"
        )

    assert limited_status == 200
    assert len(json.loads(limited_body)["bars"]) == 1

    error = _assert_error(
        status,
        headers,
        body,
        code="range_too_large",
        expected_status=400,
    )
    assert error["details"] == {"row_count": 3, "limit": 1}


def test_request_id_is_echoed_from_the_client_and_generated_otherwise(
    tmp_path: Path,
) -> None:
    inbound = "migration-oracle-42"
    url = "/api/v1/bars?interval=2m"
    store = _seed(tmp_path, [_bar_input(0)])
    with _serve(create_app(store)) as server:
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
