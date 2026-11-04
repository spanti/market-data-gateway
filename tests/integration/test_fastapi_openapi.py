# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[suspicious-url-open-usage]
"""End-to-end tests for the generated OpenAPI document (ticket 08).

The FastAPI app is served through the real `AsgiServer` on an ephemeral
socket, so these assertions observe the document clients actually download
from `/openapi.json`, never an in-process shortcut. The Data Contract remains
the normative source; this file only pins the documentation surface.
"""

from __future__ import annotations

import json
import re
import threading
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.api.ws_hub import WebSocketHub
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-fastapi-openapi"

NS_TIMESTAMP = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z"
)
DECIMAL_TEXT = re.compile(r"-?\d+(?:\.\d+)?")

REST_PATHS = (
    "/api/v1/bars",
    "/api/v1/bars/export",
    "/api/v1/instruments",
    "/api/v1/instruments/{instrument_id}",
    "/api/v1/status",
)
EXCLUDED_PATHS = (
    "/healthz",
    "/readyz",
    "/status",
    "/metrics",
    "/ws",
    "/openapi.json",
    "/docs",
    "/redoc",
)
ERROR_STATUSES = ("400", "404", "405", "429", "500")

JSON = "application/json"
ARROW_STREAM = "application/vnd.apache.arrow.stream"
ARROW_FILE = "application/vnd.apache.arrow.file"
PARQUET = "application/vnd.apache.parquet"
CSV = "text/csv; charset=utf-8"


# -- fixtures and helpers ---------------------------------------------------


@contextmanager
def _serve(tmp_path: Path) -> Generator[AsgiServer]:
    hub = WebSocketHub()
    app = create_app(
        ParquetBarStore(tmp_path / "data"),
        ws_hub=hub,
        service_role="api",
    )
    server = AsgiServer()
    try:
        server.start(
            app,
            host=HOST,
            port=0,
            thread_name=THREAD_NAME,
            on_shutdown=hub.close,
        )
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
) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(url, method=method)
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


@pytest.fixture
def schema(tmp_path: Path) -> dict[str, Any]:
    """Download and parse the real `/openapi.json` response."""
    with _serve(tmp_path) as server:
        status, headers, body = _request(f"{server.base_url}/openapi.json")

    assert status == 200
    assert _header(headers, "Content-Type") == JSON
    document = json.loads(body)
    assert isinstance(document, dict)
    return document


def _operation(schema: dict[str, Any], path: str) -> dict[str, Any]:
    return schema["paths"][path]["get"]


def _parameters(schema: dict[str, Any], path: str) -> dict[str, Any]:
    return {
        parameter["name"]: parameter
        for parameter in _operation(schema, path).get("parameters", [])
    }


def _response_schema(
    schema: dict[str, Any], path: str, status: str = "200"
) -> dict[str, Any]:
    content = _operation(schema, path)["responses"][status]["content"]
    return content[JSON]["schema"]


def _resolve(schema: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
    reference = node.get("$ref")
    if reference is None:
        return node
    prefix = "#/components/schemas/"
    assert reference.startswith(prefix), reference
    return schema["components"]["schemas"][reference[len(prefix) :]]


def _string_types(node: dict[str, Any]) -> tuple[str, ...]:
    raw_type = node.get("type", ())
    if isinstance(raw_type, str):
        return (raw_type,)
    return tuple(raw_type)


# -- document coverage ------------------------------------------------------


def test_openapi_covers_every_rest_endpoint(schema: dict[str, Any]) -> None:
    assert schema["openapi"].startswith("3.")
    assert set(REST_PATHS) <= set(schema["paths"])

    for path in REST_PATHS:
        assert set(schema["paths"][path]) == {"get"}

    for path in EXCLUDED_PATHS:
        assert path not in schema["paths"]

    status_ok = _operation(schema, "/api/v1/status")["responses"]["200"]
    assert JSON in status_ok["content"]


def test_openapi_states_the_data_contract_is_normative(
    schema: dict[str, Any],
) -> None:
    description = schema["info"]["description"].lower()

    assert "data contract" in description
    assert "normative" in description
    assert "generated documentation" in description
    assert "not a second source of truth" in description
    assert "decimal" in description
    assert "nine fractional digits" in description


# -- errors -----------------------------------------------------------------


def test_every_rest_operation_documents_the_error_envelope(
    schema: dict[str, Any],
) -> None:
    for path in REST_PATHS:
        responses = _operation(schema, path)["responses"]
        assert "422" not in responses, path
        for status in ERROR_STATUSES:
            assert status in responses, (path, status)
            media = responses[status]["content"][JSON]
            assert media["schema"] == {
                "$ref": "#/components/schemas/ErrorEnvelope"
            }


def test_no_automatic_validation_schema_leaks(schema: dict[str, Any]) -> None:
    serialized = json.dumps(schema)
    assert "422" not in serialized
    assert "HTTPValidationError" not in serialized
    assert "ValidationError" not in serialized
    assert "detail" not in schema.get("components", {}).get("schemas", {})


def test_error_envelope_component_matches_the_wire_contract(
    schema: dict[str, Any],
) -> None:
    envelope = schema["components"]["schemas"]["ErrorEnvelope"]
    assert envelope["required"] == ["error"]

    error = envelope["properties"]["error"]
    assert set(error["required"]) == {
        "code",
        "message",
        "details",
        "request_id",
    }
    properties = error["properties"]
    assert properties["code"]["type"] == "string"
    assert properties["message"]["type"] == "string"
    assert properties["details"]["type"] == "object"
    assert properties["request_id"]["type"] == "string"


# -- content types ----------------------------------------------------------


def test_bar_response_content_types_are_documented(
    schema: dict[str, Any],
) -> None:
    bars = _operation(schema, "/api/v1/bars")["responses"]["200"]["content"]
    assert set(bars) == {JSON, ARROW_STREAM, CSV}

    export = _operation(schema, "/api/v1/bars/export")["responses"]["200"][
        "content"
    ]
    assert set(export) == {PARQUET, ARROW_FILE}

    assert _resolve(schema, bars[JSON]["schema"])["type"] == "object"


# -- JSON representation ----------------------------------------------------


def test_bar_json_schema_uses_strings_for_decimals_and_timestamps(
    schema: dict[str, Any],
) -> None:
    bars_response = _resolve(
        schema, _response_schema(schema, "/api/v1/bars")
    )
    envelope = _resolve(
        schema, bars_response["properties"]["bars"]["items"]
    )
    payload = _resolve(schema, envelope["properties"]["payload"])

    for name in ("open", "high", "low", "close", "volume"):
        field = payload["properties"][name]
        assert field["type"] == "string", name
        assert "decimal" in field["description"].lower(), name
        assert DECIMAL_TEXT.fullmatch(field["example"]), name

    timestamps = (
        envelope["properties"]["event_ts"],
        envelope["properties"]["ingest_ts"],
        payload["properties"]["open_ts"],
        payload["properties"]["close_ts"],
    )
    for field in timestamps:
        assert field["type"] == "string"
        assert "iso-8601" in field["description"].lower()
        assert "nine fractional digits" in field["description"].lower()
        assert NS_TIMESTAMP.fullmatch(field["example"])


def test_instrument_json_schema_uses_strings_for_decimals_and_timestamps(
    schema: dict[str, Any],
) -> None:
    instruments_response = _resolve(
        schema, _response_schema(schema, "/api/v1/instruments")
    )
    instrument = _resolve(
        schema, instruments_response["properties"]["instruments"]["items"]
    )

    for name in ("tick_size", "lot_size", "multiplier"):
        field = instrument["properties"][name]
        assert field["type"] == "string", name
        assert "decimal" in field["description"].lower(), name
        assert DECIMAL_TEXT.fullmatch(field["example"]), name

    listing_ts = instrument["properties"]["listing_ts"]
    assert "string" in _string_types(listing_ts)
    assert "iso-8601" in listing_ts["description"].lower()
    assert NS_TIMESTAMP.fullmatch(listing_ts["example"])

    delisted_ts = instrument["properties"]["delisted_ts"]
    assert "string" in _string_types(delisted_ts)
    assert "iso-8601" in delisted_ts["description"].lower()
    assert NS_TIMESTAMP.fullmatch(delisted_ts["example"])


# -- query documentation ----------------------------------------------------


def test_query_parameters_are_documented_without_hidden_validation(
    schema: dict[str, Any],
) -> None:
    bars = _parameters(schema, "/api/v1/bars")
    assert bars["interval"]["schema"]["enum"] == [
        "1m",
        "5m",
        "15m",
        "1h",
        "1d",
    ]

    bars_format = bars["format"]["schema"]
    assert bars_format["enum"] == ["json", "arrow", "csv"]
    assert bars_format["default"] == "json"

    limit = bars["limit"]["schema"]
    assert limit["type"] == "integer"
    assert limit["minimum"] == 0

    for name in ("start", "end"):
        parameter = bars[name]["schema"]
        assert parameter["type"] == "string"
        assert "iso-8601" in parameter["description"].lower()

    export_format = _parameters(schema, "/api/v1/bars/export")["format"][
        "schema"
    ]
    assert export_format["enum"] == ["parquet", "arrow"]
    assert export_format["default"] == "parquet"


# -- docs endpoints ---------------------------------------------------------


def test_docs_is_served_and_redoc_is_not(tmp_path: Path) -> None:
    with _serve(tmp_path) as server:
        docs_status, docs_headers, docs_body = _request(
            f"{server.base_url}/docs"
        )
        redoc_status, _, redoc_body = _request(f"{server.base_url}/redoc")

    assert docs_status == 200
    assert _header(docs_headers, "Content-Type").startswith("text/html")
    assert b"swagger" in docs_body.lower()

    assert redoc_status == 404
    error = json.loads(redoc_body)["error"]
    assert error["code"] == "not_found"
