# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[suspicious-url-open-usage]
"""End-to-end Admin API foundation over a real FastAPI app and socket.

These tests observe wire behaviour only: the default deployment keeps the
Current Contract 404, an enabled admin deployment serves the capability
document with no-store, the unified error envelope covers admin routes, and the
management SQLite lease is released on shutdown.
"""

from __future__ import annotations

import json
import re
import socket
import threading
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from mdgw.admin.context import AdminContext
from mdgw.admin.db import AdminInstanceLockedError, AdminStore
from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.config import AdminConfig, load_config
from mdgw.core.time import iso_to_ns
from mdgw.service.api import ApiService
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Generator

HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-admin-api"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CAPABILITIES = "/api/v1/admin/capabilities"
NS_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z")

REQUIRED_CAPABILITIES = {
    "oidc_session",
    "feed_subscription",
    "feed_flow_control",
    "service_lifecycle",
    "export_job",
    "backfill_job",
    "persistent_backfill_gaps",
    "operator_audit",
    "monitoring_proxy",
    "ws_admin_session",
    "user_management",
}


# -- fixtures and helpers ---------------------------------------------------


def _unused_endpoint() -> str:
    """A loopback tcp endpoint with no listener, isolated from a real stack."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((HOST, 0))
        return f"tcp://{HOST}:{probe.getsockname()[1]}"


def _admin_config(tmp_path: Path, **updates) -> AdminConfig:
    config = AdminConfig(
        enabled=True, db_path=tmp_path / "admin" / "admin.db"
    )
    return config.model_copy(update=updates) if updates else config


@contextmanager
def _serve(app) -> Generator[AsgiServer]:
    server = AsgiServer()
    try:
        server.start(
            app,
            host=HOST,
            port=0,
            thread_name=THREAD_NAME,
        )
        yield server
    finally:
        server.stop()


@contextmanager
def _admin_app(
    tmp_path: Path,
    *,
    admin_config: AdminConfig | None = None,
) -> Generator[tuple[AsgiServer, AdminContext, ParquetBarStore]]:
    store = ParquetBarStore(tmp_path / "data")
    context = AdminContext(
        config=admin_config or _admin_config(tmp_path),
        storage_root=tmp_path,
        store=store,
    )
    app = create_app(store, admin=context)
    try:
        with _serve(app) as server:
            yield server, context, store
    finally:
        store.close()


def _request(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(
        url, method=method, headers=headers or {}, data=body
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


def _get_capabilities(server: AsgiServer) -> dict:
    status, headers, body = _request(server.base_url + CAPABILITIES)
    assert status == 200
    assert _header(headers, "Content-Type") == "application/json"
    assert _header(headers, "Cache-Control") == "no-store"
    assert _header(headers, "X-Request-ID")
    return json.loads(body)


def _threads_named(name: str) -> list[threading.Thread]:
    return [thread for thread in threading.enumerate() if thread.name == name]


# -- default deployment: Current Contract ----------------------------------


def test_admin_routes_are_absent_when_admin_is_disabled(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path / "data")
    app = create_app(store)
    try:
        with _serve(app) as server:
            status, headers, body = _request(
                server.base_url + CAPABILITIES
            )
    finally:
        store.close()

    assert status == 404
    envelope = json.loads(body)
    assert envelope["error"]["code"] == "not_found"
    assert envelope["error"]["request_id"] == _header(
        headers, "X-Request-ID"
    )


def test_admin_openapi_path_is_absent_when_disabled(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    app = create_app(store)
    try:
        with _serve(app) as server:
            status, _, body = _request(server.base_url + "/openapi.json")
    finally:
        store.close()

    assert status == 200
    assert CAPABILITIES not in json.loads(body)["paths"]


# -- enabled deployment: capability discovery ------------------------------


def test_capabilities_document_matches_the_contract(
    tmp_path: Path,
) -> None:
    with _admin_app(tmp_path) as (server, _, _):
        document = _get_capabilities(server)

    assert set(document) == {
        "schema_version",
        "server_time",
        "version",
        "auth",
        "adapters",
        "capabilities",
        "limits",
    }
    assert document["schema_version"] == 1
    assert NS_TIMESTAMP.fullmatch(document["server_time"])
    assert abs(iso_to_ns(document["server_time"]) - time.time_ns()) < (
        120 * 1_000_000_000
    )

    version = document["version"]
    assert version["contract"] == 1
    assert version["supported_contracts"] == [1]
    assert isinstance(version["admin_api"], str)
    assert version["admin_api"]

    auth = document["auth"]
    assert auth["required"] is True
    assert auth["mode"] == "oidc"
    assert auth["state"] in {"available", "unavailable", "unsupported"}
    assert auth["roles"] == ["SuperAdmin", "Operator", "Auditor"]
    assert auth["csrf"] == {
        "mode": "session_token",
        "header_name": "X-CSRF-Token",
    }
    assert auth["idle_timeout_s"] == 1800
    assert auth["absolute_timeout_s"] == 28800
    assert auth["expiry_warning_s"] == 300

    assert [entry["name"] for entry in document["adapters"]] == [
        "oidc",
        "admin_store",
        "audit_store",
        "supervisor",
        "collector_control",
        "export_executor",
        "prometheus",
    ]
    adapter_states = {
        entry["name"]: entry["state"] for entry in document["adapters"]
    }
    assert adapter_states["admin_store"] == "available"
    assert adapter_states["oidc"] == "unsupported"

    capabilities = document["capabilities"]
    assert set(capabilities) == REQUIRED_CAPABILITIES
    for entry in capabilities.values():
        assert entry["state"] in {
            "enabled",
            "unsupported",
            "adapter_unavailable",
        }
    assert capabilities["user_management"]["state"] == "unsupported"

    assert document["limits"] == {
        "page_default": 50,
        "page_max": 200,
        "reason_max_length": 1000,
        "operation_retention_s": 604800,
        "backfill_max_range_days": 366,
    }


def test_capabilities_is_public_even_though_auth_is_required(
    tmp_path: Path,
) -> None:
    # A18: discovery is public 200 and carries no secrets; protected admin
    # resources, not discovery, will return 401.
    with _admin_app(tmp_path) as (server, _, _):
        document = _get_capabilities(server)

    assert document["auth"]["required"] is True
    assert "client_secret" not in json.dumps(document)


def test_unimplemented_adapters_stay_unsupported_with_flags_enabled(
    tmp_path: Path,
) -> None:
    config = _admin_config(tmp_path).model_copy(
        update={
            "adapters": _admin_config(tmp_path).adapters.model_copy(
                update={"oidc": True, "audit_store": True}
            )
        }
    )
    with _admin_app(tmp_path, admin_config=config) as (server, _, _):
        document = _get_capabilities(server)

    adapter_states = {
        entry["name"]: entry["state"] for entry in document["adapters"]
    }
    assert adapter_states["oidc"] == "unsupported"
    assert adapter_states["audit_store"] == "unsupported"
    assert document["capabilities"]["oidc_session"]["state"] == (
        "unsupported"
    )
    assert document["capabilities"]["operator_audit"]["state"] == (
        "unsupported"
    )


def test_admin_openapi_path_is_additive_when_enabled(
    tmp_path: Path,
) -> None:
    with _admin_app(tmp_path) as (server, _, _):
        status, _, body = _request(server.base_url + "/openapi.json")

    assert status == 200
    paths = json.loads(body)["paths"]
    assert CAPABILITIES in paths
    assert "/api/v1/bars" in paths
    assert "/api/v1/status" in paths


# -- unified admin errors ---------------------------------------------------


def test_unknown_admin_path_uses_the_unified_envelope(
    tmp_path: Path,
) -> None:
    with _admin_app(tmp_path) as (server, _, _):
        status, headers, body = _request(
            server.base_url + "/api/v1/admin/does-not-exist"
        )

    assert status == 404
    envelope = json.loads(body)
    assert set(envelope["error"]) == {
        "code",
        "message",
        "details",
        "request_id",
    }
    assert envelope["error"]["code"] == "not_found"
    assert envelope["error"]["request_id"] == _header(
        headers, "X-Request-ID"
    )


def test_wrong_method_on_an_admin_route_uses_the_unified_envelope(
    tmp_path: Path,
) -> None:
    with _admin_app(tmp_path) as (server, _, _):
        status, headers, body = _request(
            server.base_url + CAPABILITIES, method="POST"
        )

    assert status == 405
    envelope = json.loads(body)
    assert envelope["error"]["code"] == "method_not_allowed"
    assert envelope["error"]["request_id"] == _header(
        headers, "X-Request-ID"
    )
    assert "detail" not in envelope


def test_no_cors_headers_are_added(tmp_path: Path) -> None:
    with _admin_app(tmp_path) as (server, _, _):
        status, headers, _ = _request(
            server.base_url + CAPABILITIES,
            method="OPTIONS",
            headers={
                "Origin": "https://evil.example.com",
                "Access-Control-Request-Method": "GET",
            },
        )

    assert status in {405, 404}
    assert _header(headers, "Access-Control-Allow-Origin") is None


# -- management store lifecycle --------------------------------------------


def test_admin_database_is_independent_and_lease_is_released(
    tmp_path: Path,
) -> None:
    with _admin_app(tmp_path) as (server, context, _store):
        database = context.db_path
        assert database.exists()
        assert (tmp_path / "data") not in database.parents
        assert (tmp_path / "data" / "bars") not in database.parents
        assert context.db is not None
        assert context.db.holder() == context.instance_id
        _get_capabilities(server)

    assert _threads_named(THREAD_NAME) == []

    with AdminStore(database) as probe:
        probe.claim_instance("probe-instance")
        assert probe.holder() == "probe-instance"


def test_admin_database_inside_bars_is_rejected_at_runtime(
    tmp_path: Path,
) -> None:
    config = AdminConfig(
        enabled=True, db_path=tmp_path / "bars" / "admin.db"
    )
    context = AdminContext(config=config, storage_root=tmp_path)

    with pytest.raises(ValueError, match="Bars Dataset"):
        context.ensure_open()


def test_a_second_live_context_cannot_claim_the_same_store(
    tmp_path: Path,
) -> None:
    config = _admin_config(tmp_path)
    first = AdminContext(config=config, storage_root=tmp_path)
    second = AdminContext(config=config, storage_root=tmp_path)
    first.ensure_open()
    try:
        with pytest.raises(AdminInstanceLockedError):
            second.ensure_open()
    finally:
        first.close()

    second.ensure_open()
    try:
        assert second.db is not None
        assert second.db.holder() == second.instance_id
    finally:
        second.close()


def test_api_service_mounts_admin_and_releases_the_lease_on_stop(
    tmp_path: Path,
) -> None:
    base = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = base.server.model_copy(update={"http_port": 0, "host": HOST})
    bus = base.bus.model_copy(
        update={
            "api_pub_bind": "tcp://127.0.0.1:0",
            "collector_pub_connect": _unused_endpoint(),
        }
    )
    database = tmp_path / "admin" / "admin.db"
    config = base.model_copy(
        update={
            "server": server,
            "bus": bus,
            "admin": AdminConfig(enabled=True, db_path=database),
        }
    )
    service = ApiService(config, data_root=tmp_path)
    try:
        service.start()
        # Ticket 02: the Admin WS reuses the public hub, attached before the
        # app (and therefore the admin router) is built.
        assert service._admin is not None
        assert service._admin.ws_hub is service._hub
        status, headers, body = _request(
            service.base_url + CAPABILITIES
        )
    finally:
        service.stop()

    assert status == 200
    assert _header(headers, "Cache-Control") == "no-store"
    assert json.loads(body)["version"]["contract"] == 1

    with AdminStore(database) as probe:
        probe.claim_instance("probe-after-service-stop")
        assert probe.holder() == "probe-after-service-stop"


def test_public_routes_keep_working_when_admin_is_enabled(
    tmp_path: Path,
) -> None:
    with _admin_app(tmp_path) as (server, _, _):
        health_status, _, health_body = _request(
            server.base_url + "/healthz"
        )
        bars_status, _, bars_body = _request(
            server.base_url + "/api/v1/bars?interval=1m&format=json"
        )

    assert health_status == 200
    assert json.loads(health_body)["status"] == "ok"
    assert bars_status == 200
    assert json.loads(bars_body) == {"bars": []}
