# ruff: file-ignore[implicit-namespace-package]
"""End-to-end tests for the collector/replay health server.

The server is exercised through its public interface on a real socket, never
through aiohttp/ASGI test utilities: start it on port 0, speak HTTP, stop it,
and prove the background thread exits.
"""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from typing import Any

import pytest

from mdgw.service.health_server import ServiceHealthServer

HOST = "127.0.0.1"
WAIT_TIMEOUT_S = 5.0


def _threads_named(name: str) -> list[threading.Thread]:
    return [thread for thread in threading.enumerate() if thread.name == name]


def _request(url: str) -> tuple[int, dict[str, str], bytes]:
    """Perform one HTTP GET, returning status, headers, and raw body.

    Header names are lowercased: they are case-insensitive, and the aiohttp
    and uvicorn stacks spell them differently.
    """
    try:
        with urllib.request.urlopen(  # ruff: ignore[suspicious-url-open-usage]
            url, timeout=WAIT_TIMEOUT_S
        ) as response:
            headers = {
                key.lower(): value for key, value in response.headers.items()
            }
            return response.status, headers, response.read()
    except urllib.error.HTTPError as error:
        headers = {key.lower(): value for key, value in error.headers.items()}
        return error.code, headers, error.read()


def _json_body(body: bytes) -> dict[str, Any]:
    return json.loads(body.decode("utf-8"))


def _server(
    *,
    service: str = "collector",
    ready: Any = None,
    metrics_render: Any = None,
    status: Any = None,
) -> ServiceHealthServer:
    return ServiceHealthServer(
        service=service,
        host=HOST,
        port=0,
        ready=ready if ready is not None else lambda: (True, "ready"),
        metrics_render=metrics_render,
        status=status,
    )


def test_serves_all_endpoints_on_a_real_socket() -> None:
    server = _server(
        metrics_render=lambda: "mdgw_up 1\nmdgw_ready 1\n",
        status=lambda: {"service": "collector", "active": 2},
    )
    thread_name = "mdgw-collector-health"

    with pytest.raises(RuntimeError, match="health server is not running"):
        _ = server.bound_port
    with pytest.raises(RuntimeError, match="health server is not running"):
        _ = server.base_url

    try:
        server.start()

        assert len(_threads_named(thread_name)) == 1
        port = server.bound_port
        assert port > 0
        assert server.base_url == f"http://{HOST}:{port}"
        base_url = server.base_url

        status, headers, body = _request(f"{base_url}/healthz")
        assert status == 200
        assert headers["content-type"].startswith("application/json")
        assert _json_body(body) == {"status": "ok", "service": "collector"}

        status, headers, body = _request(f"{base_url}/readyz")
        assert status == 200
        assert headers["content-type"].startswith("application/json")
        assert _json_body(body) == {
            "ready": True,
            "reason": "ready",
            "service": "collector",
        }

        status, headers, body = _request(f"{base_url}/metrics")
        assert status == 200
        assert headers["content-type"] == "text/plain; version=0.0.4"
        assert body == b"mdgw_up 1\nmdgw_ready 1\n"

        status, headers, body = _request(f"{base_url}/status")
        assert status == 200
        assert headers["content-type"].startswith("application/json")
        assert _json_body(body) == {"service": "collector", "active": 2}
    finally:
        server.stop()
        assert _threads_named(thread_name) == []

    server.stop()
    assert _threads_named(thread_name) == []
    with pytest.raises(RuntimeError, match="health server is not running"):
        _ = server.bound_port
    with pytest.raises(RuntimeError, match="health server is not running"):
        _ = server.base_url


def test_readyz_returns_503_and_the_reason_when_not_ready() -> None:
    server = _server(ready=lambda: (False, "waiting for first Bar"))
    try:
        server.start()
        status, headers, body = _request(f"{server.base_url}/readyz")
    finally:
        server.stop()

    assert status == 503
    assert headers["content-type"].startswith("application/json")
    assert _json_body(body) == {
        "ready": False,
        "reason": "waiting for first Bar",
        "service": "collector",
    }


def test_metrics_is_empty_when_no_renderer_is_configured() -> None:
    server = _server()
    try:
        server.start()
        status, headers, body = _request(f"{server.base_url}/metrics")
    finally:
        server.stop()

    assert status == 200
    assert headers["content-type"] == "text/plain; version=0.0.4"
    assert body == b""


def test_status_route_is_absent_without_a_status_provider() -> None:
    server = _server()
    try:
        server.start()
        status, _, _ = _request(f"{server.base_url}/status")
    finally:
        server.stop()

    assert status == 404


@pytest.mark.parametrize("service", ["collector", "replay"])
def test_thread_name_identifies_the_service_role(service: str) -> None:
    server = _server(service=service)
    thread_name = f"mdgw-{service}-health"
    try:
        server.start()
        assert len(_threads_named(thread_name)) == 1
    finally:
        server.stop()
        assert _threads_named(thread_name) == []


def test_start_is_idempotent() -> None:
    server = _server()
    try:
        server.start()
        first = server.bound_port
        server.start()
        assert server.bound_port == first
        assert len(_threads_named("mdgw-collector-health")) == 1
    finally:
        server.stop()
        assert _threads_named("mdgw-collector-health") == []
