# ruff: file-ignore[implicit-namespace-package]
"""Public-interface tests for the AsgiServer lifecycle module."""

from __future__ import annotations

import json
import socket
import threading
import urllib.request
from typing import Any

import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from mdgw.api.asgi_server import AsgiServer

THREAD_NAME = "mdgw-test-asgi"
HOST = "127.0.0.1"
WAIT_TIMEOUT_S = 5.0


def _ping(_request: Any) -> JSONResponse:
    return JSONResponse({"status": "ok"})


def _app() -> Starlette:
    return Starlette(routes=[Route("/ping", _ping)])


def _app_with_lifespan(events: list[str]) -> Any:
    """A minimal ASGI wrapper that records lifespan shutdown."""

    inner = _app()

    async def app(
        scope: dict[str, Any],
        receive: Any,
        send: Any,
    ) -> None:
        if scope["type"] != "lifespan":
            await inner(scope, receive, send)
            return
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                events.append("app_shutdown")
                await send({"type": "lifespan.shutdown.complete"})
                return

    return app


def _threads_named(name: str) -> list[threading.Thread]:
    return [thread for thread in threading.enumerate() if thread.name == name]


def _assert_thread_stopped() -> None:
    assert _threads_named(THREAD_NAME) == []


def _get_json(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(  # ruff: ignore[suspicious-url-open-usage]
        url, timeout=WAIT_TIMEOUT_S
    ) as response:
        assert response.status == 200
        return json.loads(response.read())


def test_start_on_port_zero_reports_bound_port_and_base_url() -> None:
    server = AsgiServer()
    try:
        port = server.start(_app(), host=HOST, port=0, thread_name=THREAD_NAME)
        assert port > 0
        assert server.bound_port == port
        assert server.base_url == f"http://{HOST}:{port}"
    finally:
        server.stop()
        _assert_thread_stopped()


@pytest.mark.parametrize(
    "host",
    ["0.0.0.0", "::"],  # ruff: ignore[hardcoded-bind-all-interfaces] - URL rewriting
)
def test_base_url_rewrites_wildcard_hosts_to_loopback(host: str) -> None:
    server = AsgiServer()
    try:
        port = server.start(_app(), host=host, port=0, thread_name=THREAD_NAME)
        assert server.base_url == f"http://127.0.0.1:{port}"
    finally:
        server.stop()
        _assert_thread_stopped()


def test_serves_a_real_http_request() -> None:
    server = AsgiServer()
    try:
        port = server.start(_app(), host=HOST, port=0, thread_name=THREAD_NAME)
        payload = _get_json(f"http://{HOST}:{port}/ping")
    finally:
        server.stop()
        _assert_thread_stopped()
    assert payload == {"status": "ok"}


def test_repeated_start_returns_same_port_and_keeps_one_thread() -> None:
    server = AsgiServer()
    try:
        first = server.start(_app(), host=HOST, port=0, thread_name=THREAD_NAME)
        second = server.start(_app(), host=HOST, port=0, thread_name=THREAD_NAME)
        assert second == first
        assert len(_threads_named(THREAD_NAME)) == 1
    finally:
        server.stop()
        _assert_thread_stopped()


def test_stop_is_idempotent() -> None:
    server = AsgiServer()
    server.stop()  # never started
    _assert_thread_stopped()

    server.start(_app(), host=HOST, port=0, thread_name=THREAD_NAME)
    server.stop()
    _assert_thread_stopped()
    server.stop()
    _assert_thread_stopped()


def test_port_is_reusable_right_after_stop() -> None:
    first = AsgiServer()
    port = first.start(_app(), host=HOST, port=0, thread_name=THREAD_NAME)
    first.stop()
    _assert_thread_stopped()

    second = AsgiServer()
    try:
        rebound = second.start(_app(), host=HOST, port=port, thread_name=THREAD_NAME)
    finally:
        second.stop()
        _assert_thread_stopped()

    assert rebound == port


def test_bind_failure_reraises_original_error_and_leaves_no_thread() -> None:
    occupied = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    occupied.bind((HOST, 0))
    occupied.listen()
    port = occupied.getsockname()[1]
    server = AsgiServer()
    try:
        with pytest.raises(OSError, match=r".*"):
            server.start(_app(), host=HOST, port=port, thread_name=THREAD_NAME)

        assert _threads_named(THREAD_NAME) == []
        with pytest.raises(RuntimeError, match="must be called first"):
            _ = server.bound_port
        with pytest.raises(RuntimeError, match="must be called first"):
            _ = server.base_url

        # The failed instance is still usable, proving the loop and thread
        # were torn down rather than leaked.
        retry_port = server.start(_app(), host=HOST, port=0, thread_name=THREAD_NAME)
        assert retry_port > 0
        assert len(_threads_named(THREAD_NAME)) == 1
    finally:
        server.stop()
        _assert_thread_stopped()
        occupied.close()


def test_start_failure_from_thread_start_reraises_without_leaking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = AsgiServer()

    def _fail_thread_start(_thread: threading.Thread) -> None:
        msg = "thread start failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(threading.Thread, "start", _fail_thread_start)

    with pytest.raises(RuntimeError, match="thread start failed"):
        server.start(_app(), host=HOST, port=0, thread_name=THREAD_NAME)

    _assert_thread_stopped()
    with pytest.raises(RuntimeError, match="must be called first"):
        _ = server.bound_port
    with pytest.raises(RuntimeError, match="must be called first"):
        _ = server.base_url


def test_on_shutdown_runs_in_lifespan_shutdown_and_errors_are_swallowed() -> None:
    events: list[str] = []

    async def on_shutdown() -> None:  # ruff: ignore[unused-async] - hook contract
        events.append("on_shutdown")
        msg = "shutdown hook failed"
        raise RuntimeError(msg)

    server = AsgiServer()
    try:
        server.start(
            _app_with_lifespan(events),
            host=HOST,
            port=0,
            thread_name=THREAD_NAME,
            on_shutdown=on_shutdown,
        )
    finally:
        server.stop()
        _assert_thread_stopped()

    # The app's lifespan shutdown handlers run first; the lifecycle hook runs
    # while the lifespan shutdown is still in progress (before uvicorn sees
    # `lifespan.shutdown.complete`).
    assert events == ["app_shutdown", "on_shutdown"]
    _assert_thread_stopped()


def test_call_soon_threadsafe_dispatches_across_threads() -> None:
    server = AsgiServer()
    server.start(_app(), host=HOST, port=0, thread_name=THREAD_NAME)
    caller = threading.current_thread()
    seen: list[tuple[str, int]] = []
    done = threading.Event()

    def callback(value: str) -> None:
        seen.append((value, threading.get_ident()))
        done.set()

    try:
        server.call_soon_threadsafe(callback, "payload")
        assert done.wait(WAIT_TIMEOUT_S)
    finally:
        server.stop()
        _assert_thread_stopped()

    assert len(seen) == 1
    assert seen[0][0] == "payload"
    assert seen[0][1] != caller.ident

    with pytest.raises(RuntimeError, match="must be called first"):
        server.call_soon_threadsafe(callback, "after-stop")


def test_stop_from_the_server_thread_raises_instead_of_deadlocking() -> None:
    server = AsgiServer()
    server.start(_app(), host=HOST, port=0, thread_name=THREAD_NAME)
    errors: list[Exception] = []
    finished = threading.Event()

    def call_stop() -> None:
        try:
            server.stop()
        except Exception as error:  # ruff: ignore[blind-except] - recorded
            errors.append(error)
        finally:
            finished.set()

    try:
        server.call_soon_threadsafe(call_stop)
        assert finished.wait(WAIT_TIMEOUT_S), "stop() deadlocked on its own thread"
    finally:
        server.stop()
        _assert_thread_stopped()

    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
