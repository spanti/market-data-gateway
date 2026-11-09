# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[unused-async]
"""HTTP response byte accounting and duration semantics (ticket 09).

The middleware sits directly on the ASGI send path, so it can count the exact
body bytes handed to the server and its duration covers the complete response
stream (including slow streamed bodies), not just time to first byte.
"""

from __future__ import annotations

import asyncio
import re

from mdgw.api.middleware import RequestContextMiddleware
from mdgw.metrics import MetricsRegistry

ROUTE = "/api/v1/bars"


async def _run(
    app,
    *,
    path: str = ROUTE,
    method: str = "GET",
) -> list[dict]:
    sent: list[dict] = []

    async def receive() -> dict:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict) -> None:
        sent.append(message)

    middleware = app
    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": [],
        "state": {},
    }
    await middleware(scope, receive, send)
    return sent


def test_response_body_bytes_written_to_the_asgi_path_are_counted() -> None:
    async def streaming_app(_scope, _receive, send) -> None:
        await send(
            {"type": "http.response.start", "status": 200, "headers": []}
        )
        await send(
            {"type": "http.response.body", "body": b"abc", "more_body": True}
        )
        await send(
            {"type": "http.response.body", "body": b"defgh", "more_body": False}
        )

    metrics = MetricsRegistry()
    middleware = RequestContextMiddleware(streaming_app, metrics=metrics)

    asyncio.run(_run(middleware))

    text = metrics.render()
    series = 'method="GET",path="/api/v1/bars",status="200"'
    assert "# TYPE mdgw_api_http_response_bytes_total counter" in text
    assert (
        f"mdgw_api_http_response_bytes_total{{{series}}} 8" in text
    )
    # Streaming responses are measured end to end, including the body chunks.
    assert (
        "complete response stream" in text
    )
    assert (
        f"mdgw_api_http_request_duration_seconds_count{{{series}}} 1" in text
    )


def test_response_byte_counter_is_cumulative_across_requests() -> None:
    async def app(_scope, _receive, send) -> None:
        await send(
            {"type": "http.response.start", "status": 200, "headers": []}
        )
        await send(
            {"type": "http.response.body", "body": b"12345", "more_body": False}
        )

    metrics = MetricsRegistry()
    middleware = RequestContextMiddleware(app, metrics=metrics)

    asyncio.run(_run(middleware))
    asyncio.run(_run(middleware))

    series = 'method="GET",path="/api/v1/bars",status="200"'
    text = metrics.render()
    assert (
        f"mdgw_api_http_response_bytes_total{{{series}}} 10" in text
    )


def test_duration_covers_the_complete_streamed_response() -> None:
    async def slow_stream(_scope, _receive, send) -> None:
        await send(
            {"type": "http.response.start", "status": 200, "headers": []}
        )
        await send(
            {"type": "http.response.body", "body": b"a", "more_body": True}
        )
        await asyncio.sleep(0.05)
        await send(
            {"type": "http.response.body", "body": b"b", "more_body": False}
        )

    metrics = MetricsRegistry()
    middleware = RequestContextMiddleware(slow_stream, metrics=metrics)

    asyncio.run(_run(middleware))

    series = 'method="GET",path="/api/v1/bars",status="200"'
    text = metrics.render()
    match = re.search(
        rf"mdgw_api_http_request_duration_seconds_sum\{{{series}\}} "
        r"([0-9.eE+-]+)",
        text,
    )
    assert match is not None
    # The full stream duration (>= the deliberate 50 ms gap) is observed, not
    # just the time until the response started.
    assert float(match.group(1)) >= 0.05
