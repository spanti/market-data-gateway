"""Pure-ASGI request middleware: request ids, request metrics, completion logs.

Streaming Arrow/CSV responses pass straight through: this middleware only
inspects the response-start message and never buffers or re-reads bodies.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from starlette.datastructures import Headers, MutableHeaders

from mdgw.api.errors import REQUEST_ID_STATE_KEY, error_response
from mdgw.api.routing import route_template
from mdgw.core.errors import INTERNAL_ERROR
from mdgw.metrics import DEFAULT_HTTP_DURATION_BUCKETS

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, MutableMapping

    from mdgw.metrics import MetricsRegistry

logger = logging.getLogger(__name__)

_INTERNAL_ERROR_MESSAGE = "internal server error"


class RequestContextMiddleware:
    """Correlate, measure, and log every HTTP request.

    The middleware runs outside Starlette's exception middleware, so it sees
    the final response (including handler-generated errors) and can attach
    ``X-Request-ID`` to it. Unexpected endpoint exceptions are turned into the
    unified ``internal_error`` envelope here, which keeps the id on the header
    even when the error escapes the endpoint.
    """

    def __init__(
        self,
        app: Any,
        *,
        metrics: MetricsRegistry | None = None,
    ) -> None:
        self.app = app
        self.metrics = metrics

    async def __call__(
        self,
        scope: MutableMapping[str, Any],
        receive: Callable[[], Awaitable[MutableMapping[str, Any]]],
        send: Callable[[MutableMapping[str, Any]], Awaitable[None]],
    ) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = _request_id(scope)
        state = scope.setdefault("state", {})
        state[REQUEST_ID_STATE_KEY] = request_id

        started = time.perf_counter()
        status = 500
        response_started = False
        response_bytes = 0

        async def send_with_request_id(
            message: MutableMapping[str, Any],
        ) -> None:
            nonlocal status, response_started, response_bytes
            if message["type"] == "http.response.start":
                status = int(message["status"])
                response_started = True
                MutableHeaders(scope=message)["X-Request-ID"] = request_id
            elif message["type"] == "http.response.body":
                # Bytes actually written to the ASGI send path, including
                # every streamed chunk. This is not a measurement of bytes
                # received by a remote client.
                response_bytes += len(message.get("body", b"") or b"")
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        except Exception:
            logger.exception(
                "http request failed",
                extra={
                    "event": "api.http_request_failed",
                    "request_id": request_id,
                    "status": 500,
                },
            )
            if response_started:
                raise
            status = 500
            response = error_response(
                code=INTERNAL_ERROR,
                message=_INTERNAL_ERROR_MESSAGE,
                status=500,
                request_id=request_id,
            )
            await response(scope, receive, send_with_request_id)
        finally:
            self._record(
                scope,
                status=status,
                request_id=request_id,
                started=started,
                response_bytes=response_bytes,
            )

    def _record(
        self,
        scope: MutableMapping[str, Any],
        *,
        status: int,
        request_id: str,
        started: float,
        response_bytes: int,
    ) -> None:
        path = _route_label(scope)
        method = str(scope.get("method", ""))
        duration_s = time.perf_counter() - started
        duration_ms = duration_s * 1000.0
        if self.metrics is not None:
            labels = {
                "method": method,
                "path": path,
                "status": str(status),
            }
            self.metrics.inc(
                "mdgw_api_http_requests_total",
                labels=labels,
                help="HTTP requests handled by the api role",
            )
            self.metrics.inc(
                "mdgw_api_http_response_bytes_total",
                amount=response_bytes,
                labels=labels,
                help=(
                    "HTTP response body bytes written to the ASGI send path "
                    "(server-side; not bytes received by the client)"
                ),
            )
            self.metrics.observe(
                "mdgw_api_http_request_duration_seconds",
                duration_s,
                labels=labels,
                help=(
                    "HTTP request duration in seconds from request start "
                    "through the complete response stream (streamed response "
                    "bodies included)"
                ),
                buckets=DEFAULT_HTTP_DURATION_BUCKETS,
            )
        logger.info(
            "http request completed",
            extra={
                "event": "api.http_request",
                "request_id": request_id,
                "status": status,
                "duration_ms": round(duration_ms, 3),
            },
        )


def _request_id(scope: MutableMapping[str, Any]) -> str:
    inbound = Headers(scope=scope).get("X-Request-ID", "").strip()
    return inbound or uuid4().hex


def _route_label(scope: MutableMapping[str, Any]) -> str:
    # The rate limiter resolves the template *before* routing, so it is the
    # only source for a 429. Once routing has run, `scope["route"].path` is
    # preferred. Both are bounded templates, never an arbitrary request path.
    return route_template(scope, fallback=str(scope.get("path", "")))


__all__ = ["RequestContextMiddleware"]
