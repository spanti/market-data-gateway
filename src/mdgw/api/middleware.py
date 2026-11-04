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
from mdgw.core.errors import INTERNAL_ERROR

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

        async def send_with_request_id(
            message: MutableMapping[str, Any],
        ) -> None:
            nonlocal status, response_started
            if message["type"] == "http.response.start":
                status = int(message["status"])
                response_started = True
                MutableHeaders(scope=message)["X-Request-ID"] = request_id
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
            )

    def _record(
        self,
        scope: MutableMapping[str, Any],
        *,
        status: int,
        request_id: str,
        started: float,
    ) -> None:
        path = _route_label(scope)
        method = str(scope.get("method", ""))
        duration_ms = (time.perf_counter() - started) * 1000.0
        if self.metrics is not None:
            self.metrics.inc(
                "mdgw_api_http_requests_total",
                labels={
                    "method": method,
                    "path": path,
                    "status": str(status),
                },
                help="HTTP requests handled by the api role",
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
    route = scope.get("route")
    path = getattr(route, "path", None)
    if isinstance(path, str) and path:
        return path
    return str(scope.get("path", ""))


__all__ = ["RequestContextMiddleware"]
