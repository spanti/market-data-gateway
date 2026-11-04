"""Pure-ASGI HTTP rate-limit middleware.

The middleware only gates request establishment: an allowed request is passed
straight through, and Arrow/CSV response bodies are never wrapped, buffered or
re-read. It runs inside `RequestContextMiddleware`, so a 429 still gets the
request id, request metrics and the completion log.

Keys are the client IP from `scope["client"]` (``X-Forwarded-For`` is never
trusted) plus the matched route template. Templates keep the metric label and
bucket count bounded; an unmatched path falls back to the raw path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from starlette.requests import Request
from starlette.routing import Match

from mdgw.api.errors import RATE_LIMITED, error_response, get_request_id

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, MutableMapping
    from typing import Any

    from mdgw.core.rate_limit import RateLimiter
    from mdgw.metrics import MetricsRegistry

# Fixed exemptions (spec: rate limiting). Not configurable.
EXEMPT_PATHS = frozenset(
    {"/healthz", "/readyz", "/metrics", "/openapi.json", "/docs"}
)
RATE_LIMITED_METRIC = "mdgw_api_rate_limited_total"
RATE_LIMITED_MESSAGE = "rate limit exceeded"


def client_ip(scope: MutableMapping[str, Any]) -> str:
    """Return the transport-level peer address; never `X-Forwarded-For`."""
    client = scope.get("client")
    if client:
        return str(client[0])
    return "unknown"


class RateLimitMiddleware:
    """Reject over-quota HTTP requests before they reach the router."""

    def __init__(
        self,
        app: Any,
        *,
        limiter: RateLimiter,
        metrics: MetricsRegistry | None = None,
        routes: Iterable[Any] = (),
    ) -> None:
        self.app = app
        self.limiter = limiter
        self.metrics = metrics
        self._routes = tuple(routes)

    async def __call__(
        self,
        scope: MutableMapping[str, Any],
        receive: Callable[[], Awaitable[MutableMapping[str, Any]]],
        send: Callable[[MutableMapping[str, Any]], Awaitable[None]],
    ) -> None:
        if scope["type"] != "http" or scope.get("path") in EXEMPT_PATHS:
            await self.app(scope, receive, send)
            return

        route = self._route_template(scope)
        retry_after_s = await self.limiter.acquire(client_ip(scope), route)
        if retry_after_s is None:
            await self.app(scope, receive, send)
            return

        if self.metrics is not None:
            self.metrics.inc(
                RATE_LIMITED_METRIC,
                labels={"path": route},
                help="HTTP requests rejected by the api rate limiter",
            )
        response = error_response(
            code=RATE_LIMITED,
            message=RATE_LIMITED_MESSAGE,
            status=429,
            request_id=get_request_id(Request(scope)),
            details={"retry_after_s": retry_after_s},
            headers={"Retry-After": str(retry_after_s)},
        )
        await response(scope, receive, send)

    def _route_template(self, scope: MutableMapping[str, Any]) -> str:
        for route in self._routes:
            match, _ = route.matches(scope)
            if match is not Match.NONE:
                path = getattr(route, "path", None)
                if isinstance(path, str) and path:
                    return path
        return str(scope.get("path", ""))


__all__ = [
    "EXEMPT_PATHS",
    "RATE_LIMITED_MESSAGE",
    "RATE_LIMITED_METRIC",
    "RateLimitMiddleware",
    "client_ip",
]
