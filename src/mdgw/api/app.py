"""FastAPI application factory for the REST / WebSocket / ops HTTP surface.

It keeps the retired `mdgw.api.server.create_app` name and every existing
keyword-only parameter (same names and order), so production call sites stayed
source-compatible. The one deliberate addition is the keyword-only
``rate_limit: RateLimitConfig | None = None`` parameter; omitting it preserves
the previous unthrottled behaviour.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import FastAPI

from mdgw.api.errors import install_exception_handlers
from mdgw.api.middleware import RequestContextMiddleware
from mdgw.api.openapi import install_openapi
from mdgw.api.rate_limit import RateLimitMiddleware
from mdgw.api.routes import bars, instruments, ops, websocket
from mdgw.core.rate_limit import RateLimiter

if TYPE_CHECKING:
    from collections.abc import Callable

    from mdgw.api.ws_hub import WebSocketHub
    from mdgw.config import RateLimitConfig
    from mdgw.metrics import MetricsRegistry
    from mdgw.reference import InstrumentStore
    from mdgw.store.parquet_dataset import ParquetBarStore


def create_app(  # ruff: ignore[too-many-arguments]  # keyword-only app configuration
    store: ParquetBarStore,
    *,
    instrument_store: InstrumentStore | None = None,
    ws_hub: WebSocketHub | None = None,
    ws_path: str = "/ws",
    metrics: MetricsRegistry | None = None,
    metrics_render: Callable[[], str] | None = None,
    status_provider: Callable[[], dict] | None = None,
    ready_provider: Callable[[], dict] | None = None,
    service_role: str | None = None,
    rate_limit: RateLimitConfig | None = None,
) -> FastAPI:
    """Build the FastAPI application.

    Bars and ops are always registered; the WebSocket route only when the
    runtime supplies a hub. ``rate_limit=None`` (and a disabled config) leave
    the app unthrottled.
    """
    app = FastAPI(
        title="Market Data Gateway API",
        openapi_url="/openapi.json",
        docs_url="/docs",
        redoc_url=None,
        redirect_slashes=False,
    )
    install_openapi(app)
    install_exception_handlers(app)

    limiter = _build_rate_limiter(rate_limit)

    app.include_router(
        ops.create_router(
            store=store,
            metrics_render=metrics_render,
            status_provider=status_provider,
            ready_provider=ready_provider,
            service_role=service_role,
        )
    )
    app.include_router(
        instruments.create_router(instrument_store=instrument_store)
    )
    app.include_router(bars.create_router(store=store))
    if ws_hub is not None:
        app.include_router(
            websocket.create_router(
                ws_hub=ws_hub,
                ws_path=ws_path,
                rate_limiter=limiter,
            )
        )

    if limiter is not None:
        # Starlette prepends each added middleware, so adding the rate limiter
        # first keeps it *inside* the request-context middleware: a 429 still
        # gets `X-Request-ID`, request metrics and the completion log.
        app.add_middleware(
            RateLimitMiddleware,
            limiter=limiter,
            metrics=metrics,
            routes=app.routes,
        )
    app.add_middleware(RequestContextMiddleware, metrics=metrics)
    return app


def _build_rate_limiter(
    rate_limit: RateLimitConfig | None,
) -> RateLimiter | None:
    if rate_limit is None or not rate_limit.enabled:
        return None
    return RateLimiter(
        requests_per_second=rate_limit.requests_per_second,
        burst=rate_limit.burst,
        max_ws_sessions_per_ip=rate_limit.max_ws_sessions_per_ip,
    )


__all__ = ["create_app"]
