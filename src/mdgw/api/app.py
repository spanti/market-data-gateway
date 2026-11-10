"""FastAPI application factory for the REST / WebSocket / ops HTTP surface.

It keeps the retired `mdgw.api.server.create_app` name and every existing
keyword-only parameter (same names and order), so production call sites stayed
source-compatible. The deliberate additions are keyword-only with defaults:
``rate_limit`` preserves the previous unthrottled behaviour, and ``admin``
leaves the ``/api/v1/admin`` routes absent unless an enabled
``AdminContext`` is supplied.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import FastAPI

from mdgw.admin.app import mount_admin
from mdgw.api.errors import install_exception_handlers
from mdgw.api.history_cursor import CursorCodec, resolve_cursor_key
from mdgw.api.middleware import RequestContextMiddleware
from mdgw.api.openapi import install_openapi
from mdgw.api.rate_limit import RateLimitMiddleware
from mdgw.api.routes import bars, instruments, ops, websocket
from mdgw.core.rate_limit import RateLimiter

if TYPE_CHECKING:
    from collections.abc import Callable

    from mdgw.admin.context import AdminContext
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
    admin: AdminContext | None = None,
    cursor_key: bytes | None = None,
) -> FastAPI:
    """Build the FastAPI application.

    Bars and ops are always registered; the WebSocket route only when the
    runtime supplies a hub. ``rate_limit=None`` (and a disabled config) leave
    the app unthrottled. ``admin=None`` or a disabled context leaves the
    ``/api/v1/admin`` subtree absent, which is the Current Contract.
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

    # Ticket 04: with Admin enabled, flow-control desired state lives in the
    # management DB and is hot-applied to this process's limiter/hub. The
    # runtime is built before the WebSocket router so the middleware is
    # present even when `[rate_limit].enabled` was false at startup.
    flow_control = (
        admin.feed if admin is not None and admin.enabled else None
    )
    if flow_control is not None:
        flow_control.reconcile_runtime_defaults(
            rate_limit=rate_limit, hub=ws_hub
        )
    limiter = _build_rate_limiter(rate_limit, flow_control=flow_control)
    if flow_control is not None:
        flow_control.bind_rate_limiter(limiter)

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
    app.include_router(
        bars.create_router(
            store=store,
            cursor_codec=CursorCodec(
                cursor_key
                if cursor_key is not None
                else resolve_cursor_key(None)
            ),
        )
    )
    if ws_hub is not None:
        app.include_router(
            websocket.create_router(
                ws_hub=ws_hub,
                ws_path=ws_path,
                rate_limiter=limiter,
                client_ip_resolver=(
                    None
                    if flow_control is None
                    else flow_control.resolve_client_ip
                ),
            )
        )
    # Admin routes are additive and mounted before the rate-limiter middleware
    # captures the route table, so their templates stay bounded in metrics.
    mount_admin(app, admin)
    if flow_control is not None:
        flow_control.bind_ws_hub(ws_hub)
    if admin is not None and admin.enabled:
        # Restart recovery: persist observed flow-control state and re-send
        # the persisted desired subscription set to the Collector.
        admin.run_startup()

    if limiter is not None:
        # Starlette prepends each added middleware, so adding the rate limiter
        # first keeps it *inside* the request-context middleware: a 429 still
        # gets `X-Request-ID`, request metrics and the completion log.
        app.add_middleware(
            RateLimitMiddleware,
            limiter=limiter,
            metrics=metrics,
            routes=app.routes,
            client_ip_resolver=(
                None
                if flow_control is None
                else flow_control.resolve_client_ip
            ),
        )
    app.add_middleware(RequestContextMiddleware, metrics=metrics)
    return app


def _build_rate_limiter(
    rate_limit: RateLimitConfig | None,
    *,
    flow_control: Any = None,
) -> RateLimiter | None:
    if flow_control is not None:
        policy = flow_control.rate_limit_policy()
        return RateLimiter(
            requests_per_second=max(policy.requests_per_second, 1e-9),
            burst=policy.burst,
            max_ws_sessions_per_ip=policy.max_ws_sessions_per_ip,
            policy_source=flow_control.rate_limit_policy,
        )
    if rate_limit is None or not rate_limit.enabled:
        return None
    return RateLimiter(
        requests_per_second=rate_limit.requests_per_second,
        burst=rate_limit.burst,
        max_ws_sessions_per_ip=rate_limit.max_ws_sessions_per_ip,
    )


__all__ = ["create_app"]
