"""FastAPI application factory for the REST / WebSocket / ops HTTP surface.

It keeps the same name and keyword-only parameter shape as the retired
`mdgw.api.server.create_app` so production call sites did not need rewriting.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import FastAPI

from mdgw.api.errors import install_exception_handlers
from mdgw.api.middleware import RequestContextMiddleware
from mdgw.api.openapi import install_openapi
from mdgw.api.routes import bars, instruments, ops, websocket

if TYPE_CHECKING:
    from collections.abc import Callable

    from mdgw.api.ws_hub import WebSocketHub
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
) -> FastAPI:
    """Build the FastAPI application.

    Bars and ops are always registered; the WebSocket route only when the
    runtime supplies a hub.
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
    app.add_middleware(RequestContextMiddleware, metrics=metrics)

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
            websocket.create_router(ws_hub=ws_hub, ws_path=ws_path)
        )
    return app


__all__ = ["create_app"]
