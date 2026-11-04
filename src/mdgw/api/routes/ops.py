"""Operational endpoints: health, readiness, Dataset status, and metrics."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter
from starlette.responses import JSONResponse, Response

if TYPE_CHECKING:
    from collections.abc import Callable

    from mdgw.store.parquet_dataset import ParquetBarStore

METRICS_CONTENT_TYPE = "text/plain; version=0.0.4"


def create_router(
    *,
    store: ParquetBarStore,
    metrics_render: Callable[[], str] | None = None,
    status_provider: Callable[[], dict[str, Any]] | None = None,
    ready_provider: Callable[[], dict[str, Any]] | None = None,
    service_role: str | None = None,
) -> APIRouter:
    """Build the ops router for one application instance."""
    router = APIRouter(include_in_schema=False)

    @router.get("/healthz")
    async def healthz() -> JSONResponse:
        return JSONResponse({"status": "ok", "service": service_role or "api"})

    @router.get("/readyz")
    async def readyz() -> JSONResponse:
        payload = (
            {"ready": True, "reason": "ok"}
            if ready_provider is None
            else ready_provider()
        )
        return JSONResponse(
            payload,
            status_code=200 if payload.get("ready") else 503,
        )

    @router.get("/status")
    async def service_status() -> JSONResponse:
        payload = dict(await asyncio.to_thread(store.status))
        if status_provider is not None:
            payload.update(status_provider())
        return JSONResponse(payload)

    @router.get("/metrics")
    async def metrics() -> Response:
        text = "" if metrics_render is None else metrics_render()
        return Response(
            content=text.encode("utf-8"),
            headers={"Content-Type": METRICS_CONTENT_TYPE},
        )

    @router.get("/api/v1/status")
    async def dataset_status() -> JSONResponse:
        return JSONResponse(await asyncio.to_thread(store.status))

    return router


__all__ = ["METRICS_CONTENT_TYPE", "create_router"]
