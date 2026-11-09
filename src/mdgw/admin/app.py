"""Admin router registration (``/api/v1/admin``).

Ticket 01 owns this composition point. The capability route is built here;
later feature tickets add their router factory through ``register_feature``
(or one import/append in this file) without touching ``api/app.py`` again.
``create_app`` calls ``mount_admin`` only when it is given an enabled
``AdminContext``, so the default deployment keeps the Current Contract 404.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter
from starlette.responses import JSONResponse

from mdgw.admin.capabilities import build_capabilities_document

if TYPE_CHECKING:
    from collections.abc import Callable

    from fastapi import FastAPI

    from mdgw.admin.context import AdminContext

ADMIN_PREFIX = "/api/v1/admin"

#: Later tickets append their router factory here (one registration each).
FEATURE_ROUTER_FACTORIES: list[Callable[[AdminContext], APIRouter]] = []


def register_feature(factory: Callable[[AdminContext], APIRouter]) -> None:
    """Register one feature router factory for every admin app."""
    FEATURE_ROUTER_FACTORIES.append(factory)


def create_admin_router(context: AdminContext) -> APIRouter:
    """Build the full admin router for one runtime context."""
    router = APIRouter(prefix=ADMIN_PREFIX, tags=["Admin"])
    router.include_router(_capabilities_router(context))
    for factory in FEATURE_ROUTER_FACTORIES:
        router.include_router(factory(context))
    return router


def mount_admin(app: FastAPI, context: AdminContext | None) -> None:
    """Mount admin routes when enabled and own the context's shutdown hook."""
    if context is None or not context.enabled:
        return
    context.ensure_open()
    app.include_router(create_admin_router(context))
    app.router.add_event_handler("shutdown", context.close)


def _capabilities_router(context: AdminContext) -> APIRouter:
    router = APIRouter()

    @router.get("/capabilities")
    async def capabilities() -> JSONResponse:
        document = build_capabilities_document(
            config=context.config,
            adapter_states=context.adapter_statuses(),
        )
        return JSONResponse(
            document, headers={"Cache-Control": "no-store"}
        )

    return router


__all__ = [
    "ADMIN_PREFIX",
    "FEATURE_ROUTER_FACTORIES",
    "create_admin_router",
    "mount_admin",
    "register_feature",
]
