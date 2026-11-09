"""Admin router registration (``/api/v1/admin``).

Ticket 01 owns this composition point. The capability route is built here;
later feature tickets add their router factory through ``register_feature``
(or one import/append in this file) without touching ``api/app.py`` again.
``create_app`` calls ``mount_admin`` only when it is given an enabled
``AdminContext``, so the default deployment keeps the Current Contract 404.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends
from starlette.responses import JSONResponse

from mdgw.admin.audit.routes import create_audit_router
from mdgw.admin.auth.dependencies import require_authenticated_request
from mdgw.admin.auth.routes import (
    create_auth_router,
    create_protected_auth_router,
)
from mdgw.admin.capabilities import build_capabilities_document
from mdgw.admin.export.routes import create_export_router
from mdgw.admin.feed.routes import create_feed_router
from mdgw.admin.jobs.routes import create_backfill_router
from mdgw.admin.lifecycle.routes import create_lifecycle_router
from mdgw.admin.monitoring.routes import create_monitoring_router

if TYPE_CHECKING:
    from collections.abc import Callable

    from fastapi import FastAPI

    from mdgw.admin.context import AdminContext

ADMIN_PREFIX = "/api/v1/admin"

#: The explicit public Admin routes. Every other current or future Admin route
#: is mounted under a router-level session dependency (and this set is pinned
#: by the route-matrix test).
PUBLIC_ADMIN_PATHS: frozenset[str] = frozenset(
    {
        "/api/v1/admin/capabilities",
        "/api/v1/admin/auth/login",
        "/api/v1/admin/auth/callback",
        "/api/v1/admin/auth/session",
    }
)

#: Later tickets append their router factory here (one registration each).
FEATURE_ROUTER_FACTORIES: list[Callable[[AdminContext], APIRouter]] = []


def register_feature(factory: Callable[[AdminContext], APIRouter]) -> None:
    """Register one feature router factory for every admin app."""
    FEATURE_ROUTER_FACTORIES.append(factory)


# Ticket 09: the authenticated, fixed-template monitoring proxy. It registers
# its ``prometheus`` adapter / ``monitoring_proxy`` capability through
# ``AdminContext.register_capabilities`` when its router is built.
register_feature(create_monitoring_router)

#: Ticket 03: read-only Operator Audit Event query routes.
register_feature(create_audit_router)
#: Ticket 04: Feed Subscription and flow-control routes.
register_feature(create_feed_router)

#: Ticket 08: persistent CSV Export Jobs, quotas, expiry and Range downloads.
register_feature(create_export_router)

#: Ticket 05: Service Instance lifecycle + Operation query routes.
register_feature(create_lifecycle_router)

#: Ticket 06: persistent Backfill Jobs executed by the Collector.
register_feature(create_backfill_router)


def create_admin_router(context: AdminContext) -> APIRouter:
    """Build the full admin router for one runtime context.

    Feature routers registered by later tickets are mounted inside a parent
    router whose session dependency runs first, so a future Admin route is
    authenticated by default rather than by remembering to add a guard.
    """
    router = APIRouter(prefix=ADMIN_PREFIX, tags=["Admin"])
    router.include_router(_capabilities_router(context))
    router.include_router(create_auth_router(context))

    protected = APIRouter(
        dependencies=[Depends(require_authenticated_request(context))]
    )
    protected.include_router(create_protected_auth_router(context))
    for factory in FEATURE_ROUTER_FACTORIES:
        protected.include_router(factory(context))
    router.include_router(protected)
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
            built_adapters=context.built_adapters(),
            built_capabilities=context.built_capabilities(),
            capability_details=context.capability_details(),
        )
        return JSONResponse(
            document, headers={"Cache-Control": "no-store"}
        )

    return router


__all__ = [
    "ADMIN_PREFIX",
    "FEATURE_ROUTER_FACTORIES",
    "PUBLIC_ADMIN_PATHS",
    "create_admin_router",
    "mount_admin",
    "register_feature",
]
