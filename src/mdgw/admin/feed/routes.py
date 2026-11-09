# ruff: file-ignore[raise-vanilla-args,raw-string-in-exception,fast-api-non-annotated-dependency,too-many-positional-arguments]
"""Feed Subscription and flow-control Admin routes (contract section 4).

Reads are available to every authenticated Access Role. Writes are limited to
SuperAdmin/Operator and go through the shared CSRF + same-origin dependency.
All mutations are fail-closed on the Operator Audit Writer (ticket 03).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, Request
from starlette.responses import JSONResponse

from mdgw.admin.auth.dependencies import require_write
from mdgw.admin.errors import InvalidParameterError
from mdgw.admin.feed.flow_control import (  # ruff: ignore[typing-only-first-party-import]  # FastAPI resolves this annotation at runtime
    ApiRateLimitUpdateBody,
    WsSlowConsumerUpdateBody,
)
from mdgw.admin.feed.models import BAR_INTERVALS, FEED_STATE_FILTERS
from mdgw.admin.feed.service import (  # ruff: ignore[typing-only-first-party-import]  # FastAPI resolves this annotation at runtime
    FeedSubscriptionCreateBody,
    FeedSubscriptionUpdateBody,
)
from mdgw.admin.pagination import (
    InvalidCursorError,
    cursor_page,
    decode_cursor,
    encode_cursor,
    parse_limit,
)
from mdgw.api.errors import get_request_id

if TYPE_CHECKING:
    from mdgw.admin.auth.store import SessionRecord
    from mdgw.admin.context import AdminContext

NO_STORE = {"Cache-Control": "no-store"}
WRITE_ROLES = ("SuperAdmin", "Operator")
_INVALID_CURSOR_REASON = "invalid_cursor"


def create_feed_router(context: AdminContext) -> APIRouter:
    """Build the feed-subscription and flow-control router for one runtime."""
    router = APIRouter()
    runtime = context.feed
    # Restart recovery: reload persisted desired state and re-dispatch it, and
    # re-apply the persisted flow-control policies to this process.
    context.register_startup(runtime.on_startup)
    subscriptions = runtime.subscriptions
    flow_control = runtime.flow_control

    # -- feed subscriptions ------------------------------------------------

    @router.get("/feed-subscriptions")
    def list_feed_subscriptions(  # ruff: ignore[too-many-arguments]  # wire query parity
        feed: str | None = None,
        instrument_id: str | None = None,
        interval: str | None = None,
        state: str | None = None,
        cursor: str | None = None,
        limit: str | None = None,
    ) -> JSONResponse:
        _validate_interval(interval)
        _validate_state(state)
        page_limit = parse_limit(
            limit,
            default=context.config.limits.page_default,
            maximum=context.config.limits.page_max,
        )
        after_created_at_ns, after_subscription_id = _cursor(cursor)
        rows = subscriptions.list(
            feed=feed,
            instrument_id=instrument_id,
            interval=interval,
            state=state,
            after_created_at_ns=after_created_at_ns,
            after_subscription_id=after_subscription_id,
            limit=page_limit + 1,
        )
        has_more = len(rows) > page_limit
        items = rows[:page_limit]
        next_cursor = (
            encode_cursor(
                {
                    "created_at": items[-1].created_at_ns,
                    "subscription_id": items[-1].subscription_id,
                }
            )
            if has_more and items
            else None
        )
        return JSONResponse(
            {
                "items": [item.to_wire() for item in items],
                "page": cursor_page(next_cursor=next_cursor),
            },
            headers=NO_STORE,
        )

    @router.post("/feed-subscriptions")
    def create_feed_subscription(
        body: FeedSubscriptionCreateBody,
        request: Request,
        session: SessionRecord = Depends(require_write(context, *WRITE_ROLES)),  # ruff: ignore[function-call-in-default-argument]
    ) -> JSONResponse:
        idempotency_key = request.headers.get("idempotency-key")
        if idempotency_key is not None and not idempotency_key.strip():
            idempotency_key = None
        wire = subscriptions.create(
            body,
            session=session,
            request_id=get_request_id(request),
            idempotency_key=idempotency_key,
        )
        replayed = bool(wire.pop("_replayed", False))
        headers = {
            **NO_STORE,
            "Location": (
                f"/api/v1/admin/feed-subscriptions/"
                f"{wire['subscription_id']}"
            ),
            "ETag": f'"{wire["desired"]["version"]}"',
        }
        if replayed:
            headers["Idempotent-Replay"] = "true"
        return JSONResponse(wire, status_code=201, headers=headers)

    @router.get("/feed-subscriptions/{subscription_id}")
    def get_feed_subscription(subscription_id: str) -> JSONResponse:
        wire = subscriptions.get(subscription_id)
        return JSONResponse(
            wire,
            headers={
                **NO_STORE,
                "ETag": f'"{wire["desired"]["version"]}"',
            },
        )

    @router.put("/feed-subscriptions/{subscription_id}")
    def update_feed_subscription(
        subscription_id: str,
        body: FeedSubscriptionUpdateBody,
        request: Request,
        session: SessionRecord = Depends(require_write(context, *WRITE_ROLES)),  # ruff: ignore[function-call-in-default-argument]
    ) -> JSONResponse:
        wire = subscriptions.update(
            subscription_id,
            body,
            if_match=request.headers.get("if-match"),
            session=session,
            request_id=get_request_id(request),
        )
        return JSONResponse(
            wire,
            status_code=202,
            headers={
                **NO_STORE,
                "ETag": f'"{wire["desired"]["version"]}"',
            },
        )

    @router.delete("/feed-subscriptions/{subscription_id}")
    def delete_feed_subscription(
        subscription_id: str,
        request: Request,
        session: SessionRecord = Depends(require_write(context, *WRITE_ROLES)),  # ruff: ignore[function-call-in-default-argument]
    ) -> JSONResponse:
        wire = subscriptions.delete(
            subscription_id,
            if_match=request.headers.get("if-match"),
            session=session,
            request_id=get_request_id(request),
        )
        return JSONResponse(
            wire,
            status_code=202,
            headers={
                **NO_STORE,
                "ETag": f'"{wire["desired"]["version"]}"',
            },
        )

    # -- flow control ------------------------------------------------------

    @router.get("/flow-control")
    def get_flow_control() -> JSONResponse:
        return JSONResponse(flow_control.get(), headers=NO_STORE)

    @router.put("/flow-control/api-rate-limit")
    async def update_api_rate_limit(
        body: ApiRateLimitUpdateBody,
        request: Request,
        session: SessionRecord = Depends(require_write(context, *WRITE_ROLES)),  # ruff: ignore[function-call-in-default-argument]
    ) -> JSONResponse:
        wire = await flow_control.update_api_rate_limit(
            body,
            if_match=request.headers.get("if-match"),
            session=session,
            request_id=get_request_id(request),
        )
        return JSONResponse(
            wire,
            status_code=202,
            headers={
                **NO_STORE,
                "ETag": f'"{wire["version"]}"',
            },
        )

    @router.put("/flow-control/ws-slow-consumer")
    async def update_ws_slow_consumer(
        body: WsSlowConsumerUpdateBody,
        request: Request,
        session: SessionRecord = Depends(require_write(context, *WRITE_ROLES)),  # ruff: ignore[function-call-in-default-argument]
    ) -> JSONResponse:
        wire = await flow_control.update_ws_slow_consumer(
            body,
            if_match=request.headers.get("if-match"),
            session=session,
            request_id=get_request_id(request),
        )
        return JSONResponse(
            wire,
            status_code=202,
            headers={
                **NO_STORE,
                "ETag": f'"{wire["version"]}"',
            },
        )

    return router


def _validate_interval(value: str | None) -> None:
    if value is None or value in BAR_INTERVALS:
        return
    raise InvalidParameterError(
        "interval is not a supported Bar Interval",
        details={"field": "interval", "allowed": list(BAR_INTERVALS)},
    )


def _validate_state(value: str | None) -> None:
    if value is None or value in FEED_STATE_FILTERS:
        return
    raise InvalidParameterError(
        "state is not a supported subscription state",
        details={
            "field": "state",
            "allowed": list(FEED_STATE_FILTERS),
        },
    )


def _cursor(value: str | None) -> tuple[int | None, str | None]:
    if value is None:
        return None, None
    try:
        payload = decode_cursor(value)
    except InvalidCursorError as exc:
        raise InvalidParameterError(
            "cursor is invalid",
            details={"field": "cursor", "reason": _INVALID_CURSOR_REASON},
        ) from exc
    created_at = payload.get("created_at")
    subscription_id = payload.get("subscription_id")
    if (
        isinstance(created_at, bool)
        or not isinstance(created_at, int)
        or not isinstance(subscription_id, str)
    ):
        raise InvalidParameterError(
            "cursor is invalid",
            details={"field": "cursor", "reason": _INVALID_CURSOR_REASON},
        )
    return created_at, subscription_id


__all__ = ["create_feed_router"]
