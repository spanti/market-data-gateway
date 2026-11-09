"""Authenticated Operator Audit Event query routes.

Only two read-only endpoints exist - the filtered keyset list and the event
detail. There is deliberately no edit, delete or export route, and the storage
layer rejects UPDATE/DELETE. Every route is mounted under the Admin router's
session dependency, so all three Access Roles may read and anonymous requests
get the stable ``401 unauthenticated`` envelope.

List semantics (contract section 8.4):

* filters are exact matches except ``from`` (inclusive) / ``to`` (exclusive);
* default order is ``sequence desc``; an ``operation_id`` query without an
  explicit ``order`` is a timeline and defaults to ``sequence asc``;
* pagination is an opaque keyset cursor over ``sequence`` with ``limit``
  bounded by ``admin.limits``; the response is ``{items, page}``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Query
from starlette.responses import JSONResponse

from mdgw.admin.audit.models import AUDIT_RESULTS, AuditQuery
from mdgw.admin.capabilities import ADMIN_ROLES
from mdgw.admin.errors import (
    AdminNotFoundError,
    InvalidParameterError,
)
from mdgw.admin.pagination import (
    InvalidCursorError,
    cursor_page,
    decode_cursor,
    encode_cursor,
    parse_limit,
    parse_order,
)
from mdgw.core.time import iso_to_ns

if TYPE_CHECKING:
    from mdgw.admin.context import AdminContext

NO_STORE = {"Cache-Control": "no-store"}
INVALID_CURSOR_REASON = "invalid_cursor"
INVALID_TIMESTAMP_REASON = "invalid_timestamp"


def create_audit_router(context: AdminContext) -> APIRouter:
    """Build the ``/audit-events`` read-only router for one runtime."""
    router = APIRouter()

    @router.get("/audit-events")
    def list_audit_events(  # ruff: ignore[too-many-arguments, too-many-positional-arguments]  # wire query parity
        from_: Annotated[str | None, Query(alias="from")] = None,
        to: str | None = None,
        actor_subject: str | None = None,
        access_role: str | None = None,
        action: str | None = None,
        target_type: str | None = None,
        target_id: str | None = None,
        result: str | None = None,
        request_id: str | None = None,
        operation_id: str | None = None,
        job_id: str | None = None,
        instance_id: str | None = None,
        order: str | None = None,
        cursor: str | None = None,
        limit: str | None = None,
    ) -> JSONResponse:
        from_ns = _parse_time(from_, "from")
        to_ns = _parse_time(to, "to")
        _validate_choice(access_role, ADMIN_ROLES, "access_role")
        _validate_choice(result, AUDIT_RESULTS, "result")
        normalized_order = parse_order(
            order, default="asc" if operation_id else "desc"
        )
        page_limit = parse_limit(
            limit,
            default=context.config.limits.page_default,
            maximum=context.config.limits.page_max,
        )
        after_sequence = (
            None if cursor is None else _cursor_sequence(cursor)
        )

        rows = context.audit.store.query(
            AuditQuery(
                from_ns=from_ns,
                to_ns=to_ns,
                actor_subject=actor_subject,
                access_role=access_role,
                action=action,
                target_type=target_type,
                target_id=target_id,
                result=result,
                request_id=request_id,
                operation_id=operation_id,
                job_id=job_id,
                instance_id=instance_id,
                order=normalized_order,
                after_sequence=after_sequence,
                limit=page_limit + 1,
            )
        )
        has_more = len(rows) > page_limit
        items = rows[:page_limit]
        next_cursor = (
            encode_cursor({"sequence": items[-1].sequence})
            if has_more and items
            else None
        )
        return JSONResponse(
            {
                "items": [event.to_wire() for event in items],
                "page": cursor_page(next_cursor=next_cursor),
            },
            headers=NO_STORE,
        )

    @router.get("/audit-events/{event_id}")
    def get_audit_event(event_id: str) -> JSONResponse:
        event = context.audit.store.get(event_id)
        if event is None:
            msg = "audit event not found"
            raise AdminNotFoundError(
                msg,
                details={"resource": "audit_event", "id": event_id},
            )
        return JSONResponse(event.to_wire(), headers=NO_STORE)

    return router


def _parse_time(value: str | None, field: str) -> int | None:
    if value is None:
        return None
    try:
        return iso_to_ns(value)
    except ValueError as exc:
        msg = f"{field} must be a UTC ISO-8601 timestamp"
        raise InvalidParameterError(
            msg,
            details={"field": field, "reason": INVALID_TIMESTAMP_REASON},
        ) from exc


def _validate_choice(
    value: str | None, allowed: tuple[str, ...], field: str
) -> None:
    if value is None or value in allowed:
        return
    msg = f"{field} must be one of {', '.join(allowed)}"
    raise InvalidParameterError(
        msg, details={"field": field, "allowed": list(allowed)}
    )


def _cursor_sequence(cursor: str) -> int:
    try:
        payload = decode_cursor(cursor)
    except InvalidCursorError as exc:
        msg = "cursor is invalid"
        raise InvalidParameterError(
            msg,
            details={"field": "cursor", "reason": INVALID_CURSOR_REASON},
        ) from exc
    sequence = payload.get("sequence")
    if isinstance(sequence, bool) or not isinstance(sequence, int):
        msg = "cursor is invalid"
        raise InvalidParameterError(
            msg,
            details={"field": "cursor", "reason": INVALID_CURSOR_REASON},
        )
    return sequence


__all__ = ["create_audit_router"]
