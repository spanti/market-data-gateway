# ruff: file-ignore[raise-vanilla-args,raw-string-in-exception,fast-api-non-annotated-dependency,too-many-positional-arguments,unused-function-argument]
"""Read-only persistent Backfill Gap routes (contract section 7.4).

Every authenticated Access Role may read the filtered keyset list and the
detail endpoint. There is deliberately no mutation, resolve or delete route:
only a successful Backfill Job resolves a Gap, and persistent history is never
physically deleted.

The list is served from the Admin management database. A Collector outage only
stops new ``collector_process`` evidence from arriving; it never hides or
deletes already-persisted gaps.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends
from starlette.responses import JSONResponse

from mdgw.admin.auth.dependencies import require_session
from mdgw.admin.errors import AdminNotFoundError, InvalidParameterError
from mdgw.admin.gaps.models import GAP_STATUSES
from mdgw.admin.gaps.service import parse_time_filter
from mdgw.admin.pagination import (
    InvalidCursorError,
    cursor_page,
    decode_cursor,
    encode_cursor,
    parse_limit,
)
from mdgw.core.schema import INTERVALS

if TYPE_CHECKING:
    from mdgw.admin.auth.store import SessionRecord
    from mdgw.admin.context import AdminContext

NO_STORE = {"Cache-Control": "no-store"}
_INVALID_CURSOR_REASON = "invalid_cursor"


def create_gaps_router(context: AdminContext) -> APIRouter:
    """Build the ``/backfill-gaps`` read-only router for one runtime."""
    context.register_capabilities(capabilities=["persistent_backfill_gaps"])
    runtime = context.gaps
    # Poll the Collector's process-local snapshot once when the Admin app is
    # mounted; a bounded worker keeps it fresh afterwards.
    context.register_startup(runtime.on_startup)

    router = APIRouter()
    session = require_session(context)
    service = runtime.service

    @router.get("/backfill-gaps")
    def list_backfill_gaps(  # ruff: ignore[too-many-arguments]  # wire query parity
        record: SessionRecord = Depends(session),  # ruff: ignore[function-call-in-default-argument]
        status: str | None = None,
        feed: str | None = None,
        instrument_id: str | None = None,
        interval: str | None = None,
        reason: str | None = None,
        discovered_from: str | None = None,
        discovered_to: str | None = None,
        resolved_from: str | None = None,
        resolved_to: str | None = None,
        cursor: str | None = None,
        limit: str | None = None,
    ) -> JSONResponse:
        _validate_status(status)
        _validate_interval(interval)
        page_limit = parse_limit(
            limit,
            default=context.config.limits.page_default,
            maximum=context.config.limits.page_max,
        )
        after = None if cursor is None else _cursor_after(cursor)
        rows = service.list_page(
            status=status,
            feed=feed,
            instrument_id=instrument_id,
            interval=interval,
            reason=reason,
            discovered_from_ns=parse_time_filter(
                discovered_from, "discovered_from"
            ),
            discovered_to_ns=parse_time_filter(
                discovered_to, "discovered_to"
            ),
            resolved_from_ns=parse_time_filter(
                resolved_from, "resolved_from"
            ),
            resolved_to_ns=parse_time_filter(resolved_to, "resolved_to"),
            after=after,
            limit=page_limit + 1,
        )
        has_more = len(rows) > page_limit
        items = rows[:page_limit]
        next_cursor = (
            encode_cursor(
                {
                    "created_at_ns": items[-1].created_at_ns,
                    "gap_id": items[-1].gap_id,
                }
            )
            if has_more and items
            else None
        )
        return JSONResponse(
            {
                "items": [gap.to_wire() for gap in items],
                "page": cursor_page(next_cursor=next_cursor),
            },
            headers=NO_STORE,
        )

    @router.get("/backfill-gaps/{gap_id}")
    def get_backfill_gap(
        gap_id: str,
        record: SessionRecord = Depends(session),  # ruff: ignore[function-call-in-default-argument]
    ) -> JSONResponse:
        gap = service.get(gap_id)
        if gap is None:
            raise AdminNotFoundError(
                "backfill gap not found",
                details={"resource": "backfill_gap", "id": gap_id},
            )
        return JSONResponse(gap.to_wire(), headers=NO_STORE)

    return router


# -- helpers ---------------------------------------------------------------


def _validate_status(value: str | None) -> None:
    if value is None or value in GAP_STATUSES:
        return
    raise InvalidParameterError(
        "status is not a supported Gap status",
        details={"field": "status", "allowed": list(GAP_STATUSES)},
    )


def _validate_interval(value: str | None) -> None:
    if value is None or value in INTERVALS:
        return
    raise InvalidParameterError(
        "interval is not a supported Bar Interval",
        details={"field": "interval", "allowed": list(INTERVALS)},
    )


def _cursor_after(cursor: str) -> tuple[int, str]:
    msg = "cursor is invalid"
    try:
        payload = decode_cursor(cursor)
    except InvalidCursorError as exc:
        raise InvalidParameterError(
            msg,
            details={"field": "cursor", "reason": _INVALID_CURSOR_REASON},
        ) from exc
    created_at_ns = payload.get("created_at_ns")
    gap_id = payload.get("gap_id")
    if (
        isinstance(created_at_ns, bool)
        or not isinstance(created_at_ns, int)
        or not isinstance(gap_id, str)
        or not gap_id
    ):
        raise InvalidParameterError(
            msg,
            details={"field": "cursor", "reason": _INVALID_CURSOR_REASON},
        )
    return created_at_ns, gap_id


__all__ = ["create_gaps_router"]
