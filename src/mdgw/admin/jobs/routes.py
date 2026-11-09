# ruff: file-ignore[raise-vanilla-args,raw-string-in-exception,fast-api-non-annotated-dependency,too-many-positional-arguments,too-many-statements,too-many-arguments,unused-function-argument]
"""Admin Backfill Job routes (contract section 7).

Reads are available to every authenticated Access Role. Create, cancel and
retry are SuperAdmin/Operator management writes: CSRF + same-origin are
enforced by the shared dependencies, Idempotency-Key is required, and the
Operator Audit Writer is fail-closed - the ``requested`` event is persisted
before the job is persisted/dispatched and a terminal event is appended after
the outcome. The API never executes a job or writes the Dataset.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Depends, Request
from starlette.responses import JSONResponse

from mdgw.admin.audit.models import AuditTarget
from mdgw.admin.audit.writer import AuditHandle, actor_from_session
from mdgw.admin.auth.dependencies import require_session, require_write
from mdgw.admin.errors import (
    AuditUnavailableError,
    ConflictError,
    InvalidParameterError,
    JobNotFoundError,
)
from mdgw.admin.jobs.models import (
    BACKFILL_STATUSES,
    QUEUED,
    RUNNING,
    BackfillJobRecord,
)
from mdgw.admin.jobs.service import (
    ACTION_CANCEL,
    ACTION_CREATE,
    ACTION_RETRY,
)
from mdgw.admin.pagination import (
    InvalidCursorError,
    cursor_page,
    decode_cursor,
    encode_cursor,
    parse_limit,
)
from mdgw.api.errors import get_request_id
from mdgw.backfill_job_plan import BACKFILL_INTERVALS, PLAN_MODES
from mdgw.core.errors import GatewayError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mdgw.admin.auth.store import SessionRecord
    from mdgw.admin.context import AdminContext

logger = logging.getLogger(__name__)

NO_STORE = {"Cache-Control": "no-store"}
WRITE_ROLES = ("SuperAdmin", "Operator")
_INVALID_CURSOR_REASON = "invalid_cursor"
_CREATE_ACTION = "backfill_job.create"
_CANCEL_ACTION = "backfill_job.cancel"
_RETRY_ACTION = "backfill_job.retry"


def create_backfill_router(context: AdminContext) -> APIRouter:
    """Build the ``/backfill-jobs`` router for one Admin runtime."""
    context.register_capabilities(
        capabilities=["backfill_job"],
        details={"backfill_job": {"plan_modes": list(PLAN_MODES)}},
    )
    runtime = context.backfill
    # Restart recovery: queued jobs are resubmitted; unconfirmed running jobs
    # are interrupted (never success).
    context.register_startup(runtime.on_startup)

    router = APIRouter()
    session = require_session(context)
    writer = require_write(context, *WRITE_ROLES)
    service = runtime.service

    @router.post("/backfill-jobs", status_code=202)
    def create_backfill_job(
        body: dict[str, Any],
        request: Request,
        record: SessionRecord = Depends(writer),  # ruff: ignore[function-call-in-default-argument]
    ) -> JSONResponse:
        request_model = service.parse_create(body)
        idempotency_key = _require_idempotency_key(request)
        request_hash = service.request_hash(request_model)
        replay = service.replay(
            record.subject,
            idempotency_key,
            action=ACTION_CREATE,
            request_hash=request_hash,
        )
        if replay is not None:
            return _job_response(replay, replay=True)

        job_id = service.new_job_id()
        handle = context.audit.begin(
            actor=actor_from_session(record),
            action=_CREATE_ACTION,
            target=AuditTarget(type="backfill_job", id=job_id),
            request_id=get_request_id(request),
            reason=request_model.reason,
            job_id=job_id,
            after={
                "job_id": job_id,
                "instrument_id": request_model.instrument_id,
                "interval": request_model.interval,
                "start_ns": request_model.start_ns,
                "end_ns": request_model.end_ns,
                "plan_mode": request_model.plan["mode"],
            },
        )
        try:
            stored, _replayed = service.create(
                request_model,
                job_id=job_id,
                subject=record.subject,
                display_name=record.display_name,
                access_role=record.access_role,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
        except GatewayError as exc:
            _terminal(handle, failure_reason=exc.code)
            raise
        runtime.kick()
        _terminal(
            handle,
            after={"job_id": stored.job_id, "status": stored.status},
        )
        return _job_response(stored, replay=False)

    @router.get("/backfill-jobs")
    def list_backfill_jobs(
        record: SessionRecord = Depends(session),  # ruff: ignore[function-call-in-default-argument]
        status: str | None = None,
        instrument_id: str | None = None,
        interval: str | None = None,
        cursor: str | None = None,
        limit: str | None = None,
    ) -> JSONResponse:
        _validate_status(status)
        _validate_interval_filter(interval)
        page_limit = parse_limit(
            limit,
            default=context.config.limits.page_default,
            maximum=context.config.limits.page_max,
        )
        after = None if cursor is None else _cursor_after(cursor)
        rows = service.list_page(
            status=status,
            instrument_id=instrument_id,
            interval=interval,
            after=after,
            limit=page_limit + 1,
        )
        has_more = len(rows) > page_limit
        items = rows[:page_limit]
        next_cursor = (
            encode_cursor(
                {
                    "created_at_ns": items[-1].created_at_ns,
                    "job_id": items[-1].job_id,
                }
            )
            if has_more and items
            else None
        )
        return JSONResponse(
            {
                "items": [job.to_wire() for job in items],
                "page": cursor_page(next_cursor=next_cursor),
            },
            headers=NO_STORE,
        )

    @router.get("/backfill-jobs/{job_id}")
    def get_backfill_job(
        job_id: str,
        record: SessionRecord = Depends(session),  # ruff: ignore[function-call-in-default-argument]
    ) -> JSONResponse:
        job = _get_or_404(service, job_id)
        return JSONResponse(job.to_wire(), headers=NO_STORE)

    @router.post("/backfill-jobs/{job_id}/cancel", status_code=202)
    def cancel_backfill_job(
        job_id: str,
        body: dict[str, Any],
        request: Request,
        record: SessionRecord = Depends(writer),  # ruff: ignore[function-call-in-default-argument]
    ) -> JSONResponse:
        job = _get_or_404(service, job_id)
        reason = service.parse_reason(body)
        idempotency_key = _require_idempotency_key(request)
        request_hash = service.action_hash(job_id=job_id, reason=reason)
        replay = service.replay(
            record.subject,
            idempotency_key,
            action=ACTION_CANCEL,
            request_hash=request_hash,
        )
        if replay is not None:
            return _job_response(replay, replay=True)
        _require_cancellable(job)

        handle = context.audit.begin(
            actor=actor_from_session(record),
            action=_CANCEL_ACTION,
            target=AuditTarget(type="backfill_job", id=job_id),
            request_id=get_request_id(request),
            reason=reason,
            job_id=job_id,
        )
        try:
            updated, _replayed = service.cancel(
                job_id,
                subject=record.subject,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
        except GatewayError as exc:
            _terminal(handle, failure_reason=exc.code)
            raise
        if updated is None:
            _terminal(handle, failure_reason="job_not_cancellable")
            latest = _get_or_404(service, job_id)
            raise ConflictError(
                "the backfill job is not cancellable",
                details={
                    "reason": "job_not_cancellable",
                    "status": latest.status,
                },
            )
        runtime.kick()
        _terminal(handle, after={"status": updated.status})
        return _job_response(updated, replay=False)

    @router.post("/backfill-jobs/{job_id}/retry", status_code=202)
    def retry_backfill_job(
        job_id: str,
        body: dict[str, Any],
        request: Request,
        record: SessionRecord = Depends(writer),  # ruff: ignore[function-call-in-default-argument]
    ) -> JSONResponse:
        previous = _get_or_404(service, job_id)
        reason = service.parse_reason(body)
        idempotency_key = _require_idempotency_key(request)
        request_hash = service.action_hash(job_id=job_id, reason=reason)
        replay = service.replay(
            record.subject,
            idempotency_key,
            action=ACTION_RETRY,
            request_hash=request_hash,
        )
        if replay is not None:
            return _job_response(replay, replay=True)

        new_job_id = service.new_job_id()
        handle = context.audit.begin(
            actor=actor_from_session(record),
            action=_RETRY_ACTION,
            target=AuditTarget(type="backfill_job", id=new_job_id),
            request_id=get_request_id(request),
            reason=reason,
            job_id=new_job_id,
            after={
                "job_id": new_job_id,
                "retry_of_job_id": job_id,
                "instrument_id": previous.instrument_id,
                "interval": previous.interval,
                "start_ns": previous.start_ns,
                "end_ns": previous.end_ns,
            },
        )
        try:
            stored, _replayed = service.retry(
                previous,
                new_job_id=new_job_id,
                reason=reason,
                subject=record.subject,
                display_name=record.display_name,
                access_role=record.access_role,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
        except GatewayError as exc:
            _terminal(handle, failure_reason=exc.code)
            raise
        runtime.kick()
        _terminal(
            handle,
            after={
                "job_id": stored.job_id,
                "retry_of_job_id": stored.retry_of_job_id,
                "status": stored.status,
            },
        )
        return _job_response(stored, replay=False)

    return router


# -- helpers ---------------------------------------------------------------


def _job_response(job: BackfillJobRecord, *, replay: bool) -> JSONResponse:
    headers = dict(NO_STORE)
    if replay:
        headers["Idempotent-Replay"] = "true"
    return JSONResponse(job.to_wire(), status_code=202, headers=headers)


def _get_or_404(service: Any, job_id: str) -> BackfillJobRecord:
    job = service.get(job_id)
    if job is None:
        raise JobNotFoundError(
            "backfill job not found", details={"job_id": job_id}
        )
    return job


def _require_cancellable(job: BackfillJobRecord) -> None:
    if job.status in {QUEUED, RUNNING}:
        return
    raise ConflictError(
        "the backfill job is not cancellable",
        details={"reason": "job_not_cancellable", "status": job.status},
    )


def _require_idempotency_key(request: Request) -> str:
    value = request.headers.get("Idempotency-Key")
    if value is None or not value.strip():
        raise InvalidParameterError(
            "the Idempotency-Key header is required",
            details={"field": "Idempotency-Key", "reason": "required"},
        )
    return value.strip()


def _validate_status(value: str | None) -> None:
    if value is None or value in BACKFILL_STATUSES:
        return
    raise InvalidParameterError(
        "status is not a supported Backfill Job status",
        details={"field": "status", "allowed": list(BACKFILL_STATUSES)},
    )


def _validate_interval_filter(value: str | None) -> None:
    if value is None or value in BACKFILL_INTERVALS:
        return
    raise InvalidParameterError(
        "interval is not a supported Bar Interval",
        details={"field": "interval", "allowed": list(BACKFILL_INTERVALS)},
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
    job_id = payload.get("job_id")
    if (
        isinstance(created_at_ns, bool)
        or not isinstance(created_at_ns, int)
        or not isinstance(job_id, str)
        or not job_id
    ):
        raise InvalidParameterError(
            msg,
            details={"field": "cursor", "reason": _INVALID_CURSOR_REASON},
        )
    return created_at_ns, job_id


def _terminal(
    handle: AuditHandle,
    *,
    after: Mapping[str, Any] | None = None,
    failure_reason: str | None = None,
) -> None:
    """Append the terminal audit event; never turn an executed write into 503."""
    try:
        if failure_reason is None:
            handle.succeeded(after=after)
        else:
            handle.failed(failure_reason=failure_reason, after=after)
    except AuditUnavailableError:  # pragma: no cover - audit failed late
        logger.exception(
            "failed to append the terminal audit event; the action executed",
            extra={"event": "admin.backfill.audit_terminal_failed"},
        )


__all__ = ["create_backfill_router"]
