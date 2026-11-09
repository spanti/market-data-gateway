"""Authenticated Export Job routes (contract section 6).

Every route re-authorizes server-side. SuperAdmin sees every job; Operator and
Auditor see and act on their own jobs only. Create and cancel are management
writes: the audit ``requested`` event is persisted before execution and a
terminal event is appended afterwards; if the audit store is unavailable the
write is not executed (503 ``audit_unavailable``).

Downloads expose only the opaque job id: the response body is the CSV file,
never a server path. A single HTTP Range is supported with ``206`` and
``Content-Range``; malformed, multiple or unsatisfiable ranges return ``416``
with the unified error envelope.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from fastapi import APIRouter, Depends, Request
from starlette.responses import FileResponse, JSONResponse

from mdgw.admin.audit.models import AuditTarget
from mdgw.admin.audit.writer import AuditHandle, actor_from_session
from mdgw.admin.auth.dependencies import (
    require_ownership,
    require_session,
    require_write,
)
from mdgw.admin.capabilities import AdapterStatus
from mdgw.admin.errors import (
    AuditUnavailableError,
    ConflictError,
    ControlUnavailableError,
    ExportExpiredError,
    InvalidParameterError,
    JobNotFoundError,
)
from mdgw.admin.export.models import QUEUED, RUNNING
from mdgw.admin.pagination import (
    InvalidCursorError,
    cursor_page,
    decode_cursor,
    encode_cursor,
    parse_limit,
)
from mdgw.api.errors import get_request_id
from mdgw.api.formats import CSV_MEDIA_TYPE
from mdgw.core.errors import INVALID_PARAMETER, GatewayError
from mdgw.core.time import ns_to_iso

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mdgw.admin.auth.store import SessionRecord
    from mdgw.admin.context import AdminContext
    from mdgw.admin.export.service import ExportService

logger = logging.getLogger(__name__)

NO_STORE = {"Cache-Control": "no-store"}
INVALID_CURSOR_REASON = "invalid_cursor"
RANGE_REASON = "range_not_satisfiable"
_RANGE_HEADER_RE = re.compile(r"^\s*bytes\s*=\s*(?P<spec>.+)$", re.IGNORECASE)


class RangeNotSatisfiableError(GatewayError):
    """A single-range download could not be satisfied (HTTP 416)."""

    code = INVALID_PARAMETER
    status = 416


def create_export_router(  # ruff: ignore[too-many-statements] - route set
    context: AdminContext,
) -> APIRouter:
    """Build the ``/export-jobs`` router for one runtime context."""
    context.register_capabilities(
        adapters=["export_executor"],
        capabilities=["export_job"],
        details={
            "export_job": {
                "formats": ["csv"],
                "min_days": context.config.export.min_days,
                "max_days": context.config.export.max_days,
                "retention_s": context.config.export.retention_s,
                "max_running_per_user": (
                    context.config.export.max_running_per_user
                ),
            }
        },
    )
    context.register_adapter_status_provider(
        "export_executor", lambda: _adapter_status(context)
    )

    router = APIRouter()
    session = require_session(context)
    writer = require_write(context)

    @router.post("/export-jobs", status_code=202)
    def create_export_job(
        body: dict[str, Any],
        request: Request,
        record: SessionRecord = Depends(writer),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
    ) -> JSONResponse:
        service = _service(context)
        parsed = service.parse_request(body)
        idempotency_key = _idempotency_key(request)
        request_hash = service.request_hash(parsed)
        if idempotency_key is not None:
            existing = service.lookup_idempotent(
                record.subject, idempotency_key, request_hash
            )
            if existing is not None:
                return _job_response(
                    existing, service, replay=True, status_code=202
                )

        job_id = service.new_job_id()
        handle = context.audit.begin(
            actor=actor_from_session(record),
            action="export_job.create",
            target=AuditTarget(type="export_job", id=job_id),
            request_id=get_request_id(request),
            job_id=job_id,
            after={
                "instrument_id": parsed.instrument_id,
                "interval": parsed.interval,
                "start": ns_to_iso(parsed.start_ns),
                "end": ns_to_iso(parsed.end_ns),
                "range_days": parsed.range_days,
                "format": parsed.format,
            },
        )
        try:
            stored, replayed = service.create(
                parsed,
                job_id=job_id,
                subject=record.subject,
                display_name=record.display_name,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
        except GatewayError as exc:
            _terminal(handle, failure_reason=exc.code)
            raise
        _terminal(
            handle,
            after={"job_id": stored.job_id, "status": stored.status},
        )
        return _job_response(
            stored, service, replay=replayed, status_code=202
        )

    @router.get("/export-jobs")
    def list_export_jobs(
        record: SessionRecord = Depends(session),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
        cursor: str | None = None,
        limit: str | None = None,
    ) -> JSONResponse:
        service = _service(context)
        page_limit = parse_limit(
            limit,
            default=context.config.limits.page_default,
            maximum=context.config.limits.page_max,
        )
        after = None if cursor is None else _cursor_after(cursor)
        owner = (
            None
            if record.access_role == "SuperAdmin"
            else record.subject
        )
        rows = service.list_page(
            owner_subject=owner, after=after, limit=page_limit + 1
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
        now = service.now_ns()
        return JSONResponse(
            {
                "items": [job.to_wire(now) for job in items],
                "page": cursor_page(next_cursor=next_cursor),
            },
            headers=NO_STORE,
        )

    @router.get("/export-jobs/{job_id}")
    def get_export_job(
        job_id: str,
        record: SessionRecord = Depends(session),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
    ) -> JSONResponse:
        service = _service(context)
        job = _owned_job(context, service, record, job_id)
        return JSONResponse(
            job.to_wire(service.now_ns()), headers=NO_STORE
        )

    @router.post("/export-jobs/{job_id}/cancel", status_code=202)
    def cancel_export_job(
        job_id: str,
        request: Request,
        record: SessionRecord = Depends(writer),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
    ) -> JSONResponse:
        service = _service(context)
        job = _owned_job(context, service, record, job_id)
        if job.status not in {QUEUED, RUNNING}:
            msg = "the export job is not cancellable"
            raise ConflictError(
                msg,
                details={
                    "reason": "job_not_cancellable",
                    "status": job.status,
                },
            )
        handle = context.audit.begin(
            actor=actor_from_session(record),
            action="export_job.cancel",
            target=AuditTarget(type="export_job", id=job_id),
            request_id=get_request_id(request),
            job_id=job_id,
        )
        if not service.cancel(job_id):
            latest = service.get(job_id) or job
            _terminal(handle, failure_reason="job_not_cancellable")
            msg = "the export job is not cancellable"
            raise ConflictError(
                msg,
                details={
                    "reason": "job_not_cancellable",
                    "status": latest.status,
                },
            )
        latest = service.get(job_id) or job
        _terminal(handle, after={"status": latest.status})
        return _job_response(
            latest, service, replay=False, status_code=202
        )

    @router.get("/export-jobs/{job_id}/download")
    def download_export_job(
        job_id: str,
        request: Request,
        record: SessionRecord = Depends(session),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
    ) -> FileResponse:
        service = _service(context)
        _owned_job(context, service, record, job_id)
        job, path = service.download_check(job_id)
        result = job.result or {}
        size_bytes = int(result["size_bytes"])
        _validate_range(request.headers.get("range"), size_bytes)
        try:
            stat_result = path.stat()
        except OSError as exc:
            # Expiry cleanup may have removed the file after the pre-check.
            msg = "the export file is no longer available"
            raise ExportExpiredError(
                msg,
                details={
                    "job_id": job_id,
                    "expires_at": (
                        None
                        if job.expires_at_ns is None
                        else ns_to_iso(job.expires_at_ns)
                    ),
                },
            ) from exc
        headers = {
            "Content-Type": CSV_MEDIA_TYPE,
            "Content-Disposition": _content_disposition(
                str(result["file_name"])
            ),
            "Content-Length": str(size_bytes),
            "Accept-Ranges": "bytes",
            "ETag": f'"{result["sha256"]}"',
            "Cache-Control": "no-store",
        }
        return FileResponse(
            path,
            media_type=CSV_MEDIA_TYPE,
            headers=headers,
            stat_result=stat_result,
        )

    return router


# -- helpers ---------------------------------------------------------------


def _service(context: AdminContext) -> ExportService:
    if not context.config.adapters.export_executor:
        msg = "the export executor is disabled by configuration"
        raise ControlUnavailableError(
            msg,
            details={"source": "export_executor", "retryable": False},
        )
    try:
        return context.exports
    except Exception as exc:  # no executor means no accepted export action
        logger.exception(
            "export executor is unavailable",
            extra={"event": "admin.export.unavailable"},
        )
        msg = "the export executor is unavailable"
        raise ControlUnavailableError(
            msg,
            details={"source": "export_executor", "retryable": True},
        ) from exc


def _adapter_status(context: AdminContext) -> Any:
    if not context.config.adapters.export_executor:
        return AdapterStatus("unsupported", "disabled by configuration")
    service = context.exports
    if service.available:
        return AdapterStatus("available")
    return AdapterStatus(
        "unavailable", "the export executor or Bars Store is not running"
    )


def _owned_job(
    context: AdminContext,
    service: ExportService,
    record: SessionRecord,
    job_id: str,
) -> Any:
    job = service.get(job_id)
    if job is None:
        msg = "export job not found"
        raise JobNotFoundError(msg, details={"job_id": job_id})
    require_ownership(
        context, record, job.created_by_subject, resource_id=job_id
    )
    return job


def _job_response(
    job: Any, service: ExportService, *, replay: bool, status_code: int
) -> JSONResponse:
    headers = dict(NO_STORE)
    if replay:
        headers["Idempotent-Replay"] = "true"
    return JSONResponse(
        job.to_wire(service.now_ns()),
        status_code=status_code,
        headers=headers,
    )


def _terminal(
    handle: AuditHandle,
    *,
    after: Mapping[str, Any] | None = None,
    failure_reason: str | None = None,
) -> None:
    """Append the terminal event; never turn an executed write into a 503."""
    try:
        if failure_reason is None:
            handle.succeeded(after=after)
        else:
            handle.failed(failure_reason=failure_reason, after=after)
    except AuditUnavailableError:  # pragma: no cover - audit failed late
        logger.exception(
            "failed to append the terminal audit event; the action executed",
            extra={"event": "admin.export.audit_terminal_failed"},
        )


def _idempotency_key(request: Request) -> str | None:
    value = request.headers.get("Idempotency-Key")
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _cursor_after(cursor: str) -> tuple[int, str]:
    msg = "cursor is invalid"
    try:
        payload = decode_cursor(cursor)
    except InvalidCursorError as exc:
        raise InvalidParameterError(
            msg,
            details={"field": "cursor", "reason": INVALID_CURSOR_REASON},
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
            details={"field": "cursor", "reason": INVALID_CURSOR_REASON},
        )
    return created_at_ns, job_id


def _validate_range(value: str | None, size_bytes: int) -> None:
    """Accept exactly one satisfiable ``bytes=`` range or raise 416."""
    if value is None:
        return
    match = _RANGE_HEADER_RE.match(value)
    if match is None:
        _raise_range()
    spec = match.group("spec").strip()
    if "," in spec or not spec or spec == "-":
        _raise_range()
    start_text, dash, end_text = spec.partition("-")
    if not dash:
        _raise_range()
    start_text = start_text.strip()
    end_text = end_text.strip()
    if not start_text:
        if not end_text.isdigit():
            _raise_range()
        suffix = int(end_text)
        if suffix <= 0 or size_bytes <= 0:
            _raise_range()
        return
    if not start_text.isdigit():
        _raise_range()
    start = int(start_text)
    if end_text:
        if not end_text.isdigit():
            _raise_range()
        if int(end_text) < start:
            _raise_range()
    if start >= size_bytes:
        _raise_range()


def _raise_range() -> None:
    msg = "the requested byte range cannot be satisfied"
    raise RangeNotSatisfiableError(msg, details={"reason": RANGE_REASON})


def _content_disposition(file_name: str) -> str:
    """A safe attachment name with both RFC 6266 forms.

    The service generates ASCII-only names, but the fallback strips quotes,
    backslashes and control characters so a future name cannot break the
    header or inject a path.
    """
    fallback = "".join(
        character
        if 32 <= ord(character) < 127 and character not in '"\\'
        else "_"
        for character in file_name
    )
    encoded = quote(file_name, safe="")
    return (
        f'attachment; filename="{fallback}"; '
        f"filename*=UTF-8''{encoded}"
    )


__all__ = ["RangeNotSatisfiableError", "create_export_router"]
