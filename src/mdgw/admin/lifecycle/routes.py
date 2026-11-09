"""Service Instance lifecycle and Operation query routes.

* ``GET /service-instances`` is readable by every authenticated role. It
  returns the Supervisor-observed registry (``{items, page}``), never the
  static config.
* Start/Stop/Reload are SuperAdmin-only, require CSRF+same-origin (the
  router-level dependency) and a mandatory ``Idempotency-Key``. The API
  audits ``requested`` before dispatch and the Supervisor audits the terminal
  outcome; the HTTP response is ``202`` + Operation the moment the Supervisor
  accepts it.
* Operation queries are readable by every authenticated role.
* A connection loss before acceptance is ``503 control_unavailable`` (nothing
  was accepted). Once a 202 was returned, a lost connection means the
  Operation may be ``unknown``; it is never treated as failure and the write
  is never auto-replayed.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from typing import TYPE_CHECKING, Annotated, Any

from fastapi import APIRouter, Depends, Header, Query, Request
from pydantic import BaseModel, ConfigDict
from starlette.responses import JSONResponse

from mdgw.admin.audit.models import AuditTarget
from mdgw.admin.audit.writer import actor_from_session
from mdgw.admin.auth.dependencies import require_session, require_write
from mdgw.admin.auth.store import (  # ruff: ignore[typing-only-first-party-import]  # FastAPI resolves this annotation at runtime
    SessionRecord,
)
from mdgw.admin.errors import (
    AdminNotFoundError,
    ConflictError,
    ControlUnavailableError,
    InvalidParameterError,
)
from mdgw.admin.lifecycle.control_client import (
    SupervisorControlError,
    SupervisorUnavailableError,
)
from mdgw.admin.lifecycle.models import (
    KIND_RELOAD,
    KIND_START,
    KIND_STOP,
    OPERATION_KINDS,
    OPERATION_STATUSES,
    SERVICE_ROLES,
    OperationQuery,
    OperationRecord,
)
from mdgw.admin.lifecycle.protocol import (
    CONTROL_ACTION_RELOAD,
    CONTROL_ACTION_START,
    CONTROL_ACTION_STOP,
)
from mdgw.admin.pagination import (
    InvalidCursorError,
    cursor_page,
    decode_cursor,
    encode_cursor,
    parse_limit,
)
from mdgw.api.errors import get_request_id
from mdgw.core.time import iso_to_ns

if TYPE_CHECKING:
    from mdgw.admin.context import AdminContext

NO_STORE = {"Cache-Control": "no-store"}
NANOS_PER_SECOND = 1_000_000_000
_KEY_RE = re.compile(r"^[A-Za-z0-9._:-]{1,256}$")
_INVALID_CURSOR_REASON = "invalid_cursor"
_INVALID_TIMESTAMP_REASON = "invalid_timestamp"
_KEY_REUSE_REASON = "idempotency_key_reuse"
_CONTROL_SOURCE = "supervisor"

_ACTION_KINDS = {"start": KIND_START, "stop": KIND_STOP, "reload": KIND_RELOAD}
_ACTION_CONTROL = {
    "start": CONTROL_ACTION_START,
    "stop": CONTROL_ACTION_STOP,
    "reload": CONTROL_ACTION_RELOAD,
}


class LifecycleActionBody(BaseModel):
    """The contract request body; extra fields are rejected."""

    model_config = ConfigDict(extra="forbid")

    reason: str = ""
    confirm: bool = False
    confirm_instance_name: str | None = None


def create_lifecycle_router(context: AdminContext) -> APIRouter:
    """Build the lifecycle router for one runtime context."""
    router = APIRouter()
    session_dependency = require_session(context)
    write_dependency = require_write(context, "SuperAdmin")

    @router.get("/service-instances")
    def list_service_instances(
        _record: SessionRecord = Depends(session_dependency),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
    ) -> JSONResponse:
        items = context.lifecycle.store.list_instances()
        return JSONResponse(
            {
                "items": [item.to_wire() for item in items],
                "page": cursor_page(next_cursor=None),
            },
            headers=NO_STORE,
        )

    @router.post("/service-instances/{instance_id}/start")
    def start_service_instance(
        instance_id: str,
        body: LifecycleActionBody,
        request: Request,
        idempotency_key: Annotated[
            str | None, Header(alias="Idempotency-Key")
        ] = None,
        record: SessionRecord = Depends(write_dependency),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
    ) -> JSONResponse:
        return _handle_action(
            context,
            request=request,
            record=record,
            instance_id=instance_id,
            action="start",
            body=body,
            idempotency_key=idempotency_key,
        )

    @router.post("/service-instances/{instance_id}/stop")
    def stop_service_instance(
        instance_id: str,
        body: LifecycleActionBody,
        request: Request,
        idempotency_key: Annotated[
            str | None, Header(alias="Idempotency-Key")
        ] = None,
        record: SessionRecord = Depends(write_dependency),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
    ) -> JSONResponse:
        return _handle_action(
            context,
            request=request,
            record=record,
            instance_id=instance_id,
            action="stop",
            body=body,
            idempotency_key=idempotency_key,
        )

    @router.post("/service-instances/{instance_id}/reload")
    def reload_service_instance(
        instance_id: str,
        body: LifecycleActionBody,
        request: Request,
        idempotency_key: Annotated[
            str | None, Header(alias="Idempotency-Key")
        ] = None,
        record: SessionRecord = Depends(write_dependency),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
    ) -> JSONResponse:
        return _handle_action(
            context,
            request=request,
            record=record,
            instance_id=instance_id,
            action="reload",
            body=body,
            idempotency_key=idempotency_key,
        )

    @router.get("/operations")
    def list_operations(  # ruff: ignore[too-many-arguments, too-many-positional-arguments]  # wire query parity
        from_: Annotated[str | None, Query(alias="from")] = None,
        to: str | None = None,
        instance_id: str | None = None,
        kind: str | None = None,
        status: str | None = None,
        cursor: str | None = None,
        limit: str | None = None,
        _record: SessionRecord = Depends(session_dependency),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
    ) -> JSONResponse:
        from_ns = _parse_time(from_, "from")
        to_ns = _parse_time(to, "to")
        _validate_choice(kind, OPERATION_KINDS, "kind")
        _validate_choice(status, OPERATION_STATUSES, "status")
        page_limit = parse_limit(
            limit,
            default=context.config.limits.page_default,
            maximum=context.config.limits.page_max,
        )
        before_sequence = (
            None if cursor is None else _cursor_sequence(cursor)
        )
        rows = context.lifecycle.store.query_operations(
            OperationQuery(
                instance_id=instance_id,
                kind=kind,
                status=status,
                from_ns=from_ns,
                to_ns=to_ns,
                before_sequence=before_sequence,
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
                "items": [item.to_wire() for item in items],
                "page": cursor_page(next_cursor=next_cursor),
            },
            headers=NO_STORE,
        )

    @router.get("/operations/{operation_id}")
    def get_operation(
        operation_id: str,
        _record: SessionRecord = Depends(session_dependency),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
    ) -> JSONResponse:
        operation = context.lifecycle.store.get_operation(operation_id)
        if operation is None:
            msg = "operation not found"
            raise AdminNotFoundError(
                msg,
                details={"resource": "operation", "id": operation_id},
            )
        return JSONResponse(operation.to_wire(), headers=NO_STORE)

    return router


def _handle_action(  # ruff: ignore[too-many-arguments, too-many-branches, too-many-statements]  # explicit action state machine
    context: AdminContext,
    *,
    request: Request,
    record: SessionRecord,
    instance_id: str,
    action: str,
    body: LifecycleActionBody,
    idempotency_key: str | None,
) -> JSONResponse:
    kind = _ACTION_KINDS[action]
    if instance_id not in SERVICE_ROLES:
        msg = "service instance not found"
        raise AdminNotFoundError(
            msg, details={"resource": "service_instance", "id": instance_id}
        )
    reason = body.reason.strip()
    if not reason:
        msg = "reason must be a non-empty string"
        raise InvalidParameterError(msg, details={"field": "reason"})
    maximum = context.config.limits.reason_max_length
    if len(reason) > maximum:
        msg = f"reason must be at most {maximum} characters"
        raise InvalidParameterError(
            msg,
            details={"field": "reason", "constraint": {"max_length": maximum}},
        )
    if body.confirm is not True:
        msg = "confirm must be true"
        raise InvalidParameterError(
            msg, details={"field": "confirm", "constraint": {"equals": True}}
        )
    if action in {"stop", "reload"} and (
        body.confirm_instance_name != instance_id
    ):
        msg = "confirm_instance_name must equal the instance id"
        raise InvalidParameterError(
            msg,
            details={
                "field": "confirm_instance_name",
                "constraint": {"equals": instance_id},
            },
        )
    if idempotency_key is None or not idempotency_key.strip():
        msg = "Idempotency-Key is required for lifecycle actions"
        raise InvalidParameterError(
            msg, details={"field": "Idempotency-Key"}
        )
    key = idempotency_key.strip()
    if _KEY_RE.fullmatch(key) is None:
        msg = "Idempotency-Key is invalid"
        raise InvalidParameterError(
            msg,
            details={
                "field": "Idempotency-Key",
                "constraint": {"pattern": _KEY_RE.pattern},
            },
        )

    store = context.lifecycle.store
    body_hash = _body_hash(
        instance_id=instance_id,
        action=action,
        reason=reason,
        confirm_instance_name=body.confirm_instance_name,
    )
    existing = store.find_idempotent(key, record.subject)
    if existing is not None:
        if existing.idempotency_body_hash != body_hash:
            msg = "the idempotency key was used with a different body"
            raise ConflictError(msg, details={"reason": _KEY_REUSE_REASON})
        return _operation_response(existing, replayed=True)

    request_id = get_request_id(request)
    now_ns = int(time.time_ns())
    operation_id = f"op_{request_id.removeprefix('req_')}"
    if len(operation_id) > 128:
        operation_id = f"op_{hashlib.sha256(request_id.encode()).hexdigest()}"
    operation_id = _safe_operation_id(operation_id)

    # Fail closed: the requested event is durable before dispatch. If the
    # audit store is unavailable this raises 503 and nothing is executed.
    handle = context.audit.begin(
        actor=actor_from_session(record),
        action=kind,
        target=AuditTarget(
            type="service_instance",
            id=instance_id,
            display_name=instance_id,
        ),
        request_id=request_id,
        reason=reason,
        operation_id=operation_id,
        instance_id=instance_id,
        before=None,
        after={"action": action, "reason": reason},
    )

    client = context.lifecycle.control_client()
    if client is None:
        handle.failed(failure_reason="control_unavailable")
        msg = "the supervisor control endpoint is not configured"
        raise ControlUnavailableError(
            msg, details={"source": _CONTROL_SOURCE, "retryable": False}
        )
    try:
        operation_wire = client.service_action(
            _ACTION_CONTROL[action],
            instance_id=instance_id,
            operation_id=operation_id,
            request_id=request_id,
            reason=reason,
            actor={
                "subject": record.subject,
                "display_name": record.display_name,
                "access_role": record.access_role,
            },
            idempotency_key=key,
            idempotency_body_hash=body_hash,
            idempotency_expires_at_ns=(
                now_ns
                + context.config.limits.operation_retention_s
                * NANOS_PER_SECOND
            ),
        )
    except SupervisorUnavailableError as exc:
        handle.failed(failure_reason="control_unavailable")
        msg = "the supervisor is unreachable"
        raise ControlUnavailableError(
            msg,
            details={"source": _CONTROL_SOURCE, "retryable": True},
        ) from exc
    except SupervisorControlError as exc:
        handle.failed(failure_reason=exc.code)
        _raise_control_rejection(exc)

    if operation_wire.get("operation_id") != operation_id:
        # A concurrent request with the same key won the race: the Supervisor
        # returned the original operation and did not execute ours.
        return _operation_response(operation_wire, replayed=True)
    return _operation_response(operation_wire)


def _raise_control_rejection(exc: SupervisorControlError) -> None:
    if exc.code == "instance_not_found":
        msg = "service instance not found"
        raise AdminNotFoundError(
            msg,
            details=exc.details
            or {"resource": "service_instance", "id": "unknown"},
        ) from exc
    if exc.code == "invalid_state":
        msg = "the service instance is not in a valid state"
        raise ConflictError(
            msg,
            details={
                "reason": exc.details.get("reason", "invalid_state"),
                **exc.details,
            },
        ) from exc
    if exc.code == "operation_in_progress":
        msg = "another operation is already in progress"
        raise ConflictError(
            msg,
            details={"reason": "operation_in_progress", **exc.details},
        ) from exc
    if exc.code == _KEY_REUSE_REASON:
        msg = "the idempotency key was used with a different body"
        raise ConflictError(
            msg, details={"reason": _KEY_REUSE_REASON}
        ) from exc
    msg = "the supervisor rejected the lifecycle action"
    raise ControlUnavailableError(
        msg,
        details={
            "source": _CONTROL_SOURCE,
            "retryable": True,
            "reason": exc.code,
        },
    ) from exc


def _operation_response(
    operation: OperationRecord | dict[str, Any],
    *,
    replayed: bool = False,
) -> JSONResponse:
    wire = (
        operation.to_wire()
        if isinstance(operation, OperationRecord)
        else dict(operation)
    )
    headers = {
        **NO_STORE,
        "Location": f"/api/v1/admin/operations/{wire['operation_id']}",
    }
    if replayed:
        headers["Idempotent-Replay"] = "true"
    return JSONResponse(wire, status_code=202, headers=headers)


def _body_hash(
    *,
    instance_id: str,
    action: str,
    reason: str,
    confirm_instance_name: str | None,
) -> str:
    payload = {
        "instance_id": instance_id,
        "action": action,
        "reason": reason,
        "confirm": True,
        "confirm_instance_name": confirm_instance_name,
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _safe_operation_id(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._:-]", "", value)
    if not cleaned or cleaned[0] in "._:-":
        cleaned = f"op_{cleaned}"
    return cleaned[:128]


def _parse_time(value: str | None, field: str) -> int | None:
    if value is None:
        return None
    try:
        return iso_to_ns(value)
    except ValueError as exc:
        msg = f"{field} must be a UTC ISO-8601 timestamp"
        raise InvalidParameterError(
            msg,
            details={"field": field, "reason": _INVALID_TIMESTAMP_REASON},
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
            details={"field": "cursor", "reason": _INVALID_CURSOR_REASON},
        ) from exc
    sequence = payload.get("sequence")
    if isinstance(sequence, bool) or not isinstance(sequence, int):
        msg = "cursor is invalid"
        raise InvalidParameterError(
            msg,
            details={"field": "cursor", "reason": _INVALID_CURSOR_REASON},
        )
    return sequence


__all__ = ["LifecycleActionBody", "create_lifecycle_router"]
