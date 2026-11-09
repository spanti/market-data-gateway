"""Operator Audit Event writer and the fail-closed requested/terminal API.

Later management-write tickets (feed, flow control, lifecycle, jobs, export)
must use this module rather than touching ``AuditStore`` directly:

``AuditWriter.begin(...) -> AuditHandle``
    Redact the diff and append the ``requested`` event **before** executing
    the high-risk action. If the append fails, ``AuditUnavailableError``
    (HTTP 503 ``audit_unavailable``) is raised and the caller must not execute
    anything. The contract details carry ``action``/``target``/``executed``.
``AuditHandle.succeeded(...)`` / ``AuditHandle.failed(...)``
    Append the terminal event with the same actor, target, reason, request id
    and operation/job/instance correlation. The requested row is never
    updated, in place.
``AuditWriter.append(...)``
    Low-level single-event append for synchronous writes that already hold
    all final state; it is still fail-closed because it raises the same stable
    error when the audit store is unavailable.
``AuditWriter.record_security_event(...)``
    Bridge installed on ``AdminAuth.security_event_sink``. It maps the ticket
    02 authentication/authorization failure stream to ``result="denied"``
    events. It never raises: a broken audit store must not turn a denied
    request into a server error.

Every diff is passed through ``mdgw.admin.audit.canonical.redact`` first, so a
secret/token/cookie/password/OIDC claim can only reach storage as the literal
``[REDACTED]`` with a JSON Pointer path.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from mdgw.admin.audit.canonical import redact
from mdgw.admin.audit.models import (
    AUDIT_RESULTS,
    HASH_ALGORITHM,
    RESULT_DENIED,
    RESULT_FAILED,
    RESULT_REQUESTED,
    RESULT_SUCCEEDED,
    AuditActor,
    AuditDiff,
    AuditTarget,
    HashChain,
    OperatorAuditEvent,
)
from mdgw.admin.db import AdminStoreError
from mdgw.admin.errors import AuditUnavailableError

if TYPE_CHECKING:
    from collections.abc import Callable

    from mdgw.admin.audit.store import AuditStore
    from mdgw.admin.auth.store import SecurityEvent, SessionRecord

logger = logging.getLogger(__name__)

#: Security-event kinds are already namespaced by the auth layer. Authorization
#: denials share the contract's action vocabulary; authentication failures are
#: login failures. Unknown kinds keep their stable kind as the action.
_AUTH_LOGIN_ACTION = "auth.login"
_ROLE_DENIED_ACTION = "auth.role.denied"

_AUDIT_STORE_ERRORS = (sqlite3.Error, AdminStoreError, OSError)


class _UnsetType:
    """Sentinel distinguishing an omitted terminal diff from ``None``."""

    def __repr__(self) -> str:
        return "<unset>"


_UNSET = _UnsetType()


def _event_id() -> str:
    return f"evt_{uuid4().hex}"


def actor_from_session(record: SessionRecord) -> AuditActor:
    """Snapshot the authenticated session into an audit actor.

    Later management-write tickets call this instead of rebuilding the actor
    by hand; the role is the one resolved server-side at action time.
    """
    return AuditActor(
        subject=record.subject,
        display_name=record.display_name,
        access_role=record.access_role,
    )


@dataclass(frozen=True)
class AuditHandle:
    """Correlation handle for one requested high-risk action."""

    event: OperatorAuditEvent
    _writer: AuditWriter = field(repr=False)
    _actor: AuditActor = field(repr=False)
    _action: str = field(repr=False)
    _target: AuditTarget = field(repr=False)
    _reason: str | None = field(repr=False)
    _request_id: str = field(repr=False)
    _operation_id: str | None = field(repr=False)
    _job_id: str | None = field(repr=False)
    _instance_id: str | None = field(repr=False)

    def succeeded(
        self,
        *,
        after: Any = _UNSET,
        reason: str | None = None,
    ) -> OperatorAuditEvent:
        """Append the terminal ``succeeded`` event for this action."""
        return self._finish(
            result=RESULT_SUCCEEDED,
            failure_reason=None,
            after=after,
            reason=reason,
        )

    def failed(
        self,
        *,
        failure_reason: str,
        after: Any = _UNSET,
        reason: str | None = None,
    ) -> OperatorAuditEvent:
        """Append the terminal ``failed`` event for this action."""
        if not failure_reason:
            msg = "failure_reason must be a non-empty stable string"
            raise ValueError(msg)
        return self._finish(
            result=RESULT_FAILED,
            failure_reason=failure_reason,
            after=after,
            reason=reason,
        )

    def _finish(
        self,
        *,
        result: str,
        failure_reason: str | None,
        after: Any,
        reason: str | None,
    ) -> OperatorAuditEvent:
        return self._writer._append(
            actor=self._actor,
            action=self._action,
            target=self._target,
            result=result,
            request_id=self._request_id,
            reason=self._reason if reason is None else reason,
            failure_reason=failure_reason,
            operation_id=self._operation_id,
            job_id=self._job_id,
            instance_id=self._instance_id,
            before=None,
            after=None if isinstance(after, _UnsetType) else after,
            phase="terminal",
            executed=True,
        )


class AuditWriter:
    """Fail-closed append front end over one ``AuditStore``."""

    def __init__(
        self,
        store: AuditStore,
        *,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self._store = store
        self._clock = time.time_ns if clock is None else clock

    @property
    def available(self) -> bool:
        """True while the underlying store can accept an append."""
        return self._store.is_open

    @property
    def store(self) -> AuditStore:
        """Read access for the query routes (writes go through this writer)."""
        return self._store

    def now_ns(self) -> int:
        return int(self._clock())

    def close(self) -> None:
        self._store.close()

    # -- high-risk requested/terminal --------------------------------------

    def begin(  # ruff: ignore[too-many-arguments]  # explicit event fields
        self,
        *,
        actor: AuditActor,
        action: str,
        target: AuditTarget,
        request_id: str,
        reason: str | None = None,
        operation_id: str | None = None,
        job_id: str | None = None,
        instance_id: str | None = None,
        before: Any = None,
        after: Any = None,
    ) -> AuditHandle:
        """Append ``requested``; callers execute only after this returns."""
        event = self._append(
            actor=actor,
            action=action,
            target=target,
            result=RESULT_REQUESTED,
            request_id=request_id,
            reason=reason,
            failure_reason=None,
            operation_id=operation_id,
            job_id=job_id,
            instance_id=instance_id,
            before=before,
            after=after,
            phase="requested",
            executed=False,
        )
        return AuditHandle(
            event=event,
            _writer=self,
            _actor=actor,
            _action=action,
            _target=target,
            _reason=reason,
            _request_id=request_id,
            _operation_id=operation_id,
            _job_id=job_id,
            _instance_id=instance_id,
        )

    # -- single append / security bridge -----------------------------------

    def append(  # ruff: ignore[too-many-arguments]  # explicit event fields
        self,
        *,
        actor: AuditActor,
        action: str,
        target: AuditTarget,
        result: str,
        request_id: str,
        reason: str | None = None,
        failure_reason: str | None = None,
        operation_id: str | None = None,
        job_id: str | None = None,
        instance_id: str | None = None,
        before: Any = None,
        after: Any = None,
        occurred_at_ns: int | None = None,
    ) -> OperatorAuditEvent:
        """Append one complete event (synchronous writes, denied failures)."""
        return self._append(
            actor=actor,
            action=action,
            target=target,
            result=result,
            request_id=request_id,
            reason=reason,
            failure_reason=failure_reason,
            operation_id=operation_id,
            job_id=job_id,
            instance_id=instance_id,
            before=before,
            after=after,
            phase="append",
            executed=False,
            occurred_at_ns=occurred_at_ns,
        )

    def record_security_event(
        self, event: SecurityEvent
    ) -> OperatorAuditEvent | None:
        """Bridge one auth/authz security event into a denied audit event.

        Never raises: the caller is already rejecting a request, and audit
        telemetry must not change that outcome. The raw event detail is
        redacted before storage even though the auth layer never puts bearer
        material in it.
        """
        action, target = _security_event_scope(event)
        detail = dict(event.detail) if event.detail else None
        try:
            return self.append(
                actor=AuditActor(
                    subject=event.subject or "anonymous",
                    display_name=event.subject or "anonymous",
                    access_role=event.access_role,
                ),
                action=action,
                target=target,
                result=RESULT_DENIED,
                request_id=event.request_id or uuid4().hex,
                reason=None,
                failure_reason=event.reason,
                before=None,
                after=detail,
                occurred_at_ns=event.occurred_at_ns,
            )
        except Exception:  # audit failures must not break the denial path
            logger.exception(
                "failed to record operator audit security event",
                extra={
                    "event": "admin.audit.security_event.write_failed",
                    "audit_kind": event.kind,
                },
            )
            return None

    # -- internals ---------------------------------------------------------

    def _append(  # ruff: ignore[too-many-arguments]  # explicit event fields
        self,
        *,
        actor: AuditActor,
        action: str,
        target: AuditTarget,
        result: str,
        request_id: str,
        reason: str | None,
        failure_reason: str | None,
        operation_id: str | None,
        job_id: str | None,
        instance_id: str | None,
        before: Any,
        after: Any,
        phase: str,
        executed: bool,
        occurred_at_ns: int | None = None,
    ) -> OperatorAuditEvent:
        _validate_action(action)
        _validate_request_id(request_id)
        if result not in AUDIT_RESULTS:
            msg = f"audit result must be one of {', '.join(AUDIT_RESULTS)}"
            raise ValueError(msg)

        redacted_before, before_paths = redact(before)
        redacted_after, after_paths = redact(after)
        redacted_paths = tuple(sorted({*before_paths, *after_paths}))
        occurred = (
            self.now_ns() if occurred_at_ns is None else int(occurred_at_ns)
        )
        prototype = OperatorAuditEvent(
            event_id=_event_id(),
            sequence=0,
            occurred_at_ns=occurred,
            actor=actor,
            action=action,
            target=target,
            result=result,
            failure_reason=failure_reason,
            reason=reason,
            request_id=request_id,
            operation_id=operation_id,
            job_id=job_id,
            instance_id=instance_id,
            diff=AuditDiff(
                before=redacted_before,
                after=redacted_after,
                redacted_paths=redacted_paths,
            ),
            hash_chain=HashChain(HASH_ALGORITHM, None, ""),
        )
        try:
            return self._store.append(prototype)
        except _AUDIT_STORE_ERRORS as exc:
            logger.exception(
                "operator audit append failed",
                extra={
                    "event": "admin.audit.append_failed",
                    "audit_action": action,
                    "audit_phase": phase,
                },
            )
            msg = "the audit store is unavailable"
            raise AuditUnavailableError(
                msg,
                details={
                    "action": action,
                    "target": {"type": target.type, "id": target.id},
                    "phase": phase,
                    "executed": executed,
                },
            ) from exc


def _validate_action(action: str) -> None:
    if not isinstance(action, str) or not action.strip():
        msg = "action must be a non-empty stable string"
        raise ValueError(msg)


def _validate_request_id(request_id: str) -> None:
    if not isinstance(request_id, str) or not request_id.strip():
        msg = "request_id must be a non-empty correlation id"
        raise ValueError(msg)


def _security_event_scope(event: SecurityEvent) -> tuple[str, AuditTarget]:
    """Map a security-event kind onto the contract action and target scope."""
    kind = event.kind
    if kind.startswith("authz."):
        required = event.detail.get("required_role")
        if isinstance(required, list):
            required_role = ",".join(str(role) for role in required)
        elif required is None:
            required_role = None
        else:
            required_role = str(required)
        return (
            _ROLE_DENIED_ACTION,
            AuditTarget(
                type="access_role",
                id=event.access_role or "anonymous",
                display_name=required_role,
            ),
        )
    if kind.startswith("auth.") or kind == "session.invalid":
        return (
            _AUTH_LOGIN_ACTION,
            AuditTarget(
                type="session",
                id=event.subject or "anonymous",
            ),
        )
    return kind, AuditTarget(
        type="session", id=event.subject or "anonymous"
    )


__all__ = [
    "AuditHandle",
    "AuditWriter",
    "actor_from_session",
]
