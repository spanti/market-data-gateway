"""Stable Admin API error codes and exceptions.

Every admin failure is rendered by the shared HTTP exception handlers as
``{"error": {"code", "message", "details", "request_id"}}`` with the same id in
``X-Request-ID``. The codes and statuses below are the wire contract from the
Operator Console contract target, section 11.2; clients branch on ``code``, not
on the status or the prose.

Public Data Contract codes (``invalid_range``, ``invalid_interval`` and the
other bar-query codes) keep their existing meaning. Admin resources only use
the codes defined here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mdgw.api.errors import RATE_LIMITED, error_response
from mdgw.core.errors import INTERNAL_ERROR, INVALID_PARAMETER, GatewayError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from starlette.responses import JSONResponse

UNAUTHENTICATED = "unauthenticated"
FORBIDDEN = "forbidden"
CONFLICT = "conflict"
NOT_FOUND = "not_found"
AUDIT_UNAVAILABLE = "audit_unavailable"
CONTROL_UNAVAILABLE = "control_unavailable"
JOB_NOT_FOUND = "job_not_found"
EXPORT_EXPIRED = "export_expired"
QUOTA_EXCEEDED = "quota_exceeded"
METHOD_NOT_ALLOWED = "method_not_allowed"
SCHEMA_VERSION_MISMATCH = "schema_version_mismatch"

#: Stable admin code to HTTP status mapping (contract target section 11.2).
ADMIN_ERROR_STATUSES: dict[str, int] = {
    UNAUTHENTICATED: 401,
    FORBIDDEN: 403,
    INVALID_PARAMETER: 400,
    NOT_FOUND: 404,
    JOB_NOT_FOUND: 404,
    CONFLICT: 409,
    EXPORT_EXPIRED: 410,
    QUOTA_EXCEEDED: 429,
    RATE_LIMITED: 429,
    AUDIT_UNAVAILABLE: 503,
    CONTROL_UNAVAILABLE: 503,
    METHOD_NOT_ALLOWED: 405,
    SCHEMA_VERSION_MISMATCH: 409,
    INTERNAL_ERROR: 500,
}

DEFAULT_ERROR_STATUS = 500


def admin_error_status(code: str, *, strict: bool = False) -> int:
    """Return the contract HTTP status for one admin code.

    ``strict=True`` turns an unknown code into a programming error so a typo
    never silently becomes an opaque ``500`` during development.
    """
    status = ADMIN_ERROR_STATUSES.get(code)
    if status is not None:
        return status
    if strict:
        msg = f"unknown admin error code: {code!r}"
        raise ValueError(msg)
    return DEFAULT_ERROR_STATUS


def admin_error_response(  # ruff: ignore[too-many-arguments]  # explicit wire contract
    *,
    code: str,
    message: str,
    request_id: str,
    details: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    status: int | None = None,
) -> JSONResponse:
    """Build one admin failure in the shared envelope."""
    return error_response(
        code=code,
        message=message,
        details=details,
        status=admin_error_status(code) if status is None else status,
        request_id=request_id,
        headers=headers,
    )


class AdminError(GatewayError):
    """Base class for admin failures; subclasses fix code and status."""

    code = INTERNAL_ERROR
    status = DEFAULT_ERROR_STATUS


class UnauthenticatedError(AdminError):
    """No valid server session, or OIDC authentication failed."""

    code = UNAUTHENTICATED
    status = 401


class ForbiddenError(AdminError):
    """Authenticated but the role, ownership, CSRF or origin check failed."""

    code = FORBIDDEN
    status = 403


class InvalidParameterError(AdminError):
    """Missing or invalid admin field, range, cursor or confirmation."""

    code = INVALID_PARAMETER
    status = 400


class AdminNotFoundError(AdminError):
    """An admin route or non-job resource does not exist."""

    code = NOT_FOUND
    status = 404


class ConflictError(AdminError):
    """Version conflict, duplicate, invalid state transition or active job."""

    code = CONFLICT
    status = 409


class JobNotFoundError(AdminError):
    """An Export or Backfill Job id is unknown."""

    code = JOB_NOT_FOUND
    status = 404


class ExportExpiredError(AdminError):
    """The Export file passed its 24-hour expiry."""

    code = EXPORT_EXPIRED
    status = 410


class QuotaExceededError(AdminError):
    """A per-user, global-concurrency or global-storage quota is exhausted."""

    code = QUOTA_EXCEEDED
    status = 429


class RateLimitedError(AdminError):
    """The admin request rate limit rejected the request."""

    code = RATE_LIMITED
    status = 429


class AuditUnavailableError(AdminError):
    """The audit store is unavailable; the management action was not executed."""

    code = AUDIT_UNAVAILABLE
    status = 503


class ControlUnavailableError(AdminError):
    """A controlled IPC/executor source is unavailable; nothing was accepted."""

    code = CONTROL_UNAVAILABLE
    status = 503


__all__ = [
    "ADMIN_ERROR_STATUSES",
    "AUDIT_UNAVAILABLE",
    "CONFLICT",
    "CONTROL_UNAVAILABLE",
    "EXPORT_EXPIRED",
    "FORBIDDEN",
    "INVALID_PARAMETER",
    "JOB_NOT_FOUND",
    "METHOD_NOT_ALLOWED",
    "NOT_FOUND",
    "QUOTA_EXCEEDED",
    "RATE_LIMITED",
    "SCHEMA_VERSION_MISMATCH",
    "UNAUTHENTICATED",
    "AdminError",
    "AdminNotFoundError",
    "AuditUnavailableError",
    "ConflictError",
    "ControlUnavailableError",
    "ExportExpiredError",
    "ForbiddenError",
    "InvalidParameterError",
    "JobNotFoundError",
    "QuotaExceededError",
    "RateLimitedError",
    "UnauthenticatedError",
    "admin_error_response",
    "admin_error_status",
]
