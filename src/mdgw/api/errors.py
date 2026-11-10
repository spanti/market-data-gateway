"""Unified HTTP error envelope and exception handlers.

Every failure the API can produce is rendered as
``{"error": {"code", "message", "details", "request_id"}}`` and always carries
an ``X-Request-ID`` header. FastAPI's ``422`` and ``{"detail": ...}`` shapes are
never exposed; validation failures are mapped back onto the stable codes from
`mdgw.core.errors`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import JSONResponse

from mdgw.core.errors import (
    INTERNAL_ERROR,
    INVALID_FORMAT,
    INVALID_INSTRUMENT,
    INVALID_INTERVAL,
    INVALID_PARAMETER,
    INVALID_RANGE,
    GatewayError,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from fastapi import FastAPI
    from starlette.requests import Request

logger = logging.getLogger(__name__)

REQUEST_ID_STATE_KEY = "request_id"

NOT_FOUND = "not_found"
METHOD_NOT_ALLOWED = "method_not_allowed"
RATE_LIMITED = "rate_limited"

#: Admin validation failures always use `invalid_parameter`, even when the
#: offending parameter name (`limit`, `start`, ...) has a more specific public
#: Bars-query code. The public mapping below stays unchanged.
ADMIN_PATH_PREFIX = "/api/v1/admin"

# Stable parameter-name to error-code mapping (spec: REST error contract).
_PARAMETER_CODES: dict[str, str] = {
    "interval": INVALID_INTERVAL,
    "instrument": INVALID_INSTRUMENT,
    "instrument_id": INVALID_INSTRUMENT,
    "start": INVALID_RANGE,
    "end": INVALID_RANGE,
    "limit": INVALID_RANGE,
    "format": INVALID_FORMAT,
}

_HTTP_STATUS_CODES: dict[int, str] = {
    404: NOT_FOUND,
    405: METHOD_NOT_ALLOWED,
    429: RATE_LIMITED,
}

def get_request_id(request: Request) -> str:
    """Return the request id chosen by the middleware, generating a fallback."""
    state = request.scope.get("state") or {}
    request_id = state.get(REQUEST_ID_STATE_KEY)
    if isinstance(request_id, str) and request_id:
        return request_id
    return uuid4().hex


def error_response(  # ruff: ignore[too-many-arguments]  # explicit wire contract
    *,
    code: str,
    message: str,
    status: int,
    request_id: str,
    details: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    """Build one error response in the unified envelope."""
    response_headers = dict(headers or {})
    response_headers["X-Request-ID"] = request_id
    return JSONResponse(
        {
            "error": {
                "code": code,
                "message": message,
                "details": dict(details or {}),
                "request_id": request_id,
            }
        },
        status_code=status,
        headers=response_headers,
    )


def is_admin_path(path: str | None) -> bool:
    """True for the admin API subtree (never for a lookalike prefix)."""
    if not isinstance(path, str):
        return False
    return path == ADMIN_PATH_PREFIX or path.startswith(
        ADMIN_PATH_PREFIX + "/"
    )


def validation_error_code(
    errors: Sequence[Mapping[str, Any]],
) -> str:
    """Map a `RequestValidationError` onto a stable gateway error code."""
    for error in errors:
        location = error.get("loc") or ()
        for part in location:
            if not isinstance(part, str):
                continue
            mapped = _PARAMETER_CODES.get(part)
            if mapped is not None:
                return mapped
    return INVALID_PARAMETER


def _validation_message(errors: Sequence[Mapping[str, Any]]) -> str:
    for error in errors:
        message = error.get("msg")
        if isinstance(message, str) and message:
            return message
    return "invalid request parameter"


def install_exception_handlers(app: FastAPI) -> None:
    """Register the handlers that own every error shape leaving the app."""
    app.add_exception_handler(GatewayError, _handle_gateway_error)
    app.add_exception_handler(RequestValidationError, _handle_validation_error)
    app.add_exception_handler(StarletteHTTPException, _handle_http_exception)
    app.add_exception_handler(Exception, _handle_unexpected_exception)


def _handle_gateway_error(request: Request, exc: GatewayError) -> JSONResponse:
    return error_response(
        code=exc.code,
        message=exc.message,
        details=exc.details,
        status=exc.status,
        request_id=get_request_id(request),
    )


def _handle_validation_error(
    request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    errors = exc.errors()
    path = request.scope.get("path")
    code = (
        INVALID_PARAMETER
        if is_admin_path(path if isinstance(path, str) else None)
        else validation_error_code(errors)
    )
    return error_response(
        code=code,
        message=_validation_message(errors),
        status=400,
        request_id=get_request_id(request),
    )


def _handle_http_exception(
    request: Request,
    exc: StarletteHTTPException,
) -> JSONResponse:
    status = exc.status_code
    code = _HTTP_STATUS_CODES.get(
        status,
        INTERNAL_ERROR if status >= 500 else INVALID_PARAMETER,
    )
    message = exc.detail if isinstance(exc.detail, str) and exc.detail else (
        "request failed"
    )
    return error_response(
        code=code,
        message=message,
        status=status,
        request_id=get_request_id(request),
        headers=exc.headers,
    )


def _handle_unexpected_exception(
    request: Request,
    _exc: Exception,
) -> JSONResponse:
    request_id = get_request_id(request)
    logger.exception(  # ruff: ignore[log-exception-outside-except-handler] - handler runs inside ServerErrorMiddleware's except
        "unhandled exception",
        extra={
            "event": "api.unhandled_exception",
            "request_id": request_id,
            "status": 500,
        },
    )
    return error_response(
        code=INTERNAL_ERROR,
        message="internal server error",
        status=500,
        request_id=request_id,
    )


__all__ = [
    "ADMIN_PATH_PREFIX",
    "METHOD_NOT_ALLOWED",
    "NOT_FOUND",
    "RATE_LIMITED",
    "REQUEST_ID_STATE_KEY",
    "error_response",
    "get_request_id",
    "install_exception_handlers",
    "is_admin_path",
    "validation_error_code",
]
