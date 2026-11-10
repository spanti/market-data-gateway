# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[suspicious-url-open-usage]
"""Stable Admin API error codes and envelope reuse (ticket 01).

Every admin failure must use the shared ``{"error": {...}}`` envelope with the
same id in ``X-Request-ID``; FastAPI's ``422``/``detail`` shape must never
reach an admin client. The fake app below is served through the real ASGI
server so the assertions observe the wire, not in-process shortcuts.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from contextlib import contextmanager
from typing import TYPE_CHECKING, Annotated

import pytest
from fastapi import FastAPI, Query

from mdgw.admin.errors import (
    ADMIN_ERROR_STATUSES,
    AdminError,
    AdminNotFoundError,
    AuditUnavailableError,
    ConflictError,
    ControlUnavailableError,
    ExportExpiredError,
    ForbiddenError,
    InvalidParameterError,
    JobNotFoundError,
    QuotaExceededError,
    UnauthenticatedError,
    admin_error_status,
)
from mdgw.api.asgi_server import AsgiServer
from mdgw.api.errors import install_exception_handlers
from mdgw.api.middleware import RequestContextMiddleware

if TYPE_CHECKING:
    from collections.abc import Generator

HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-admin-errors"
ADMIN_PREFIX = "/api/v1/admin/test"


def test_required_admin_codes_have_the_contract_statuses() -> None:
    assert ADMIN_ERROR_STATUSES == {
        "unauthenticated": 401,
        "forbidden": 403,
        "invalid_parameter": 400,
        "not_found": 404,
        "job_not_found": 404,
        "conflict": 409,
        "export_expired": 410,
        "quota_exceeded": 429,
        "rate_limited": 429,
        "audit_unavailable": 503,
        "control_unavailable": 503,
        "method_not_allowed": 405,
        "schema_version_mismatch": 409,
        "internal_error": 500,
    }


@pytest.mark.parametrize(
    ("exception_type", "code", "status"),
    [
        (UnauthenticatedError, "unauthenticated", 401),
        (ForbiddenError, "forbidden", 403),
        (ConflictError, "conflict", 409),
        (InvalidParameterError, "invalid_parameter", 400),
        (AdminNotFoundError, "not_found", 404),
        (AuditUnavailableError, "audit_unavailable", 503),
        (ControlUnavailableError, "control_unavailable", 503),
        (JobNotFoundError, "job_not_found", 404),
        (ExportExpiredError, "export_expired", 410),
        (QuotaExceededError, "quota_exceeded", 429),
    ],
)
def test_admin_exceptions_carry_stable_code_and_status(
    exception_type: type[AdminError], code: str, status: int
) -> None:
    error = exception_type("failure", details={"reason": "test"})

    assert isinstance(error, AdminError)
    assert error.code == code
    assert error.status == status
    assert error.details == {"reason": "test"}
    assert ADMIN_ERROR_STATUSES[code] == status


def test_admin_error_status_uses_the_contract_mapping() -> None:
    assert admin_error_status("control_unavailable") == 503
    assert admin_error_status("unknown_code") == 500


def test_unsupported_admin_codes_are_not_silently_accepted() -> None:
    with pytest.raises(ValueError, match="unknown admin error code"):
        admin_error_status("not_a_real_code", strict=True)


def _fake_app() -> FastAPI:
    app = FastAPI()
    install_exception_handlers(app)
    app.add_middleware(RequestContextMiddleware)

    @app.get(f"{ADMIN_PREFIX}/unauthenticated")
    async def _unauthenticated() -> None:
        msg = "session required"
        raise UnauthenticatedError(msg, details={"reason": "missing"})

    @app.get(f"{ADMIN_PREFIX}/forbidden")
    async def _forbidden() -> None:
        msg = "denied"
        raise ForbiddenError(msg, details={"reason": "role"})

    @app.get(f"{ADMIN_PREFIX}/not-found")
    async def _not_found() -> None:
        msg = "resource missing"
        raise AdminNotFoundError(msg, details={"resource": "thing"})

    @app.get(f"{ADMIN_PREFIX}/invalid")
    async def _invalid(
        limit: Annotated[int, Query()],
    ) -> dict[str, int]:
        return {"limit": limit}

    return app


@contextmanager
def _serve() -> Generator[AsgiServer]:
    server = AsgiServer()
    try:
        server.start(
            _fake_app(),
            host=HOST,
            port=0,
            thread_name=THREAD_NAME,
        )
        yield server
    finally:
        server.stop()


def _request(
    url: str, headers: dict[str, str] | None = None
) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), error.read()


def _header(headers: dict[str, str], name: str) -> str | None:
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


@pytest.mark.parametrize(
    ("path", "status", "code"),
    [
        ("unauthenticated", 401, "unauthenticated"),
        ("forbidden", 403, "forbidden"),
        ("not-found", 404, "not_found"),
    ],
)
def test_admin_exceptions_render_the_unified_envelope(
    path: str, status: int, code: str
) -> None:
    with _serve() as server:
        response_status, headers, body = _request(
            f"{server.base_url}{ADMIN_PREFIX}/{path}"
        )

    envelope = json.loads(body)
    assert response_status == status
    assert set(envelope) == {"error"}
    assert set(envelope["error"]) == {
        "code",
        "message",
        "details",
        "request_id",
    }
    assert envelope["error"]["code"] == code
    assert envelope["error"]["request_id"] == _header(
        headers, "X-Request-ID"
    )


def test_validation_failures_use_the_envelope_not_fastapi_422() -> None:
    with _serve() as server:
        status, headers, body = _request(
            f"{server.base_url}{ADMIN_PREFIX}/invalid"
        )

    envelope = json.loads(body)
    assert status == 400
    assert envelope["error"]["code"] == "invalid_parameter"
    assert "detail" not in envelope
    assert envelope["error"]["request_id"] == _header(
        headers, "X-Request-ID"
    )


def test_inbound_request_id_is_preserved_in_admin_errors() -> None:
    with _serve() as server:
        _, headers, body = _request(
            f"{server.base_url}{ADMIN_PREFIX}/forbidden",
            headers={"X-Request-ID": "req-abc-123"},
        )

    assert _header(headers, "X-Request-ID") == "req-abc-123"
    assert json.loads(body)["error"]["request_id"] == "req-abc-123"


def test_error_envelope_json_contains_no_fastapi_detail() -> None:
    with _serve() as server:
        _, _, body = _request(f"{server.base_url}{ADMIN_PREFIX}/invalid")

    serialized = body.decode("utf-8")
    assert '"detail"' not in serialized
    assert "HTTPValidationError" not in serialized
