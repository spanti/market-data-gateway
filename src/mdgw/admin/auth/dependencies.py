"""Reusable FastAPI dependencies for server-side Admin authorization.

Every protected Admin route resolves a real server session; roles, CSRF and
same-origin checks are enforced here, never in the SPA. Later tickets compose
these dependencies in their own routers:

``require_session(context)``
    Authenticated session or ``401 unauthenticated``.
``require_role(context, *roles)``
    Authenticated session with one of the given roles or ``403 role``.
``require_csrf(context)``
    Same-origin + CSRF check for every unsafe method (returns the session).
``require_write(context, *roles)``
    ``require_csrf`` plus an optional role check, resolving once.
``owner_or_superadmin(record, owner_subject)``
    Ownership policy for Export/Job resources.

The router-level ``require_session`` dependency also protects any future
feature router registered through ``register_feature`` by default.
"""

from __future__ import annotations

import ipaddress
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from fastapi import Depends
from starlette.datastructures import Headers
from starlette.requests import (  # ruff: ignore[typing-only-third-party-import]  # FastAPI resolves this annotation at runtime
    Request,
)
from starlette.websockets import (
    WebSocket,  # ruff: ignore[typing-only-third-party-import]  # FastAPI resolves this annotation at runtime
)

from mdgw.admin.auth.service import (
    SESSION_COOKIE_NAME,
    AdminAuth,
)
from mdgw.admin.auth.sessions import SessionAuthError
from mdgw.admin.auth.store import (  # ruff: ignore[typing-only-first-party-import]  # FastAPI resolves this annotation at runtime
    SessionRecord,
)
from mdgw.admin.capabilities import ADMIN_ROLES, CSRF_HEADER_NAME
from mdgw.admin.errors import ForbiddenError, UnauthenticatedError
from mdgw.api.errors import get_request_id

if TYPE_CHECKING:
    from collections.abc import Callable

    from mdgw.admin.context import AdminContext

_DEFAULT_PORTS = {"http": 80, "https": 443}
_WEBSOCKET_SCHEMES = {"ws": "http", "wss": "https"}
_MISSING_ORIGIN_REASON = "origin"
_CSRF_REASON = "csrf"


def auth_for(context: AdminContext) -> AdminAuth:
    """Return the runtime authentication facade (opens the store lazily)."""
    return context.auth


def session_cookie_token(cookies: object) -> str | None:
    """Read the opaque session token from a cookie mapping, if present."""
    getter = getattr(cookies, "get", None)
    if getter is None:
        return None
    value = getter(SESSION_COOKIE_NAME)
    return value if isinstance(value, str) and value else None


def client_ip(context: AdminContext, request: Request | WebSocket) -> str:
    """Resolve the real client address under the trusted-proxy policy."""
    scope = request.scope
    client = scope.get("client")
    peer = client[0] if isinstance(client, tuple) and client else ""
    headers = Headers(scope=scope)
    return context.client_ip_resolver.resolve_from_headers(
        str(peer), headers
    )


def request_headers(request: Request | WebSocket) -> Headers:
    return Headers(scope=request.scope)


def _normalize_origin(value: str) -> str | None:
    """Return ``scheme://host:port`` with defaults made explicit."""
    try:
        parts = urlsplit(value)
        scheme = parts.scheme.lower()
        hostname = parts.hostname
        port = parts.port
    except ValueError:
        return None
    scheme = _WEBSOCKET_SCHEMES.get(scheme, scheme)
    if scheme not in _DEFAULT_PORTS or not hostname:
        return None
    if port is None:
        port = _DEFAULT_PORTS[scheme]
    host = hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    return f"{scheme}://{host}:{port}"


def _origin_of(value: str) -> str | None:
    return _normalize_origin(value)


def supplied_origin(request: Request | WebSocket) -> str | None:
    """Return the request Origin, falling back to the Referer origin."""
    headers = request_headers(request)
    origin = headers.get("origin")
    if origin:
        return _origin_of(origin)
    referer = headers.get("referer")
    if referer:
        return _origin_of(referer)
    return None


def expected_origin(
    context: AdminContext, request: Request | WebSocket
) -> str:
    """The canonical same-origin authority for this deployment/request."""
    configured = context.config.proxy.public_origin
    if configured:
        normalized = _normalize_origin(configured)
        if normalized is not None:
            return normalized
    scope = request.scope
    scheme = str(scope.get("scheme") or "http").lower()
    scheme = _WEBSOCKET_SCHEMES.get(scheme, scheme)
    headers = request_headers(request)
    host = headers.get("host") or ""
    candidate = f"{scheme}://{host}"
    normalized = _normalize_origin(candidate)
    return normalized if normalized is not None else candidate


def same_origin(context: AdminContext, request: Request | WebSocket) -> bool:
    """True when Origin/Referer identifies exactly this deployment origin."""
    supplied = supplied_origin(request)
    if supplied is None:
        return False
    return supplied == expected_origin(context, request)


def _request_origin_url(context: AdminContext, request: Request) -> str:
    """The origin used to build absolute redirect URIs at login time."""
    configured = context.config.proxy.public_origin
    if configured:
        return configured.rstrip("/")
    scope = request.scope
    scheme = str(scope.get("scheme") or "http").lower()
    host = request_headers(request).get("host") or ""
    return f"{scheme}://{host}".rstrip("/")


def login_origin(context: AdminContext, request: Request) -> str:
    """Origin used for the OIDC redirect URI (public origin preferred)."""
    return _request_origin_url(context, request)


# -- session dependencies ----------------------------------------------------


def _resolve_or_raise(
    context: AdminContext, request: Request
) -> SessionRecord:
    auth = auth_for(context)
    token = session_cookie_token(request.cookies)
    try:
        return auth.resolve_session(token)
    except SessionAuthError as exc:
        if token:
            auth.record_security_event(
                kind="session.invalid",
                reason=exc.reason,
                request_id=_request_id(request),
                client_ip=client_ip(context, request),
            )
        msg = "authentication required"
        raise UnauthenticatedError(
            msg, details={"reason": exc.reason}
        ) from exc


def _request_id(request: Request) -> str:
    return get_request_id(request)


def require_session(
    context: AdminContext,
) -> Callable[[Request], SessionRecord]:
    """Dependency: authenticated session or 401 unauthenticated."""

    def dependency(request: Request) -> SessionRecord:
        return _resolve_or_raise(context, request)

    return dependency


def _check_role(
    context: AdminContext,
    request: Request,
    record: SessionRecord,
    roles: tuple[str, ...],
) -> None:
    if not roles or record.access_role in roles:
        return
    auth_for(context).record_security_event(
        kind="authz.role_denied",
        reason="role",
        subject=record.subject,
        access_role=record.access_role,
        request_id=_request_id(request),
        client_ip=client_ip(context, request),
        detail={"required_role": list(roles)},
    )
    msg = "the session does not have a required Access Role"
    raise ForbiddenError(
        msg,
        details={"reason": "role", "required_role": list(roles)},
    )


def _check_origin_and_csrf(
    context: AdminContext,
    request: Request,
    record: SessionRecord,
) -> None:
    auth = auth_for(context)
    if not same_origin(context, request):
        auth.record_security_event(
            kind="authz.origin_denied",
            reason=_MISSING_ORIGIN_REASON,
            subject=record.subject,
            access_role=record.access_role,
            request_id=_request_id(request),
            client_ip=client_ip(context, request),
        )
        msg = "the request origin is not same-origin"
        raise ForbiddenError(
            msg,
            details={"reason": _MISSING_ORIGIN_REASON},
        )
    provided = request_headers(request).get(CSRF_HEADER_NAME)
    if not auth.sessions.csrf_matches(record, provided):
        auth.record_security_event(
            kind="authz.csrf_denied",
            reason=_CSRF_REASON,
            subject=record.subject,
            access_role=record.access_role,
            request_id=_request_id(request),
            client_ip=client_ip(context, request),
        )
        msg = "the CSRF token is missing or invalid"
        raise ForbiddenError(
            msg,
            details={"reason": _CSRF_REASON},
        )


def _validate_roles(roles: tuple[str, ...]) -> tuple[str, ...]:
    unknown = [role for role in roles if role not in ADMIN_ROLES]
    if unknown:
        msg = f"unknown Access Role(s): {', '.join(unknown)}"
        raise ValueError(msg)
    return roles


def require_role(
    context: AdminContext, *roles: str
) -> Callable[[Request], SessionRecord]:
    """Dependency: authenticated session restricted to the given roles."""
    required = _validate_roles(tuple(roles))
    base = require_session(context)

    def dependency(
        request: Request,
        record: SessionRecord = Depends(base),  # ruff: ignore[function-call-in-default-argument]
    ) -> SessionRecord:
        _check_role(context, request, record, required)
        return record

    return dependency


_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


def require_authenticated_request(
    context: AdminContext,
) -> Callable[[Request], SessionRecord]:
    """Router-level dependency for every protected Admin route.

    Authenticates the session, and for unsafe methods additionally enforces
    the same-origin and CSRF checks. Mounting this on the parent router means
    a future Admin write route cannot accidentally skip CSRF protection just
    by omitting its own dependency.
    """
    base = require_session(context)

    def dependency(
        request: Request,
        record: SessionRecord = Depends(base),  # ruff: ignore[function-call-in-default-argument]
    ) -> SessionRecord:
        if request.method.upper() not in _SAFE_METHODS:
            _check_origin_and_csrf(context, request, record)
        return record

    return dependency


def require_csrf(
    context: AdminContext,
) -> Callable[[Request], SessionRecord]:
    """Dependency: authenticated session + same-origin + CSRF."""
    base = require_session(context)

    def dependency(
        request: Request,
        record: SessionRecord = Depends(base),  # ruff: ignore[function-call-in-default-argument]
    ) -> SessionRecord:
        _check_origin_and_csrf(context, request, record)
        return record

    return dependency


def require_write(
    context: AdminContext, *roles: str
) -> Callable[[Request], SessionRecord]:
    """Dependency: origin + CSRF + optional role check, one resolution."""
    required = _validate_roles(tuple(roles))
    base = require_session(context)

    def dependency(
        request: Request,
        record: SessionRecord = Depends(base),  # ruff: ignore[function-call-in-default-argument]
    ) -> SessionRecord:
        _check_role(context, request, record, required)
        _check_origin_and_csrf(context, request, record)
        return record

    return dependency


# -- ownership ---------------------------------------------------------------


def owner_or_superadmin(
    record: SessionRecord, owner_subject: str | None
) -> bool:
    """Ownership policy: the resource owner or any SuperAdmin."""
    if record.access_role == "SuperAdmin":
        return True
    return (
        owner_subject is not None
        and record.subject is not None
        and record.subject == owner_subject
    )


def require_ownership(
    context: AdminContext,
    record: SessionRecord,
    owner_subject: str | None,
    *,
    resource_id: str | None = None,
) -> None:
    """Raise ``403 ownership`` unless the session owns the resource."""
    if owner_or_superadmin(record, owner_subject):
        return
    auth_for(context).record_security_event(
        kind="authz.ownership_denied",
        reason="ownership",
        subject=record.subject,
        access_role=record.access_role,
        detail={"resource_id": resource_id} if resource_id else None,
    )
    msg = "the session does not own this resource"
    raise ForbiddenError(msg, details={"reason": "ownership"})


def is_ip_address(value: str) -> bool:
    """Small helper kept public for proxy-aware rate/quota code."""
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return False
    return True


__all__ = [
    "auth_for",
    "client_ip",
    "expected_origin",
    "is_ip_address",
    "login_origin",
    "owner_or_superadmin",
    "request_headers",
    "require_authenticated_request",
    "require_csrf",
    "require_ownership",
    "require_role",
    "require_session",
    "require_write",
    "same_origin",
    "session_cookie_token",
    "supplied_origin",
]
