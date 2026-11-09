"""Admin authentication routes: login, callback, session, refresh, logout.

Only four Admin paths are public (capabilities, login, callback, session
status). Everything else - including every router later tickets register
through ``register_feature`` - is mounted under a router-level
``require_session`` dependency by ``create_admin_router``.

OIDC failures redirect the browser back to the SPA login with a stable
``?error=`` value; they never expose provider text, tokens or secrets. The
status probe is a no-refresh read that rotates only the CSRF token.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from starlette.responses import (
    JSONResponse,
    RedirectResponse,
    Response,
)

from mdgw.admin.auth.dependencies import (
    client_ip,
    login_origin,
    session_cookie_token,
)
from mdgw.admin.auth.oidc import OidcError
from mdgw.admin.auth.service import (
    ERROR_IDP_ERROR,
    SESSION_COOKIE_NAME,
    TX_COOKIE_NAME,
    AdminAuth,
    CallbackFailureError,
    OidcUnavailableError,
)
from mdgw.admin.auth.sessions import InvalidReturnToError, SessionAuthError
from mdgw.admin.auth.ws import add_admin_ws_route
from mdgw.admin.errors import InvalidParameterError, UnauthenticatedError
from mdgw.api.errors import get_request_id

if TYPE_CHECKING:
    from mdgw.admin.context import AdminContext

NO_STORE = {"Cache-Control": "no-store"}

# Handlers are synchronous on purpose: the OIDC client uses blocking stdlib
# urllib and the session store uses sqlite3, so FastAPI runs these in its
# threadpool instead of blocking the server event loop.


def create_auth_router(context: AdminContext) -> APIRouter:
    """Public authentication routes plus the authenticated Admin WebSocket."""
    router = APIRouter()

    @router.get("/auth/login")
    def login(
        request: Request, return_to: str | None = None
    ) -> RedirectResponse:
        auth = context.auth
        request_id = get_request_id(request)
        try:
            start = auth.start_login(
                return_to,
                origin=login_origin(context, request),
                request_id=request_id,
                client_ip=client_ip(context, request),
            )
        except InvalidReturnToError as exc:
            msg = "return_to must be a same-origin relative path"
            raise InvalidParameterError(
                msg,
                details={"field": "return_to", "reason": "invalid_return_to"},
            ) from exc
        except OidcUnavailableError as exc:
            auth.record_security_event(
                kind="auth.login.idp_error",
                reason="oidc_not_configured",
                request_id=request_id,
                client_ip=client_ip(context, request),
            )
            msg = "OIDC authentication is not available"
            raise UnauthenticatedError(
                msg, details={"reason": "idp_error"}
            ) from exc
        except OidcError as exc:
            auth.record_security_event(
                kind="auth.login.idp_error",
                reason=exc.reason,
                request_id=request_id,
                client_ip=client_ip(context, request),
            )
            msg = "OIDC authentication is not available"
            raise UnauthenticatedError(
                msg, details={"reason": "idp_error"}
            ) from exc

        response = RedirectResponse(
            start.authorization_url, status_code=302
        )
        _set_cookie(response, auth.tx_cookie(start.state))
        return response

    @router.get("/auth/callback")
    def callback(
        request: Request,
        code: str | None = None,
        state: str | None = None,
        error: str | None = None,
    ) -> RedirectResponse:
        auth = context.auth
        request_id = get_request_id(request)
        tx_cookie = request.cookies.get(TX_COOKIE_NAME)
        if error is not None:
            auth.record_security_event(
                kind="auth.login.idp_error",
                reason=_stable_provider_error(error),
                request_id=request_id,
                client_ip=client_ip(context, request),
            )
            return _error_redirect(context, ERROR_IDP_ERROR)
        try:
            success = auth.complete_login(
                code=code,
                state=state,
                tx_cookie=tx_cookie,
                request_id=request_id,
                client_ip=client_ip(context, request),
            )
        except CallbackFailureError as failure:
            return _error_redirect(context, failure.error)

        # Session fixation: any pre-login cookie is replaced and invalidated
        # server-side; the new session always gets a fresh opaque id.
        previous_token = session_cookie_token(request.cookies)
        if previous_token:
            auth.logout(previous_token)

        response = RedirectResponse(success.return_to, status_code=303)
        _set_cookie(response, auth.session_cookie(success.session.token))
        _set_cookie(response, auth.clear_tx_cookie())
        return response

    @router.get("/auth/session")
    def session_status(request: Request) -> JSONResponse:
        auth = context.auth
        payload = auth.status_payload(
            session_cookie_token(request.cookies)
        )
        return JSONResponse(payload, headers=NO_STORE)

    add_admin_ws_route(router, context)
    return router


def create_protected_auth_router(context: AdminContext) -> APIRouter:
    """Session-bound routes; mounted under the protected admin router."""
    router = APIRouter()

    @router.post("/auth/refresh")
    def refresh(request: Request) -> JSONResponse:
        auth = context.auth
        try:
            payload = auth.refresh_payload(
                session_cookie_token(request.cookies)
            )
        except SessionAuthError as exc:
            msg = "authentication required"
            raise UnauthenticatedError(
                msg, details={"reason": exc.reason}
            ) from exc
        return JSONResponse(payload, headers=NO_STORE)

    @router.post("/auth/logout")
    def logout(request: Request) -> Response:
        auth = context.auth
        auth.logout(session_cookie_token(request.cookies))
        response = Response(status_code=204)
        _set_cookie(response, auth.clear_session_cookie())
        return response

    return router


def _stable_provider_error(value: str) -> str:
    """Keep a short provider error code; never echo arbitrary text."""
    cleaned = value.strip().lower()
    if cleaned and len(cleaned) <= 64 and cleaned.isascii() and all(
        character.isalnum() or character in "._-" for character in cleaned
    ):
        return cleaned
    return "provider_error"


def _error_redirect(
    context: AdminContext, error: str
) -> RedirectResponse:
    path = context.config.oidc.error_redirect_path
    separator = "&" if "?" in path else "?"
    location = f"{path}{separator}{urlencode({'error': error})}"
    response = RedirectResponse(location, status_code=303)
    # The transaction cookie is single-use; drop it on every terminal path.
    _set_cookie(response, AdminAuth.clear_tx_cookie())
    return response


def _set_cookie(response: Response, spec: dict[str, object]) -> None:
    response.set_cookie(**spec)


__all__ = [
    "SESSION_COOKIE_NAME",
    "create_auth_router",
    "create_protected_auth_router",
]
