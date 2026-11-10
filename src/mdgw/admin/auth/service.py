"""Admin authentication orchestration: OIDC flow, cookies, session payloads.

This is the seam between HTTP handlers and the two lower-level modules:
``OidcClient`` (provider/token/claims) and ``SessionManager`` (opaque server
sessions). It owns the wire-specific pieces:

* the ``mdgw_session`` and ``mdgw_oidc_tx`` cookie attributes;
* mapping login failures onto stable SPA ``?error=`` values;
* the exact ``AdminSessionPayload`` shape;
* persisted security events for role, token, session, CSRF and origin
  failures (the stream ticket 03 bridges into Operator Audit Events).

No token, cookie value, client secret or raw OIDC claim is ever persisted in a
security event or included in an exception message.
"""

from __future__ import annotations

import hmac
import logging
import secrets
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mdgw.admin.auth.oidc import (
    IdTokenInvalidError,
    OidcClient,
    OidcError,
    RoleResolutionError,
    pkce_challenge,
    resolve_access_role,
)
from mdgw.admin.auth.sessions import (
    InvalidReturnToError,
    IssuedSession,
    SessionAuthError,
    SessionManager,
    token_hash,
    validate_return_to,
)
from mdgw.admin.auth.store import AuthStore, AuthTransaction, SecurityEvent
from mdgw.core.time import ns_to_iso

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from mdgw.admin.auth.store import SessionRecord
    from mdgw.config import AdminConfig

SESSION_COOKIE_NAME = "mdgw_session"
TX_COOKIE_NAME = "mdgw_oidc_tx"
TX_COOKIE_PATH = "/api/v1/admin/auth"
CSRF_HEADER_NAME = "X-CSRF-Token"

_STATE_BYTES = 32
_NONCE_BYTES = 32
_VERIFIER_BYTES = 64
_MAX_SUBJECT_LENGTH = 256
_MAX_DISPLAY_NAME_LENGTH = 256
_MAX_EMAIL_LENGTH = 320

logger = logging.getLogger(__name__)

#: Stable SPA login error values (never provider text, never tokens).
ERROR_STATE_INVALID = "state_invalid"
ERROR_IDP_ERROR = "idp_error"
ERROR_TOKEN_INVALID = (
    "token_invalid"  # ruff: ignore[hardcoded-password-string]  # wire value, not a secret
)
ERROR_ROLE_UNMAPPED = "role_unmapped"
ERROR_ROLE_CONFLICT = "role_conflict"


class OidcUnavailableError(Exception):
    """OIDC is not configured; authentication fails closed."""

    reason = "idp_error"


class CallbackFailureError(Exception):
    """A callback that must redirect to the SPA login with a stable error."""

    def __init__(self, error: str, reason: str) -> None:
        super().__init__(reason)
        self.error = error
        self.reason = reason


@dataclass(frozen=True)
class LoginStart:
    authorization_url: str
    state: str
    transaction_ttl_s: int


@dataclass(frozen=True)
class CallbackSuccess:
    return_to: str
    session: IssuedSession


class AdminAuth:
    """Request-time authentication facade for one Admin API runtime."""

    def __init__(
        self,
        config: AdminConfig,
        store: AuthStore,
        *,
        oidc: OidcClient | None = None,
        clock: Callable[[], int] | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        self.config = config
        self.store = store
        self.oidc = oidc
        self._clock = time.time_ns if clock is None else clock
        self._environ = environ
        self.sessions = SessionManager(store, config.sessions, clock=clock)
        #: Optional bridge installed by ticket 03 (Operator Audit Events).
        self.security_event_sink: Callable[[SecurityEvent], None] | None = None

    # -- state -------------------------------------------------------------

    @property
    def oidc_available(self) -> bool:
        return self.oidc is not None

    def now_ns(self) -> int:
        return self._clock()

    def close(self) -> None:
        self.store.close()

    # -- login -------------------------------------------------------------

    def start_login(
        self,
        return_to: str | None,
        *,
        origin: str,
        request_id: str | None = None,
        client_ip: str | None = None,
    ) -> LoginStart:
        if self.oidc is None:
            msg = "OIDC authentication is not configured"
            raise OidcUnavailableError(msg)
        try:
            safe_return_to = validate_return_to(
                return_to, self.config.oidc.default_return_to
            )
        except InvalidReturnToError:
            self.record_security_event(
                kind="auth.login.invalid_return_to",
                reason="invalid_return_to",
                request_id=request_id,
                client_ip=client_ip,
            )
            raise

        now = self.now_ns()
        state = secrets.token_urlsafe(_STATE_BYTES)
        nonce = secrets.token_urlsafe(_NONCE_BYTES)
        verifier = secrets.token_urlsafe(_VERIFIER_BYTES)
        redirect_uri = self._redirect_uri(origin)
        ttl_s = int(self.config.oidc.transaction_ttl_s)

        self.store.create_transaction(
            AuthTransaction(
                state_hash=token_hash(state),
                nonce_hash=token_hash(nonce),
                code_verifier=verifier,
                redirect_uri=redirect_uri,
                return_to=safe_return_to,
                created_at_ns=now,
                expires_at_ns=now + ttl_s * 1_000_000_000,
            )
        )
        authorization_url = self.oidc.authorization_url(
            state=state,
            nonce=nonce,
            code_challenge=pkce_challenge(verifier),
            redirect_uri=redirect_uri,
        )
        return LoginStart(
            authorization_url=authorization_url,
            state=state,
            transaction_ttl_s=ttl_s,
        )

    def _redirect_uri(self, origin: str) -> str:
        configured = self.config.oidc.redirect_uri
        if configured is not None:
            return configured
        return f"{origin.rstrip('/')}/api/v1/admin/auth/callback"

    def complete_login(
        self,
        *,
        code: str | None,
        state: str | None,
        tx_cookie: str | None,
        request_id: str | None = None,
        client_ip: str | None = None,
    ) -> CallbackSuccess:
        now = self.now_ns()
        if not state or not tx_cookie:
            self._callback_failure(
                ERROR_STATE_INVALID,
                "state_missing",
                request_id=request_id,
                client_ip=client_ip,
            )
        if not hmac.compare_digest(
            state.encode("utf-8"), tx_cookie.encode("utf-8")
        ):
            self._callback_failure(
                ERROR_STATE_INVALID,
                "state_cookie_mismatch",
                request_id=request_id,
                client_ip=client_ip,
            )
        transaction = self.store.consume_transaction(
            token_hash(state), now_ns=now
        )
        if transaction is None:
            self._callback_failure(
                ERROR_STATE_INVALID,
                "transaction_unknown_or_expired",
                request_id=request_id,
                client_ip=client_ip,
            )

        if self.oidc is None:
            self._callback_failure(
                ERROR_IDP_ERROR,
                "oidc_not_configured",
                request_id=request_id,
                client_ip=client_ip,
            )
        if not code:
            self._callback_failure(
                ERROR_IDP_ERROR,
                "authorization_code_missing",
                request_id=request_id,
                client_ip=client_ip,
            )

        try:
            id_token = self.oidc.exchange_code(
                code=code,
                code_verifier=transaction.code_verifier,
                redirect_uri=transaction.redirect_uri,
            )
        except OidcError as exc:
            self._callback_failure(
                ERROR_IDP_ERROR,
                exc.reason,
                request_id=request_id,
                client_ip=client_ip,
            )

        try:
            claims = self.oidc.verify_id_token(
                id_token,
                expected_nonce_hash=transaction.nonce_hash,
                now_ns=now,
            )
        except IdTokenInvalidError as exc:
            self._callback_failure(
                ERROR_TOKEN_INVALID,
                exc.reason,
                request_id=request_id,
                client_ip=client_ip,
            )

        try:
            access_role = resolve_access_role(
                claims,
                claim_name=self.config.oidc.roles_claim,
                role_mapping=self.config.oidc.role_mapping,
            )
        except RoleResolutionError as exc:
            self.record_security_event(
                kind=(
                    "auth.role_conflict"
                    if exc.reason == "role_conflict"
                    else "auth.role_unmapped"
                ),
                reason=exc.reason,
                request_id=request_id,
                client_ip=client_ip,
            )
            raise CallbackFailureError(exc.reason, exc.reason) from exc

        identity = _identity_from_claims(claims)
        issued = self.sessions.create(
            subject=identity.subject,
            display_name=identity.display_name,
            email=identity.email,
            access_role=access_role,
            now_ns=now,
        )
        return CallbackSuccess(
            return_to=transaction.return_to, session=issued
        )

    def _callback_failure(
        self,
        error: str,
        reason: str,
        *,
        request_id: str | None,
        client_ip: str | None,
    ) -> None:
        kind = {
            ERROR_STATE_INVALID: "auth.login.state_invalid",
            ERROR_IDP_ERROR: "auth.login.idp_error",
            ERROR_TOKEN_INVALID: "auth.login.token_invalid",
        }.get(error, "auth.login.failed")
        self.record_security_event(
            kind=kind,
            reason=reason,
            request_id=request_id,
            client_ip=client_ip,
            detail={"error": error},
        )
        raise CallbackFailureError(error, reason)

    # -- session endpoints -------------------------------------------------

    def anonymous_payload(self, *, now_ns: int | None = None) -> dict[str, Any]:
        now = self.now_ns() if now_ns is None else int(now_ns)
        return {
            "authenticated": False,
            "server_time": ns_to_iso(now),
            "user": None,
            "access_role": None,
            "session": None,
            "csrf": None,
        }

    def session_payload(
        self,
        record: SessionRecord,
        csrf_token: str,
        *,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        now = self.now_ns() if now_ns is None else int(now_ns)
        return {
            "authenticated": True,
            "server_time": ns_to_iso(now),
            "user": {
                "subject": record.subject,
                "display_name": record.display_name,
                "email": record.email,
            },
            "access_role": record.access_role,
            "session": {
                "issued_at": ns_to_iso(record.issued_at_ns),
                "last_activity_at": ns_to_iso(record.last_activity_ns),
                "idle_expires_at": ns_to_iso(
                    self.sessions.idle_expiry_ns(record)
                ),
                "absolute_expires_at": ns_to_iso(
                    record.absolute_expires_ns
                ),
            },
            "csrf": {"header_name": CSRF_HEADER_NAME, "token": csrf_token},
        }

    def status_payload(
        self, token: str | None, *, now_ns: int | None = None
    ) -> dict[str, Any]:
        """Non-refreshing status probe; rotates CSRF only (never idle)."""
        record = self.sessions.probe(token, now_ns=now_ns)
        if record is None:
            return self.anonymous_payload(now_ns=now_ns)
        try:
            updated, csrf_token = self.sessions.rotate_csrf(
                token, now_ns=now_ns
            )
        except SessionAuthError:
            return self.anonymous_payload(now_ns=now_ns)
        return self.session_payload(updated, csrf_token, now_ns=now_ns)

    def refresh_payload(
        self, token: str | None, *, now_ns: int | None = None
    ) -> dict[str, Any]:
        record, csrf_token = self.sessions.refresh(token, now_ns=now_ns)
        return self.session_payload(record, csrf_token, now_ns=now_ns)

    def resolve_session(
        self, token: str | None, *, now_ns: int | None = None
    ) -> SessionRecord:
        return self.sessions.resolve(token, now_ns=now_ns)

    def logout(self, token: str | None) -> None:
        self.sessions.revoke(token)

    # -- cookies -----------------------------------------------------------

    def session_cookie(self, token: str) -> dict[str, Any]:
        return {
            "key": SESSION_COOKIE_NAME,
            "value": token,
            "max_age": int(self.config.sessions.absolute_timeout_s),
            "path": "/",
            "secure": True,
            "httponly": True,
            "samesite": "lax",
        }

    def tx_cookie(self, state: str) -> dict[str, Any]:
        return {
            "key": TX_COOKIE_NAME,
            "value": state,
            "max_age": int(self.config.oidc.transaction_ttl_s),
            "path": TX_COOKIE_PATH,
            "secure": True,
            "httponly": True,
            "samesite": "lax",
        }

    @staticmethod
    def clear_session_cookie() -> dict[str, Any]:
        return {
            "key": SESSION_COOKIE_NAME,
            "value": "",
            "max_age": 0,
            "path": "/",
            "secure": True,
            "httponly": True,
            "samesite": "lax",
        }

    @staticmethod
    def clear_tx_cookie() -> dict[str, Any]:
        return {
            "key": TX_COOKIE_NAME,
            "value": "",
            "max_age": 0,
            "path": TX_COOKIE_PATH,
            "secure": True,
            "httponly": True,
            "samesite": "lax",
        }

    # -- security events ---------------------------------------------------

    def record_security_event(  # ruff: ignore[too-many-arguments]  # explicit event fields
        self,
        *,
        kind: str,
        reason: str,
        subject: str | None = None,
        access_role: str | None = None,
        request_id: str | None = None,
        client_ip: str | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        """Persist one security event; never include bearer material.

        ``security_event_sink`` is the narrow bridge ticket 03 can install to
        forward these events into Operator Audit Events. A sink failure never
        changes the authorization outcome.
        """
        event = SecurityEvent(
            kind=kind,
            reason=reason,
            occurred_at_ns=self.now_ns(),
            subject=subject,
            access_role=access_role,
            request_id=request_id,
            client_ip=client_ip,
            detail=dict(detail or {}),
        )
        try:
            self.store.record_security_event(event)
        except Exception:  # security telemetry must not break auth itself
            logger.exception(
                "failed to persist admin security event",
                extra={"event": "admin.security_event.write_failed"},
            )
        sink = self.security_event_sink
        if sink is not None:
            try:
                sink(event)
            except Exception:  # the bridge must not break auth either
                logger.exception(
                    "admin security event sink failed",
                    extra={"event": "admin.security_event.sink_failed"},
                )


@dataclass(frozen=True)
class _Identity:
    subject: str
    display_name: str
    email: str | None


def _identity_from_claims(claims: Mapping[str, Any]) -> _Identity:
    subject = _bounded_text(claims.get("sub"), _MAX_SUBJECT_LENGTH) or ""
    display_name = (
        _bounded_text(claims.get("name"), _MAX_DISPLAY_NAME_LENGTH)
        or _bounded_text(
            claims.get("preferred_username"), _MAX_DISPLAY_NAME_LENGTH
        )
        or subject
    )
    email = _bounded_text(claims.get("email"), _MAX_EMAIL_LENGTH)
    return _Identity(
        subject=subject, display_name=display_name, email=email
    )


def _bounded_text(value: object, limit: int) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    return cleaned[:limit]


__all__ = [
    "CSRF_HEADER_NAME",
    "ERROR_IDP_ERROR",
    "ERROR_ROLE_CONFLICT",
    "ERROR_ROLE_UNMAPPED",
    "ERROR_STATE_INVALID",
    "ERROR_TOKEN_INVALID",
    "SESSION_COOKIE_NAME",
    "TX_COOKIE_NAME",
    "TX_COOKIE_PATH",
    "AdminAuth",
    "CallbackFailureError",
    "CallbackSuccess",
    "LoginStart",
    "OidcUnavailableError",
]
