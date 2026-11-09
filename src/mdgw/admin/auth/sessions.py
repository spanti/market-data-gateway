"""Opaque server-side sessions with idle and absolute expiries.

The browser holds only an opaque random session token in an HttpOnly cookie.
The database stores its SHA-256 hash, the identity/role snapshot taken at
login, the CSRF token hash, and explicit idle/absolute deadlines. Every admin
request resolves and re-authorizes the session server-side; nothing is trusted
from the client beyond the cookie itself.

Contract semantics: 30 minutes idle and 8 hours absolute (configurable). The
status probe never refreshes the idle timer; only an explicit refresh does,
and it can never move the absolute deadline.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from mdgw.admin.auth.store import SessionRecord

if TYPE_CHECKING:
    from collections.abc import Callable

    from mdgw.admin.auth.store import AuthStore
    from mdgw.config import AdminSessionConfig

SESSION_TOKEN_BYTES = 32
CSRF_TOKEN_BYTES = 32
MAX_RETURN_TO_LENGTH = 2_048
NANOS_PER_SECOND = 1_000_000_000


REASON_MISSING = "missing"
REASON_INVALID = "invalid"
REASON_IDLE_TIMEOUT = "idle_timeout"
REASON_ABSOLUTE_TIMEOUT = "absolute_timeout"


class SessionAuthError(Exception):
    """A session could not be resolved; ``reason`` is the contract value."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class InvalidReturnToError(ValueError):
    """``return_to`` was not a safe same-origin relative SPA path."""

    reason = "invalid_return_to"


@dataclass(frozen=True)
class IssuedSession:
    """A freshly created session plus the one-time bearer values."""

    record: SessionRecord
    token: str
    csrf_token: str


def token_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def validate_return_to(
    value: str | None, default: str
) -> str:
    """Return a safe relative SPA path or raise ``InvalidReturnToError``.

    Missing values use the configured default. Anything that could leave the
    SPA origin - absolute URLs, protocol-relative hosts, control characters,
    backslashes - is rejected rather than silently rewritten.
    """
    if value is None:
        return default
    if not isinstance(value, str):
        msg = "return_to must be a string"
        raise InvalidReturnToError(msg)
    if len(value) > MAX_RETURN_TO_LENGTH:
        msg = "return_to is too long"
        raise InvalidReturnToError(msg)
    if not value.startswith("/") or value.startswith("//"):
        msg = "return_to must be a same-origin relative path"
        raise InvalidReturnToError(msg)
    if "\\" in value or any(
        ord(character) < 0x20 or ord(character) == 0x7F for character in value
    ):
        msg = "return_to must not contain control characters or backslashes"
        raise InvalidReturnToError(msg)
    return value


class SessionManager:
    """Create, resolve, refresh and revoke opaque server sessions."""

    def __init__(
        self,
        store: AuthStore,
        config: AdminSessionConfig,
        *,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self._store = store
        self._config = config
        self._clock = time.time_ns if clock is None else clock

    @property
    def idle_timeout_ns(self) -> int:
        return int(self._config.idle_timeout_s) * NANOS_PER_SECOND

    @property
    def absolute_timeout_ns(self) -> int:
        return int(self._config.absolute_timeout_s) * NANOS_PER_SECOND

    def absolute_expiry_ns(self, issued_at_ns: int) -> int:
        return issued_at_ns + self.absolute_timeout_ns

    def idle_expiry_ns(self, record: SessionRecord) -> int:
        """Idle deadline, never past the absolute deadline."""
        return min(
            record.last_activity_ns + self.idle_timeout_ns,
            record.absolute_expires_ns,
        )

    # -- lifecycle ---------------------------------------------------------

    def create(
        self,
        *,
        subject: str,
        display_name: str,
        email: str | None,
        access_role: str,
        now_ns: int | None = None,
    ) -> IssuedSession:
        now = self._now(now_ns)
        token = secrets.token_urlsafe(SESSION_TOKEN_BYTES)
        csrf_token = secrets.token_urlsafe(CSRF_TOKEN_BYTES)
        record = SessionRecord(
            session_hash=token_hash(token),
            subject=subject,
            display_name=display_name,
            email=email,
            access_role=access_role,
            issued_at_ns=now,
            last_activity_ns=now,
            absolute_expires_ns=self.absolute_expiry_ns(now),
            csrf_hash=token_hash(csrf_token),
            revoked=False,
        )
        # Opportunistic bounded cleanup; a session creation is also the
        # natural point to forget abandoned login transactions.
        self._store.purge_expired_sessions(now_ns=now)
        self._store.purge_expired_transactions(now_ns=now)
        self._store.insert_session(record)
        return IssuedSession(record=record, token=token, csrf_token=csrf_token)

    def resolve(
        self, token: str | None, *, now_ns: int | None = None
    ) -> SessionRecord:
        """Resolve and authorize one session; raise on any failure."""
        now = self._now(now_ns)
        if not token:
            raise SessionAuthError(REASON_MISSING)
        record = self._store.find_session(token_hash(token))
        if record is None or record.revoked:
            raise SessionAuthError(REASON_INVALID)
        if now >= record.absolute_expires_ns:
            self._store.revoke_session(
                record.session_hash, reason=REASON_ABSOLUTE_TIMEOUT
            )
            raise SessionAuthError(REASON_ABSOLUTE_TIMEOUT)
        if now >= self.idle_expiry_ns(record):
            self._store.revoke_session(
                record.session_hash, reason=REASON_IDLE_TIMEOUT
            )
            raise SessionAuthError(REASON_IDLE_TIMEOUT)
        return record

    def probe(
        self, token: str | None, *, now_ns: int | None = None
    ) -> SessionRecord | None:
        """Non-refreshing status lookup; any failure means anonymous."""
        try:
            return self.resolve(token, now_ns=now_ns)
        except SessionAuthError:
            return None

    def refresh(
        self, token: str | None, *, now_ns: int | None = None
    ) -> tuple[SessionRecord, str]:
        """Extend idle activity, rotate CSRF, never extend absolute."""
        now = self._now(now_ns)
        record = self.resolve(token, now_ns=now)
        csrf_token = secrets.token_urlsafe(CSRF_TOKEN_BYTES)
        updated = self._store.refresh_session(
            record.session_hash,
            last_activity_ns=now,
            csrf_hash=token_hash(csrf_token),
        )
        if not updated:
            raise SessionAuthError(REASON_INVALID)
        refreshed = self._store.find_session(record.session_hash)
        if refreshed is None or refreshed.revoked:
            raise SessionAuthError(REASON_INVALID)
        return refreshed, csrf_token

    def rotate_csrf(
        self, token: str | None, *, now_ns: int | None = None
    ) -> tuple[SessionRecord, str]:
        """Issue a fresh CSRF token without touching the idle deadline."""
        record = self.resolve(token, now_ns=now_ns)
        csrf_token = secrets.token_urlsafe(CSRF_TOKEN_BYTES)
        if not self._store.rotate_csrf(
            record.session_hash, csrf_hash=token_hash(csrf_token)
        ):
            raise SessionAuthError(REASON_INVALID)
        updated = self._store.find_session(record.session_hash)
        if updated is None or updated.revoked:
            raise SessionAuthError(REASON_INVALID)
        return updated, csrf_token

    def revoke(self, token: str | None) -> None:
        """Invalidate a session server-side; unknown tokens are a no-op."""
        if not token:
            return
        self._store.revoke_session(token_hash(token), reason="logout")

    # -- CSRF --------------------------------------------------------------

    @staticmethod
    def csrf_matches(record: SessionRecord, provided: str | None) -> bool:
        if not provided:
            return False
        return hmac.compare_digest(token_hash(provided), record.csrf_hash)

    def _now(self, now_ns: int | None) -> int:
        return self._clock() if now_ns is None else int(now_ns)


__all__ = [
    "InvalidReturnToError",
    "IssuedSession",
    "SessionAuthError",
    "SessionManager",
    "token_hash",
    "validate_return_to",
]
