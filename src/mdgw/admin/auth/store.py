"""SQLite persistence for admin authentication state.

Sessions and login transactions live in the same management database as the
foundation tables, but use their own connection so the migration runner in
``mdgw.admin.db`` stays the only owner of schema evolution. Only SHA-256 hashes
of bearer material (session token, CSRF token, opaque state) are stored; the
raw values exist in the HttpOnly cookie / response body and nowhere else.

Security events are the persisted failure stream for authentication,
authorization, CSRF and origin failures. They deliberately carry stable
reason codes and metadata only - never tokens, cookies, secrets or raw OIDC
claims - so ticket 03 can bridge them into Operator Audit Events.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

from mdgw.admin.db import AdminStoreError

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping

_SQLITE_BUSY_TIMEOUT_MS = 5_000
_MIN_AUTH_SCHEMA_VERSION = 2
_MAX_EVENT_DETAIL_BYTES = 4_096


@dataclass(frozen=True)
class AuthTransaction:
    """One server-side OIDC login transaction (state/nonce/PKCE)."""

    state_hash: str
    nonce_hash: str
    code_verifier: str
    redirect_uri: str
    return_to: str
    created_at_ns: int
    expires_at_ns: int


@dataclass(frozen=True)
class SessionRecord:
    """One opaque server session; bearer values are represented by hashes."""

    session_hash: str
    subject: str
    display_name: str
    email: str | None
    access_role: str
    issued_at_ns: int
    last_activity_ns: int
    absolute_expires_ns: int
    csrf_hash: str
    revoked: bool = False


@dataclass(frozen=True)
class SecurityEvent:
    """A persisted authentication/authorization failure."""

    kind: str
    reason: str
    occurred_at_ns: int
    subject: str | None = None
    access_role: str | None = None
    request_id: str | None = None
    client_ip: str | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)


class AuthStore:
    """Thread-safe access to the auth tables of the management database."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = sqlite3.connect(
            str(self.path),
            check_same_thread=False,
            isolation_level=None,
            timeout=_SQLITE_BUSY_TIMEOUT_MS / 1000,
        )
        self._conn.row_factory = sqlite3.Row
        try:
            self._configure()
            self._require_schema()
        except BaseException:
            self.close()
            raise

    # -- transactions ------------------------------------------------------

    @contextlib.contextmanager
    def transaction(self) -> Generator[sqlite3.Connection]:
        with self._lock:
            connection = self._connection()
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            connection.execute("COMMIT")

    # -- login transactions ------------------------------------------------

    def create_transaction(self, transaction: AuthTransaction) -> None:
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO auth_transactions (
                    state_hash, nonce_hash, code_verifier, redirect_uri,
                    return_to, created_at_ns, expires_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    transaction.state_hash,
                    transaction.nonce_hash,
                    transaction.code_verifier,
                    transaction.redirect_uri,
                    transaction.return_to,
                    transaction.created_at_ns,
                    transaction.expires_at_ns,
                ),
            )

    def consume_transaction(
        self, state_hash: str, *, now_ns: int
    ) -> AuthTransaction | None:
        """Atomically read-and-delete an unexpired transaction."""
        with self.transaction() as connection:
            row = connection.execute(
                """
                DELETE FROM auth_transactions
                WHERE state_hash = ?
                RETURNING state_hash, nonce_hash, code_verifier,
                          redirect_uri, return_to, created_at_ns,
                          expires_at_ns
                """,
                (state_hash,),
            ).fetchone()
            if row is None or int(row["expires_at_ns"]) <= now_ns:
                return None
            return AuthTransaction(
                state_hash=str(row["state_hash"]),
                nonce_hash=str(row["nonce_hash"]),
                code_verifier=str(row["code_verifier"]),
                redirect_uri=str(row["redirect_uri"]),
                return_to=str(row["return_to"]),
                created_at_ns=int(row["created_at_ns"]),
                expires_at_ns=int(row["expires_at_ns"]),
            )

    def purge_expired_transactions(self, *, now_ns: int) -> int:
        with self.transaction() as connection:
            cursor = connection.execute(
                "DELETE FROM auth_transactions WHERE expires_at_ns <= ?",
                (now_ns,),
            )
            return int(cursor.rowcount)

    # -- sessions ----------------------------------------------------------

    def insert_session(self, record: SessionRecord) -> None:
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO admin_sessions (
                    session_hash, subject, display_name, email, access_role,
                    issued_at_ns, last_activity_ns, absolute_expires_ns,
                    csrf_hash, revoked, revoked_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    record.session_hash,
                    record.subject,
                    record.display_name,
                    record.email,
                    record.access_role,
                    record.issued_at_ns,
                    record.last_activity_ns,
                    record.absolute_expires_ns,
                    record.csrf_hash,
                    int(record.revoked),
                ),
            )

    def find_session(self, session_hash: str) -> SessionRecord | None:
        with self._lock:
            row = self._connection().execute(
                """
                SELECT session_hash, subject, display_name, email,
                       access_role, issued_at_ns, last_activity_ns,
                       absolute_expires_ns, csrf_hash, revoked
                FROM admin_sessions WHERE session_hash = ?
                """,
                (session_hash,),
            ).fetchone()
        return None if row is None else _record_from_row(row)

    def touch_session(self, session_hash: str, *, last_activity_ns: int) -> bool:
        with self.transaction() as connection:
            cursor = connection.execute(
                "UPDATE admin_sessions SET last_activity_ns = ? "
                "WHERE session_hash = ? AND revoked = 0",
                (last_activity_ns, session_hash),
            )
            return cursor.rowcount > 0

    def rotate_csrf(self, session_hash: str, *, csrf_hash: str) -> bool:
        with self.transaction() as connection:
            cursor = connection.execute(
                "UPDATE admin_sessions SET csrf_hash = ? "
                "WHERE session_hash = ? AND revoked = 0",
                (csrf_hash, session_hash),
            )
            return cursor.rowcount > 0

    def refresh_session(
        self,
        session_hash: str,
        *,
        last_activity_ns: int,
        csrf_hash: str,
    ) -> bool:
        """Extend idle activity and rotate CSRF in one transaction."""
        with self.transaction() as connection:
            cursor = connection.execute(
                "UPDATE admin_sessions SET last_activity_ns = ?, "
                "csrf_hash = ? WHERE session_hash = ? AND revoked = 0",
                (last_activity_ns, csrf_hash, session_hash),
            )
            return cursor.rowcount > 0

    def revoke_session(self, session_hash: str, *, reason: str) -> bool:
        with self.transaction() as connection:
            # Idempotent: an already-revoked session still reports success as
            # long as the row exists, while the first reason is retained.
            cursor = connection.execute(
                "UPDATE admin_sessions SET revoked = 1, "
                "revoked_reason = COALESCE(revoked_reason, ?) "
                "WHERE session_hash = ?",
                (reason, session_hash),
            )
            return cursor.rowcount > 0

    def purge_expired_sessions(self, *, now_ns: int) -> int:
        with self.transaction() as connection:
            cursor = connection.execute(
                "DELETE FROM admin_sessions WHERE absolute_expires_ns <= ?",
                (now_ns,),
            )
            return int(cursor.rowcount)

    # -- security events ---------------------------------------------------

    def record_security_event(self, event: SecurityEvent) -> None:
        detail_json = json.dumps(
            dict(event.detail), sort_keys=True, separators=(",", ":")
        )
        if len(detail_json.encode("utf-8")) > _MAX_EVENT_DETAIL_BYTES:
            detail_json = "{}"
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO admin_security_events (
                    occurred_at_ns, kind, reason, subject, access_role,
                    request_id, client_ip, detail_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.occurred_at_ns,
                    event.kind,
                    event.reason,
                    event.subject,
                    event.access_role,
                    event.request_id,
                    event.client_ip,
                    detail_json,
                ),
            )

    def recent_security_events(self, *, limit: int = 50) -> list[SecurityEvent]:
        bounded = max(1, min(int(limit), 200))
        with self._lock:
            rows = self._connection().execute(
                """
                SELECT occurred_at_ns, kind, reason, subject, access_role,
                       request_id, client_ip, detail_json
                FROM admin_security_events
                ORDER BY event_id DESC LIMIT ?
                """,
                (bounded,),
            ).fetchall()
        return [_event_from_row(row) for row in rows]

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    # -- internals ---------------------------------------------------------

    def _configure(self) -> None:
        connection = self._connection()
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {_SQLITE_BUSY_TIMEOUT_MS}")
        connection.execute("PRAGMA synchronous = NORMAL")

    def _require_schema(self) -> None:
        row = self._connection().execute(
            "SELECT version FROM schema_version WHERE id = 1"
        ).fetchone()
        version = 0 if row is None else int(row[0])
        if version < _MIN_AUTH_SCHEMA_VERSION:
            msg = (
                "admin auth tables require schema version "
                f"{_MIN_AUTH_SCHEMA_VERSION}, found {version}"
            )
            raise AdminStoreError(msg)

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            msg = "AuthStore is closed"
            raise sqlite3.ProgrammingError(msg)
        return self._conn


def _record_from_row(row: sqlite3.Row) -> SessionRecord:
    return SessionRecord(
        session_hash=str(row["session_hash"]),
        subject=str(row["subject"]),
        display_name=str(row["display_name"]),
        email=None if row["email"] is None else str(row["email"]),
        access_role=str(row["access_role"]),
        issued_at_ns=int(row["issued_at_ns"]),
        last_activity_ns=int(row["last_activity_ns"]),
        absolute_expires_ns=int(row["absolute_expires_ns"]),
        csrf_hash=str(row["csrf_hash"]),
        revoked=bool(row["revoked"]),
    )


def _event_from_row(row: sqlite3.Row) -> SecurityEvent:
    try:
        detail = json.loads(str(row["detail_json"]))
    except ValueError:
        detail = {}
    if not isinstance(detail, dict):
        detail = {}
    return SecurityEvent(
        kind=str(row["kind"]),
        reason=str(row["reason"]),
        occurred_at_ns=int(row["occurred_at_ns"]),
        subject=None if row["subject"] is None else str(row["subject"]),
        access_role=(
            None if row["access_role"] is None else str(row["access_role"])
        ),
        request_id=(
            None if row["request_id"] is None else str(row["request_id"])
        ),
        client_ip=(
            None if row["client_ip"] is None else str(row["client_ip"])
        ),
        detail=detail,
    )


__all__ = [
    "AuthStore",
    "AuthTransaction",
    "SecurityEvent",
    "SessionRecord",
]
