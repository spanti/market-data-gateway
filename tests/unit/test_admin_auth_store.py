# ruff: file-ignore[implicit-namespace-package]
"""Auth persistence: hashed session tokens, one-shot OIDC transactions, events.

The management database is the server-side authority for sessions. Tests check
the persisted representation (hashes, not bearer secrets) and one-shot
transaction consumption, because those are the security properties later
tickets rely on.
"""

from __future__ import annotations

import hashlib
import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

from mdgw.admin.auth.store import (
    AuthStore,
    AuthTransaction,
    SecurityEvent,
    SessionRecord,
)
from mdgw.admin.db import ADMIN_STORE_SCHEMA_VERSION, AdminStore, discover_migrations

SECONDS = 1_000_000_000


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _open(path: Path) -> tuple[AdminStore, AuthStore]:
    admin = AdminStore(path)
    return admin, AuthStore(path)


def _transaction(now_ns: int = 1_000 * SECONDS) -> AuthTransaction:
    return AuthTransaction(
        state_hash=_hash("state-value"),
        nonce_hash=_hash("nonce-value"),
        code_verifier="verifier-value",
        redirect_uri="https://console.example.com/api/v1/admin/auth/callback",
        return_to="/monitoring",
        created_at_ns=now_ns,
        expires_at_ns=now_ns + 300 * SECONDS,
    )


def _session(now_ns: int = 1_000 * SECONDS) -> SessionRecord:
    return SessionRecord(
        session_hash=_hash("session-token"),
        subject="oidc-subject-1",
        display_name="Ada Operator",
        email="ada@example.com",
        access_role="Operator",
        issued_at_ns=now_ns,
        last_activity_ns=now_ns,
        absolute_expires_ns=now_ns + 28_800 * SECONDS,
        csrf_hash=_hash("csrf-token"),
        revoked=False,
    )


def test_migrations_are_discovered_in_numeric_order() -> None:
    versions = [version for version, _ in discover_migrations()]

    # 003 is ticket 03's Operator Audit Event schema.
    assert versions == [1, 2, 3]
    assert ADMIN_STORE_SCHEMA_VERSION == 3


def test_auth_tables_exist_in_the_management_database(
    tmp_path: Path,
) -> None:
    admin, auth = _open(tmp_path / "admin.db")
    try:
        with admin.transaction() as connection:
            names = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
    finally:
        auth.close()
        admin.close()

    assert {
        "auth_transactions",
        "admin_sessions",
        "admin_security_events",
    } <= names


def test_login_transaction_is_one_shot_and_keyed_by_the_state_hash(
    tmp_path: Path,
) -> None:
    admin, auth = _open(tmp_path / "admin.db")
    try:
        transaction = _transaction()
        auth.create_transaction(transaction)

        with admin.transaction() as connection:
            raw_columns = connection.execute(
                "SELECT state_hash FROM auth_transactions"
            ).fetchall()
        assert raw_columns == [(transaction.state_hash,)]

        consumed = auth.consume_transaction(
            transaction.state_hash, now_ns=1_000 * SECONDS
        )
        assert consumed == transaction

        assert (
            auth.consume_transaction(
                transaction.state_hash, now_ns=1_000 * SECONDS
            )
            is None
        )
    finally:
        auth.close()
        admin.close()


def test_expired_login_transactions_are_rejected_and_purged(
    tmp_path: Path,
) -> None:
    admin, auth = _open(tmp_path / "admin.db")
    try:
        transaction = _transaction()
        auth.create_transaction(transaction)

        assert (
            auth.consume_transaction(
                transaction.state_hash,
                now_ns=transaction.expires_at_ns,
            )
            is None
        )

        auth.create_transaction(_transaction())
        assert (
            auth.purge_expired_transactions(
                now_ns=1_000 * SECONDS + 301 * SECONDS
            )
            == 1
        )
    finally:
        auth.close()
        admin.close()


def test_session_record_stores_hashes_not_bearer_values(
    tmp_path: Path,
) -> None:
    admin, auth = _open(tmp_path / "admin.db")
    try:
        record = _session()
        auth.insert_session(record)

        loaded = auth.find_session(record.session_hash)
        assert loaded == record

        with sqlite3.connect(tmp_path / "admin.db") as raw:
            columns = [
                row[1]
                for row in raw.execute("PRAGMA table_info(admin_sessions)")
            ]
            row = raw.execute(
                "SELECT * FROM admin_sessions WHERE session_hash = ?",
                (record.session_hash,),
            ).fetchone()
        assert "session_token" not in columns
        assert "csrf_token" not in columns
        assert record.session_hash in row
        assert "session-token" not in repr(row)
        assert "csrf-token" not in repr(row)
    finally:
        auth.close()
        admin.close()


def test_touch_rotate_csrf_and_revoke_are_persisted(
    tmp_path: Path,
) -> None:
    admin, auth = _open(tmp_path / "admin.db")
    try:
        record = _session()
        auth.insert_session(record)

        assert auth.touch_session(
            record.session_hash, last_activity_ns=1_200 * SECONDS
        )
        touched = auth.find_session(record.session_hash)
        assert touched is not None
        assert touched.last_activity_ns == 1_200 * SECONDS

        assert auth.rotate_csrf(
            record.session_hash, csrf_hash=_hash("new-csrf")
        )
        rotated = auth.find_session(record.session_hash)
        assert rotated is not None
        assert rotated.csrf_hash == _hash("new-csrf")

        assert auth.revoke_session(
            record.session_hash, reason="logout"
        )
        revoked = auth.find_session(record.session_hash)
        assert revoked is not None
        assert revoked.revoked is True

        # Revoking twice is idempotent, and unknown sessions report False.
        assert auth.revoke_session(record.session_hash, reason="logout")
        assert not auth.revoke_session("missing", reason="logout")
    finally:
        auth.close()
        admin.close()


def test_security_events_persist_without_secret_material(
    tmp_path: Path,
) -> None:
    admin, auth = _open(tmp_path / "admin.db")
    try:
        auth.record_security_event(
            SecurityEvent(
                kind="auth.role_conflict",
                reason="conflict",
                occurred_at_ns=1_000 * SECONDS,
                subject="oidc-subject-1",
                access_role=None,
                request_id="req-1",
                client_ip="127.0.0.1",
                detail={"claim": "groups"},
            )
        )
        auth.record_security_event(
            SecurityEvent(
                kind="session.invalid",
                reason="idle_timeout",
                occurred_at_ns=1_001 * SECONDS,
            )
        )

        events = auth.recent_security_events(limit=10)
    finally:
        auth.close()
        admin.close()

    assert [event.kind for event in events] == [
        "session.invalid",
        "auth.role_conflict",
    ]
    conflict = events[1]
    assert conflict.subject == "oidc-subject-1"
    assert conflict.reason == "conflict"
    assert conflict.request_id == "req-1"
    assert conflict.detail == {"claim": "groups"}
