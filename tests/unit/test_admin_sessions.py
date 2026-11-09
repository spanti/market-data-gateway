# ruff: file-ignore[implicit-namespace-package]
"""Opaque server session semantics: 30 min idle / 8 h absolute, CSRF rotation.

The clock is injected so expiry boundaries are exact and no test sleeps.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from mdgw.admin.auth.sessions import (
    InvalidReturnToError,
    IssuedSession,
    SessionAuthError,
    SessionManager,
    validate_return_to,
)
from mdgw.admin.auth.store import AuthStore
from mdgw.admin.db import AdminStore
from mdgw.config import AdminSessionConfig

NS = 1_000_000_000
NOW_NS = 1_800_000_000 * NS


class Clock:
    def __init__(self, now_ns: int = NOW_NS) -> None:
        self.now_ns = now_ns

    def __call__(self) -> int:
        return self.now_ns


def _manager(
    tmp_path: Path, *, clock: Clock | None = None
) -> tuple[AdminStore, AuthStore, SessionManager]:
    admin = AdminStore(tmp_path / "admin.db")
    store = AuthStore(tmp_path / "admin.db")
    manager = SessionManager(
        store,
        AdminSessionConfig(),
        clock=clock or Clock(),
    )
    return admin, store, manager


def _create(manager: SessionManager) -> IssuedSession:
    return manager.create(
        subject="oidc-subject-1",
        display_name="Ada Operator",
        email="ada@example.com",
        access_role="Operator",
    )


def test_issued_session_carries_opaque_token_and_fresh_csrf(
    tmp_path: Path,
) -> None:
    admin, store, manager = _manager(tmp_path)
    try:
        issued = _create(manager)
    finally:
        store.close()
        admin.close()

    assert len(issued.token) >= 43
    assert len(issued.csrf_token) >= 43
    assert issued.record.session_hash == hashlib.sha256(
        issued.token.encode("utf-8")
    ).hexdigest()
    assert issued.record.csrf_hash == hashlib.sha256(
        issued.csrf_token.encode("utf-8")
    ).hexdigest()
    assert issued.record.access_role == "Operator"
    assert issued.record.issued_at_ns == NOW_NS
    assert issued.record.last_activity_ns == NOW_NS
    assert (
        issued.record.absolute_expires_ns
        == NOW_NS + 28_800 * NS
    )


def test_each_login_issues_a_new_session_id(tmp_path: Path) -> None:
    admin, store, manager = _manager(tmp_path)
    try:
        first = _create(manager)
        second = _create(manager)
    finally:
        store.close()
        admin.close()

    assert first.token != second.token
    assert first.record.session_hash != second.record.session_hash


def test_session_resolves_until_the_idle_boundary(tmp_path: Path) -> None:
    clock = Clock()
    admin, store, manager = _manager(tmp_path, clock=clock)
    try:
        issued = _create(manager)

        clock.now_ns = NOW_NS + 1_800 * NS - 1
        assert manager.resolve(issued.token).session_hash == (
            issued.record.session_hash
        )

        clock.now_ns = NOW_NS + 1_800 * NS
        with pytest.raises(SessionAuthError) as captured:
            manager.resolve(issued.token)
        assert captured.value.reason == "idle_timeout"

        # The expired session is revoked server-side and never revives.
        clock.now_ns = NOW_NS + 100 * NS
        with pytest.raises(SessionAuthError) as captured:
            manager.resolve(issued.token)
        assert captured.value.reason == "invalid"
    finally:
        store.close()
        admin.close()


def test_absolute_timeout_wins_even_with_recent_activity(
    tmp_path: Path,
) -> None:
    clock = Clock()
    admin, store, manager = _manager(tmp_path, clock=clock)
    try:
        issued = _create(manager)
        # Stay active well inside the idle window until the 8-hour mark.
        for _ in range(28):
            clock.now_ns += 1_000 * NS
            manager.refresh(issued.token)

        clock.now_ns = NOW_NS + 28_799 * NS
        manager.refresh(issued.token)

        clock.now_ns = NOW_NS + 28_800 * NS
        with pytest.raises(SessionAuthError) as captured:
            manager.resolve(issued.token)
        assert captured.value.reason == "absolute_timeout"
    finally:
        store.close()
        admin.close()


def test_refresh_extends_idle_only_and_rotates_csrf(tmp_path: Path) -> None:
    clock = Clock()
    admin, store, manager = _manager(tmp_path, clock=clock)
    try:
        issued = _create(manager)
        clock.now_ns = NOW_NS + 1_000 * NS

        refreshed, csrf_token = manager.refresh(issued.token)

        assert refreshed.last_activity_ns == clock.now_ns
        assert refreshed.issued_at_ns == NOW_NS
        assert refreshed.absolute_expires_ns == NOW_NS + 28_800 * NS
        assert csrf_token != issued.csrf_token
        assert refreshed.csrf_hash == hashlib.sha256(
            csrf_token.encode("utf-8")
        ).hexdigest()
        assert not manager.csrf_matches(refreshed, issued.csrf_token)
        assert manager.csrf_matches(refreshed, csrf_token)
    finally:
        store.close()
        admin.close()


def test_idle_expiry_is_capped_by_the_absolute_expiry(tmp_path: Path) -> None:
    clock = Clock()
    admin, store, manager = _manager(tmp_path, clock=clock)
    try:
        issued = _create(manager)
        for _ in range(28):
            clock.now_ns += 1_000 * NS
            manager.refresh(issued.token)
        clock.now_ns = NOW_NS + 28_700 * NS

        refreshed, _ = manager.refresh(issued.token)

        assert manager.idle_expiry_ns(refreshed) == (
            refreshed.absolute_expires_ns
        )
    finally:
        store.close()
        admin.close()


@pytest.mark.parametrize("token", [None, "", "not-a-session"])
def test_missing_and_unknown_tokens_are_rejected(
    tmp_path: Path, token: str | None
) -> None:
    admin, store, manager = _manager(tmp_path)
    try:
        with pytest.raises(SessionAuthError) as captured:
            manager.resolve(token)
    finally:
        store.close()
        admin.close()

    assert captured.value.reason in {"missing", "invalid"}


def test_probe_returns_none_instead_of_raising(tmp_path: Path) -> None:
    clock = Clock()
    admin, store, manager = _manager(tmp_path, clock=clock)
    try:
        issued = _create(manager)
        assert manager.probe(issued.token) is not None
        assert manager.probe("unknown") is None
        assert manager.probe(None) is None

        clock.now_ns = NOW_NS + 1_800 * NS
        assert manager.probe(issued.token) is None
    finally:
        store.close()
        admin.close()


def test_logout_revokes_the_server_side_session(tmp_path: Path) -> None:
    admin, store, manager = _manager(tmp_path)
    try:
        issued = _create(manager)
        manager.revoke(issued.token)

        with pytest.raises(SessionAuthError) as captured:
            manager.resolve(issued.token)
        assert captured.value.reason == "invalid"

        # Repeating logout for an already-revoked token is harmless.
        manager.revoke(issued.token)
    finally:
        store.close()
        admin.close()


def test_csrf_comparison_is_exact(tmp_path: Path) -> None:
    admin, store, manager = _manager(tmp_path)
    try:
        issued = _create(manager)
        record = manager.resolve(issued.token)
    finally:
        store.close()
        admin.close()

    assert manager.csrf_matches(record, issued.csrf_token)
    assert not manager.csrf_matches(record, "wrong")
    assert not manager.csrf_matches(record, None)
    assert not manager.csrf_matches(record, "")


def test_creating_a_session_purges_expired_records(tmp_path: Path) -> None:
    clock = Clock()
    admin, store, manager = _manager(tmp_path, clock=clock)
    try:
        first = _create(manager)
        clock.now_ns = NOW_NS + 28_800 * NS + 1
        _create(manager)
        assert store.find_session(first.record.session_hash) is None
    finally:
        store.close()
        admin.close()


# -- return_to validation ---------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "/monitoring",
        "/security?tab=audit",
        "/",
    ],
)
def test_validate_return_to_accepts_relative_paths(value: str) -> None:
    assert validate_return_to(value, "/") == value


@pytest.mark.parametrize(
    "value",
    [
        "https://evil.example/steal",
        "//evil.example/steal",
        "javascript:alert(1)",
        "/monitoring\u0000",
        "/monitoring\n",
        "/monitoring\\evil",
        "",
        "   ",
        "monitoring",
    ],
)
def test_validate_return_to_rejects_unsafe_paths(value: str) -> None:
    with pytest.raises(InvalidReturnToError):
        validate_return_to(value, "/monitoring")


def test_validate_return_to_uses_the_configured_default_when_missing() -> None:
    assert validate_return_to(None, "/monitoring") == "/monitoring"
