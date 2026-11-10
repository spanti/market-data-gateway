# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[hardcoded-password-string]
"""Audit writer/helper API consumed by later management-write tickets.

The requested event is the fail-closed gate: it is appended before the caller
executes anything, and an append failure raises ``AuditUnavailableError`` so
the action never runs. Terminal events are appended separately and share the
requested event's correlation ids. The security-event bridge turns the ticket
02 failure stream into denied audit events without ever leaking bearer
material or turning a denial into a server error.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from mdgw.admin.audit.models import (
    RESULT_DENIED,
    RESULT_FAILED,
    RESULT_REQUESTED,
    RESULT_SUCCEEDED,
    AuditActor,
    AuditQuery,
    AuditTarget,
)
from mdgw.admin.audit.store import AuditStore
from mdgw.admin.audit.writer import AuditWriter, actor_from_session
from mdgw.admin.auth.store import SecurityEvent, SessionRecord
from mdgw.admin.db import AdminStore
from mdgw.admin.errors import AuditUnavailableError

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

SECOND = 1_000_000_000
BASE_NS = 1_700_000_000 * SECOND

ACTOR = AuditActor(
    subject="oidc-subject-1",
    display_name="Ada Operator",
    access_role="Operator",
)
TARGET = AuditTarget(
    type="service_instance", id="api", display_name="api instance"
)


class Clock:
    def __init__(self, now_ns: int = BASE_NS) -> None:
        self.now_ns = now_ns

    def __call__(self) -> int:
        return self.now_ns

    def advance_s(self, seconds: int) -> None:
        self.now_ns += seconds * SECOND


def _open(
    tmp_path: Path,
) -> Generator[tuple[AuditWriter, AuditStore, Clock]]:
    path = tmp_path / "admin.db"
    AdminStore(path).close()
    store = AuditStore(path)
    clock = Clock()
    writer = AuditWriter(store, clock=clock)
    try:
        yield writer, store, clock
    finally:
        writer.close()


@pytest.fixture
def audit(tmp_path: Path) -> Generator[tuple[AuditWriter, AuditStore, Clock]]:
    yield from _open(tmp_path)


def _begin(writer: AuditWriter, **overrides: object):
    fields: dict[str, object] = {
        "actor": ACTOR,
        "action": "service_instance.start",
        "target": TARGET,
        "request_id": "req_1",
        "reason": "start api",
        "operation_id": "op_1",
        "instance_id": "api",
        "before": {"desired_state": "stopped"},
        "after": {"desired_state": "running"},
    }
    fields.update(overrides)
    return writer.begin(**fields)


def _events(store: AuditStore) -> list:
    return store.query(AuditQuery(order="asc", limit=50))


# -- requested / terminal lifecycle ----------------------------------------


def test_begin_appends_requested_then_terminal_appends_succeeded(
    audit: tuple[AuditWriter, AuditStore, Clock],
) -> None:
    writer, store, _clock = audit

    handle = _begin(writer)
    terminal = handle.succeeded(
        after={"desired_state": "running", "ready": True}
    )

    events = _events(store)
    assert [event.result for event in events] == [
        RESULT_REQUESTED,
        RESULT_SUCCEEDED,
    ]
    assert handle.event.event_id.startswith("evt_")
    assert handle.event.event_id != terminal.event_id
    assert terminal.sequence == handle.event.sequence + 1
    assert terminal.hash_chain.previous_event_hash == (
        handle.event.hash_chain.event_hash
    )
    assert terminal.action == handle.event.action
    assert terminal.actor == handle.event.actor
    assert terminal.target == handle.event.target
    assert terminal.request_id == "req_1"
    assert terminal.operation_id == "op_1"
    assert terminal.job_id is None
    assert terminal.instance_id == "api"
    assert terminal.failure_reason is None
    assert terminal.reason == "start api"
    assert terminal.diff.before is None
    assert terminal.diff.after == {"desired_state": "running", "ready": True}


def test_requested_event_carries_the_planned_diff_and_identity(
    audit: tuple[AuditWriter, AuditStore, Clock],
) -> None:
    writer, store, clock = audit

    _begin(writer)

    (requested,) = _events(store)
    assert requested.result == RESULT_REQUESTED
    assert requested.occurred_at_ns == clock.now_ns
    assert requested.actor == ACTOR
    assert requested.target == TARGET
    assert requested.reason == "start api"
    assert requested.diff.before == {"desired_state": "stopped"}
    assert requested.diff.after == {"desired_state": "running"}


def test_failed_terminal_records_the_stable_failure_reason(
    audit: tuple[AuditWriter, AuditStore, Clock],
) -> None:
    writer, store, _clock = audit
    handle = _begin(writer)

    terminal = handle.failed(
        failure_reason="control_unavailable",
        after={"desired_state": "stopped"},
    )

    assert terminal.result == RESULT_FAILED
    assert terminal.failure_reason == "control_unavailable"
    assert [event.result for event in _events(store)] == [
        RESULT_REQUESTED,
        RESULT_FAILED,
    ]


def test_secrets_are_redacted_before_the_event_is_stored(
    audit: tuple[AuditWriter, AuditStore, Clock],
) -> None:
    writer, store, _clock = audit
    secret = "client-secret-value"

    handle = _begin(
        writer,
        before={
            "enabled": False,
            "client_secret": secret,
            "headers": {"Authorization": "Bearer abc.def.ghi"},
        },
        after={"enabled": True},
    )

    requested = handle.event
    payload = json.dumps(requested.to_wire())
    assert secret not in payload
    assert "abc.def.ghi" not in payload
    assert requested.diff.before["client_secret"] == "[REDACTED]"
    assert requested.diff.before["headers"]["Authorization"] == "[REDACTED]"
    assert requested.diff.after == {"enabled": True}
    assert set(requested.diff.redacted_paths) == {
        "/client_secret",
        "/headers/Authorization",
    }
    assert _events(store)[0].to_wire() == requested.to_wire()


def test_event_ids_are_unique_and_prefixed(
    audit: tuple[AuditWriter, AuditStore, Clock],
) -> None:
    writer, _store, _clock = audit

    first = _begin(writer)
    second = _begin(writer)

    assert first.event.event_id.startswith("evt_")
    assert second.event.event_id.startswith("evt_")
    assert first.event.event_id != second.event.event_id


def test_clock_controls_requested_and_terminal_timestamps(
    audit: tuple[AuditWriter, AuditStore, Clock],
) -> None:
    writer, store, clock = audit
    handle = _begin(writer)
    clock.advance_s(4)
    handle.succeeded()

    requested, terminal = _events(store)
    assert requested.occurred_at_ns == BASE_NS
    assert terminal.occurred_at_ns == BASE_NS + 4 * SECOND


# -- fail-closed ------------------------------------------------------------


def test_audit_unavailable_blocks_the_action_before_execution(
    audit: tuple[AuditWriter, AuditStore, Clock],
) -> None:
    writer, _store, _clock = audit
    executed: list[str] = []
    writer.close()

    def high_risk_action() -> None:
        handle = _begin(writer)
        executed.append("ran")
        handle.succeeded()

    with pytest.raises(AuditUnavailableError) as caught:
        high_risk_action()

    error = caught.value
    assert executed == []
    assert error.code == "audit_unavailable"
    assert error.status == 503
    assert error.details == {
        "action": "service_instance.start",
        "target": {"type": "service_instance", "id": "api"},
        "phase": "requested",
        "executed": False,
    }


def test_terminal_append_failure_is_reported_as_executed_action(
    audit: tuple[AuditWriter, AuditStore, Clock],
) -> None:
    writer, store, _clock = audit
    handle = _begin(writer)
    store.close()

    with pytest.raises(AuditUnavailableError) as caught:
        handle.succeeded()

    assert caught.value.details["phase"] == "terminal"
    assert caught.value.details["executed"] is True


def test_invalid_result_request_id_and_action_are_programming_errors(
    audit: tuple[AuditWriter, AuditStore, Clock],
) -> None:
    writer, _store, _clock = audit

    with pytest.raises(ValueError, match="result"):
        writer.append(
            actor=ACTOR,
            action="service_instance.start",
            target=TARGET,
            result="maybe",
            request_id="req_1",
        )
    with pytest.raises(ValueError, match="request_id"):
        writer.append(
            actor=ACTOR,
            action="service_instance.start",
            target=TARGET,
            result=RESULT_SUCCEEDED,
            request_id="",
        )
    with pytest.raises(ValueError, match="action"):
        writer.append(
            actor=ACTOR,
            action="",
            target=TARGET,
            result=RESULT_SUCCEEDED,
            request_id="req_1",
        )


# -- security-event bridge --------------------------------------------------


def test_security_event_role_denial_becomes_a_denied_audit_event(
    audit: tuple[AuditWriter, AuditStore, Clock],
) -> None:
    writer, store, _clock = audit
    event = SecurityEvent(
        kind="authz.role_denied",
        reason="role",
        occurred_at_ns=BASE_NS + 3,
        subject="oidc-subject-1",
        access_role="Operator",
        request_id="req_denied",
        client_ip="127.0.0.1",
        detail={"required_role": ["SuperAdmin"]},
    )

    recorded = writer.record_security_event(event)

    assert recorded is not None
    assert recorded.action == "auth.role.denied"
    assert recorded.result == RESULT_DENIED
    assert recorded.failure_reason == "role"
    assert recorded.actor == AuditActor(
        subject="oidc-subject-1",
        display_name="oidc-subject-1",
        access_role="Operator",
    )
    assert recorded.target.type == "access_role"
    assert recorded.target.id == "Operator"
    assert recorded.occurred_at_ns == BASE_NS + 3
    assert recorded.request_id == "req_denied"
    assert recorded.diff.before is None
    assert recorded.diff.after == {"required_role": ["SuperAdmin"]}
    assert _events(store)[0].to_wire() == recorded.to_wire()


def test_security_event_login_failure_becomes_an_auth_login_denial(
    audit: tuple[AuditWriter, AuditStore, Clock],
) -> None:
    writer, _store, _clock = audit
    event = SecurityEvent(
        kind="auth.role_unmapped",
        reason="role_unmapped",
        occurred_at_ns=BASE_NS,
        request_id="req_login",
    )

    recorded = writer.record_security_event(event)

    assert recorded is not None
    assert recorded.action == "auth.login"
    assert recorded.result == RESULT_DENIED
    assert recorded.actor.subject == "anonymous"
    assert recorded.actor.display_name == "anonymous"
    assert recorded.actor.access_role is None
    assert recorded.target.type == "session"
    assert recorded.target.id == "anonymous"


def test_security_event_detail_is_redacted_and_never_leaks_tokens(
    audit: tuple[AuditWriter, AuditStore, Clock],
) -> None:
    writer, store, _clock = audit
    secret = "sensitive-token-value"

    recorded = writer.record_security_event(
        SecurityEvent(
            kind="authz.csrf_denied",
            reason="csrf",
            occurred_at_ns=BASE_NS,
            subject="subject",
            access_role="Auditor",
            request_id="req_csrf",
            detail={
                "csrf_token": secret,
                "cookie": f"mdgw_session={secret}",
                "safe": "keep",
            },
        )
    )

    assert recorded is not None
    payload = json.dumps(
        next(iter(store.query(AuditQuery(limit=10)))).to_wire()
    )
    assert secret not in payload
    assert recorded.diff.after == {
        "csrf_token": "[REDACTED]",
        "cookie": "[REDACTED]",
        "safe": "keep",
    }
    assert set(recorded.diff.redacted_paths) == {"/csrf_token", "/cookie"}


def test_actor_from_session_snapshots_the_server_side_role() -> None:
    record = SessionRecord(
        session_hash="hash",
        subject="oidc-subject-1",
        display_name="Ada Operator",
        email="ada@example.com",
        access_role="Operator",
        issued_at_ns=BASE_NS,
        last_activity_ns=BASE_NS,
        absolute_expires_ns=BASE_NS + 28_800 * SECOND,
        csrf_hash="csrf-hash",
    )

    assert actor_from_session(record) == AuditActor(
        subject="oidc-subject-1",
        display_name="Ada Operator",
        access_role="Operator",
    )


def test_security_event_bridge_failure_is_swallowed(
    audit: tuple[AuditWriter, AuditStore, Clock],
) -> None:
    writer, _store, _clock = audit
    writer.close()

    recorded = writer.record_security_event(
        SecurityEvent(
            kind="authz.role_denied",
            reason="role",
            occurred_at_ns=BASE_NS,
            request_id="req_1",
        )
    )

    assert recorded is None
