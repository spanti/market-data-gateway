# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[multiple-with-statements]
"""Append-only Operator Audit Event storage, query and tamper detection.

The store is the persistence seam: the writer above it computes redaction and
the HTTP routes only read. These tests pin the parts the ticket is responsible
for: monotonic sequence, a verifiable SHA-256 chain, append-only triggers,
every documented query filter/cursor, and the honest threat model (an attacker
who controls the database can drop the triggers; the chain only makes that
tampering evident, never prevented).
"""

from __future__ import annotations

import shutil
import sqlite3
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from mdgw.admin.audit.models import (
    AuditActor,
    AuditDiff,
    AuditQuery,
    AuditTarget,
    HashChain,
    OperatorAuditEvent,
)
from mdgw.admin.audit.store import AuditStore
from mdgw.admin.db import MIGRATIONS_DIR, AdminStore, AdminStoreError

if TYPE_CHECKING:
    from pathlib import Path

SECOND = 1_000_000_000
BASE_NS = 1_700_000_000 * SECOND


def _migrated(path: Path) -> Path:
    """Create the management schema, then close the migration connection."""
    AdminStore(path).close()
    return path


def _prototype(  # ruff: ignore[too-many-arguments]  # explicit event fields
    *,
    event_id: str,
    occurred_at_ns: int,
    action: str = "feed_subscription.update",
    subject: str = "oidc-subject",
    display_name: str = "Ada Operator",
    access_role: str | None = "Operator",
    target_type: str = "feed_subscription",
    target_id: str = "fsub_1",
    target_display_name: str | None = "BTC 1m",
    result: str = "requested",
    failure_reason: str | None = None,
    reason: str | None = "add",
    request_id: str = "req_1",
    operation_id: str | None = None,
    job_id: str | None = None,
    instance_id: str | None = None,
    before: object = None,
    after: object = None,
    redacted_paths: tuple[str, ...] = (),
) -> OperatorAuditEvent:
    return OperatorAuditEvent(
        event_id=event_id,
        sequence=0,
        occurred_at_ns=occurred_at_ns,
        actor=AuditActor(
            subject=subject,
            display_name=display_name,
            access_role=access_role,
        ),
        action=action,
        target=AuditTarget(
            type=target_type,
            id=target_id,
            display_name=target_display_name,
        ),
        result=result,
        failure_reason=failure_reason,
        reason=reason,
        request_id=request_id,
        operation_id=operation_id,
        job_id=job_id,
        instance_id=instance_id,
        diff=AuditDiff(
            before=before, after=after, redacted_paths=redacted_paths
        ),
        hash_chain=HashChain("sha256", None, ""),
    )


def _append(
    store: AuditStore,
    index: int,
    **overrides: object,
) -> OperatorAuditEvent:
    fields: dict[str, object] = {
        "event_id": f"evt_{index}",
        "occurred_at_ns": BASE_NS + index * SECOND,
    }
    fields.update(overrides)
    return store.append(_prototype(**fields))


# -- append and chain -------------------------------------------------------


def test_append_assigns_monotonic_sequence_and_links_the_hash_chain(
    tmp_path: Path,
) -> None:
    with AuditStore(_migrated(tmp_path / "admin.db")) as store:
        first = _append(store, 1)
        second = _append(store, 2)
        third = _append(store, 3)
        verification = store.verify_chain()

    assert [first.sequence, second.sequence, third.sequence] == [1, 2, 3]
    assert len({first.event_id, second.event_id, third.event_id}) == 3
    assert first.hash_chain.previous_event_hash is None
    assert second.hash_chain.previous_event_hash == first.hash_chain.event_hash
    assert third.hash_chain.previous_event_hash == second.hash_chain.event_hash
    for event in (first, second, third):
        assert event.hash_chain.algorithm == "sha256"
        digest = event.hash_chain.event_hash
        assert len(digest) == 64
        assert digest == digest.lower()
        int(digest, 16)
    assert verification.ok is True
    assert verification.checked == 3
    assert verification.first_broken_sequence is None
    assert verification.reason is None


def test_event_round_trips_through_reopen_without_losing_diff(
    tmp_path: Path,
) -> None:
    path = _migrated(tmp_path / "admin.db")
    with AuditStore(path) as store:
        event = _append(
            store,
            1,
            operation_id="op_1",
            job_id="bf_1",
            instance_id="api",
            before={"enabled": False, "nested": {"keep": 1}},
            after={"enabled": True},
            redacted_paths=("/nested/token",),
        )
        expected_wire = event.to_wire()

    with AuditStore(path) as reopened:
        loaded = reopened.get("evt_1")
        assert loaded is not None
        assert loaded.to_wire() == expected_wire
        assert loaded.sequence == 1
        assert loaded.diff.before == {"enabled": False, "nested": {"keep": 1}}
        assert loaded.diff.after == {"enabled": True}
        assert loaded.diff.redacted_paths == ("/nested/token",)
        assert reopened.get("evt_missing") is None


def test_migration_003_upgrades_a_version_2_database_in_place(
    tmp_path: Path,
) -> None:
    """A deployed v2 admin DB gains the audit table without losing data."""
    legacy_migrations = tmp_path / "legacy-migrations"
    legacy_migrations.mkdir()
    for name in ("001_foundation.sql", "002_auth.sql"):
        shutil.copy(MIGRATIONS_DIR / name, legacy_migrations / name)
    path = tmp_path / "admin.db"

    with AdminStore(path, migrations_dir=legacy_migrations) as legacy:
        assert legacy.schema_version == 2
        with legacy.transaction() as connection:
            connection.execute(
                """
                INSERT INTO admin_sessions (
                    session_hash, subject, display_name, email, access_role,
                    issued_at_ns, last_activity_ns, absolute_expires_ns,
                    csrf_hash, revoked
                ) VALUES ('hash', 'legacy-subject', 'Legacy', NULL,
                          'Auditor', 1, 1, 2, 'csrf', 0)
                """
            )

    with AdminStore(path) as upgraded:
        # Later tickets add migrations; 003 must still have run.
        assert upgraded.schema_version >= 3
        with upgraded.transaction() as connection:
            subject = connection.execute(
                "SELECT subject FROM admin_sessions"
            ).fetchone()[0]

    with AuditStore(path) as store:
        assert store.verify_chain().ok is True
        assert _append(store, 1).sequence == 1

    assert subject == "legacy-subject"


def test_sequence_survives_reopen_and_continues_monotonically(
    tmp_path: Path,
) -> None:
    path = _migrated(tmp_path / "admin.db")
    with AuditStore(path) as store:
        _append(store, 1)
        _append(store, 2)

    with AuditStore(path) as reopened:
        third = _append(reopened, 3)

    assert third.sequence == 3


# -- query ------------------------------------------------------------------


def _seeded(path: Path) -> AuditStore:
    store = AuditStore(_migrated(path))
    _append(store, 0, action="auth.login", result="denied")
    _append(
        store,
        1,
        subject="auditor",
        access_role="Auditor",
        action="export_job.create",
        target_type="export_job",
        target_id="exp_1",
        result="succeeded",
        request_id="req_export",
        operation_id="op_export",
        job_id="exp_1",
        instance_id="api",
    )
    _append(
        store,
        2,
        subject="operator-2",
        action="backfill_job.create",
        target_type="backfill_job",
        target_id="bf_1",
        result="failed",
        failure_reason="control_unavailable",
        request_id="req_backfill",
        job_id="bf_1",
    )
    return store


def test_time_filter_is_from_inclusive_and_to_exclusive(
    tmp_path: Path,
) -> None:
    with _seeded(tmp_path / "admin.db") as store:
        selected = store.query(
            AuditQuery(
                from_ns=BASE_NS + SECOND,
                to_ns=BASE_NS + 2 * SECOND,
                limit=50,
            )
        )

    assert [event.sequence for event in selected] == [2]


@pytest.mark.parametrize(
    ("field", "value", "expected_sequences"),
    [
        ("actor_subject", "auditor", [2]),
        ("access_role", "Auditor", [2]),
        ("action", "export_job.create", [2]),
        ("target_type", "backfill_job", [3]),
        ("target_id", "bf_1", [3]),
        ("result", "denied", [1]),
        ("request_id", "req_backfill", [3]),
        ("operation_id", "op_export", [2]),
        ("job_id", "bf_1", [3]),
        ("instance_id", "api", [2]),
    ],
)
def test_every_documented_filter_is_exact(
    tmp_path: Path,
    field: str,
    value: str,
    expected_sequences: list[int],
) -> None:
    with _seeded(tmp_path / "admin.db") as store:
        selected = store.query(
            AuditQuery(**{field: value}, order="asc", limit=50)
        )

    assert [event.sequence for event in selected] == expected_sequences


def test_order_defaults_are_caller_controlled_and_cursor_is_keyset(
    tmp_path: Path,
) -> None:
    with _seeded(tmp_path / "admin.db") as store:
        descending = store.query(AuditQuery(order="desc", limit=2))
        ascending = store.query(AuditQuery(order="asc", limit=2))
        after_desc = store.query(
            AuditQuery(order="desc", after_sequence=2, limit=2)
        )
        after_asc = store.query(
            AuditQuery(order="asc", after_sequence=2, limit=2)
        )

    assert [event.sequence for event in descending] == [3, 2]
    assert [event.sequence for event in ascending] == [1, 2]
    assert [event.sequence for event in after_desc] == [1]
    assert [event.sequence for event in after_asc] == [3]


def test_invalid_order_is_rejected_before_touching_sql(
    tmp_path: Path,
) -> None:
    with _seeded(tmp_path / "admin.db") as store:
        with pytest.raises(ValueError, match="order"):
            store.query(AuditQuery(order="sideways", limit=1))


# -- append-only and tamper evidence ---------------------------------------


def test_update_and_delete_are_rejected_by_append_only_triggers(
    tmp_path: Path,
) -> None:
    with AuditStore(_migrated(tmp_path / "admin.db")) as store:
        event = _append(store, 1)

        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            with store.transaction() as connection:
                connection.execute(
                    "UPDATE operator_audit_events SET reason = 'rewritten' "
                    "WHERE event_id = ?",
                    (event.event_id,),
                )

        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            with store.transaction() as connection:
                connection.execute(
                    "DELETE FROM operator_audit_events WHERE event_id = ?",
                    (event.event_id,),
                )

    with AuditStore(tmp_path / "admin.db") as reopened:
        assert reopened.verify_chain().ok is True
        assert reopened.get(event.event_id) is not None


def test_hash_chain_detects_a_modified_row(tmp_path: Path) -> None:
    with AuditStore(_migrated(tmp_path / "admin.db")) as store:
        _append(store, 1)
        _append(store, 2)
        _append(store, 3)

        with store.transaction() as connection:
            # Simulate an attacker who controls the database file and can
            # remove the defence-in-depth triggers.
            connection.execute(
                "DROP TRIGGER operator_audit_events_append_only_update"
            )
            connection.execute(
                "UPDATE operator_audit_events SET reason = 'rewritten' "
                "WHERE sequence = 2"
            )

        verification = store.verify_chain()

    assert verification.ok is False
    assert verification.checked == 1
    assert verification.first_broken_sequence == 2
    assert verification.reason == "event_hash_mismatch"


def test_hash_chain_detects_a_deleted_middle_row(tmp_path: Path) -> None:
    with AuditStore(_migrated(tmp_path / "admin.db")) as store:
        _append(store, 1)
        _append(store, 2)
        _append(store, 3)

        with store.transaction() as connection:
            connection.execute(
                "DROP TRIGGER operator_audit_events_append_only_delete"
            )
            connection.execute(
                "DELETE FROM operator_audit_events WHERE sequence = 2"
            )

        verification = store.verify_chain()

    assert verification.ok is False
    assert verification.checked == 1
    assert verification.first_broken_sequence == 3
    assert verification.reason == "previous_hash_mismatch"


def test_empty_chain_is_valid(tmp_path: Path) -> None:
    with AuditStore(_migrated(tmp_path / "admin.db")) as store:
        verification = store.verify_chain()

    assert verification.ok is True
    assert verification.checked == 0


# -- schema guard -----------------------------------------------------------


def test_an_unmigrated_database_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "empty.db"
    sqlite3.connect(path).close()

    with pytest.raises(AdminStoreError, match="schema version 3"):
        AuditStore(path)


def test_store_is_closed_after_close(tmp_path: Path) -> None:
    store = AuditStore(_migrated(tmp_path / "admin.db"))
    store.close()

    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        store.get("evt_1")


def test_prototype_sequence_and_hash_are_overwritten(
    tmp_path: Path,
) -> None:
    with AuditStore(_migrated(tmp_path / "admin.db")) as store:
        prototype = _prototype(event_id="evt_1", occurred_at_ns=BASE_NS)
        prototype = replace(
            prototype,
            sequence=99,
            hash_chain=HashChain("sha256", "not-the-previous", "not-a-hash"),
        )

        appended = store.append(prototype)

    assert appended.sequence == 1
    assert appended.hash_chain.previous_event_hash is None
    assert appended.hash_chain.event_hash != "not-a-hash"
