# ruff: file-ignore[implicit-namespace-package]
"""Lifecycle persistence: instance registry, operations and idempotency.

The migration, every operation transition, keyset ordering and the
"interrupted is never success" recovery rule are pinned here against the
public ``LifecycleStore`` methods (never private SQL).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from mdgw.admin.db import AdminStore
from mdgw.admin.lifecycle.models import (
    InstanceSnapshot,
    NewOperation,
    OperationQuery,
)
from mdgw.admin.lifecycle.store import (
    IdempotencyKeyReuseError,
    LifecycleStore,
)

if TYPE_CHECKING:
    from pathlib import Path

SECOND = 1_000_000_000
BASE_NS = 1_700_000_000 * SECOND


def _open(tmp_path: Path) -> LifecycleStore:
    path = tmp_path / "admin" / "admin.db"
    # The admin migration runner is the only schema owner.
    AdminStore(path).close()
    return LifecycleStore(path)


def _snapshot(  # explicit registry fields
    instance_id: str,
    *,
    observed_state: str = "running",
    ready: bool = True,
    pid: int = 100,
    now_ns: int = BASE_NS,
) -> InstanceSnapshot:
    return InstanceSnapshot(
        instance_id=instance_id,
        service_role=instance_id,
        display_name=instance_id,
        desired_state=(
            "running" if observed_state != "stopped" else "stopped"
        ),
        observed_state=observed_state,
        ready=ready,
        ready_reason=f"{instance_id} ready",
        pid=None if observed_state == "stopped" else pid,
        started_at_ns=None if observed_state == "stopped" else now_ns,
        last_observed_at_ns=now_ns,
        last_error=None,
        updated_at_ns=now_ns,
    )


def _new_operation(  # ruff: ignore[too-many-arguments]  # explicit operation fields
    operation_id: str = "op_one",
    *,
    kind: str = "service_instance.stop",
    instance_id: str = "api",
    requested_at_ns: int = BASE_NS,
    reason: str = "operator request",
    idempotency_key: str | None = None,
    idempotency_body_hash: str | None = None,
) -> NewOperation:
    return NewOperation(
        operation_id=operation_id,
        kind=kind,
        instance_id=instance_id,
        service_role=instance_id,
        actor_subject="subject-superadmin",
        actor_display_name="Super Admin",
        actor_access_role="SuperAdmin",
        reason=reason,
        request_id="req_1",
        requested_at_ns=requested_at_ns,
        idempotency_key=idempotency_key,
        idempotency_body_hash=idempotency_body_hash,
        idempotency_expires_at_ns=(
            None
            if idempotency_key is None
            else requested_at_ns + 86_400 * SECOND
        ),
    )


def test_migration_creates_the_lifecycle_schema(tmp_path: Path) -> None:
    path = tmp_path / "admin" / "admin.db"
    admin = AdminStore(path)
    # The shared schema also carries later migrations (008 is ticket 08's
    # Export Job schema); the lifecycle migration (005) must have run.
    assert admin.schema_version >= 5
    admin.close()

    store = LifecycleStore(path)
    try:
        assert store.list_instances() == []
        assert store.get_operation("op_missing") is None
    finally:
        store.close()


def test_instance_snapshots_round_trip_in_dependency_order(
    tmp_path: Path,
) -> None:
    store = _open(tmp_path)
    try:
        store.upsert_instance(_snapshot("replay", now_ns=BASE_NS + 3))
        store.upsert_instance(_snapshot("api", now_ns=BASE_NS + 2))
        store.upsert_instance(_snapshot("collector", now_ns=BASE_NS + 1))

        items = store.list_instances()
        assert [item.instance_id for item in items] == [
            "collector",
            "api",
            "replay",
        ]
        wire = items[0].to_wire()
        assert set(wire) == {
            "instance_id",
            "service_role",
            "display_name",
            "desired_state",
            "observed_state",
            "health",
            "ready",
            "ready_reason",
            "pid",
            "started_at",
            "last_observed_at",
            "last_error",
            "available_actions",
        }
        assert wire["started_at"] == "2023-11-14T22:13:20.000000001Z"
        assert wire["available_actions"] == ["stop", "reload"]

        stopped = _snapshot("api", observed_state="stopped")
        store.upsert_instance(stopped)
        assert store.get_instance("api").to_wire()["available_actions"] == [
            "start"
        ]
    finally:
        store.close()


def test_operations_persist_every_transition(tmp_path: Path) -> None:
    store = _open(tmp_path)
    try:
        created = store.create_operation(_new_operation())
        assert created.status == "accepted"
        assert created.started_at_ns is None
        assert created.finished_at_ns is None
        assert created.to_wire()["status"] == "accepted"

        running = store.start_operation(
            "op_one", now_ns=BASE_NS + SECOND
        )
        assert running.status == "running"
        assert running.started_at_ns == BASE_NS + SECOND

        finished = store.finish_operation(
            "op_one",
            status="succeeded",
            now_ns=BASE_NS + 2 * SECOND,
            result={
                "observed_state": "stopped",
                "ready": False,
                "forced": False,
                "drained": True,
                "timed_out": False,
                "duration_s": 1.0,
            },
        )
        assert finished.status == "succeeded"
        assert finished.finished_at_ns == BASE_NS + 2 * SECOND
        wire = finished.to_wire()
        assert wire["result"]["drained"] is True
        assert wire["error"] is None
        assert wire["requested_at"] == "2023-11-14T22:13:20.000000000Z"
        assert wire["started_at"] == "2023-11-14T22:13:21.000000000Z"
        assert wire["finished_at"] == "2023-11-14T22:13:22.000000000Z"
        assert wire["target"] == {"instance_id": "api", "service_role": "api"}
        assert wire["requested_by"] == {
            "subject": "subject-superadmin",
            "display_name": "Super Admin",
            "access_role": "SuperAdmin",
        }

        store.create_operation(
            _new_operation("op_two", kind="service_instance.reload")
        )
        store.finish_operation(
            "op_two",
            status="failed",
            now_ns=BASE_NS + 3 * SECOND,
            error={
                "code": "config_invalid",
                "message": "configuration is invalid",
                "details": {"field": "server.port"},
            },
        )
        assert store.get_operation("op_two").to_wire()["error"]["code"] == (
            "config_invalid"
        )
    finally:
        store.close()


def test_terminal_operations_cannot_transition_again(
    tmp_path: Path,
) -> None:
    store = _open(tmp_path)
    try:
        store.create_operation(_new_operation())
        store.finish_operation(
            "op_one", status="succeeded", now_ns=BASE_NS + SECOND
        )
        with pytest.raises(ValueError, match="terminal"):
            store.start_operation("op_one", now_ns=BASE_NS + 2 * SECOND)
    finally:
        store.close()


def test_idempotency_replays_the_original_operation(tmp_path: Path) -> None:
    store = _open(tmp_path)
    try:
        first = store.create_operation(
            _new_operation(
                idempotency_key="key-1",
                idempotency_body_hash="hash-a",
            )
        )
        store.start_operation("op_one", now_ns=BASE_NS + SECOND)
        replays = [
            store.create_operation(
                _new_operation(
                    operation_id=f"op_other_{index}",
                    idempotency_key="key-1",
                    idempotency_body_hash="hash-a",
                )
            )
            for index in range(2)
        ]

        assert [item.operation_id for item in replays] == [
            first.operation_id,
            first.operation_id,
        ]
        assert replays[0].status == "running"

        with pytest.raises(IdempotencyKeyReuseError):
            store.create_operation(
                _new_operation(
                    operation_id="op_reuse",
                    idempotency_key="key-1",
                    idempotency_body_hash="hash-b",
                )
            )
    finally:
        store.close()


def test_operations_filter_and_page_with_an_opaque_keyset(
    tmp_path: Path,
) -> None:
    store = _open(tmp_path)
    try:
        kinds = [
            "service_instance.start",
            "service_instance.stop",
            "service_instance.reload",
            "service_instance.stop",
        ]
        for index, kind in enumerate(kinds):
            store.create_operation(
                _new_operation(
                    f"op_{index}",
                    kind=kind,
                    instance_id="collector" if index == 3 else "api",
                    requested_at_ns=BASE_NS + index * SECOND,
                )
            )
        store.finish_operation(
            "op_0", status="succeeded", now_ns=BASE_NS + 10 * SECOND
        )

        def ids(**filters: object) -> list[str]:
            rows = store.query_operations(
                OperationQuery(limit=10, **filters)
            )
            return [row.operation_id for row in rows]

        assert ids() == ["op_3", "op_2", "op_1", "op_0"]
        assert ids(kind="service_instance.stop") == ["op_3", "op_1"]
        assert ids(instance_id="api") == ["op_2", "op_1", "op_0"]
        assert ids(status="succeeded") == ["op_0"]
        assert ids(
            from_ns=BASE_NS + SECOND, to_ns=BASE_NS + 3 * SECOND
        ) == ["op_2", "op_1"]

        first_page = store.query_operations(OperationQuery(limit=2))
        assert [row.operation_id for row in first_page] == ["op_3", "op_2"]
        second_page = store.query_operations(
            OperationQuery(limit=2, before_sequence=first_page[-1].sequence)
        )
        assert [row.operation_id for row in second_page] == ["op_1", "op_0"]
    finally:
        store.close()


def test_interrupted_accepted_and_running_operations_become_unknown(
    tmp_path: Path,
) -> None:
    store = _open(tmp_path)
    try:
        store.create_operation(_new_operation("op_accepted"))
        store.create_operation(_new_operation("op_running"))
        store.start_operation("op_running", now_ns=BASE_NS + SECOND)
        store.create_operation(_new_operation("op_done"))
        store.finish_operation(
            "op_done", status="succeeded", now_ns=BASE_NS + SECOND
        )

        recovered = store.mark_interrupted(now_ns=BASE_NS + 5 * SECOND)

        assert recovered == 2
        for operation_id in ("op_accepted", "op_running"):
            operation = store.get_operation(operation_id)
            assert operation.status == "unknown"
            assert operation.error["code"] == "interrupted"
            assert operation.finished_at_ns == BASE_NS + 5 * SECOND
        assert store.get_operation("op_done").status == "succeeded"
        assert store.mark_interrupted(now_ns=BASE_NS + 6 * SECOND) == 0
    finally:
        store.close()
