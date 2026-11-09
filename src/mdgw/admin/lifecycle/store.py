# ruff: file-ignore[hardcoded-sql-expression]
"""SQLite persistence for the Service Instance registry and Operations.

The migration runner in ``mdgw.admin.db`` owns the schema; this module only
reads and writes rows in ``service_instances`` and ``service_operations``
(schema version 5). The Supervisor process is the lifecycle executor and is
the primary writer; the API process reads operations for its query routes and
checks idempotency before dispatching.

Operation rows are never deleted. Each transition updates the row's status and
timestamps so an accepted request can never be mistaken for a success: only
``finish_operation`` records a terminal success, and ``mark_interrupted``
turns stale accepted/running rows into ``unknown`` with a stable error code.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Self

from mdgw.admin.db import AdminStore, AdminStoreError
from mdgw.admin.lifecycle.models import (
    INSTANCE_ORDER,
    OPERATION_KINDS,
    OPERATION_STATUSES,
    SERVICE_ROLES,
    STATUS_ACCEPTED,
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_SUCCEEDED,
    STATUS_UNKNOWN,
    InstanceSnapshot,
    NewOperation,
    OperationQuery,
    OperationRecord,
)

if TYPE_CHECKING:
    from collections.abc import Generator
    from typing import Any

MIN_LIFECYCLE_SCHEMA_VERSION = 5
_SQLITE_BUSY_TIMEOUT_MS = 5_000

_OPERATION_COLUMNS = """
    sequence, operation_id, kind, target_instance_id, target_service_role,
    requested_by_subject, requested_by_display_name, requested_by_access_role,
    reason, status, requested_at_ns, started_at_ns, finished_at_ns,
    result_json, error_json, request_id, idempotency_key,
    idempotency_body_hash, idempotency_actor_subject,
    idempotency_expires_at_ns, updated_at_ns
"""

_INSTANCE_COLUMNS = """
    instance_id, service_role, display_name, desired_state, observed_state,
    ready, ready_reason, pid, started_at_ns, last_observed_at_ns,
    last_error_json, updated_at_ns
"""

_TERMINAL = frozenset({STATUS_SUCCEEDED, STATUS_FAILED, STATUS_UNKNOWN})
_INTERRUPTED_ERROR = {
    "code": "interrupted",
    "message": "the supervisor restarted before the operation completed",
}
_TERMINAL_MESSAGE = "operation is already terminal"


class LifecycleStoreError(AdminStoreError):
    """Base class for lifecycle persistence failures."""


class IdempotencyKeyReuseError(LifecycleStoreError):
    """The same key was used with a different request body."""


class LifecycleStore:
    """Thread-safe lifecycle registry and operation history."""

    def __init__(self, path: Path, *, migrate: bool = True) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if migrate:
            # Run the shared migration runner before opening our connection.
            AdminStore(self.path).close()
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

    # -- registry ----------------------------------------------------------

    def upsert_instance(self, snapshot: InstanceSnapshot) -> None:
        """Insert or replace one Supervisor observation."""
        _validate_instance(snapshot)
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO service_instances (
                    instance_id, service_role, display_name, desired_state,
                    observed_state, ready, ready_reason, pid, started_at_ns,
                    last_observed_at_ns, last_error_json, updated_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (instance_id) DO UPDATE SET
                    service_role = excluded.service_role,
                    display_name = excluded.display_name,
                    desired_state = excluded.desired_state,
                    observed_state = excluded.observed_state,
                    ready = excluded.ready,
                    ready_reason = excluded.ready_reason,
                    pid = excluded.pid,
                    started_at_ns = excluded.started_at_ns,
                    last_observed_at_ns = excluded.last_observed_at_ns,
                    last_error_json = excluded.last_error_json,
                    updated_at_ns = excluded.updated_at_ns
                """,
                _instance_parameters(snapshot),
            )

    def list_instances(self) -> list[InstanceSnapshot]:
        """Return the registry in dependency order (collector, api, replay)."""
        with self._lock:
            rows = self._connection().execute(
                f"SELECT {_INSTANCE_COLUMNS} FROM service_instances"
            ).fetchall()
        snapshots = [_instance_from_row(row) for row in rows]
        snapshots.sort(
            key=lambda item: (
                INSTANCE_ORDER.get(item.service_role, len(INSTANCE_ORDER)),
                item.instance_id,
            )
        )
        return snapshots

    def get_instance(self, instance_id: str) -> InstanceSnapshot | None:
        with self._lock:
            row = self._connection().execute(
                f"SELECT {_INSTANCE_COLUMNS} FROM service_instances "
                "WHERE instance_id = ?",
                (instance_id,),
            ).fetchone()
        return None if row is None else _instance_from_row(row)

    # -- operations --------------------------------------------------------

    def create_operation(self, operation: NewOperation) -> OperationRecord:
        """Persist a new ``accepted`` operation, or replay an idempotent one.

        A repeated ``(idempotency_key, actor_subject)`` with the same body hash
        returns the original row unchanged. A different body raises
        ``IdempotencyKeyReuseError`` so the API can answer ``409`` without
        executing anything.
        """
        _validate_operation(operation)
        with self.transaction() as connection:
            if operation.idempotency_key is not None:
                existing = self._find_idempotent(
                    connection,
                    operation.idempotency_key,
                    operation.actor_subject,
                )
                if existing is not None:
                    if (
                        existing.idempotency_body_hash
                        == operation.idempotency_body_hash
                    ):
                        return existing
                    msg = (
                        "the idempotency key was already used with a "
                        "different request body"
                    )
                    raise IdempotencyKeyReuseError(msg)
            try:
                connection.execute(
                    """
                    INSERT INTO service_operations (
                        operation_id, kind, target_instance_id,
                        target_service_role, requested_by_subject,
                        requested_by_display_name, requested_by_access_role,
                        reason, status, requested_at_ns, started_at_ns,
                        finished_at_ns, result_json, error_json, request_id,
                        idempotency_key, idempotency_body_hash,
                        idempotency_actor_subject, idempotency_expires_at_ns,
                        updated_at_ns
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL,
                              NULL, NULL, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        operation.operation_id,
                        operation.kind,
                        operation.instance_id,
                        operation.service_role,
                        operation.actor_subject,
                        operation.actor_display_name,
                        operation.actor_access_role,
                        operation.reason,
                        STATUS_ACCEPTED,
                        operation.requested_at_ns,
                        operation.request_id,
                        operation.idempotency_key,
                        operation.idempotency_body_hash,
                        (
                            operation.actor_subject
                            if operation.idempotency_key is not None
                            else None
                        ),
                        operation.idempotency_expires_at_ns,
                        operation.requested_at_ns,
                    ),
                )
            except sqlite3.IntegrityError:
                # A concurrent request won the idempotency race; re-read it.
                if operation.idempotency_key is not None:
                    existing = self._find_idempotent(
                        connection,
                        operation.idempotency_key,
                        operation.actor_subject,
                    )
                    if (
                        existing is not None
                        and existing.idempotency_body_hash
                        == operation.idempotency_body_hash
                    ):
                        return existing
                    msg = (
                        "the idempotency key was already used with a "
                        "different request body"
                    )
                    raise IdempotencyKeyReuseError(msg) from None
                raise
            record = self._get_operation(connection, operation.operation_id)
            assert record is not None
            return record

    def start_operation(
        self, operation_id: str, *, now_ns: int
    ) -> OperationRecord:
        """Transition ``accepted -> running`` and record ``started_at``."""
        with self.transaction() as connection:
            record = self._require_operation(connection, operation_id)
            if record.terminal:
                raise ValueError(_TERMINAL_MESSAGE)
            if record.status != STATUS_ACCEPTED:
                msg = (
                    "operation can only start from accepted, not "
                    f"{record.status}"
                )
                raise ValueError(msg)
            connection.execute(
                "UPDATE service_operations SET status = ?, started_at_ns = ?, "
                "updated_at_ns = ? WHERE sequence = ?",
                (STATUS_RUNNING, int(now_ns), int(now_ns), record.sequence),
            )
            updated = self._get_operation(connection, operation_id)
            assert updated is not None
            return updated

    def finish_operation(  # explicit terminal fields
        self,
        operation_id: str,
        *,
        status: str,
        now_ns: int,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
    ) -> OperationRecord:
        """Record a terminal ``succeeded``/``failed``/``unknown`` transition."""
        if status not in _TERMINAL:
            msg = f"finish status must be terminal, not {status!r}"
            raise ValueError(msg)
        if status == STATUS_FAILED and error is None:
            msg = "a failed operation requires an error object"
            raise ValueError(msg)
        with self.transaction() as connection:
            record = self._require_operation(connection, operation_id)
            if record.terminal:
                raise ValueError(_TERMINAL_MESSAGE)
            connection.execute(
                """
                UPDATE service_operations SET status = ?, finished_at_ns = ?,
                    result_json = ?, error_json = ?, updated_at_ns = ?
                WHERE sequence = ?
                """,
                (
                    status,
                    int(now_ns),
                    _json_or_none(result),
                    _json_or_none(error),
                    int(now_ns),
                    record.sequence,
                ),
            )
            updated = self._get_operation(connection, operation_id)
            assert updated is not None
            return updated

    def mark_interrupted(self, *, now_ns: int) -> int:
        """Turn stale accepted/running rows into ``unknown`` (never success).

        Called when a new Supervisor process starts: it is the only executor,
        so any operation left accepted/running by a previous process cannot
        have completed and must not be reconciled as success.
        """
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE service_operations
                SET status = ?, finished_at_ns = ?, error_json = ?,
                    updated_at_ns = ?
                WHERE status IN (?, ?)
                """,
                (
                    STATUS_UNKNOWN,
                    int(now_ns),
                    json.dumps(_INTERRUPTED_ERROR, separators=(",", ":")),
                    int(now_ns),
                    STATUS_ACCEPTED,
                    STATUS_RUNNING,
                ),
            )
            return int(cursor.rowcount)

    def get_operation(self, operation_id: str) -> OperationRecord | None:
        with self._lock:
            return self._get_operation(self._connection(), operation_id)

    def find_idempotent(
        self, key: str, actor_subject: str
    ) -> OperationRecord | None:
        """Return the operation stored for one key+actor, if any."""
        with self._lock:
            return self._find_idempotent(
                self._connection(), key, actor_subject
            )

    def query_operations(self, query: OperationQuery) -> list[OperationRecord]:
        """Return one ordered keyset page (newest first, at most ``limit``)."""
        if query.limit < 1:
            msg = "operation query limit must be positive"
            raise ValueError(msg)
        if query.kind is not None and query.kind not in OPERATION_KINDS:
            msg = f"unknown operation kind: {query.kind!r}"
            raise ValueError(msg)
        if query.status is not None and query.status not in OPERATION_STATUSES:
            msg = f"unknown operation status: {query.status!r}"
            raise ValueError(msg)

        clauses: list[str] = []
        parameters: list[Any] = []
        if query.instance_id is not None:
            clauses.append("target_instance_id = ?")
            parameters.append(query.instance_id)
        if query.kind is not None:
            clauses.append("kind = ?")
            parameters.append(query.kind)
        if query.status is not None:
            clauses.append("status = ?")
            parameters.append(query.status)
        if query.from_ns is not None:
            clauses.append("requested_at_ns >= ?")
            parameters.append(int(query.from_ns))
        if query.to_ns is not None:
            clauses.append("requested_at_ns < ?")
            parameters.append(int(query.to_ns))
        if query.before_sequence is not None:
            clauses.append("sequence < ?")
            parameters.append(int(query.before_sequence))
        where = "" if not clauses else " WHERE " + " AND ".join(clauses)
        with self._lock:
            rows = self._connection().execute(
                f"SELECT {_OPERATION_COLUMNS} FROM service_operations"
                f"{where} ORDER BY sequence DESC LIMIT ?",
                (*parameters, int(query.limit)),
            ).fetchall()
        return [_operation_from_row(row) for row in rows]

    # -- lifecycle ---------------------------------------------------------

    @property
    def is_open(self) -> bool:
        return self._conn is not None

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
        if version < MIN_LIFECYCLE_SCHEMA_VERSION:
            msg = (
                "lifecycle store requires admin schema version "
                f"{MIN_LIFECYCLE_SCHEMA_VERSION}, found {version}"
            )
            raise LifecycleStoreError(msg)

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            msg = "LifecycleStore is closed"
            raise sqlite3.ProgrammingError(msg)
        return self._conn

    @staticmethod
    def _get_operation(
        connection: sqlite3.Connection, operation_id: str
    ) -> OperationRecord | None:
        row = connection.execute(
            f"SELECT {_OPERATION_COLUMNS} FROM service_operations "
            "WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        return None if row is None else _operation_from_row(row)

    def _require_operation(
        self, connection: sqlite3.Connection, operation_id: str
    ) -> OperationRecord:
        record = self._get_operation(connection, operation_id)
        if record is None:
            msg = f"operation not found: {operation_id!r}"
            raise LifecycleStoreError(msg)
        return record

    @staticmethod
    def _find_idempotent(
        connection: sqlite3.Connection,
        key: str,
        actor_subject: str,
    ) -> OperationRecord | None:
        row = connection.execute(
            f"SELECT {_OPERATION_COLUMNS} FROM service_operations "
            "WHERE idempotency_key = ? AND idempotency_actor_subject = ?",
            (key, actor_subject),
        ).fetchone()
        return None if row is None else _operation_from_row(row)


def _validate_instance(snapshot: InstanceSnapshot) -> None:
    if snapshot.service_role not in SERVICE_ROLES:
        msg = f"unknown Service Role: {snapshot.service_role!r}"
        raise ValueError(msg)
    if not snapshot.instance_id:
        msg = "instance_id must be a non-empty string"
        raise ValueError(msg)


def _validate_operation(operation: NewOperation) -> None:
    if operation.kind not in OPERATION_KINDS:
        msg = f"unknown operation kind: {operation.kind!r}"
        raise ValueError(msg)
    if operation.service_role not in SERVICE_ROLES:
        msg = f"unknown Service Role: {operation.service_role!r}"
        raise ValueError(msg)
    if not operation.operation_id or not operation.actor_subject:
        msg = "operation_id and actor subject must be non-empty"
        raise ValueError(msg)
    if not operation.reason.strip():
        msg = "reason must be a non-empty string"
        raise ValueError(msg)


def _instance_parameters(snapshot: InstanceSnapshot) -> tuple[Any, ...]:
    return (
        snapshot.instance_id,
        snapshot.service_role,
        snapshot.display_name,
        snapshot.desired_state,
        snapshot.observed_state,
        1 if snapshot.ready else 0,
        snapshot.ready_reason,
        snapshot.pid,
        snapshot.started_at_ns,
        snapshot.last_observed_at_ns,
        _json_or_none(snapshot.last_error),
        snapshot.updated_at_ns,
    )


def _operation_from_row(row: sqlite3.Row) -> OperationRecord:
    return OperationRecord(
        sequence=int(row["sequence"]),
        operation_id=str(row["operation_id"]),
        kind=str(row["kind"]),
        instance_id=str(row["target_instance_id"]),
        service_role=str(row["target_service_role"]),
        actor_subject=str(row["requested_by_subject"]),
        actor_display_name=str(row["requested_by_display_name"]),
        actor_access_role=(
            None
            if row["requested_by_access_role"] is None
            else str(row["requested_by_access_role"])
        ),
        reason=str(row["reason"]),
        status=str(row["status"]),
        requested_at_ns=int(row["requested_at_ns"]),
        started_at_ns=_int_or_none(row["started_at_ns"]),
        finished_at_ns=_int_or_none(row["finished_at_ns"]),
        result=_json_value(row["result_json"]),
        error=_json_value(row["error_json"]),
        request_id=str(row["request_id"]),
        idempotency_key=(
            None
            if row["idempotency_key"] is None
            else str(row["idempotency_key"])
        ),
        idempotency_body_hash=(
            None
            if row["idempotency_body_hash"] is None
            else str(row["idempotency_body_hash"])
        ),
        idempotency_expires_at_ns=_int_or_none(
            row["idempotency_expires_at_ns"]
        ),
        updated_at_ns=int(row["updated_at_ns"]),
    )


def _instance_from_row(row: sqlite3.Row) -> InstanceSnapshot:
    return InstanceSnapshot(
        instance_id=str(row["instance_id"]),
        service_role=str(row["service_role"]),
        display_name=str(row["display_name"]),
        desired_state=str(row["desired_state"]),
        observed_state=str(row["observed_state"]),
        ready=bool(row["ready"]),
        ready_reason=(
            None if row["ready_reason"] is None else str(row["ready_reason"])
        ),
        pid=None if row["pid"] is None else int(row["pid"]),
        started_at_ns=_int_or_none(row["started_at_ns"]),
        last_observed_at_ns=_int_or_none(row["last_observed_at_ns"]),
        last_error=_json_value(row["last_error_json"]),
        updated_at_ns=int(row["updated_at_ns"]),
    )


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    return json.loads(value)


def _int_or_none(value: Any) -> int | None:
    return None if value is None else int(value)


__all__ = [
    "MIN_LIFECYCLE_SCHEMA_VERSION",
    "IdempotencyKeyReuseError",
    "LifecycleStore",
    "LifecycleStoreError",
]
