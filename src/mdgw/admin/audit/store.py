# ruff: file-ignore[hardcoded-sql-expression]
"""Append-only SQLite storage for Operator Audit Events.

The management database is the only store; this module owns one connection to
it and never mutates the schema (the migration runner in ``mdgw.admin.db`` is
the sole owner of schema evolution). SQLite triggers reject UPDATE/DELETE, so
the append-only rule is enforced by the database as well as by the absence of
edit/delete methods.

The SHA-256 chain is computed inside the same write transaction that assigns
the monotonic ``sequence``, so concurrent appends cannot fork the chain. Each
hash covers the previous event hash plus the canonical JSON of the event
(``mdgw.admin.audit.canonical``). ``verify_chain`` reports the first broken
sequence; it deliberately does not claim immutability against an attacker who
can drop triggers and recompute hashes.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

from mdgw.admin.audit.canonical import (
    canonical_event_payload,
    compute_event_hash,
)
from mdgw.admin.audit.models import (
    HASH_ALGORITHM,
    AuditActor,
    AuditDiff,
    AuditQuery,
    AuditTarget,
    ChainVerification,
    HashChain,
    OperatorAuditEvent,
)
from mdgw.admin.db import AdminStoreError

if TYPE_CHECKING:
    from collections.abc import Generator

_SQLITE_BUSY_TIMEOUT_MS = 5_000
MIN_AUDIT_SCHEMA_VERSION = 3

_COLUMNS = """
    sequence, event_id, occurred_at_ns, actor_subject, actor_display_name,
    actor_access_role, action, target_type, target_id, target_display_name,
    result, failure_reason, reason, request_id, operation_id, job_id,
    instance_id, diff_before_json, diff_after_json, redacted_paths_json,
    hash_algorithm, previous_event_hash, event_hash
"""


class AuditStore:
    """Thread-safe append-only access to ``operator_audit_events``."""

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

    # -- append ------------------------------------------------------------

    def append(self, prototype: OperatorAuditEvent) -> OperatorAuditEvent:
        """Append one event, assigning its sequence and chain hashes.

        The prototype's own ``sequence``/``hash_chain`` are ignored: the store
        is the only authority for ordering and linkage.
        """
        with self.transaction() as connection:
            previous_row = connection.execute(
                "SELECT event_hash FROM operator_audit_events "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            previous_hash = (
                None if previous_row is None else str(previous_row["event_hash"])
            )
            next_row = connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence "
                "FROM operator_audit_events"
            ).fetchone()
            sequence = int(next_row["next_sequence"])
            chained = replace(
                prototype,
                sequence=sequence,
                hash_chain=HashChain(HASH_ALGORITHM, previous_hash, ""),
            )
            event_hash = compute_event_hash(
                previous_hash, canonical_event_payload(chained)
            )
            event = replace(
                chained,
                hash_chain=HashChain(
                    HASH_ALGORITHM, previous_hash, event_hash
                ),
            )
            connection.execute(
                f"""
                INSERT INTO operator_audit_events ({_COLUMNS})
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?)
                """,
                _insert_parameters(event),
            )
        return event

    # -- reads -------------------------------------------------------------

    def get(self, event_id: str) -> OperatorAuditEvent | None:
        """Return one event by opaque id, or None."""
        with self._lock:
            row = self._connection().execute(
                f"SELECT {_COLUMNS} FROM operator_audit_events "
                "WHERE event_id = ?",
                (event_id,),
            ).fetchone()
        return None if row is None else _event_from_row(row)

    def query(self, query: AuditQuery) -> list[OperatorAuditEvent]:
        """Return one ordered keyset page (at most ``query.limit`` rows)."""
        if query.order not in {"asc", "desc"}:
            msg = "audit order must be 'asc' or 'desc'"
            raise ValueError(msg)
        if query.limit < 1:
            msg = "audit query limit must be positive"
            raise ValueError(msg)

        clauses, parameters = _filters(query)
        direction = "DESC" if query.order == "desc" else "ASC"
        sql = (
            f"SELECT {_COLUMNS} FROM operator_audit_events "
            f"{_where(clauses)} ORDER BY sequence {direction} LIMIT ?"
        )
        with self._lock:
            rows = self._connection().execute(
                sql, (*parameters, query.limit)
            ).fetchall()
        return [_event_from_row(row) for row in rows]

    # -- tamper detection --------------------------------------------------

    def verify_chain(self) -> ChainVerification:
        """Walk the whole chain and report the first inconsistent event."""
        with self._lock:
            rows = self._connection().execute(
                f"SELECT {_COLUMNS} FROM operator_audit_events "
                "ORDER BY sequence ASC"
            ).fetchall()

        previous_hash: str | None = None
        previous_sequence: int | None = None
        for checked, row in enumerate(rows):
            event = _event_from_row(row)
            if (
                previous_sequence is not None
                and event.sequence <= previous_sequence
            ):
                return ChainVerification(
                    ok=False,
                    checked=checked,
                    first_broken_sequence=event.sequence,
                    reason="sequence_order",
                )
            if event.hash_chain.algorithm != HASH_ALGORITHM:
                return ChainVerification(
                    ok=False,
                    checked=checked,
                    first_broken_sequence=event.sequence,
                    reason="algorithm_mismatch",
                )
            if event.hash_chain.previous_event_hash != previous_hash:
                return ChainVerification(
                    ok=False,
                    checked=checked,
                    first_broken_sequence=event.sequence,
                    reason="previous_hash_mismatch",
                )
            recomputed = compute_event_hash(
                previous_hash, canonical_event_payload(event)
            )
            if recomputed != event.hash_chain.event_hash:
                return ChainVerification(
                    ok=False,
                    checked=checked,
                    first_broken_sequence=event.sequence,
                    reason="event_hash_mismatch",
                )
            previous_hash = event.hash_chain.event_hash
            previous_sequence = event.sequence

        return ChainVerification(ok=True, checked=len(rows), reason=None)

    # -- lifecycle ---------------------------------------------------------

    @property
    def is_open(self) -> bool:
        return self._conn is not None

    @contextlib.contextmanager
    def transaction(self) -> Generator[sqlite3.Connection]:
        """Run one explicit write transaction; roll back on any exception."""
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
        try:
            row = self._connection().execute(
                "SELECT version FROM schema_version WHERE id = 1"
            ).fetchone()
        except sqlite3.OperationalError:
            row = None
        version = 0 if row is None else int(row[0])
        if version < MIN_AUDIT_SCHEMA_VERSION:
            msg = (
                "operator audit tables require schema version "
                f"{MIN_AUDIT_SCHEMA_VERSION}, found {version}"
            )
            raise AdminStoreError(msg)

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            msg = "AuditStore is closed"
            raise sqlite3.ProgrammingError(msg)
        return self._conn


def _filters(query: AuditQuery) -> tuple[list[str], list[Any]]:
    clauses: list[str] = []
    parameters: list[Any] = []

    def equals(column: str, value: object) -> None:
        if value is None:
            return
        clauses.append(f"{column} = ?")
        parameters.append(value)

    if query.from_ns is not None:
        clauses.append("occurred_at_ns >= ?")
        parameters.append(query.from_ns)
    if query.to_ns is not None:
        clauses.append("occurred_at_ns < ?")
        parameters.append(query.to_ns)
    equals("actor_subject", query.actor_subject)
    equals("actor_access_role", query.access_role)
    equals("action", query.action)
    equals("target_type", query.target_type)
    equals("target_id", query.target_id)
    equals("result", query.result)
    equals("request_id", query.request_id)
    equals("operation_id", query.operation_id)
    equals("job_id", query.job_id)
    equals("instance_id", query.instance_id)
    if query.after_sequence is not None:
        operator = "<" if query.order == "desc" else ">"
        clauses.append(f"sequence {operator} ?")
        parameters.append(query.after_sequence)
    return clauses, parameters


def _where(clauses: list[str]) -> str:
    if not clauses:
        return ""
    return "WHERE " + " AND ".join(clauses)


def _insert_parameters(event: OperatorAuditEvent) -> tuple[Any, ...]:
    return (
        event.sequence,
        event.event_id,
        event.occurred_at_ns,
        event.actor.subject,
        event.actor.display_name,
        event.actor.access_role,
        event.action,
        event.target.type,
        event.target.id,
        event.target.display_name,
        event.result,
        event.failure_reason,
        event.reason,
        event.request_id,
        event.operation_id,
        event.job_id,
        event.instance_id,
        _json_or_none(event.diff.before),
        _json_or_none(event.diff.after),
        json.dumps(
            list(event.diff.redacted_paths),
            separators=(",", ":"),
            ensure_ascii=False,
        ),
        event.hash_chain.algorithm,
        event.hash_chain.previous_event_hash,
        event.hash_chain.event_hash,
    )


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _event_from_row(row: sqlite3.Row) -> OperatorAuditEvent:
    return OperatorAuditEvent(
        event_id=str(row["event_id"]),
        sequence=int(row["sequence"]),
        occurred_at_ns=int(row["occurred_at_ns"]),
        actor=AuditActor(
            subject=str(row["actor_subject"]),
            display_name=str(row["actor_display_name"]),
            access_role=(
                None
                if row["actor_access_role"] is None
                else str(row["actor_access_role"])
            ),
        ),
        action=str(row["action"]),
        target=AuditTarget(
            type=str(row["target_type"]),
            id=str(row["target_id"]),
            display_name=(
                None
                if row["target_display_name"] is None
                else str(row["target_display_name"])
            ),
        ),
        result=str(row["result"]),
        failure_reason=(
            None
            if row["failure_reason"] is None
            else str(row["failure_reason"])
        ),
        reason=None if row["reason"] is None else str(row["reason"]),
        request_id=str(row["request_id"]),
        operation_id=(
            None if row["operation_id"] is None else str(row["operation_id"])
        ),
        job_id=None if row["job_id"] is None else str(row["job_id"]),
        instance_id=(
            None if row["instance_id"] is None else str(row["instance_id"])
        ),
        diff=AuditDiff(
            before=_json_or_none_value(row["diff_before_json"]),
            after=_json_or_none_value(row["diff_after_json"]),
            redacted_paths=tuple(
                str(path)
                for path in json.loads(str(row["redacted_paths_json"]))
            ),
        ),
        hash_chain=HashChain(
            algorithm=str(row["hash_algorithm"]),
            previous_event_hash=(
                None
                if row["previous_event_hash"] is None
                else str(row["previous_event_hash"])
            ),
            event_hash=str(row["event_hash"]),
        ),
    )


def _json_or_none_value(value: object) -> Any:
    if value is None:
        return None
    return json.loads(str(value))


__all__ = [
    "MIN_AUDIT_SCHEMA_VERSION",
    "AuditStore",
]
