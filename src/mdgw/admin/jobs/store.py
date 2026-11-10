# ruff: file-ignore[hardcoded-sql-expression]
"""SQLite persistence for Admin Backfill Jobs.

The management database is the only store; the migration runner in
``mdgw.admin.db`` owns schema evolution. Jobs are operational state, never
market data, and this module never touches the Bars Dataset or the HWM.

The active-job conflict check, the idempotency lookup and the insert run in
one ``BEGIN IMMEDIATE`` transaction, so two concurrent create requests for the
same ``(instrument_id, interval)`` can never both be accepted. Status updates
are conditional ``UPDATE ... WHERE status IN (...)`` statements: a late
Collector poll can never overwrite a terminal state, and an interrupted job is
written as ``failed``/``interrupted``, never success.
"""

from __future__ import annotations

import contextlib
import sqlite3
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

from mdgw.admin.db import AdminStoreError
from mdgw.admin.errors import ConflictError
from mdgw.admin.jobs.models import (
    ACTIVE_STATUSES,
    BACKFILL_STATUSES,
    CANCELLING,
    FAILED,
    INTERRUPTED_ERROR,
    QUEUED,
    RUNNING,
    TERMINAL_STATUSES,
    BackfillJobRecord,
    decode_gap_ids,
    decode_json,
    decode_range,
    empty_counters,
    encode_gap_ids,
    encode_json,
    encode_range,
)

if TYPE_CHECKING:
    from collections.abc import Generator

_SQLITE_BUSY_TIMEOUT_MS = 5_000
MIN_BACKFILL_SCHEMA_VERSION = 9

_COUNTER_COLUMNS = (
    "scanned_bars",
    "gap_bars",
    "backfilled_bars",
    "written_bars",
    "rejected_bars",
)

_COLUMNS = """
    job_id, status, instrument_id, interval, start_ns, end_ns, plan_json,
    current_range_json, scanned_bars, gap_bars, backfilled_bars,
    written_bars, rejected_bars, requested_by_subject,
    requested_by_display_name, requested_by_access_role, reason,
    retry_of_job_id, created_at_ns, started_at_ns, finished_at_ns,
    error_json, resolved_gap_ids_json, dispatched_at_ns, updated_at_ns
"""

_ACTIVE_PLACEHOLDERS = ",".join("?" for _ in ACTIVE_STATUSES)


class BackfillJobStore:
    """Thread-safe Backfill Job persistence over the management database."""

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

    # -- create / conflict / idempotency -----------------------------------

    def insert(
        self,
        record: BackfillJobRecord,
        *,
        idempotency_key: str | None = None,
        action: str = "create",
        request_hash: str | None = None,
    ) -> tuple[BackfillJobRecord, bool]:
        """Insert one queued job; replay an identical Idempotency-Key."""
        with self.transaction() as connection:
            replayed = self._insert_or_replay(
                connection,
                record,
                idempotency_key=idempotency_key,
                action=action,
                request_hash=request_hash,
            )
        if replayed is not None:
            return replayed, True
        stored = self.get(record.job_id)
        return (record if stored is None else stored), False

    def insert_retry(
        self,
        record: BackfillJobRecord,
        *,
        idempotency_key: str | None = None,
        request_hash: str | None = None,
    ) -> tuple[BackfillJobRecord, bool]:
        """Insert a linked retry job; the predecessor is never touched."""
        return self.insert(
            record,
            idempotency_key=idempotency_key,
            action="retry",
            request_hash=request_hash,
        )

    def lookup_idempotent(
        self, subject: str, idempotency_key: str
    ) -> tuple[str, str, BackfillJobRecord] | None:
        """Return ``(action, request_hash, record)`` for one stored key."""
        with self._lock:
            row = self._connection().execute(
                "SELECT action, request_hash, job_id "
                "FROM backfill_job_idempotency "
                "WHERE subject = ? AND idempotency_key = ?",
                (subject, idempotency_key),
            ).fetchone()
            if row is None:
                return None
            job = self._get_locked(str(row["job_id"]))
        if job is None:  # pragma: no cover - jobs are never deleted
            return None
        return str(row["action"]), str(row["request_hash"]), job

    def _insert_or_replay(
        self,
        connection: sqlite3.Connection,
        record: BackfillJobRecord,
        *,
        idempotency_key: str | None,
        action: str,
        request_hash: str | None,
    ) -> BackfillJobRecord | None:
        if idempotency_key is not None:
            existing = connection.execute(
                "SELECT action, request_hash, job_id "
                "FROM backfill_job_idempotency "
                "WHERE subject = ? AND idempotency_key = ?",
                (record.requested_by_subject, idempotency_key),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["action"]) != action
                    or str(existing["request_hash"]) != str(request_hash)
                ):
                    self._raise_key_reuse(idempotency_key)
                job = self._get_on(connection, str(existing["job_id"]))
                if job is None:  # pragma: no cover - jobs are never deleted
                    self._raise_key_reuse(idempotency_key)
                return job

        active = connection.execute(
            "SELECT job_id FROM backfill_jobs "
            "WHERE instrument_id = ? AND interval = ? "
            f"AND status IN ({_ACTIVE_PLACEHOLDERS}) "
            "ORDER BY created_at_ns ASC, job_id ASC LIMIT 1",
            (record.instrument_id, record.interval, *ACTIVE_STATUSES),
        ).fetchone()
        if active is not None:
            msg = "an active backfill job already exists for this range"
            raise ConflictError(
                msg,
                details={
                    "reason": "active_job",
                    "active_job_id": str(active["job_id"]),
                },
            )

        connection.execute(
            f"INSERT INTO backfill_jobs ({_COLUMNS}) "
            f"VALUES ({','.join('?' for _ in range(25))})",
            _insert_parameters(record),
        )
        if idempotency_key is not None:
            connection.execute(
                "INSERT INTO backfill_job_idempotency "
                "(subject, idempotency_key, action, request_hash, job_id, "
                " created_at_ns) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    record.requested_by_subject,
                    idempotency_key,
                    action,
                    request_hash,
                    record.job_id,
                    record.created_at_ns,
                ),
            )
        return None

    @staticmethod
    def _raise_key_reuse(idempotency_key: str) -> None:
        msg = "the Idempotency-Key was reused with a different request"
        raise ConflictError(
            msg,
            details={
                "reason": "idempotency_key_reuse",
                "idempotency_key": idempotency_key,
            },
        )

    # -- reads -------------------------------------------------------------

    def get(self, job_id: str) -> BackfillJobRecord | None:
        with self._lock:
            return self._get_locked(job_id)

    def _get_locked(self, job_id: str) -> BackfillJobRecord | None:
        return self._get_on(self._connection(), job_id)

    @staticmethod
    def _get_on(
        connection: sqlite3.Connection, job_id: str
    ) -> BackfillJobRecord | None:
        row = connection.execute(
            f"SELECT {_COLUMNS} FROM backfill_jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        return None if row is None else _record_from_row(row)

    def list_jobs(
        self,
        *,
        status: str | None,
        instrument_id: str | None,
        interval: str | None,
        after: tuple[int, str] | None,
        limit: int,
    ) -> list[BackfillJobRecord]:
        """One keyset page ordered by ``created_at_ns desc, job_id desc``."""
        clauses: list[str] = []
        parameters: list[Any] = []
        if status is not None:
            clauses.append("status = ?")
            parameters.append(status)
        if instrument_id is not None:
            clauses.append("instrument_id = ?")
            parameters.append(instrument_id)
        if interval is not None:
            clauses.append("interval = ?")
            parameters.append(interval)
        if after is not None:
            created_at_ns, job_id = after
            clauses.append(
                "(created_at_ns < ? OR "
                "(created_at_ns = ? AND job_id < ?))"
            )
            parameters.extend([created_at_ns, created_at_ns, job_id])
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        parameters.append(limit)
        with self._lock:
            rows = self._connection().execute(
                f"SELECT {_COLUMNS} FROM backfill_jobs {where} "
                "ORDER BY created_at_ns DESC, job_id DESC LIMIT ?",
                parameters,
            ).fetchall()
        return [_record_from_row(row) for row in rows]

    def active_jobs(self) -> list[BackfillJobRecord]:
        with self._lock:
            rows = self._connection().execute(
                f"SELECT {_COLUMNS} FROM backfill_jobs "
                f"WHERE status IN ({_ACTIVE_PLACEHOLDERS}) "
                "ORDER BY created_at_ns ASC, job_id ASC",
                ACTIVE_STATUSES,
            ).fetchall()
        return [_record_from_row(row) for row in rows]

    # -- transitions -------------------------------------------------------

    def mark_dispatched(self, job_id: str, now_ns: int) -> bool:
        """Record a successful ``backfill.job.start`` acknowledgement."""
        with self._lock:
            cursor = self._connection().execute(
                "UPDATE backfill_jobs SET dispatched_at_ns = "
                "COALESCE(dispatched_at_ns, ?), error_json = NULL, "
                "updated_at_ns = ? WHERE job_id = ? "
                f"AND status IN ({_ACTIVE_PLACEHOLDERS})",
                (now_ns, now_ns, job_id, *ACTIVE_STATUSES),
            )
            return cursor.rowcount > 0

    def record_dispatch_error(
        self, job_id: str, error: dict[str, Any], now_ns: int
    ) -> bool:
        """Keep a queued job queued with its bounded last dispatch error."""
        with self._lock:
            cursor = self._connection().execute(
                "UPDATE backfill_jobs SET error_json = ?, updated_at_ns = ? "
                "WHERE job_id = ? AND status = ?",
                (encode_json(error), now_ns, job_id, QUEUED),
            )
            return cursor.rowcount > 0

    def request_cancel(
        self, job_id: str, now_ns: int
    ) -> BackfillJobRecord | None:
        """Move a queued/running job to cancelling (or cancelled if unstarted).

        A queued job that was never acknowledged by the Collector has nothing
        to cancel: it is finished locally as ``cancelled``. A dispatched or
        running job stays ``cancelling`` until the Collector confirms a
        terminal outcome; this method never claims that confirmation.
        """
        with self.transaction() as connection:
            return self._request_cancel_on(connection, job_id, now_ns)

    def request_cancel_idempotent(
        self,
        job_id: str,
        now_ns: int,
        *,
        subject: str,
        idempotency_key: str,
        request_hash: str,
    ) -> tuple[BackfillJobRecord | None, bool]:
        """Cancel once per Idempotency-Key; a replay returns the same job."""
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT action, request_hash, job_id "
                "FROM backfill_job_idempotency "
                "WHERE subject = ? AND idempotency_key = ?",
                (subject, idempotency_key),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["action"]) != "cancel"
                    or str(existing["request_hash"]) != request_hash
                ):
                    self._raise_key_reuse(idempotency_key)
                return self._get_on(
                    connection, str(existing["job_id"])
                ), True
            record = self._request_cancel_on(connection, job_id, now_ns)
            if record is None:
                return None, False
            connection.execute(
                "INSERT INTO backfill_job_idempotency "
                "(subject, idempotency_key, action, request_hash, job_id, "
                " created_at_ns) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    subject,
                    idempotency_key,
                    "cancel",
                    request_hash,
                    job_id,
                    now_ns,
                ),
            )
            return record, False

    def _request_cancel_on(
        self,
        connection: sqlite3.Connection,
        job_id: str,
        now_ns: int,
    ) -> BackfillJobRecord | None:
        row = connection.execute(
            "SELECT status FROM backfill_jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        if row is None:
            return None
        if str(row["status"]) not in {QUEUED, RUNNING}:
            return None
        # The response is always the cooperative `cancelling` state. The
        # runtime may finish it as `cancelled` without a Collector round-trip
        # only when the start was never acknowledged (nothing was accepted).
        connection.execute(
            "UPDATE backfill_jobs SET status = ?, updated_at_ns = ? "
            "WHERE job_id = ?",
            (CANCELLING, now_ns, job_id),
        )
        return self._get_on(connection, job_id)

    def apply_status(  # ruff: ignore[too-many-arguments] - explicit snapshot
        self,
        job_id: str,
        *,
        status: str,
        counters: dict[str, Any] | None,
        current_range: dict[str, Any] | None,
        started_at_ns: int | None,
        finished_at_ns: int | None,
        error: dict[str, Any] | None,
        resolved_gap_ids: Any,
        now_ns: int,
    ) -> bool:
        """Persist one observed Collector transition.

        Only an active row is updated: a late poll can never resurrect a
        terminal job. Terminal transitions always carry ``finished_at_ns`` and
        clear ``current_range`` (the contract reports ``null`` when finished).
        """
        if status not in BACKFILL_STATUSES:
            msg = f"unknown backfill status: {status!r}"
            raise ValueError(msg)
        terminal = status in TERMINAL_STATUSES
        effective_range = None if terminal else encode_range(current_range)
        effective_finished = (
            now_ns if terminal and finished_at_ns is None else finished_at_ns
        )
        # A cancellation observed locally must not be overwritten by a late
        # "queued"/"running" start snapshot: the worker still sends the
        # cooperative cancel and only a Collector terminal state can close it.
        source_statuses = (
            (QUEUED, RUNNING)
            if status in {QUEUED, RUNNING}
            else ACTIVE_STATUSES
        )
        placeholders = ",".join("?" for _ in source_statuses)
        with self._lock:
            cursor = self._connection().execute(
                "UPDATE backfill_jobs SET status = ?, current_range_json = ?, "
                "scanned_bars = ?, gap_bars = ?, backfilled_bars = ?, "
                "written_bars = ?, rejected_bars = ?, error_json = ?, "
                "resolved_gap_ids_json = ?, "
                "started_at_ns = COALESCE(started_at_ns, ?), "
                "finished_at_ns = ?, updated_at_ns = ? "
                f"WHERE job_id = ? AND status IN ({placeholders})",
                (
                    status,
                    effective_range,
                    *counters_parameters(counters),
                    encode_json(error),
                    encode_gap_ids(resolved_gap_ids),
                    started_at_ns,
                    effective_finished,
                    now_ns,
                    job_id,
                    *source_statuses,
                ),
            )
            return cursor.rowcount > 0

    def set_resolved_gap_ids(
        self, job_id: str, gap_ids: tuple[str, ...], now_ns: int
    ) -> bool:
        """Persist the Gap ids a successful job resolved (ticket 07 linkage)."""
        with self._lock:
            cursor = self._connection().execute(
                "UPDATE backfill_jobs SET resolved_gap_ids_json = ?, "
                "updated_at_ns = ? WHERE job_id = ?",
                (encode_gap_ids(gap_ids), now_ns, job_id),
            )
            return cursor.rowcount > 0

    def mark_interrupted(self, job_id: str, now_ns: int) -> bool:
        """Fail an unconfirmed active job with the stable ``interrupted`` code."""
        with self._lock:
            cursor = self._connection().execute(
                "UPDATE backfill_jobs SET status = ?, error_json = ?, "
                "current_range_json = NULL, finished_at_ns = ?, "
                "updated_at_ns = ? WHERE job_id = ? "
                "AND status IN (?, ?)",
                (
                    FAILED,
                    encode_json(INTERRUPTED_ERROR.to_json()),
                    now_ns,
                    now_ns,
                    job_id,
                    RUNNING,
                    CANCELLING,
                ),
            )
            return cursor.rowcount > 0

    # -- internals ---------------------------------------------------------

    def _configure(self) -> None:
        connection = self._connection()
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {_SQLITE_BUSY_TIMEOUT_MS}")
        connection.execute("PRAGMA synchronous = NORMAL")

    def _require_schema(self) -> None:
        row = self._connection().execute(
            "SELECT version FROM schema_version WHERE id = 1"
        ).fetchone()
        version = 0 if row is None else int(row[0])
        if version < MIN_BACKFILL_SCHEMA_VERSION:
            msg = (
                "backfill jobs require admin schema version "
                f"{MIN_BACKFILL_SCHEMA_VERSION}, found {version}"
            )
            raise AdminStoreError(msg)
        # A database migrated past 009 (for example by a later ticket before
        # this one merged) still needs the table; fail with a clear message
        # instead of an opaque "no such table" later.
        table = self._connection().execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name = 'backfill_jobs'"
        ).fetchone()
        if table is None:
            msg = (
                "admin schema version "
                f"{version} does not contain the backfill_jobs table"
            )
            raise AdminStoreError(msg)

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            msg = "BackfillJobStore is closed"
            raise sqlite3.ProgrammingError(msg)
        return self._conn

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


def counters_parameters(counters: dict[str, Any] | None) -> tuple[int, ...]:
    normalized = (
        empty_counters()
        if counters is None
        else {**empty_counters(), **counters}
    )
    return tuple(int(normalized.get(name, 0)) for name in _COUNTER_COLUMNS)


def _insert_parameters(record: BackfillJobRecord) -> tuple[Any, ...]:
    return (
        record.job_id,
        record.status,
        record.instrument_id,
        record.interval,
        record.start_ns,
        record.end_ns,
        encode_json(dict(record.plan)),
        encode_range(record.current_range),
        *counters_parameters(dict(record.counters)),
        record.requested_by_subject,
        record.requested_by_display_name,
        record.requested_by_access_role,
        record.reason,
        record.retry_of_job_id,
        record.created_at_ns,
        record.started_at_ns,
        record.finished_at_ns,
        encode_json(record.error),
        encode_gap_ids(record.resolved_gap_ids),
        record.dispatched_at_ns,
        record.updated_at_ns or record.created_at_ns,
    )


def _record_from_row(row: sqlite3.Row) -> BackfillJobRecord:
    return BackfillJobRecord(
        job_id=str(row["job_id"]),
        status=str(row["status"]),
        instrument_id=str(row["instrument_id"]),
        interval=str(row["interval"]),
        start_ns=int(row["start_ns"]),
        end_ns=int(row["end_ns"]),
        plan=decode_json(row["plan_json"]) or {},
        current_range=decode_range(row["current_range_json"]),
        counters={name: int(row[name]) for name in _COUNTER_COLUMNS},
        requested_by_subject=str(row["requested_by_subject"]),
        requested_by_display_name=str(row["requested_by_display_name"]),
        requested_by_access_role=str(row["requested_by_access_role"]),
        reason=str(row["reason"]),
        retry_of_job_id=(
            None
            if row["retry_of_job_id"] is None
            else str(row["retry_of_job_id"])
        ),
        created_at_ns=int(row["created_at_ns"]),
        started_at_ns=(
            None
            if row["started_at_ns"] is None
            else int(row["started_at_ns"])
        ),
        finished_at_ns=(
            None
            if row["finished_at_ns"] is None
            else int(row["finished_at_ns"])
        ),
        error=decode_json(row["error_json"]),
        resolved_gap_ids=decode_gap_ids(row["resolved_gap_ids_json"]),
        dispatched_at_ns=(
            None
            if row["dispatched_at_ns"] is None
            else int(row["dispatched_at_ns"])
        ),
        updated_at_ns=int(row["updated_at_ns"]),
    )


__all__ = [
    "MIN_BACKFILL_SCHEMA_VERSION",
    "BackfillJobStore",
    "counters_parameters",
]
