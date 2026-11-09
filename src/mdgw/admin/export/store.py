# ruff: file-ignore[hardcoded-sql-expression]
"""SQLite persistence for Export Jobs.

The management database is the only store; this module owns its own
connection (the migration runner in ``mdgw.admin.db`` remains the sole owner
of schema evolution). Jobs are operational state, never market data.

Quota checks and the insert run inside one ``BEGIN IMMEDIATE`` transaction so
two concurrent HTTP requests cannot both pass the per-user or global
concurrency check. State transitions are conditional ``UPDATE ... WHERE
status = ...`` statements, so a late worker result can never overwrite an
observed cancellation or a terminal state.
"""

from __future__ import annotations

import contextlib
import sqlite3
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

from mdgw.admin.db import AdminStoreError
from mdgw.admin.errors import ConflictError, QuotaExceededError
from mdgw.admin.export.models import (
    ACTIVE_STATUSES,
    CANCELLED,
    CANCELLING,
    EXPIRED,
    FAILED,
    INTERRUPTED_ERROR,
    QUEUED,
    RUNNING,
    SUCCEEDED,
    ExportJobError,
    ExportJobRecord,
    ExportJobResult,
    decode_json,
    encode_json,
)

if TYPE_CHECKING:
    from collections.abc import Generator

_SQLITE_BUSY_TIMEOUT_MS = 5_000
MIN_EXPORT_SCHEMA_VERSION = 8

_COLUMNS = """
    job_id, status, instrument_id, interval, start_ns, end_ns, range_days,
    format, created_by_subject, created_by_display_name, processed_rows,
    processed_bytes, total_rows, result_json, error_json, created_at_ns,
    started_at_ns, finished_at_ns, cancelled_at_ns, expires_at_ns,
    cancel_requested
"""

_ACTIVE_PLACEHOLDERS = ",".join("?" for _ in ACTIVE_STATUSES)


class ExportStore:
    """Thread-safe Export Job persistence over the management database."""

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

    # -- create / quota ----------------------------------------------------

    def insert(  # ruff: ignore[too-many-arguments] - one explicit quota snapshot
        self,
        record: ExportJobRecord,
        *,
        max_running_per_user: int,
        global_concurrency: int,
        storage_quota_bytes: int,
        now_ns: int,
        idempotency_key: str | None = None,
        request_hash: str | None = None,
    ) -> tuple[ExportJobRecord, bool]:
        """Insert one queued job, enforcing every quota atomically.

        Returns ``(record, replayed)``. ``replayed`` is True when the same
        actor presented the same Idempotency-Key with the same body; the
        original job is returned and nothing is inserted.
        """
        with self.transaction() as connection:
            if idempotency_key is not None:
                existing = connection.execute(
                    f"SELECT {_COLUMNS}, request_hash FROM export_jobs "
                    "WHERE created_by_subject = ? AND idempotency_key = ?",
                    (record.created_by_subject, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if existing["request_hash"] != request_hash:
                        msg = "the Idempotency-Key was reused with a different body"
                        raise ConflictError(
                            msg,
                            details={
                                "reason": "idempotency_key_reuse",
                                "idempotency_key": idempotency_key,
                            },
                        )
                    return _record_from_row(existing), True

            active_user = connection.execute(
                "SELECT COUNT(*) FROM export_jobs "
                "WHERE created_by_subject = ? "
                f"AND status IN ({_ACTIVE_PLACEHOLDERS})",
                (record.created_by_subject, *ACTIVE_STATUSES),
            ).fetchone()[0]
            if int(active_user) >= max_running_per_user:
                _raise_quota(
                    "running_jobs",
                    limit=max_running_per_user,
                    current=int(active_user),
                )

            active_global = connection.execute(
                "SELECT COUNT(*) FROM export_jobs "
                f"WHERE status IN ({_ACTIVE_PLACEHOLDERS})",
                ACTIVE_STATUSES,
            ).fetchone()[0]
            if int(active_global) >= global_concurrency:
                _raise_quota(
                    "global_concurrency",
                    limit=global_concurrency,
                    current=int(active_global),
                )

            stored = self._stored_bytes(connection, now_ns=now_ns)
            if storage_quota_bytes > 0 and stored >= storage_quota_bytes:
                _raise_quota(
                    "global_storage",
                    limit=storage_quota_bytes,
                    current=stored,
                )

            connection.execute(
                f"""
                INSERT INTO export_jobs ({_COLUMNS}, idempotency_key,
                                         request_hash)
                VALUES ({",".join("?" for _ in range(21))}, ?, ?)
                """,
                (*_insert_parameters(record), idempotency_key, request_hash),
            )
        return record, False

    # -- reads -------------------------------------------------------------

    def get(self, job_id: str) -> ExportJobRecord | None:
        with self._lock:
            row = self._connection().execute(
                f"SELECT {_COLUMNS} FROM export_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return None if row is None else _record_from_row(row)

    def list_page(
        self,
        *,
        owner_subject: str | None,
        after: tuple[int, str] | None,
        limit: int,
    ) -> list[ExportJobRecord]:
        """One keyset page ordered by ``created_at_ns desc, job_id desc``."""
        clauses: list[str] = []
        parameters: list[Any] = []
        if owner_subject is not None:
            clauses.append("created_by_subject = ?")
            parameters.append(owner_subject)
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
                f"SELECT {_COLUMNS} FROM export_jobs {where} "
                "ORDER BY created_at_ns DESC, job_id DESC LIMIT ?",
                parameters,
            ).fetchall()
        return [_record_from_row(row) for row in rows]

    def get_idempotent(
        self, subject: str, idempotency_key: str
    ) -> tuple[ExportJobRecord, str | None] | None:
        """Return the job created for ``(subject, key)`` and its body hash."""
        with self._lock:
            row = self._connection().execute(
                f"SELECT {_COLUMNS}, request_hash FROM export_jobs "
                "WHERE created_by_subject = ? AND idempotency_key = ?",
                (subject, idempotency_key),
            ).fetchone()
        if row is None:
            return None
        return _record_from_row(row), row["request_hash"]

    def queued_job_ids(self) -> list[str]:
        with self._lock:
            rows = self._connection().execute(
                "SELECT job_id FROM export_jobs WHERE status = ? "
                "ORDER BY created_at_ns ASC, job_id ASC",
                (QUEUED,),
            ).fetchall()
        return [str(row["job_id"]) for row in rows]

    def cancel_requested(self, job_id: str) -> bool:
        with self._lock:
            row = self._connection().execute(
                "SELECT cancel_requested, status FROM export_jobs "
                "WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return bool(row is not None and row["cancel_requested"])

    # -- transitions -------------------------------------------------------

    def mark_running(self, job_id: str, now_ns: int) -> bool:
        with self._lock:
            cursor = self._connection().execute(
                "UPDATE export_jobs SET status = ?, started_at_ns = ? "
                "WHERE job_id = ? AND status = ? AND cancel_requested = 0",
                (RUNNING, now_ns, job_id, QUEUED),
            )
            return cursor.rowcount > 0

    def request_cancel(self, job_id: str, now_ns: int) -> bool:
        """Mark a queued/running job cancelling; False if not cancellable."""
        placeholders = ",".join("?" for _ in (QUEUED, RUNNING))
        with self._lock:
            cursor = self._connection().execute(
                "UPDATE export_jobs SET status = ?, cancel_requested = 1, "
                "cancelled_at_ns = COALESCE(cancelled_at_ns, ?) "
                f"WHERE job_id = ? AND status IN ({placeholders})",
                (CANCELLING, now_ns, job_id, QUEUED, RUNNING),
            )
            return cursor.rowcount > 0

    def update_progress(
        self, job_id: str, *, processed_rows: int, processed_bytes: int
    ) -> None:
        with self._lock:
            self._connection().execute(
                "UPDATE export_jobs SET processed_rows = ?, "
                "processed_bytes = ? WHERE job_id = ? "
                "AND status IN (?, ?)",
                (
                    int(processed_rows),
                    int(processed_bytes),
                    job_id,
                    RUNNING,
                    CANCELLING,
                ),
            )

    def mark_succeeded(  # ruff: ignore[too-many-arguments] - explicit completion
        self,
        job_id: str,
        *,
        result: ExportJobResult,
        expires_at_ns: int,
        finished_ns: int,
        processed_rows: int,
        processed_bytes: int,
    ) -> bool:
        """Complete a running job; refuses once a cancel was observed."""
        with self._lock:
            cursor = self._connection().execute(
                "UPDATE export_jobs SET status = ?, result_json = ?, "
                "error_json = NULL, finished_at_ns = ?, expires_at_ns = ?, "
                "processed_rows = ?, processed_bytes = ? "
                "WHERE job_id = ? AND status = ? AND cancel_requested = 0",
                (
                    SUCCEEDED,
                    encode_json(result.to_json()),
                    finished_ns,
                    expires_at_ns,
                    int(processed_rows),
                    int(processed_bytes),
                    job_id,
                    RUNNING,
                ),
            )
            return cursor.rowcount > 0

    def mark_cancelled(self, job_id: str, now_ns: int) -> bool:
        with self._lock:
            cursor = self._connection().execute(
                "UPDATE export_jobs SET status = ?, cancelled_at_ns = ?, "
                "finished_at_ns = ? WHERE job_id = ? AND status IN (?, ?, ?)",
                (
                    CANCELLED,
                    now_ns,
                    now_ns,
                    job_id,
                    QUEUED,
                    RUNNING,
                    CANCELLING,
                ),
            )
            return cursor.rowcount > 0

    def mark_failed(
        self, job_id: str, error: ExportJobError, now_ns: int
    ) -> bool:
        """Fail a queued/running job unless a cancel is already pending."""
        with self._lock:
            cursor = self._connection().execute(
                "UPDATE export_jobs SET status = ?, error_json = ?, "
                "finished_at_ns = ? WHERE job_id = ? AND status IN (?, ?) "
                "AND cancel_requested = 0",
                (
                    FAILED,
                    encode_json(error.to_json()),
                    now_ns,
                    job_id,
                    QUEUED,
                    RUNNING,
                ),
            )
            return cursor.rowcount > 0

    def recover_interrupted(self, now_ns: int) -> int:
        """Turn unconfirmed ``running`` work into ``failed/interrupted``.

        A graceful or crashed API restart can never confirm that a running
        export finished, so it must never become ``succeeded``. A job that
        was already cancelling has no file and becomes ``cancelled``.
        """
        with self.transaction() as connection:
            failed = connection.execute(
                "UPDATE export_jobs SET status = ?, error_json = ?, "
                "finished_at_ns = ? WHERE status = ?",
                (
                    FAILED,
                    encode_json(INTERRUPTED_ERROR.to_json()),
                    now_ns,
                    RUNNING,
                ),
            ).rowcount
            cancelled = connection.execute(
                "UPDATE export_jobs SET status = ?, cancelled_at_ns = ?, "
                "finished_at_ns = ? WHERE status = ?",
                (CANCELLED, now_ns, now_ns, CANCELLING),
            ).rowcount
        return int(failed) + int(cancelled)

    def expire_due(self, now_ns: int) -> list[str]:
        """Mark succeeded jobs past ``expires_at`` as expired.

        Returns the ``file_name`` values whose result files the caller must
        delete. Metadata rows are retained; only Export files are removed.
        """
        with self.transaction() as connection:
            rows = connection.execute(
                "SELECT job_id, result_json FROM export_jobs "
                "WHERE status = ? AND expires_at_ns IS NOT NULL "
                "AND expires_at_ns <= ?",
                (SUCCEEDED, now_ns),
            ).fetchall()
            names: list[str] = []
            for row in rows:
                result = decode_json(row["result_json"])
                if result is not None and isinstance(
                    result.get("file_name"), str
                ):
                    names.append(str(result["file_name"]))
            connection.executemany(
                "UPDATE export_jobs SET status = ? WHERE job_id = ?",
                [(EXPIRED, str(row["job_id"])) for row in rows],
            )
        return names

    # -- internals ---------------------------------------------------------

    @staticmethod
    def _stored_bytes(
        connection: sqlite3.Connection, *, now_ns: int
    ) -> int:
        rows = connection.execute(
            "SELECT result_json FROM export_jobs WHERE status = ? "
            "AND (expires_at_ns IS NULL OR expires_at_ns > ?)",
            (SUCCEEDED, now_ns),
        ).fetchall()
        total = 0
        for row in rows:
            result = decode_json(row["result_json"])
            if result is None:
                continue
            try:
                total += int(result.get("size_bytes", 0))
            except (TypeError, ValueError):  # pragma: no cover - corrupt row
                continue
        return total

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
        if version < MIN_EXPORT_SCHEMA_VERSION:
            msg = (
                "export jobs require admin schema version "
                f"{MIN_EXPORT_SCHEMA_VERSION}, found {version}"
            )
            raise AdminStoreError(msg)

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            msg = "ExportStore is closed"
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


def _raise_quota(quota: str, *, limit: int, current: int) -> None:
    msg = f"export quota exceeded: {quota}"
    raise QuotaExceededError(
        msg,
        details={"quota": quota, "limit": int(limit), "current": int(current)},
    )


def _insert_parameters(record: ExportJobRecord) -> tuple[Any, ...]:
    return (
        record.job_id,
        record.status,
        record.instrument_id,
        record.interval,
        record.start_ns,
        record.end_ns,
        record.range_days,
        record.format,
        record.created_by_subject,
        record.created_by_display_name,
        record.processed_rows,
        record.processed_bytes,
        record.total_rows,
        encode_json(record.result),
        encode_json(record.error),
        record.created_at_ns,
        record.started_at_ns,
        record.finished_at_ns,
        record.cancelled_at_ns,
        record.expires_at_ns,
        1 if record.cancel_requested else 0,
    )


def _record_from_row(row: sqlite3.Row) -> ExportJobRecord:
    return ExportJobRecord(
        job_id=str(row["job_id"]),
        status=str(row["status"]),
        instrument_id=str(row["instrument_id"]),
        interval=str(row["interval"]),
        start_ns=int(row["start_ns"]),
        end_ns=int(row["end_ns"]),
        range_days=int(row["range_days"]),
        format=str(row["format"]),
        created_by_subject=str(row["created_by_subject"]),
        created_by_display_name=str(row["created_by_display_name"]),
        created_at_ns=int(row["created_at_ns"]),
        processed_rows=int(row["processed_rows"]),
        processed_bytes=int(row["processed_bytes"]),
        total_rows=(
            None if row["total_rows"] is None else int(row["total_rows"])
        ),
        result=decode_json(row["result_json"]),
        error=decode_json(row["error_json"]),
        started_at_ns=(
            None if row["started_at_ns"] is None
            else int(row["started_at_ns"])
        ),
        finished_at_ns=(
            None if row["finished_at_ns"] is None
            else int(row["finished_at_ns"])
        ),
        cancelled_at_ns=(
            None if row["cancelled_at_ns"] is None
            else int(row["cancelled_at_ns"])
        ),
        expires_at_ns=(
            None if row["expires_at_ns"] is None
            else int(row["expires_at_ns"])
        ),
        cancel_requested=bool(row["cancel_requested"]),
    )


__all__ = ["MIN_EXPORT_SCHEMA_VERSION", "ExportStore"]
