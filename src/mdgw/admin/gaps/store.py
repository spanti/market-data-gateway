# ruff: file-ignore[hardcoded-sql-expression]
"""SQLite persistence for persistent Unrecoverable Backfill Gaps.

The management database is the only store; the migration runner in
``mdgw.admin.db`` owns schema evolution. This module never touches the Bars
Dataset or the HWM.

Natural-key discovery runs in one ``BEGIN IMMEDIATE`` transaction: concurrent
Collector snapshot polls and Backfill Job reconciliation can never insert two
rows for the same ``(feed, instrument_id, interval, reason, start, end)``.
Occurrences are append-only; no method updates or deletes one.
"""

from __future__ import annotations

import contextlib
import sqlite3
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self
from uuid import uuid4

from mdgw.admin.db import AdminStoreError
from mdgw.admin.gaps.models import (
    GAP_OPEN,
    GAP_RESOLVED,
    PHASE_DISCOVERED,
    PHASE_RESOLVED,
    SOURCE_BACKFILL_JOB,
    GapOccurrence,
    GapRecord,
    decode_job_ids,
    encode_job_ids,
    merge_job_id,
    validate_reason,
)

if TYPE_CHECKING:
    from collections.abc import Generator

_SQLITE_BUSY_TIMEOUT_MS = 5_000
MIN_GAP_SCHEMA_VERSION = 10

_COLUMNS = """
    gap_id, status, feed, instrument_id, interval, start_ns, end_ns, reason,
    first_discovered_at_ns, last_discovered_at_ns, occurrence_count,
    resolved_at_ns, resolved_by_job_id, related_job_ids_json,
    created_at_ns, updated_at_ns
"""

_OCCURRENCE_COLUMNS = """
    occurrence_id, gap_id, observed_at_ns, source, phase, job_id,
    start_ns, end_ns
"""

_CONTAINED = """
    status = ? AND start_ns IS NOT NULL AND end_ns IS NOT NULL
    AND start_ns >= ? AND end_ns <= ?
"""


class GapStore:
    """Thread-safe persistent Gap storage over the management database."""

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

    # -- discovery ---------------------------------------------------------

    def upsert_discovery(  # ruff: ignore[too-many-arguments] - explicit observation
        self,
        *,
        source: str,
        feed: str,
        instrument_id: str,
        interval: str,
        start_ns: int | None,
        end_ns: int | None,
        reason: str,
        observed_at_ns: int,
        job_id: str | None = None,
    ) -> GapRecord:
        """Record one discovery, updating the one natural-key row in place."""
        reason = validate_reason(reason)
        observed = int(observed_at_ns)
        with self.transaction() as connection:
            row = connection.execute(
                f"SELECT {_COLUMNS} FROM backfill_gaps "
                "WHERE feed = ? AND instrument_id = ? AND interval = ? "
                "AND reason = ? AND start_ns IS ? AND end_ns IS ?",
                (feed, instrument_id, interval, reason, start_ns, end_ns),
            ).fetchone()
            if row is None:
                gap_id = f"gap_{uuid4().hex}"
                related = merge_job_id((), job_id)
                connection.execute(
                    "INSERT INTO backfill_gaps (gap_id, status, feed, "
                    "instrument_id, interval, start_ns, end_ns, reason, "
                    "first_discovered_at_ns, last_discovered_at_ns, "
                    "occurrence_count, resolved_at_ns, resolved_by_job_id, "
                    "related_job_ids_json, created_at_ns, updated_at_ns) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL, NULL, "
                    "?, ?, ?)",
                    (
                        gap_id,
                        GAP_OPEN,
                        feed,
                        instrument_id,
                        interval,
                        start_ns,
                        end_ns,
                        reason,
                        observed,
                        observed,
                        encode_job_ids(related),
                        observed,
                        observed,
                    ),
                )
            else:
                record = _record_from_row(row)
                gap_id = record.gap_id
                if _observation_exists(
                    connection,
                    gap_id=gap_id,
                    observed_at_ns=observed,
                    source=source,
                    phase=PHASE_DISCOVERED,
                    job_id=job_id,
                    start_ns=start_ns,
                    end_ns=end_ns,
                ):
                    # Polling an unchanged snapshot is not a new discovery.
                    return record
                related = merge_job_id(record.related_job_ids, job_id)
                # Re-discovering a resolved gap reopens the same row: history
                # (first discovery and every occurrence) is never lost.
                connection.execute(
                    "UPDATE backfill_gaps SET status = ?, "
                    "last_discovered_at_ns = ?, "
                    "occurrence_count = occurrence_count + 1, "
                    "resolved_at_ns = NULL, resolved_by_job_id = NULL, "
                    "related_job_ids_json = ?, updated_at_ns = ? "
                    "WHERE gap_id = ?",
                    (
                        GAP_OPEN,
                        max(record.last_discovered_at_ns, observed),
                        encode_job_ids(related),
                        max(record.updated_at_ns, observed),
                        gap_id,
                    ),
                )
            _append_occurrence(
                connection,
                gap_id=gap_id,
                observed_at_ns=observed,
                source=source,
                phase=PHASE_DISCOVERED,
                job_id=job_id,
                start_ns=start_ns,
                end_ns=end_ns,
            )
            return _get_on(connection, gap_id)

    # -- resolution / linkage ---------------------------------------------

    def resolve_contained(  # ruff: ignore[too-many-arguments] - explicit range
        self,
        *,
        feed: str,
        instrument_id: str,
        interval: str,
        start_ns: int,
        end_ns: int,
        job_id: str,
        observed_at_ns: int,
    ) -> tuple[str, ...]:
        """Resolve open gaps fully contained in a successful job's range.

        A gap with no precise range is never resolved: the job cannot prove
        that it covered the unknown hole.
        """
        observed = int(observed_at_ns)
        resolved: list[str] = []
        with self.transaction() as connection:
            rows = _contained_rows(
                connection,
                feed=feed,
                instrument_id=instrument_id,
                interval=interval,
                start_ns=start_ns,
                end_ns=end_ns,
            )
            for row in rows:
                record = _record_from_row(row)
                related = merge_job_id(record.related_job_ids, job_id)
                connection.execute(
                    "UPDATE backfill_gaps SET status = ?, resolved_at_ns = ?, "
                    "resolved_by_job_id = ?, related_job_ids_json = ?, "
                    "updated_at_ns = ? WHERE gap_id = ?",
                    (
                        GAP_RESOLVED,
                        observed,
                        job_id,
                        encode_job_ids(related),
                        max(record.updated_at_ns, observed),
                        record.gap_id,
                    ),
                )
                _append_occurrence(
                    connection,
                    gap_id=record.gap_id,
                    observed_at_ns=observed,
                    source=SOURCE_BACKFILL_JOB,
                    phase=PHASE_RESOLVED,
                    job_id=job_id,
                    start_ns=record.start_ns,
                    end_ns=record.end_ns,
                )
                resolved.append(record.gap_id)
        return tuple(resolved)

    def add_related_contained(  # ruff: ignore[too-many-arguments] - explicit range
        self,
        *,
        feed: str,
        instrument_id: str,
        interval: str,
        start_ns: int,
        end_ns: int,
        job_id: str,
        now_ns: int,
    ) -> tuple[str, ...]:
        """Link a finished job to open gaps it attempted; never resolves."""
        linked: list[str] = []
        with self.transaction() as connection:
            rows = _contained_rows(
                connection,
                feed=feed,
                instrument_id=instrument_id,
                interval=interval,
                start_ns=start_ns,
                end_ns=end_ns,
            )
            for row in rows:
                record = _record_from_row(row)
                related = merge_job_id(record.related_job_ids, job_id)
                if related == record.related_job_ids:
                    linked.append(record.gap_id)
                    continue
                connection.execute(
                    "UPDATE backfill_gaps SET related_job_ids_json = ?, "
                    "updated_at_ns = ? WHERE gap_id = ?",
                    (
                        encode_job_ids(related),
                        max(record.updated_at_ns, int(now_ns)),
                        record.gap_id,
                    ),
                )
                linked.append(record.gap_id)
        return tuple(linked)

    # (contained-range selection and occurrence append are module helpers)

    # -- reads -------------------------------------------------------------

    def get(self, gap_id: str) -> GapRecord | None:
        with self._lock:
            row = self._connection().execute(
                f"SELECT {_COLUMNS} FROM backfill_gaps WHERE gap_id = ?",
                (gap_id,),
            ).fetchone()
        return None if row is None else _record_from_row(row)

    def list_gaps(  # ruff: ignore[too-many-arguments] - explicit query filters
        self,
        *,
        status: str | None = None,
        feed: str | None = None,
        instrument_id: str | None = None,
        interval: str | None = None,
        reason: str | None = None,
        discovered_from_ns: int | None = None,
        discovered_to_ns: int | None = None,
        resolved_from_ns: int | None = None,
        resolved_to_ns: int | None = None,
        after: tuple[int, str] | None = None,
        limit: int = 50,
    ) -> list[GapRecord]:
        """One keyset page ordered by ``created_at_ns desc, gap_id desc``."""
        clauses: list[str] = []
        parameters: list[Any] = []
        for column, value in (
            ("status", status),
            ("feed", feed),
            ("instrument_id", instrument_id),
            ("interval", interval),
            ("reason", reason),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                parameters.append(value)
        if discovered_from_ns is not None:
            clauses.append("first_discovered_at_ns >= ?")
            parameters.append(int(discovered_from_ns))
        if discovered_to_ns is not None:
            clauses.append("first_discovered_at_ns < ?")
            parameters.append(int(discovered_to_ns))
        if resolved_from_ns is not None:
            clauses.append("resolved_at_ns >= ?")
            parameters.append(int(resolved_from_ns))
        if resolved_to_ns is not None:
            clauses.append("resolved_at_ns < ?")
            parameters.append(int(resolved_to_ns))
        if after is not None:
            created_at_ns, gap_id = after
            clauses.append(
                "(created_at_ns < ? OR "
                "(created_at_ns = ? AND gap_id < ?))"
            )
            parameters.extend([created_at_ns, created_at_ns, gap_id])
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        parameters.append(int(limit))
        with self._lock:
            rows = self._connection().execute(
                f"SELECT {_COLUMNS} FROM backfill_gaps {where} "
                "ORDER BY created_at_ns DESC, gap_id DESC LIMIT ?",
                parameters,
            ).fetchall()
        return [_record_from_row(row) for row in rows]

    def occurrences(self, gap_id: str) -> list[GapOccurrence]:
        """Return the append-only observation history for one gap."""
        with self._lock:
            rows = self._connection().execute(
                f"SELECT {_OCCURRENCE_COLUMNS} FROM backfill_gap_occurrences "
                "WHERE gap_id = ? ORDER BY occurrence_id ASC",
                (gap_id,),
            ).fetchall()
        return [_occurrence_from_row(row) for row in rows]

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
        if version < MIN_GAP_SCHEMA_VERSION:
            msg = (
                "persistent backfill gaps require admin schema version "
                f"{MIN_GAP_SCHEMA_VERSION}, found {version}"
            )
            raise AdminStoreError(msg)
        table = self._connection().execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name = 'backfill_gaps'"
        ).fetchone()
        if table is None:
            msg = (
                "admin schema version "
                f"{version} does not contain the backfill_gaps table"
            )
            raise AdminStoreError(msg)

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            msg = "GapStore is closed"
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


def _get_on(connection: sqlite3.Connection, gap_id: str) -> GapRecord:
    row = connection.execute(
        f"SELECT {_COLUMNS} FROM backfill_gaps WHERE gap_id = ?",
        (gap_id,),
    ).fetchone()
    if row is None:
        msg = f"backfill gap not found: {gap_id}"
        raise KeyError(msg)
    return _record_from_row(row)


def _record_from_row(row: sqlite3.Row) -> GapRecord:
    return GapRecord(
        gap_id=str(row["gap_id"]),
        status=str(row["status"]),
        feed=str(row["feed"]),
        instrument_id=str(row["instrument_id"]),
        interval=str(row["interval"]),
        start_ns=None if row["start_ns"] is None else int(row["start_ns"]),
        end_ns=None if row["end_ns"] is None else int(row["end_ns"]),
        reason=str(row["reason"]),
        first_discovered_at_ns=int(row["first_discovered_at_ns"]),
        last_discovered_at_ns=int(row["last_discovered_at_ns"]),
        occurrence_count=int(row["occurrence_count"]),
        resolved_at_ns=(
            None
            if row["resolved_at_ns"] is None
            else int(row["resolved_at_ns"])
        ),
        resolved_by_job_id=(
            None
            if row["resolved_by_job_id"] is None
            else str(row["resolved_by_job_id"])
        ),
        related_job_ids=decode_job_ids(row["related_job_ids_json"]),
        created_at_ns=int(row["created_at_ns"]),
        updated_at_ns=int(row["updated_at_ns"]),
    )


def _observation_exists(  # ruff: ignore[too-many-arguments] - explicit key
    connection: sqlite3.Connection,
    *,
    gap_id: str,
    observed_at_ns: int,
    source: str,
    phase: str,
    job_id: str | None,
    start_ns: int | None,
    end_ns: int | None,
) -> bool:
    row = connection.execute(
        "SELECT 1 FROM backfill_gap_occurrences "
        "WHERE gap_id = ? AND observed_at_ns = ? AND source = ? "
        "AND phase = ? AND job_id IS ? AND start_ns IS ? AND end_ns IS ? "
        "LIMIT 1",
        (
            gap_id,
            int(observed_at_ns),
            source,
            phase,
            job_id,
            start_ns,
            end_ns,
        ),
    ).fetchone()
    return row is not None


def _contained_rows(  # ruff: ignore[too-many-arguments] - explicit range
    connection: sqlite3.Connection,
    *,
    feed: str,
    instrument_id: str,
    interval: str,
    start_ns: int,
    end_ns: int,
) -> list[sqlite3.Row]:
    return connection.execute(
        f"SELECT {_COLUMNS} FROM backfill_gaps "
        f"WHERE feed = ? AND instrument_id = ? AND interval = ? "
        f"AND {_CONTAINED} "
        "ORDER BY created_at_ns ASC, gap_id ASC",
        (
            feed,
            instrument_id,
            interval,
            GAP_OPEN,
            int(start_ns),
            int(end_ns),
        ),
    ).fetchall()


def _append_occurrence(  # ruff: ignore[too-many-arguments] - explicit row
    connection: sqlite3.Connection,
    *,
    gap_id: str,
    observed_at_ns: int,
    source: str,
    phase: str,
    job_id: str | None,
    start_ns: int | None,
    end_ns: int | None,
) -> None:
    """Append one observation; this is the only occurrence write path."""
    connection.execute(
        "INSERT INTO backfill_gap_occurrences "
        "(gap_id, observed_at_ns, source, phase, job_id, start_ns, "
        " end_ns) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            gap_id,
            int(observed_at_ns),
            source,
            phase,
            job_id,
            start_ns,
            end_ns,
        ),
    )


def _occurrence_from_row(row: sqlite3.Row) -> GapOccurrence:
    return GapOccurrence(
        occurrence_id=int(row["occurrence_id"]),
        gap_id=str(row["gap_id"]),
        observed_at_ns=int(row["observed_at_ns"]),
        source=str(row["source"]),
        phase=str(row["phase"]),
        job_id=None if row["job_id"] is None else str(row["job_id"]),
        start_ns=None if row["start_ns"] is None else int(row["start_ns"]),
        end_ns=None if row["end_ns"] is None else int(row["end_ns"]),
    )


__all__ = ["MIN_GAP_SCHEMA_VERSION", "GapStore"]
