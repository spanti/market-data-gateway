"""Independent management SQLite store for the Admin API.

This database is operational state (sessions, desired state, jobs, audit,
export metadata), never market data. It deliberately lives outside the Bars
and Quarantine Datasets, uses only the standard library, runs WAL with foreign
keys enabled, and records an explicit schema version.

Migrations are ordered ``NNN_name.sql`` files under ``admin/migrations``. The
runner in this module is the only place that applies them, so later tickets
add the next numbered file and never edit this file. The single-API-instance
constraint is a lease row, not a convention: a second live instance fails to
claim the store, while a crashed instance is taken over once its lease expires.
"""

from __future__ import annotations

import contextlib
import operator
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from collections.abc import Generator

DEFAULT_INSTANCE_LEASE_S = 60.0
MIGRATIONS_DIR = Path(__file__).with_name("migrations")
SINGLETON_ROW = 1

_SCHEMA_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS schema_version (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL
)
"""

_MIGRATION_FILENAME_RE = re.compile(
    r"^(?P<version>\d{3})_(?P<name>[a-z0-9_]+)\.sql$"
)
_SQLITE_BUSY_TIMEOUT_MS = 5_000
_NANOS_PER_SECOND = 1_000_000_000


class AdminStoreError(RuntimeError):
    """Base class for management-store failures."""


class AdminSchemaVersionError(AdminStoreError):
    """The database was written by a newer Admin API schema."""


class AdminInstanceLockedError(AdminStoreError):
    """Another live API instance holds the single-instance lease."""


def discover_migrations(
    migrations_dir: Path | None = None,
) -> tuple[tuple[int, Path], ...]:
    """Return ``(version, path)`` pairs ordered by version.

    A duplicate or malformed file name is a startup error: silently skipping a
    migration would leave the database in a version the code does not expect.
    """
    directory = MIGRATIONS_DIR if migrations_dir is None else Path(migrations_dir)
    discovered: list[tuple[int, Path]] = []
    if not directory.is_dir():
        return ()
    for path in directory.glob("*.sql"):
        match = _MIGRATION_FILENAME_RE.match(path.name)
        if match is None:
            msg = f"invalid admin migration file name: {path.name!r}"
            raise AdminStoreError(msg)
        discovered.append((int(match.group("version")), path))
    discovered.sort(key=operator.itemgetter(0))
    versions = [version for version, _ in discovered]
    if len(versions) != len(set(versions)):
        msg = "duplicate admin migration version in migrations directory"
        raise AdminStoreError(msg)
    return tuple(discovered)


_MIGRATIONS = discover_migrations()
ADMIN_STORE_SCHEMA_VERSION = (
    _MIGRATIONS[-1][0] if _MIGRATIONS else 0
)


def _execute_script(
    connection: sqlite3.Connection, script: str
) -> None:
    """Execute a migration script without ``executescript``'s implicit COMMIT.

    ``sqlite3.Connection.executescript`` commits the caller's transaction
    first, which would defeat the per-migration atomicity this runner needs.
    ``sqlite3.complete_statement`` splits on real statement boundaries instead.
    """
    buffer = ""
    for character in script:
        buffer += character
        if character != ";" or not sqlite3.complete_statement(buffer):
            continue
        statement = buffer.strip()
        buffer = ""
        if statement:
            connection.execute(statement)
    remainder = buffer.strip()
    if remainder and not all(
        not line.strip() or line.strip().startswith("--")
        for line in remainder.splitlines()
    ):
        connection.execute(remainder)


class AdminStore:
    """Thread-safe SQLite persistence for the admin management plane."""

    def __init__(
        self,
        path: Path,
        *,
        lease_ttl_s: float = DEFAULT_INSTANCE_LEASE_S,
        migrations_dir: Path | None = None,
    ) -> None:
        if lease_ttl_s <= 0:
            msg = "lease_ttl_s must be positive"
            raise ValueError(msg)
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lease_ttl_s = float(lease_ttl_s)
        self._migrations = discover_migrations(migrations_dir)
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = sqlite3.connect(
            str(self.path),
            check_same_thread=False,
            isolation_level=None,
            timeout=_SQLITE_BUSY_TIMEOUT_MS / 1000,
        )
        try:
            self._configure()
            self._ensure_schema_version_table()
            self.migrate()
        except BaseException:
            self.close()
            raise

    # -- schema ------------------------------------------------------------

    @property
    def schema_version(self) -> int:
        """The schema version recorded in the database."""
        with self._lock:
            return _read_schema_version(self._connection())

    @property
    def latest_schema_version(self) -> int:
        """The newest migration this build knows about."""
        return self._migrations[-1][0] if self._migrations else 0

    def migrate(self) -> None:
        """Apply pending migrations; idempotent for an up-to-date database."""
        with self._lock:
            connection = self._connection()
            current = _read_schema_version(connection)
            latest = self.latest_schema_version
            if current > latest:
                msg = (
                    "admin store schema version "
                    f"{current} is newer than this build supports ({latest})"
                )
                raise AdminSchemaVersionError(msg)
            for target, path in self._migrations:
                if target <= current:
                    continue
                script = path.read_text(encoding="utf-8")
                with self.transaction() as transaction:
                    _execute_script(transaction, script)
                    transaction.execute(
                        """
                        INSERT INTO schema_version (id, version)
                        VALUES (1, ?)
                        ON CONFLICT (id) DO UPDATE
                            SET version = excluded.version
                        """,
                        (target,),
                    )

    # -- transactions ------------------------------------------------------

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

    @contextlib.contextmanager
    def read(self) -> Generator[sqlite3.Connection]:
        """Run read-only statements under the store's connection lock.

        The admin store keeps one shared SQLite connection (``check_same_thread
        = False``); holding the lock for the duration of a read prevents a
        concurrent transaction from re-entering a different cursor state.
        """
        with self._lock:
            yield self._connection()

    # -- single API instance constraint ------------------------------------

    def claim_instance(
        self,
        instance_id: str,
        *,
        now_ns: int | None = None,
        lease_ttl_s: float | None = None,
    ) -> None:
        """Claim or refresh the single-instance lease.

        A live holder is rejected; a holder whose heartbeat is older than the
        lease is treated as crashed and replaced.
        """
        holder_id = _require_instance_id(instance_id)
        now = time.time_ns() if now_ns is None else int(now_ns)
        ttl_s = self._lease_ttl_s if lease_ttl_s is None else lease_ttl_s
        if ttl_s <= 0:
            msg = "lease_ttl_s must be positive"
            raise ValueError(msg)
        ttl_ns = int(ttl_s * _NANOS_PER_SECOND)

        with self.transaction() as connection:
            row = connection.execute(
                "SELECT instance_id, heartbeat_ns FROM admin_instance "
                "WHERE singleton = ?",
                (SINGLETON_ROW,),
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO admin_instance "
                    "(singleton, instance_id, acquired_at_ns, heartbeat_ns) "
                    "VALUES (?, ?, ?, ?)",
                    (SINGLETON_ROW, holder_id, now, now),
                )
                return
            current_holder, heartbeat_ns = str(row[0]), int(row[1])
            if current_holder == holder_id:
                connection.execute(
                    "UPDATE admin_instance SET heartbeat_ns = ? "
                    "WHERE singleton = ?",
                    (now, SINGLETON_ROW),
                )
                return
            if now - heartbeat_ns >= ttl_ns:
                connection.execute(
                    "UPDATE admin_instance SET instance_id = ?, "
                    "acquired_at_ns = ?, heartbeat_ns = ? "
                    "WHERE singleton = ?",
                    (holder_id, now, now, SINGLETON_ROW),
                )
                return
            msg = (
                "admin store is locked by another live API instance: "
                f"{current_holder!r}"
            )
            raise AdminInstanceLockedError(msg)

    def heartbeat(self, instance_id: str, *, now_ns: int | None = None) -> None:
        """Refresh the lease for the current holder, rejecting others."""
        holder_id = _require_instance_id(instance_id)
        now = time.time_ns() if now_ns is None else int(now_ns)
        with self.transaction() as connection:
            cursor = connection.execute(
                "UPDATE admin_instance SET heartbeat_ns = ? "
                "WHERE singleton = ? AND instance_id = ?",
                (now, SINGLETON_ROW, holder_id),
            )
            if cursor.rowcount == 0:
                msg = (
                    "admin store lease is not held by this instance: "
                    f"{holder_id!r}"
                )
                raise AdminInstanceLockedError(msg)

    def release_instance(self, instance_id: str) -> None:
        """Release the lease when this instance still holds it."""
        holder_id = _require_instance_id(instance_id)
        with self.transaction() as connection:
            connection.execute(
                "DELETE FROM admin_instance "
                "WHERE singleton = ? AND instance_id = ?",
                (SINGLETON_ROW, holder_id),
            )

    def holder(self) -> str | None:
        """Return the current lease holder, or None when unclaimed."""
        with self._lock:
            row = self._connection().execute(
                "SELECT instance_id FROM admin_instance WHERE singleton = ?",
                (SINGLETON_ROW,),
            ).fetchone()
        return None if row is None else str(row[0])

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        """Close the SQLite connection; later operations raise."""
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

    def _ensure_schema_version_table(self) -> None:
        self._connection().execute(_SCHEMA_VERSION_TABLE)

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            msg = "AdminStore is closed"
            raise sqlite3.ProgrammingError(msg)
        return self._conn


def _read_schema_version(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT version FROM schema_version WHERE id = 1"
    ).fetchone()
    return 0 if row is None else int(row[0])


def _require_instance_id(instance_id: str) -> str:
    cleaned = instance_id.strip()
    if not cleaned:
        msg = "instance_id must be a non-empty string"
        raise ValueError(msg)
    return cleaned


__all__ = [
    "ADMIN_STORE_SCHEMA_VERSION",
    "DEFAULT_INSTANCE_LEASE_S",
    "MIGRATIONS_DIR",
    "AdminInstanceLockedError",
    "AdminSchemaVersionError",
    "AdminStore",
    "AdminStoreError",
    "discover_migrations",
]
