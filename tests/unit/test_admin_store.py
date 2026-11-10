# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[multiple-with-statements]
# ruff: file-ignore[pytest-raises-with-multiple-statements]
"""Independent admin SQLite store: migrations, transactions, instance lease.

The admin database is operational state, never market data. It must create its
own schema idempotently, survive reopen, run WAL with foreign keys, and make
the single-API-instance constraint explicit rather than hopeful.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from mdgw.admin.db import (
    ADMIN_STORE_SCHEMA_VERSION,
    AdminInstanceLockedError,
    AdminSchemaVersionError,
    AdminStore,
    AdminStoreError,
    discover_migrations,
)


def _store(path: Path, *, lease_ttl_s: float = 60.0) -> AdminStore:
    return AdminStore(path, lease_ttl_s=lease_ttl_s)


def test_schema_is_created_idempotently_and_survives_reopen(
    tmp_path: Path,
) -> None:
    path = tmp_path / "admin" / "admin.db"

    with _store(path) as store:
        assert store.path == path
        assert store.schema_version == ADMIN_STORE_SCHEMA_VERSION

    assert path.exists()

    with _store(path) as reopened:
        assert reopened.schema_version == ADMIN_STORE_SCHEMA_VERSION
        with reopened.transaction() as connection:
            row = connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('schema_version', 'admin_instance') "
                "ORDER BY name"
            ).fetchall()
    assert [name for (name,) in row] == ["admin_instance", "schema_version"]


def test_store_uses_wal_and_foreign_keys(tmp_path: Path) -> None:
    with _store(tmp_path / "admin.db") as store:
        with store.transaction() as connection:
            journal_mode = connection.execute(
                "PRAGMA journal_mode"
            ).fetchone()[0]
            foreign_keys = connection.execute(
                "PRAGMA foreign_keys"
            ).fetchone()[0]

    assert journal_mode.lower() == "wal"
    assert foreign_keys == 1


def test_transaction_commits_and_rolls_back_atomically(tmp_path: Path) -> None:
    with _store(tmp_path / "admin.db") as store:
        with store.transaction() as connection:
            connection.execute(
                "CREATE TABLE probe (id INTEGER PRIMARY KEY, value TEXT)"
            )
            connection.execute("INSERT INTO probe (value) VALUES ('kept')")

        with pytest.raises(RuntimeError, match="boom"):
            with store.transaction() as connection:
                connection.execute("INSERT INTO probe (value) VALUES ('lost')")
                msg = "boom"
                raise RuntimeError(msg)

        with store.transaction() as connection:
            rows = connection.execute(
                "SELECT value FROM probe ORDER BY id"
            ).fetchall()

    assert rows == [("kept",)]


def test_single_api_instance_lease_is_explicit(tmp_path: Path) -> None:
    path = tmp_path / "admin.db"

    with _store(path) as first, _store(path) as second:
        first.claim_instance("api-first", now_ns=1_000)
        assert first.holder() == "api-first"

        with pytest.raises(AdminInstanceLockedError, match="api-first"):
            second.claim_instance("api-second", now_ns=2_000)

        first.release_instance("api-first")
        second.claim_instance("api-second", now_ns=3_000)
        assert second.holder() == "api-second"


def test_same_instance_can_refresh_its_lease(tmp_path: Path) -> None:
    with _store(tmp_path / "admin.db") as store:
        store.claim_instance("api-one", now_ns=1_000)
        store.claim_instance("api-one", now_ns=2_000)
        store.heartbeat("api-one", now_ns=3_000)

        with store.transaction() as connection:
            row = connection.execute(
                "SELECT instance_id, heartbeat_ns FROM admin_instance"
            ).fetchone()

    assert row == ("api-one", 3_000)


def test_stale_instance_lease_is_taken_over(tmp_path: Path) -> None:
    path = tmp_path / "admin.db"
    lease_ns = 60 * 1_000_000_000

    with _store(path) as first, _store(path) as second:
        first.claim_instance("api-crashed", now_ns=1_000)

        with pytest.raises(AdminInstanceLockedError):
            second.claim_instance("api-new", now_ns=lease_ns)

        second.claim_instance("api-new", now_ns=1_000 + lease_ns + 1)
        assert second.holder() == "api-new"


def test_heartbeat_of_a_released_instance_is_rejected(tmp_path: Path) -> None:
    with _store(tmp_path / "admin.db") as store:
        store.claim_instance("api-one", now_ns=1_000)
        store.release_instance("api-one")

        with pytest.raises(AdminInstanceLockedError):
            store.heartbeat("api-one", now_ns=2_000)


def test_future_schema_version_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "admin.db"
    with _store(path) as store:
        with store.transaction() as connection:
            connection.execute(
                "UPDATE schema_version SET version = ? WHERE id = 1",
                (ADMIN_STORE_SCHEMA_VERSION + 1,),
            )

    with pytest.raises(AdminSchemaVersionError, match="newer"):
        _store(path)


def test_operations_after_close_raise(tmp_path: Path) -> None:
    store = _store(tmp_path / "admin.db")
    store.close()

    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        with store.transaction():
            pass


def test_migrations_are_discovered_in_numeric_order(tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "002_second.sql").write_text(
        "SELECT 1;\n", encoding="utf-8"
    )
    (migrations / "001_first.sql").write_text(
        "SELECT 1;\n", encoding="utf-8"
    )

    discovered = discover_migrations(migrations)

    assert [version for version, _ in discovered] == [1, 2]
    assert ADMIN_STORE_SCHEMA_VERSION >= 1


def test_backfill_migration_upgrades_an_existing_schema_8_database(
    tmp_path: Path,
) -> None:
    """009/010 must reach a database already at schema 8 (export schema).

    ``migrate`` skips every migration whose version is not greater than the
    recorded one, so these migrations must be numbered above 8 to apply to an
    existing export-era database.
    """
    versions = [version for version, _ in discover_migrations()]
    # Ticket 07 adds 010; migrations remain ordered above the export schema.
    assert versions == [1, 2, 3, 4, 5, 8, 9, 10]

    path = tmp_path / "admin" / "admin.db"
    partial = tmp_path / "migrations"
    partial.mkdir()
    for version, migration in discover_migrations():
        if version <= 5 or version == 8:
            (partial / migration.name).write_text(
                migration.read_text(encoding="utf-8"), encoding="utf-8"
            )

    with AdminStore(path, migrations_dir=partial) as legacy:
        assert legacy.schema_version == 8

    with _store(path) as upgraded:
        assert upgraded.schema_version == ADMIN_STORE_SCHEMA_VERSION >= 9
        with upgraded.transaction() as connection:
            table = connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name = 'backfill_jobs'"
            ).fetchone()

    assert table == ("backfill_jobs",)


def test_migration_file_names_and_versions_are_validated(
    tmp_path: Path,
) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "one.sql").write_text(
        "SELECT 1;\n", encoding="utf-8"
    )
    with pytest.raises(AdminStoreError, match="invalid"):
        discover_migrations(migrations)

    (migrations / "one.sql").unlink()
    (migrations / "001_a.sql").write_text(
        "SELECT 1;\n", encoding="utf-8"
    )
    (migrations / "001_b.sql").write_text(
        "SELECT 1;\n", encoding="utf-8"
    )
    with pytest.raises(AdminStoreError, match="duplicate"):
        discover_migrations(migrations)


def test_failed_migration_rolls_back_and_keeps_the_previous_version(
    tmp_path: Path,
) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "001_ok.sql").write_text(
        "CREATE TABLE first_table (id INTEGER);\n", encoding="utf-8"
    )
    (migrations / "002_bad.sql").write_text(
        "CREATE TABLE second_table (id INTEGER); THIS IS NOT SQL;\n",
        encoding="utf-8",
    )
    database = tmp_path / "admin.db"

    with pytest.raises(sqlite3.OperationalError):
        AdminStore(database, migrations_dir=migrations)

    connection = sqlite3.connect(database)
    try:
        version = connection.execute(
            "SELECT version FROM schema_version WHERE id = 1"
        ).fetchone()[0]
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    finally:
        connection.close()

    assert version == 1
    assert "first_table" in tables
    assert "second_table" not in tables
