# ruff: file-ignore[implicit-namespace-package]
"""Admin migration upgrade matrix and Git-base numbering guard tests."""

from __future__ import annotations

import runpy
from pathlib import Path

import pytest

from mdgw.admin.db import (
    ADMIN_STORE_SCHEMA_VERSION,
    AdminStore,
    discover_migrations,
)

PROJECT_ROOT = Path(__file__).parents[2]
MIGRATIONS = discover_migrations()
TABLES_BY_VERSION = {
    1: {"admin_instance"},
    2: {"auth_transactions", "admin_sessions", "admin_security_events"},
    3: {"operator_audit_events"},
    4: {"feed_subscriptions", "flow_control", "admin_idempotency_keys"},
    5: {"service_instances", "service_operations"},
    8: {"export_jobs"},
    9: {"backfill_jobs", "backfill_job_idempotency"},
    10: {"backfill_gaps", "backfill_gap_occurrences"},
}
ALL_TABLES = set().union(*TABLES_BY_VERSION.values())


def test_table_contract_covers_every_migration() -> None:
    assert set(TABLES_BY_VERSION) == {version for version, _ in MIGRATIONS}


@pytest.mark.parametrize(
    "prefix",
    range(1, len(MIGRATIONS)),
    ids=[f"schema-{version}" for version, _ in MIGRATIONS[:-1]],
)
def test_every_historical_schema_prefix_upgrades_to_latest(
    tmp_path: Path, prefix: int
) -> None:
    legacy_dir = tmp_path / f"schema-{prefix}"
    legacy_dir.mkdir()
    for _version, migration in MIGRATIONS[:prefix]:
        (legacy_dir / migration.name).write_text(
            migration.read_text(encoding="utf-8"), encoding="utf-8"
        )
    database = tmp_path / f"admin-{prefix}.db"
    legacy_version = MIGRATIONS[prefix - 1][0]

    with AdminStore(database, migrations_dir=legacy_dir) as legacy:
        assert legacy.schema_version == legacy_version

    with AdminStore(database) as upgraded:
        assert upgraded.schema_version == ADMIN_STORE_SCHEMA_VERSION
        with upgraded.read() as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }

    # Checking only the recorded version would miss a skipped migration whose
    # number was below the legacy DB version. Require every table instead.
    assert tables >= ALL_TABLES


_GUARD = runpy.run_path(
    str(PROJECT_ROOT / "scripts" / "check_admin_migrations.py")
)
_validate_added = _GUARD["validate_added_migrations"]


def test_migration_guard_accepts_versions_above_the_base() -> None:
    added = _validate_added(
        ["001_a.sql", "008_b.sql", "009_c.sql"],
        ["001_a.sql", "008_b.sql"],
    )
    assert added == [(9, "009_c.sql")]


def test_migration_guard_allows_changes_to_existing_files() -> None:
    assert _validate_added(["001_a.sql"], ["001_a.sql"]) == []


def test_migration_guard_rejects_a_new_version_below_the_base() -> None:
    version = 6
    with pytest.raises(ValueError, match="above the base schema"):
        _validate_added(
            ["001_a.sql", "008_b.sql", f"{version:03d}_late.sql"],
            ["001_a.sql", "008_b.sql"],
        )


def test_migration_guard_rejects_duplicate_versions() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        _validate_added(["001_a.sql", "001_b.sql"])


def test_migration_guard_rejects_duplicate_base_versions() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        _validate_added(["002_c.sql"], ["001_a.sql", "001_b.sql"])


def test_migration_guard_rejects_malformed_names() -> None:
    with pytest.raises(ValueError, match="invalid admin migration"):
        _validate_added(["not-a-migration.sql"])
