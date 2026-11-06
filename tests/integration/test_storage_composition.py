# ruff: file-ignore[implicit-namespace-package]
"""Storage Composition interface tests.

These tests exercise the production composition seam with real temporary
directories, real DuckDB and real Parquet behaviour. They assert only what
callers can observe: the returned objects, their public roots and settings,
the effective Rule Version, file-system side effects and public errors.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from mdgw.config import GatewayConfig
from mdgw.core.bar import Bar
from mdgw.normalize.version import NORMALIZATION_RULE_VERSION
from mdgw.quarantine import QuarantineStore
from mdgw.service.api import ApiService
from mdgw.service.replay import ReplayServiceRole
from mdgw.store.composition import StorageBundle, StorageComposition
from mdgw.store.parquet_dataset import ParquetBarStore

BAR = Bar(
    schema_version=1,
    data_type="bar",
    instrument_id="binance:BTC/USDT",
    interval="1m",
    open_ts=datetime(2026, 10, 30, tzinfo=UTC),
    close_ts=datetime(2026, 10, 30, 0, 1, tzinfo=UTC),
    event_ts=datetime(2026, 10, 30, 0, 1, tzinfo=UTC),
    ingest_ts=datetime(2026, 10, 30, 0, 1, tzinfo=UTC),
    feed="fake",
    seq=None,
    aggregation_source="external",
    open=Decimal(100),
    high=Decimal(101),
    low=Decimal(99),
    close=Decimal("100.5"),
    volume=Decimal(1),
    trade_count=1,
    revision=0,
)


def _config(
    root: Path,
    *,
    quarantine: dict | None = None,
    **storage: object,
) -> GatewayConfig:
    storage_payload: dict[str, object] = {"root": str(root), **storage}
    if quarantine is not None:
        storage_payload["quarantine"] = quarantine
    return GatewayConfig.model_validate({"storage": storage_payload})


def _copied_config_with_quarantine_root(
    root: Path, quarantine_root: Path
) -> GatewayConfig:
    """Build a config that bypasses the config model's independence validator."""
    config = _config(root)
    quarantine = config.storage.quarantine.model_copy(
        update={"root": quarantine_root}
    )
    storage = config.storage.model_copy(update={"quarantine": quarantine})
    return config.model_copy(update={"storage": storage})


def test_bars_operation_returns_a_store_at_the_configured_root(
    tmp_path: Path,
) -> None:
    configured_root = tmp_path / "data"
    composition = StorageComposition(_config(configured_root))

    store = composition.bars()
    try:
        assert isinstance(store, ParquetBarStore)
        assert composition.effective_root == configured_root
        assert composition.bars_dataset_root == configured_root / "bars"
        assert store.root == configured_root
        assert store.dataset_root == configured_root / "bars"
        # Unconfigured DuckDB spill stays anchored at the actual Store root.
        assert store.duckdb.temp_directory == configured_root / ".duckdb_tmp"
        assert (configured_root / ".duckdb_tmp").is_dir()
        # Bars-only composition never creates a Quarantine Dataset.
        assert not (configured_root / "quarantine").exists()
    finally:
        store.close()


def test_root_override_wins_and_each_bars_call_returns_a_new_store(
    tmp_path: Path,
) -> None:
    composition = StorageComposition(
        _config(tmp_path / "configured"), root=tmp_path / "override"
    )

    first = composition.bars()
    second = composition.bars()
    try:
        assert composition.effective_root == tmp_path / "override"
        assert composition.bars_dataset_root == tmp_path / "override" / "bars"
        assert isinstance(first, ParquetBarStore)
        assert isinstance(second, ParquetBarStore)
        assert first is not second
        assert first.root == second.root == tmp_path / "override"
        assert first.duckdb_connections is not second.duckdb_connections
        # The configured root must not be touched when an override wins.
        assert not (tmp_path / "configured").exists()
    finally:
        first.close()
        second.close()


def test_bars_store_uses_the_storage_and_duckdb_mapping(tmp_path: Path) -> None:
    root = tmp_path / "data"
    config = _config(
        root,
        compression="snappy",
        row_group_size=4096,
        format_version="2.4",
        schema_version=1,
        duckdb={
            "memory_limit": "256MB",
            "threads": 2,
            "arrow_batch_size": 1024,
        },
    )

    store = StorageComposition(config).bars()
    try:
        assert store.compression == "snappy"
        assert store.row_group_size == 4096
        assert store.format_version == "2.4"
        assert store.schema_version == 1
        assert store.duckdb.memory_limit == "256MB"
        assert store.duckdb.threads == 2
        assert store.duckdb.arrow_batch_size == 1024
    finally:
        store.close()


def test_explicit_duckdb_temp_directory_is_respected(tmp_path: Path) -> None:
    temp_directory = tmp_path / "duckdb-spill"
    config = _config(
        tmp_path / "data",
        duckdb={"temp_directory": str(temp_directory)},
    )

    store = StorageComposition(config).bars()
    try:
        assert store.duckdb.temp_directory == temp_directory
        assert temp_directory.is_dir()
    finally:
        store.close()


def _assert_nothing_created(directory: Path) -> None:
    assert list(directory.iterdir()) == []


def test_bars_rejects_quarantine_equal_to_the_bars_dataset_root(
    tmp_path: Path,
) -> None:
    configured_root = tmp_path / "data"
    # Built through model_copy so the config model's validator cannot hide
    # the composition-time check behind an earlier failure.
    config = _copied_config_with_quarantine_root(
        configured_root, configured_root / "bars"
    )
    composition = StorageComposition(config)

    with pytest.raises(ValueError, match="must not be inside the Bars Dataset"):
        composition.bars()

    _assert_nothing_created(tmp_path)


def test_bars_rejects_quarantine_nested_inside_the_bars_dataset(
    tmp_path: Path,
) -> None:
    configured_root = tmp_path / "data"
    config = _copied_config_with_quarantine_root(
        configured_root, configured_root / "bars" / "audit" / "quarantine"
    )

    with pytest.raises(ValueError, match="must not be inside the Bars Dataset"):
        StorageComposition(config).bars()

    # No Dataset descriptor, Manifest, Quarantine root or DuckDB temp dir.
    _assert_nothing_created(tmp_path)


def test_root_override_cannot_move_quarantine_inside_the_actual_bars_dataset(
    tmp_path: Path,
) -> None:
    # Valid against the configured root, invalid only once the override is
    # applied: this is exactly what the config model cannot see.
    config = _config(
        tmp_path / "configured",
        quarantine={"root": str(tmp_path / "override" / "bars" / "quarantine")},
    )
    composition = StorageComposition(config, root=tmp_path / "override")

    with pytest.raises(ValueError, match="must not be inside the Bars Dataset"):
        composition.bars()

    assert not (tmp_path / "override").exists()


def test_parent_directory_fragments_do_not_bypass_the_independence_check(
    tmp_path: Path,
) -> None:
    configured_root = tmp_path / "data"
    fragment_root = configured_root / "bars" / ".." / "bars" / "nested"
    config = _copied_config_with_quarantine_root(configured_root, fragment_root)

    with pytest.raises(ValueError, match="must not be inside the Bars Dataset"):
        StorageComposition(config).bars()

    _assert_nothing_created(tmp_path)


def test_relative_paths_are_checked_against_the_process_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    config = _copied_config_with_quarantine_root(
        Path("data"), Path("data") / "bars" / "nested"
    )

    with pytest.raises(ValueError, match="must not be inside the Bars Dataset"):
        StorageComposition(config).bars()

    _assert_nothing_created(tmp_path)


def test_allowed_non_nested_layouts_still_compose(tmp_path: Path) -> None:
    configured_root = tmp_path / "data"
    # The Quarantine root may be the storage root itself (an ancestor of the
    # Bars Dataset) — only nesting *inside* the Bars Dataset is forbidden.
    root_level_quarantine = _config(
        configured_root, quarantine={"root": str(configured_root)}
    )
    ancestor_quarantine = _config(
        tmp_path / "audit" / "store",
        quarantine={"root": str(tmp_path / "audit")},
    )

    for config in (root_level_quarantine, ancestor_quarantine):
        store = StorageComposition(config).bars()
        try:
            assert isinstance(store, ParquetBarStore)
        finally:
            store.close()


def test_default_quarantine_root_follows_the_effective_storage_root(
    tmp_path: Path,
) -> None:
    configured_root = tmp_path / "configured"
    override_root = tmp_path / "override"

    without_override = StorageComposition(_config(configured_root))
    with_override = StorageComposition(
        _config(configured_root), root=override_root
    )

    assert without_override.quarantine_root == configured_root / "quarantine"
    assert with_override.quarantine_root == override_root / "quarantine"


def test_explicit_quarantine_root_does_not_follow_the_root_override(
    tmp_path: Path,
) -> None:
    explicit_root = tmp_path / "audit" / "quarantine"
    composition = StorageComposition(
        _config(
            tmp_path / "configured",
            quarantine={"root": str(explicit_root)},
        ),
        root=tmp_path / "override",
    )

    assert composition.quarantine_root == explicit_root

    # A relative explicit root keeps its existing "relative to the process
    # cwd" meaning and is never re-anchored or rewritten by composition.
    relative = StorageComposition(
        _config(
            tmp_path / "configured",
            quarantine={"root": "audit/quarantine"},
        ),
        root=tmp_path / "override",
    )
    assert relative.quarantine_root == Path("audit/quarantine")


def test_quarantine_operation_uses_independent_dataset_settings(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    config = _config(
        root,
        compression="snappy",
        row_group_size=4096,
        format_version="2.4",
        quarantine={
            "schema_version": 3,
            "retention_days": 90,
        },
    )
    composition = StorageComposition(config)

    store = composition.quarantine()
    try:
        assert isinstance(store, QuarantineStore)
        assert store.root == root / "quarantine"
        assert store.schema_version == 3
        assert store.retention_days == 90
        # Quarantine physical-format defaults must not inherit Bars settings.
        assert store.compression == "zstd"
        assert store.row_group_size == 1_000_000
        assert store.format_version == "2.6"
    finally:
        # QuarantineStore has no filesystem side effect on construction.
        assert not root.exists()


def test_quarantine_operation_rejects_a_nested_layout(tmp_path: Path) -> None:
    configured_root = tmp_path / "data"
    config = _copied_config_with_quarantine_root(
        configured_root, configured_root / "bars" / "nested"
    )

    with pytest.raises(ValueError, match="must not be inside the Bars Dataset"):
        StorageComposition(config).quarantine()

    _assert_nothing_created(tmp_path)


def test_joint_composition_returns_named_independent_objects(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    explicit_quarantine = tmp_path / "audit" / "quarantine"
    composition = StorageComposition(
        _config(root, quarantine={"root": str(explicit_quarantine)}),
        root=tmp_path / "override",
    )

    first = composition.bars_and_quarantine()
    second = composition.bars_and_quarantine()
    try:
        assert isinstance(first, StorageBundle)
        assert isinstance(first.bars, ParquetBarStore)
        assert isinstance(first.quarantine, QuarantineStore)
        assert first.bars is not second.bars
        assert first.quarantine is not second.quarantine
        assert first.bars.root == tmp_path / "override"
        assert first.bars.dataset_root == tmp_path / "override" / "bars"
        assert first.quarantine.root == explicit_quarantine
    finally:
        first.bars.close()
        second.bars.close()


def test_joint_composition_validates_before_creating_either_store(
    tmp_path: Path,
) -> None:
    configured_root = tmp_path / "data"
    config = _copied_config_with_quarantine_root(
        configured_root, configured_root / "bars" / "nested"
    )

    with pytest.raises(ValueError, match="must not be inside the Bars Dataset"):
        StorageComposition(config).bars_and_quarantine()

    _assert_nothing_created(tmp_path)


def test_effective_rule_version_defaults_and_follows_the_config(
    tmp_path: Path,
) -> None:
    default = StorageComposition(_config(tmp_path / "default"))
    custom = StorageComposition(
        _config(tmp_path / "custom", quarantine={"rule_version": "rules-2"})
    )

    assert default.rule_version == NORMALIZATION_RULE_VERSION
    assert custom.rule_version == "rules-2"


def test_custom_rule_version_reaches_the_bars_manifest_entries(
    tmp_path: Path,
) -> None:
    config = _config(
        tmp_path / "data", quarantine={"rule_version": "rules-2"}
    )
    composition = StorageComposition(config)

    store = composition.bars()
    try:
        entries = store.write_bars([BAR], asset_class="crypto")
        assert [entry["rule_versions"] for entry in entries] == [["rules-2"]]
    finally:
        store.close()


def test_api_service_composes_bars_only_at_its_actual_root(
    tmp_path: Path,
) -> None:
    configured_root = tmp_path / "configured"
    override_root = tmp_path / "override"
    service = ApiService(_config(configured_root), data_root=override_root)
    try:
        assert isinstance(service.store, ParquetBarStore)
        assert service.store.root == override_root
        assert service.store.dataset_root == override_root / "bars"
        assert not (configured_root).exists()
        assert not (override_root / "quarantine").exists()
    finally:
        service.store.close()


def test_replay_service_role_composes_bars_only_at_its_actual_root(
    tmp_path: Path,
) -> None:
    configured_root = tmp_path / "configured"
    override_root = tmp_path / "override"
    config = GatewayConfig.model_validate(
        {
            "storage": {"root": str(configured_root)},
            "replay": {"control_port": 0, "publish_port": 0},
            "service": {"replay_health_port": 0},
        }
    )
    role = ReplayServiceRole(config, data_root=override_root)
    try:
        assert isinstance(role.store, ParquetBarStore)
        assert role.store.root == override_root
        assert role.store.dataset_root == override_root / "bars"
        assert not (configured_root).exists()
        assert not (override_root / "quarantine").exists()
    finally:
        role.stop()
