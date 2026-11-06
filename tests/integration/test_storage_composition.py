# ruff: file-ignore[implicit-namespace-package]
"""Storage Composition interface tests.

These tests exercise the production composition seam with real temporary
directories, real DuckDB and real Parquet behaviour. They assert only what
callers can observe: the returned objects, their public roots and settings,
the effective Rule Version, file-system side effects and public errors.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from mdgw.config import FeedConfig, GatewayConfig, load_config
from mdgw.core.bar import Bar, RawBar
from mdgw.ingest import ccxt_feed
from mdgw.normalize.batch import BatchNormalizationError
from mdgw.normalize.version import NORMALIZATION_RULE_VERSION
from mdgw.quarantine import QuarantineStore
from mdgw.runtime import GatewayRuntime
from mdgw.service.api import ApiService
from mdgw.service.collector import CollectorService
from mdgw.service.collector_core import CollectorCore
from mdgw.service.replay import ReplayServiceRole
from mdgw.store.composition import StorageBundle, StorageComposition
from mdgw.store.manifest import active_manifest_entries
from mdgw.store.parquet_dataset import ParquetBarStore

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BTC = "binance:BTC/USDT"
BASE_TS = datetime(2026, 1, 1, tzinfo=UTC)

INSTRUMENT_YAML = """\
- instrument_id: "binance:BTC/USDT"
  venue: binance
  symbol: "BTC/USDT"
  asset_class: crypto
  instrument_type: spot
  base: BTC
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.00001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
"""

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
    instruments: dict | None = None,
    **storage: object,
) -> GatewayConfig:
    storage_payload: dict[str, object] = {"root": str(root), **storage}
    if quarantine is not None:
        storage_payload["quarantine"] = quarantine
    payload: dict[str, object] = {"storage": storage_payload}
    if instruments is not None:
        payload["instruments"] = instruments
    return GatewayConfig.model_validate(payload)


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


def _runtime_config(
    *,
    rule_version: str | None = None,
    quarantine_root: Path | None = None,
    feeds: tuple[FeedConfig, ...] | None = None,
) -> GatewayConfig:
    """The checked-in runtime config, with only the knobs this file needs."""
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    updates: dict[str, object] = {}
    if rule_version is not None:
        updates["rule_version"] = rule_version
    if quarantine_root is not None:
        updates["root"] = quarantine_root
    quarantine = config.storage.quarantine.model_copy(update=updates)
    storage = config.storage.model_copy(update={"quarantine": quarantine})
    replace: dict[str, object] = {
        "storage": storage,
        "server": config.server.model_copy(update={"http_port": 0}),
        "instruments": config.instruments.model_copy(
            update={"path": PROJECT_ROOT / "config" / "instruments"}
        ),
    }
    if feeds is not None:
        replace["feeds"] = feeds
    return config.model_copy(update=replace)


def _raw(
    minute: int,
    *,
    values: tuple[object, object, object, object, object] | None = None,
) -> RawBar:
    open_value, high, low, close, volume = values or (
        Decimal(100),
        Decimal(101),
        Decimal(99),
        Decimal("100.5"),
        Decimal(1),
    )
    return RawBar(
        instrument_id=BTC,
        interval="1m",
        open_ts=BASE_TS + timedelta(minutes=minute),
        open=open_value,  # type: ignore[arg-type]
        high=high,  # type: ignore[arg-type]
        low=low,  # type: ignore[arg-type]
        close=close,  # type: ignore[arg-type]
        volume=volume,  # type: ignore[arg-type]
        trade_count=1,
        revision=0,
    )


class _AttachableFeed:
    """Scripted feed exposing the same Quarantine attach seam as ccxt."""

    name = "attachable"
    supports_history = False

    def __init__(self, batches: list[list[RawBar]] | None = None) -> None:
        self._batches = list(batches or [])
        self.quarantine = None
        self.rule_version = NORMALIZATION_RULE_VERSION

    @property
    def instrument_ids(self) -> tuple[str, ...]:
        return (BTC,)

    def iter_bars(self):
        return iter(self._batches.pop(0)) if self._batches else iter(())

    def fetch_history(  # ruff: ignore[no-self-use] - Feed Handler protocol
        self, _instrument_id, _interval, _start, _end
    ):
        return ()

    def attach_quarantine(self, quarantine, *, rule_version=None) -> None:
        # Mirrors the production ccxt seam: never replace an existing Store.
        if self.quarantine is None:
            self.quarantine = quarantine
        if rule_version is not None:
            self.rule_version = rule_version


def _idle_exchange():
    """Minimal ccxt.pro surface; construction must not touch the network."""
    return SimpleNamespace(
        rateLimit=50,
        enableRateLimit=False,
        has={
            "watchOHLCV": True,
            "watchOHLCVForSymbols": False,
            "fetchOHLCV": False,
        },
        close=lambda: None,
    )



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


def test_quarantine_operation_returns_a_new_store_each_time(
    tmp_path: Path,
) -> None:
    composition = StorageComposition(_config(tmp_path / "data"))

    first = composition.quarantine()
    second = composition.quarantine()

    assert isinstance(first, QuarantineStore)
    assert isinstance(second, QuarantineStore)
    assert first is not second
    assert first.root == second.root == tmp_path / "data" / "quarantine"
    # Quarantine-only composition has no construction-time file-system effect.
    assert not (tmp_path / "data").exists()


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


# -- collector composition: joint layout, identity and Rule Version ---------


def test_gateway_runtime_rejects_a_layout_invalid_only_after_the_root_override(
    tmp_path: Path,
) -> None:
    override_root = tmp_path / "override"
    # Valid against the configured "./data" root; only the runtime override
    # puts Quarantine inside the actual Bars Dataset.
    config = _runtime_config(
        quarantine_root=override_root / "bars" / "quarantine"
    )

    with pytest.raises(ValueError, match="must not be inside the Bars Dataset"):
        GatewayRuntime(
            feed_handler=_AttachableFeed(),
            config=config,
            data_root=override_root,
        )

    # The Bars Store (and its DuckDB temp dir) must not have been created.
    assert list(tmp_path.iterdir()) == []


def test_collector_service_rejects_a_layout_invalid_only_after_the_root_override(
    tmp_path: Path,
) -> None:
    override_root = tmp_path / "override"
    config = _runtime_config(
        quarantine_root=override_root / "bars" / "quarantine"
    )

    with pytest.raises(ValueError, match="must not be inside the Bars Dataset"):
        CollectorService(
            config,
            data_root=override_root,
            feed_handler=_AttachableFeed(),
        )

    assert list(tmp_path.iterdir()) == []


# -- CollectorCore: default path uses composition, seams stay intact --------


def test_collector_core_anchors_default_quarantine_on_an_injected_store_root(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path / "injected-store")
    core = CollectorCore(
        feed_handler=_AttachableFeed(),
        config=_config(tmp_path / "configured"),
        store=store,
    )
    try:
        assert core.store is store
        assert core.quarantine.root == tmp_path / "injected-store" / "quarantine"
        assert not (tmp_path / "configured").exists()
    finally:
        store.close()


def test_collector_core_keeps_a_configured_quarantine_root_with_an_injected_store(
    tmp_path: Path,
) -> None:
    explicit_root = tmp_path / "audit" / "quarantine"
    store = ParquetBarStore(tmp_path / "injected-store")
    core = CollectorCore(
        feed_handler=_AttachableFeed(),
        config=_config(
            tmp_path / "configured",
            quarantine={"root": str(explicit_root)},
        ),
        store=store,
    )
    try:
        # An explicit configured root wins over the injected Store root.
        assert core.quarantine.root == explicit_root
    finally:
        store.close()


def test_collector_core_uses_an_injected_quarantine_and_defaults_the_store(
    tmp_path: Path,
) -> None:
    injected = QuarantineStore(tmp_path / "injected-quarantine")
    core = CollectorCore(
        feed_handler=_AttachableFeed(),
        config=_config(tmp_path / "configured"),
        data_root=tmp_path / "data",
        quarantine=injected,
    )
    try:
        assert core.quarantine is injected
        assert core.store.root == tmp_path / "data"
        assert core.store.dataset_root == tmp_path / "data" / "bars"
        assert not (tmp_path / "configured").exists()
    finally:
        core.store.close()


def test_collector_core_keeps_both_injected_objects(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path / "injected-store")
    injected = QuarantineStore(tmp_path / "injected-quarantine")
    core = CollectorCore(
        feed_handler=_AttachableFeed(),
        config=_config(tmp_path / "configured"),
        data_root=tmp_path / "ignored-data-root",
        store=store,
        quarantine=injected,
    )
    try:
        assert core.store is store
        assert core.quarantine is injected
    finally:
        store.close()


def test_collector_core_anchors_snapshot_and_hwm_on_the_effective_root(
    tmp_path: Path,
) -> None:
    configured_root = tmp_path / "configured"
    override_root = tmp_path / "override"
    reference_dir = tmp_path / "reference"
    reference_dir.mkdir()
    (reference_dir / "instruments.yaml").write_text(
        INSTRUMENT_YAML, encoding="utf-8"
    )
    config = _config(
        configured_root, instruments={"path": str(reference_dir)}
    )
    bundle = StorageComposition(
        config, root=override_root
    ).bars_and_quarantine()
    core = CollectorCore(
        feed_handler=_AttachableFeed(),
        config=config,
        store=bundle.bars,
        quarantine=bundle.quarantine,
    )
    core.open_state()
    try:
        assert core.instrument_store is not None
        assert core.hwm is not None
        # Both derived paths stay under the actual (override) root even though
        # the Stores were composed outside CollectorCore.
        assert core.instrument_store.snapshot_path.is_relative_to(
            override_root / "instruments"
        )
        assert core.hwm.path == override_root / config.collector.state_db_name
        assert not configured_root.exists()
    finally:
        core.close_state()
        bundle.bars.close()


def test_collector_core_rejects_a_quarantine_inside_an_injected_store(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path / "injected-store")
    config = _copied_config_with_quarantine_root(
        tmp_path / "configured",
        tmp_path / "injected-store" / "bars" / "nested",
    )
    try:
        with pytest.raises(
            ValueError, match="must not be inside the Bars Dataset"
        ):
            CollectorCore(
                feed_handler=_AttachableFeed(),
                config=config,
                store=store,
            )
    finally:
        store.close()


# -- one collector composition: shared Quarantine and Rule Version ----------


def test_gateway_runtime_shares_one_quarantine_with_its_feed_handler(
    tmp_path: Path,
) -> None:
    feed = _AttachableFeed()
    runtime = GatewayRuntime(
        feed_handler=feed,
        config=_runtime_config(rule_version="rules-2"),
        data_root=tmp_path,
    )
    try:
        assert runtime.quarantine is feed.quarantine
        assert runtime.quarantine.root == tmp_path / "quarantine"
        assert runtime.store.root == tmp_path
        assert feed.rule_version == "rules-2"
    finally:
        runtime.store.close()


def test_collector_service_shares_one_quarantine_with_its_feed_handler(
    tmp_path: Path,
) -> None:
    feed = _AttachableFeed()
    service = CollectorService(
        _runtime_config(rule_version="rules-2"),
        data_root=tmp_path,
        feed_handler=feed,
    )
    try:
        assert service.core.quarantine is feed.quarantine
        assert service.core.quarantine.root == tmp_path / "quarantine"
        assert service.core.store.root == tmp_path
        assert feed.rule_version == "rules-2"
    finally:
        service.stop()


@pytest.mark.parametrize(
    ("configured_rule_version", "expected"),
    [
        (None, NORMALIZATION_RULE_VERSION),
        ("rules-2", "rules-2"),
    ],
)
def test_runtime_rule_version_reaches_bars_and_quarantine_writes(
    tmp_path: Path,
    configured_rule_version: str | None,
    expected: str,
) -> None:
    rejected = [
        _raw(
            99,
            values=(
                1.5,
                Decimal(101),
                Decimal(99),
                Decimal("100.5"),
                Decimal(1),
            ),
        )
    ]
    accepted = [_raw(minute) for minute in range(5)]
    feed = _AttachableFeed([rejected, accepted])
    runtime = GatewayRuntime(
        feed_handler=feed,
        config=_runtime_config(rule_version=configured_rule_version),
        data_root=tmp_path,
    )
    runtime.start()
    try:
        with pytest.raises(BatchNormalizationError):
            runtime.ingest()
        records = runtime.quarantine.read_records().to_pylist()
        assert [record["rule_version"] for record in records] == [expected]

        runtime.ingest()
        entries = active_manifest_entries(runtime.store.dataset_root)
        assert entries
        assert {tuple(entry["rule_versions"]) for entry in entries} == {
            (expected,)
        }
        # The same value reached the handler through its attach seam.
        assert feed.rule_version == expected
    finally:
        runtime.stop()


def test_collector_service_default_feed_handler_build_uses_the_rule_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # CollectorService builds its own ccxt handler from config; replace only
    # the outbound exchange factory so no network is touched.
    monkeypatch.setattr(
        ccxt_feed, "_create_exchange", lambda _feed: _idle_exchange()
    )
    feeds = (
        FeedConfig(
            name="binance_spot",
            kind="ccxt",
            venue="binance",
            symbols=("BTC/USDT",),
        ),
    )
    service = CollectorService(
        _runtime_config(rule_version="rules-2", feeds=feeds),
        data_root=tmp_path,
    )
    try:
        handler = service.core.feed_handler
        assert handler.name == "binance_spot"
        assert handler.rule_version == "rules-2"
        assert handler.quarantine is service.core.quarantine
        assert service.core.store.root == tmp_path
    finally:
        service.stop()
