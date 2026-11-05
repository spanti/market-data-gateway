"""Rule Version lineage through the real Collector and Backfill paths.

Quarantine records and Bars Manifest entries must name the Normalization
rules that actually ran: live Polars batches and the scalar Backfill/archive
rules have separate audit identities. Rule Version stays audit metadata; the
Data Contract Version remains the Schema authority.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from mdgw.config import GatewayConfig, load_config
from mdgw.core.bar import RawBar
from mdgw.core.time import utc_now
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.normalize.batch import BatchNormalizationError
from mdgw.normalize.version import (
    NORMALIZATION_RULE_VERSION,
    SCALAR_NORMALIZATION_RULE_VERSION,
)
from mdgw.quarantine import QUARANTINE_SCHEMA_VERSION
from mdgw.runtime import GatewayRuntime
from mdgw.service.collector_core import CollectorCore
from mdgw.store.manifest import active_manifest_entries

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE = datetime(2026, 1, 1, tzinfo=UTC)
BTC = "binance:BTC/USDT"


def _config(rule_version: str | None = None) -> GatewayConfig:
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    storage = config.storage
    if rule_version is not None:
        storage = storage.model_copy(
            update={
                "quarantine": storage.quarantine.model_copy(
                    update={"rule_version": rule_version}
                )
            }
        )
    return config.model_copy(
        update={
            "storage": storage,
            "server": config.server.model_copy(update={"http_port": 0}),
            "instruments": config.instruments.model_copy(
                update={"path": PROJECT_ROOT / "config" / "instruments"}
            ),
        }
    )


def _raw(
    minute: int,
    *,
    values: tuple[object, object, object, object, object] | None = None,
) -> RawBar:
    open_value, high, low, close, volume = values or (
        Decimal("100"),
        Decimal("101"),
        Decimal("99"),
        Decimal("100.5"),
        Decimal("1"),
    )
    return RawBar(
        instrument_id=BTC,
        interval="1m",
        open_ts=BASE + timedelta(minutes=minute),
        open=open_value,  # type: ignore[arg-type]
        high=high,  # type: ignore[arg-type]
        low=low,  # type: ignore[arg-type]
        close=close,  # type: ignore[arg-type]
        volume=volume,  # type: ignore[arg-type]
        trade_count=1,
        revision=0,
    )


class _ScriptedFeed:
    name = "scripted"
    supports_history = False

    def __init__(self, batches: list[list[RawBar]]) -> None:
        self._batches = list(batches)

    @property
    def instrument_ids(self) -> tuple[str, ...]:
        return (BTC,)

    def iter_bars(self):
        return iter(self._batches.pop(0)) if self._batches else iter(())

    def fetch_history(self, instrument_id, interval, start, end):
        return ()


def test_live_quarantine_and_manifest_share_the_rule_version_source(
    tmp_path: Path,
) -> None:
    rejected = _raw(
        0,
        values=(1.5, Decimal("101"), Decimal("99"), Decimal("100.5"), Decimal("1")),
    )
    feed = _ScriptedFeed([[rejected], [_raw(index) for index in range(5)]])
    core = CollectorCore(feed_handler=feed, config=_config(), data_root=tmp_path)
    core.open_state()
    try:
        with pytest.raises(BatchNormalizationError):
            core.ingest()

        (record,) = core.quarantine.read_records().to_pylist()
        # Rule Version is a string audit identity, distinct from the uint16
        # Quarantine Schema Version and the Bar Data Contract Version.
        assert record["rule_version"] == NORMALIZATION_RULE_VERSION
        assert record["quarantine_schema_version"] == QUARANTINE_SCHEMA_VERSION
        assert NORMALIZATION_RULE_VERSION != str(QUARANTINE_SCHEMA_VERSION)
        assert NORMALIZATION_RULE_VERSION != str(core.store.schema_version)

        core.ingest()
        entries = active_manifest_entries(core.store.dataset_root)
        assert entries
        assert {
            tuple(entry["rule_versions"]) for entry in entries
        } == {(NORMALIZATION_RULE_VERSION,)}
    finally:
        core.close_state()


def test_configured_live_rule_version_reaches_quarantine_and_manifest(
    tmp_path: Path,
) -> None:
    rejected = _raw(
        0,
        values=(1.5, Decimal("101"), Decimal("99"), Decimal("100.5"), Decimal("1")),
    )
    feed = _ScriptedFeed([[rejected], [_raw(index) for index in range(5)]])
    core = CollectorCore(
        feed_handler=feed,
        config=_config(rule_version="rules-2"),
        data_root=tmp_path,
    )
    core.open_state()
    try:
        with pytest.raises(BatchNormalizationError):
            core.ingest()
        (record,) = core.quarantine.read_records().to_pylist()
        assert record["rule_version"] == "rules-2"

        core.ingest()
        entries = active_manifest_entries(core.store.dataset_root)
        assert entries
        assert {tuple(entry["rule_versions"]) for entry in entries} == {
            ("rules-2",)
        }
    finally:
        core.close_state()


def test_backfill_stamps_the_scalar_rule_version(tmp_path: Path) -> None:
    now = utc_now().replace(second=0, microsecond=0)
    history = [
        BarInput(
            open_ts=now - timedelta(minutes=minutes_ago),
            open="100",
            high="101",
            low="99",
            close="100.5",
            volume="1",
            trade_count=1,
        )
        for minutes_ago in (10, 9, 8)
    ]
    runtime = GatewayRuntime(
        feed_handler=FakeFeedHandler(
            instrument_id=BTC, bars=[], history=history
        ),
        config=_config(),
        data_root=tmp_path,
    )
    with runtime:
        runtime.start()
        entries = active_manifest_entries(runtime.store.dataset_root)
        assert entries
        # Backfill uses the scalar reference rules, not the live Polars rules.
        assert {
            tuple(entry["rule_versions"]) for entry in entries
        } == {(SCALAR_NORMALIZATION_RULE_VERSION,)}
        assert runtime.store.count_bars(interval="1m") == 3
