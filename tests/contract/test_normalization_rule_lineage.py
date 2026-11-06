"""Normalization Rule Version lineage in the Bars Manifest and Quarantine.

Rule Version is audit metadata, not a second Schema authority. New Data File
Manifest entries record the set of Normalization Rule Versions that produced
their rows; missing metadata on old entries means "legacy/unknown". Compaction
unions the source sets and never re-normalizes values. Readers decide Schema
compatibility from the Data Contract Version alone.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from mdgw.core.bar import Bar
from mdgw.core.errors import SchemaVersionMismatch
from mdgw.normalize.version import NORMALIZATION_RULE_VERSION
from mdgw.store.manifest import active_manifest_entries, normalize_manifest_entry
from mdgw.store.parquet_dataset import ParquetBarStore

UTC = timezone.utc


def _bar(
    minute: int,
    *,
    interval: str = "1m",
    revision: int = 0,
) -> Bar:
    open_ts = datetime(2024, 1, 1, 0, 0, tzinfo=UTC) + timedelta(minutes=minute)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id="binance:BTC/USDT",
        interval=interval,
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=None,
        aggregation_source="external",
        open=Decimal("65000.1"),
        high=Decimal("65010.0"),
        low=Decimal("64990.0"),
        close=Decimal("65005.5"),
        volume=Decimal("12.34"),
        trade_count=128,
        revision=revision,
    )


def _manifest(dataset_root: Path) -> dict:
    return json.loads((dataset_root / "_manifest.json").read_text(encoding="utf-8"))


def _write_manifest(dataset_root: Path, manifest: dict) -> None:
    (dataset_root / "_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )


def _active_entries(dataset_root: Path) -> list[dict]:
    return [entry for entry in _manifest(dataset_root)["files"] if entry["active"]]


def _sorted_table(store: ParquetBarStore):
    return store.read_bars(interval="1m").sort_by("open_ts")


def test_new_manifest_entry_records_the_current_rule_version(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0)], asset_class="crypto")

    entry = _manifest(store.dataset_root)["files"][0]
    assert entry["rule_versions"] == [NORMALIZATION_RULE_VERSION]


def test_write_bars_stamps_sorted_unique_explicit_rule_versions(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [_bar(0)],
        asset_class="crypto",
        rule_versions=["rules-b", "rules-a", "rules-b"],
    )

    entry = _manifest(store.dataset_root)["files"][0]
    assert entry["rule_versions"] == ["rules-a", "rules-b"]


def test_missing_rule_versions_means_legacy_and_stays_readable(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0)], asset_class="crypto")
    store.write_bars([_bar(5)], asset_class="crypto")

    manifest = _manifest(store.dataset_root)
    for entry in manifest["files"]:
        entry.pop("rule_versions", None)
    _write_manifest(store.dataset_root, manifest)

    normalized = normalize_manifest_entry(
        _manifest(store.dataset_root)["files"][0], store.dataset_root
    )
    assert normalized["rule_versions"] == []
    assert active_manifest_entries(store.dataset_root)[0]["rule_versions"] == []

    # Legacy metadata never makes the Data Files unreadable.
    assert store.count_bars(interval="1m") == 2
    assert store.read_bars(interval="1m").num_rows == 2
    status = store.status()
    assert status["active_file_count"] == 2
    # The public status shape stays unchanged; lineage is Manifest metadata,
    # not a second status authority.
    assert "rule_versions" not in status


def test_explicit_empty_and_unknown_markers_are_preserved(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0)], asset_class="crypto")
    store.write_bars([_bar(5)], asset_class="crypto")

    manifest = _manifest(store.dataset_root)
    manifest["files"][0]["rule_versions"] = []
    manifest["files"][1]["rule_versions"] = ["legacy", "unknown"]
    _write_manifest(store.dataset_root, manifest)

    entries = active_manifest_entries(store.dataset_root)
    assert entries[0]["rule_versions"] == []
    assert entries[1]["rule_versions"] == ["legacy", "unknown"]
    assert store.count_bars(interval="1m") == 2


def test_unknown_rule_version_never_blocks_a_read_but_contract_version_does(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [_bar(0)], asset_class="crypto", rule_versions=["future-rules-99"]
    )

    # Rule Version is audit metadata: an unknown future value is still read.
    assert store.count_bars(interval="1m") == 1
    assert store.read_bars(interval="1m").num_rows == 1

    descriptor_path = store.dataset_root / "_dataset.json"
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    descriptor["schema_version"] = 2
    descriptor_path.write_text(json.dumps(descriptor), encoding="utf-8")

    # The Data Contract Version alone rejects an incompatible Schema.
    with pytest.raises(SchemaVersionMismatch):
        store.read_bars(interval="1m")


def test_compaction_unions_rule_versions_without_changing_bars(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [_bar(0, revision=3)], asset_class="crypto", rule_versions=["rules-b"]
    )
    store.write_bars(
        [_bar(1, revision=4)],
        asset_class="crypto",
        rule_versions=["rules-a", "rules-b"],
    )
    before = _sorted_table(store)

    report = store.compact(
        older_than=timedelta(seconds=0),
        now_ns=time.time_ns() + 1_000_000_000,
    )

    assert len(report.new_files) == 1
    (entry,) = _active_entries(store.dataset_root)
    assert entry["rule_versions"] == ["rules-a", "rules-b"]
    after = _sorted_table(store)
    assert after.equals(before)
    assert after.column("revision").to_pylist() == [3, 4]


def test_compaction_unions_legacy_unknown_with_known_versions(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [_bar(0)], asset_class="crypto", rule_versions=["rules-a"]
    )
    store.write_bars([_bar(1)], asset_class="crypto")

    manifest = _manifest(store.dataset_root)
    manifest["files"][0].pop("rule_versions")
    manifest["files"][1]["rule_versions"] = ["rules-b"]
    _write_manifest(store.dataset_root, manifest)

    store.compact(
        older_than=timedelta(seconds=0),
        now_ns=time.time_ns() + 1_000_000_000,
    )

    (entry,) = _active_entries(store.dataset_root)
    # The legacy entry contributes nothing because its set is unknown; no
    # historical version is invented for it.
    assert entry["rule_versions"] == ["rules-b"]
    assert store.count_bars(interval="1m") == 2


def test_compaction_of_only_legacy_entries_keeps_an_empty_set(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0)], asset_class="crypto")
    store.write_bars([_bar(1)], asset_class="crypto")

    manifest = _manifest(store.dataset_root)
    for entry in manifest["files"]:
        entry.pop("rule_versions", None)
    _write_manifest(store.dataset_root, manifest)

    store.compact(
        older_than=timedelta(seconds=0),
        now_ns=time.time_ns() + 1_000_000_000,
    )

    (entry,) = _active_entries(store.dataset_root)
    assert entry["rule_versions"] == []
    assert store.count_bars(interval="1m") == 2


def test_repeated_compaction_preserves_the_union_without_duplicates(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [_bar(0, revision=1)], asset_class="crypto", rule_versions=["rules-a"]
    )
    store.write_bars(
        [_bar(1, revision=2)], asset_class="crypto", rule_versions=["rules-b"]
    )

    first = store.compact(
        older_than=timedelta(seconds=0), now_ns=time.time_ns() + 1_000_000_000
    )
    assert len(first.new_files) == 1
    (merged,) = _active_entries(store.dataset_root)
    assert merged["rule_versions"] == ["rules-a", "rules-b"]

    store.write_bars(
        [_bar(2, revision=3)], asset_class="crypto", rule_versions=["rules-c"]
    )
    before = _sorted_table(store)
    second = store.compact(
        older_than=timedelta(seconds=0), now_ns=time.time_ns() + 2_000_000_000
    )
    assert len(second.new_files) == 1
    (merged_again,) = _active_entries(store.dataset_root)
    assert merged_again["rule_versions"] == ["rules-a", "rules-b", "rules-c"]

    # A third pass over the single merged file changes nothing.
    third = store.compact(
        older_than=timedelta(seconds=0), now_ns=time.time_ns() + 3_000_000_000
    )
    assert third.new_files == ()
    (still,) = _active_entries(store.dataset_root)
    assert still["rule_versions"] == ["rules-a", "rules-b", "rules-c"]

    after = _sorted_table(store)
    assert after.equals(before)
    assert after.column("revision").to_pylist() == [1, 2, 3]
