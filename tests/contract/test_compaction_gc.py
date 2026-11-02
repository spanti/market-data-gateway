"""Compaction and Retention/Garbage Collection through the Store interface."""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from mdgw.core.bar import Bar
from mdgw.store.maintenance import parse_duration
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import create_session_lock, release_session_lock

UTC = timezone.utc


def test_parse_duration_supports_operator_notation() -> None:
    assert parse_duration("24h") == timedelta(hours=24)
    assert parse_duration("7d") == timedelta(days=7)
    assert parse_duration("0s") == timedelta(0)
    with pytest.raises(ValueError):
        parse_duration("tomorrow")


def _bar(
    open_ts: datetime,
    *,
    instrument_id: str = "binance:BTC/USDT",
    interval: str = "1m",
) -> Bar:
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=instrument_id,
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
        revision=0,
    )


def _write_minutes(
    store: ParquetBarStore,
    minutes: range | list[int],
    *,
    instrument_id: str = "binance:BTC/USDT",
    start: datetime = datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
) -> None:
    for minute in minutes:
        store.write_bars(
            [_bar(start + timedelta(minutes=minute), instrument_id=instrument_id)],
            asset_class="crypto",
        )


def _manifest(dataset_root: Path) -> dict:
    return json.loads((dataset_root / "_manifest.json").read_text(encoding="utf-8"))


def _active_entries(dataset_root: Path) -> list[dict]:
    return [entry for entry in _manifest(dataset_root)["files"] if entry["active"]]


def _inactive_entries(dataset_root: Path) -> list[dict]:
    return [entry for entry in _manifest(dataset_root)["files"] if not entry["active"]]


def _sorted_table(store: ParquetBarStore) -> pa.Table:
    return store.read_bars(interval="1m").sort_by("open_ts")


def test_compaction_merges_small_files_and_switches_manifest(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write_minutes(store, [0, 1, 2])
    before = _sorted_table(store)
    old_entries = _active_entries(store.dataset_root)
    old_paths = [store.dataset_root / entry["path"] for entry in old_entries]
    assert len(old_entries) == 3

    report = store.compact(
        older_than=timedelta(seconds=0),
        now_ns=time.time_ns() + 1_000_000_000,
    )

    active = _active_entries(store.dataset_root)
    inactive = _inactive_entries(store.dataset_root)
    assert len(active) == 1
    assert len(inactive) == 3
    assert len(report.new_files) == 1
    assert len(report.removed_files) == 3
    assert report.merged_partitions

    new_path = store.dataset_root / active[0]["path"]
    assert new_path.exists()
    assert active[0]["row_count"] == 3
    assert "/symbol=BTC%2FUSDT/" in active[0]["path"]
    assert all(not path.exists() for path in old_paths)
    assert not (store.dataset_root / "_manifest.json.tmp").exists()

    after = _sorted_table(store)
    assert after.schema == before.schema
    assert after.equals(before)


def test_compaction_is_idempotent_and_skips_single_files(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write_minutes(store, [0, 1])

    first = store.compact(
        older_than=timedelta(seconds=0),
        now_ns=time.time_ns() + 1_000_000_000,
    )
    assert len(first.new_files) == 1
    active_path = _active_entries(store.dataset_root)[0]["path"]

    second = store.compact(
        older_than=timedelta(seconds=0),
        now_ns=time.time_ns() + 2_000_000_000,
    )
    assert second.new_files == ()
    assert second.removed_files == ()
    assert second.skipped_single
    assert _active_entries(store.dataset_root)[0]["path"] == active_path


def test_compaction_only_processes_files_older_than_threshold(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write_minutes(store, [0, 1])
    oldest_created = min(
        entry["created_ns"] for entry in _active_entries(store.dataset_root)
    )

    report = store.compact(
        older_than=timedelta(hours=1),
        now_ns=oldest_created,
    )

    assert report.new_files == ()
    assert len(_active_entries(store.dataset_root)) == 2


def test_compaction_skips_partitions_with_active_session_lock(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write_minutes(store, [0, 1])
    create_session_lock(
        store.dataset_root,
        "replay-session",
        subscriptions=[{"instrument_id": "binance:BTC/USDT", "interval": "1m"}],
    )

    blocked = store.compact(
        older_than=timedelta(seconds=0),
        now_ns=time.time_ns() + 1_000_000_000,
    )
    assert blocked.new_files == ()
    assert blocked.skipped_locked
    assert len(_active_entries(store.dataset_root)) == 2

    release_session_lock(store.dataset_root, "replay-session")
    allowed = store.compact(
        older_than=timedelta(seconds=0),
        now_ns=time.time_ns() + 2_000_000_000,
    )
    assert len(allowed.new_files) == 1
    assert len(_active_entries(store.dataset_root)) == 1


def test_gc_dry_run_reports_without_deleting(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [_bar(datetime(2024, 1, 15, tzinfo=UTC))], asset_class="crypto"
    )
    now = datetime(2024, 6, 15, tzinfo=UTC)

    report = store.gc(bars_days=90, apply=False, now=now)

    assert report.dry_run is True
    assert report.removed_files
    assert report.removed_partitions == ("2024-01",)
    for entry in _active_entries(store.dataset_root):
        assert (store.dataset_root / entry["path"]).exists()
    assert len(_active_entries(store.dataset_root)) == 1


def test_gc_apply_deletes_whole_old_months_and_keeps_current_month(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [_bar(datetime(2024, 1, 15, tzinfo=UTC), instrument_id="binance:BTC/USDT")],
        asset_class="crypto",
    )
    store.write_bars(
        [_bar(datetime(2024, 2, 15, tzinfo=UTC), instrument_id="binance:ETH/USDT")],
        asset_class="crypto",
    )
    store.write_bars(
        [_bar(datetime(2024, 6, 15, tzinfo=UTC), instrument_id="binance:SOL/USDT")],
        asset_class="crypto",
    )
    descriptor_before = json.loads(
        (store.dataset_root / "_dataset.json").read_text(encoding="utf-8")
    )
    now = datetime(2024, 6, 15, tzinfo=UTC)

    report = store.gc(bars_days=90, apply=True, now=now)

    assert report.dry_run is False
    assert report.removed_partitions == ("2024-01", "2024-02")
    assert report.skipped_current_month == ("2024-06",)
    assert report.failures == ()
    assert len(_active_entries(store.dataset_root)) == 1
    assert len(_inactive_entries(store.dataset_root)) == 2
    assert not (store.dataset_root / "asset_class=crypto/venue=binance/symbol=BTC%2FUSDT").exists()
    assert not (store.dataset_root / "asset_class=crypto/venue=binance/symbol=ETH%2FUSDT").exists()
    assert (store.dataset_root / "asset_class=crypto/venue=binance/symbol=SOL%2FUSDT").exists()
    descriptor_after = json.loads(
        (store.dataset_root / "_dataset.json").read_text(encoding="utf-8")
    )
    assert descriptor_after == descriptor_before


def test_gc_skips_month_with_active_session_lock(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [_bar(datetime(2024, 1, 15, tzinfo=UTC))], asset_class="crypto"
    )
    create_session_lock(
        store.dataset_root,
        "replay-session",
        subscriptions=[{"instrument_id": "binance:BTC/USDT", "interval": "1m"}],
    )
    now = datetime(2024, 6, 15, tzinfo=UTC)

    blocked = store.gc(bars_days=90, apply=True, now=now)
    assert blocked.removed_partitions == ()
    assert blocked.skipped_locked == ("2024-01",)
    assert len(_active_entries(store.dataset_root)) == 1

    release_session_lock(store.dataset_root, "replay-session")
    allowed = store.gc(bars_days=90, apply=True, now=now)
    assert allowed.removed_partitions == ("2024-01",)
    assert _active_entries(store.dataset_root) == []


def test_gc_is_a_no_op_when_retention_is_permanent(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [_bar(datetime(2024, 1, 15, tzinfo=UTC))], asset_class="crypto"
    )

    report = store.gc(bars_days=0, apply=True, now=datetime(2024, 6, 15, tzinfo=UTC))

    assert report.removed_files == ()
    assert len(_active_entries(store.dataset_root)) == 1
