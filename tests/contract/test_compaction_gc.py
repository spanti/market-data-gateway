"""Compaction and Retention/Garbage Collection through the Store interface."""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from mdgw.core.bar import Bar
from mdgw.store import maintenance
from mdgw.store.maintenance import parse_duration
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import (
    DatasetReadLease,
    active_session_locks,
    create_session_lock,
    release_session_lock,
)

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


# -- Dataset Read Lease cooperation -----------------------------------------


def _lease_for_partition(
    store: ParquetBarStore, *, lease_id: str, file_index: int = 0
) -> DatasetReadLease:
    partition = _active_entries(store.dataset_root)[file_index]["partition"]
    return DatasetReadLease.acquire(
        store.dataset_root,
        lease_id,
        partitions=[partition],
        months=[f"{partition['year']}-{partition['month']}"],
        heartbeat_s=0.05,
    )


def test_compaction_skips_partition_covered_by_a_read_lease_and_compacts_others(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write_minutes(store, [0, 1], instrument_id="binance:BTC/USDT")
    _write_minutes(store, [0, 1], instrument_id="binance:ETH/USDT")
    btc_lease = _lease_for_partition(store, lease_id="read-btc", file_index=0)
    try:
        report = store.compact(
            older_than=timedelta(seconds=0),
            now_ns=time.time_ns() + 1_000_000_000,
        )
    finally:
        btc_lease.close()

    assert any("BTC/USDT" in label for label in report.skipped_locked), report
    assert any("ETH/USDT" in label for label in report.merged_partitions), report
    remaining = _active_entries(store.dataset_root)
    btc = [entry for entry in remaining if "BTC" in entry["path"]]
    eth = [entry for entry in remaining if "ETH" in entry["path"]]
    assert len(btc) == 2
    assert len(eth) == 1
    # The unprotected rows are still complete after re-reading.
    assert store.count_bars(interval="1m") == 4


def test_gc_skips_month_covered_by_a_read_lease_and_removes_other_months(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [_bar(datetime(2024, 1, 15, tzinfo=UTC), instrument_id="binance:BTC/USDT")],
        asset_class="crypto",
    )
    store.write_bars(
        [_bar(datetime(2024, 2, 15, tzinfo=UTC), instrument_id="binance:ETH/USDT")],
        asset_class="crypto",
    )
    january_lease = _lease_for_partition(store, lease_id="read-january")
    try:
        report = store.gc(
            bars_days=90, apply=True, now=datetime(2024, 6, 15, tzinfo=UTC)
        )
    finally:
        january_lease.close()

    assert "2024-01" in report.skipped_locked
    assert "2024-02" in report.removed_partitions
    remaining = _active_entries(store.dataset_root)
    assert len(remaining) == 1
    assert remaining[0]["partition"]["month"] == "01"
    # After the lease is gone the protected month can be collected.
    later = store.gc(bars_days=90, apply=True, now=datetime(2024, 6, 15, tzinfo=UTC))
    assert "2024-01" in later.removed_partitions


def test_expired_read_lease_does_not_block_maintenance(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write_minutes(store, [0, 1])
    stale = DatasetReadLease.acquire(
        store.dataset_root,
        "crashed-reader",
        partitions=[_active_entries(store.dataset_root)[0]["partition"]],
        months=["2024-01"],
        heartbeat_s=3600,
        now_ns=0,
    )
    try:
        assert active_session_locks(store.dataset_root, timeout_s=60) == []
        report = store.compact(
            older_than=timedelta(seconds=0),
            now_ns=time.time_ns() + 1_000_000_000,
        )
        assert len(report.new_files) == 1
        assert report.skipped_locked == ()
        assert len(_active_entries(store.dataset_root)) == 1
    finally:
        stale.close()


def test_lease_acquired_during_compaction_rolls_back_the_merge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write_minutes(store, [0, 1])
    old_paths = [
        store.dataset_root / entry["path"]
        for entry in _active_entries(store.dataset_root)
    ]

    original_write_compacted_file = maintenance._write_compacted_file
    merged_file_written = threading.Event()
    resume_merge = threading.Event()

    def paused_write_compacted_file(store_, partition, eligible):
        entry = original_write_compacted_file(store_, partition, eligible)
        merged_file_written.set()
        assert resume_merge.wait(5), "test did not resume compaction"
        return entry

    monkeypatch.setattr(
        maintenance, "_write_compacted_file", paused_write_compacted_file
    )

    result: dict[str, object] = {}

    def compact() -> None:
        try:
            result["report"] = store.compact(
                older_than=timedelta(seconds=0),
                now_ns=time.time_ns() + 1_000_000_000,
            )
        except BaseException as exc:  # noqa: BLE001 - asserted below
            result["error"] = exc

    thread = threading.Thread(target=compact, name="mdgw-compaction-race")
    thread.start()
    assert merged_file_written.wait(5)

    # The lease appears after compaction's first lock scan but before the
    # Manifest switch; the second guard must roll the merge back.
    lease = _lease_for_partition(store, lease_id="read-mid-compaction")
    resume_merge.set()
    thread.join(10)
    assert not thread.is_alive()
    lease.close()
    if "error" in result:
        raise result["error"]
    report = result["report"]
    assert report.new_files == ()
    assert report.skipped_locked
    assert len(_active_entries(store.dataset_root)) == 2
    assert all(path.exists() for path in old_paths)

    # With the lease gone the same work succeeds.
    allowed = store.compact(
        older_than=timedelta(seconds=0),
        now_ns=time.time_ns() + 2_000_000_000,
    )
    assert len(allowed.new_files) == 1
    assert len(_active_entries(store.dataset_root)) == 1


def test_lease_acquired_after_the_manifest_switch_is_not_unlinked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lease that appears mid-maintenance keeps its old files on disk.

    Maintenance is paused immediately before `write_manifest`, i.e. after its
    initial lock scan. The read acquires a lease for the old generation, which
    the pre-unlink re-check must observe.
    """
    store = ParquetBarStore(tmp_path)
    _write_minutes(store, [0, 1])
    old_paths = [
        store.dataset_root / entry["path"]
        for entry in _active_entries(store.dataset_root)
    ]

    original_write_manifest = maintenance.write_manifest
    reached_switch = threading.Event()
    resume_switch = threading.Event()

    def paused_write_manifest(dataset_root, manifest):
        reached_switch.set()
        assert resume_switch.wait(5), "test did not resume maintenance"
        original_write_manifest(dataset_root, manifest)

    monkeypatch.setattr(maintenance, "write_manifest", paused_write_manifest)

    result: dict[str, object] = {}

    def compact() -> None:
        try:
            result["report"] = store.compact(
                older_than=timedelta(seconds=0),
                now_ns=time.time_ns() + 1_000_000_000,
            )
        except BaseException as exc:  # noqa: BLE001 - asserted below
            result["error"] = exc

    thread = threading.Thread(target=compact, name="mdgw-maintenance-race")
    thread.start()
    assert reached_switch.wait(5)

    # The Manifest has not switched yet, so this lease covers the old files.
    read = store.open_read(interval="1m")
    assert read.count() == 2

    resume_switch.set()
    thread.join(10)
    assert not thread.is_alive()
    if "error" in result:
        raise result["error"]
    report = result["report"]
    assert report.skipped_locked
    assert all(path.exists() for path in old_paths)

    # The held snapshot still reads the rows it selected; closing the lease
    # lets a later maintenance run remove the now-inactive files.
    assert read.count() == 2
    read.close()
    assert active_session_locks(store.dataset_root) == []


def test_gc_lease_acquired_after_the_manifest_switch_is_not_unlinked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [_bar(datetime(2024, 1, 15, tzinfo=UTC))], asset_class="crypto"
    )
    old_path = store.dataset_root / _active_entries(store.dataset_root)[0]["path"]

    original_write_manifest = maintenance.write_manifest
    reached_switch = threading.Event()
    resume_switch = threading.Event()

    def paused_write_manifest(dataset_root, manifest):
        reached_switch.set()
        assert resume_switch.wait(5), "test did not resume maintenance"
        original_write_manifest(dataset_root, manifest)

    monkeypatch.setattr(maintenance, "write_manifest", paused_write_manifest)

    result: dict[str, object] = {}

    def gc() -> None:
        try:
            result["report"] = store.gc(
                bars_days=90,
                apply=True,
                now=datetime(2024, 6, 15, tzinfo=UTC),
            )
        except BaseException as exc:  # noqa: BLE001 - asserted below
            result["error"] = exc

    thread = threading.Thread(target=gc, name="mdgw-gc-race")
    thread.start()
    assert reached_switch.wait(5)

    read = store.open_read(interval="1m")
    assert read.count() == 1

    resume_switch.set()
    thread.join(10)
    assert not thread.is_alive()
    if "error" in result:
        raise result["error"]
    report = result["report"]
    assert "2024-01" in report.skipped_locked
    assert old_path.exists()
    # The skipped entry was put back, so the data is not silently orphaned.
    assert len(_active_entries(store.dataset_root)) == 1
    assert read.count() == 1
    read.close()
    # A later GC can now collect the month.
    later = store.gc(bars_days=90, apply=True, now=datetime(2024, 6, 15, tzinfo=UTC))
    assert "2024-01" in later.removed_partitions
    assert not old_path.exists()
