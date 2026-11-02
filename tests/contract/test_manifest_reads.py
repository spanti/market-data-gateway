"""Manifest-driven reads, Dataset descriptor metadata and `/status` payload.

The Manifest is the authority for which Data Files are readable. A File that is
marked inactive must never be scanned, even when it still exists on disk.
"""

from __future__ import annotations

import json
import statistics
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mdgw.core.bar import Bar
from mdgw.core.time import datetime_to_ns, ns_to_iso
from mdgw.store.parquet_dataset import ParquetBarStore

UTC = timezone.utc


def _bar(
    minute: int,
    *,
    interval: str = "1m",
    instrument_id: str = "binance:BTC/USDT",
    ingest_ts: datetime | None = None,
) -> Bar:
    open_ts = datetime(2024, 1, 1, 0, 0, tzinfo=UTC) + timedelta(minutes=minute)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=instrument_id,
        interval=interval,
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=ingest_ts or close_ts,
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


def _manifest(dataset_root: Path) -> dict:
    return json.loads((dataset_root / "_manifest.json").read_text(encoding="utf-8"))


def _descriptor(dataset_root: Path) -> dict:
    return json.loads((dataset_root / "_dataset.json").read_text(encoding="utf-8"))


def _write_manifest(dataset_root: Path, manifest: dict) -> None:
    (dataset_root / "_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )


def test_manifest_entries_carry_operational_metadata(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0), _bar(1)], asset_class="crypto")

    entry = _manifest(store.dataset_root)["files"][0]
    for field in (
        "path",
        "start_ns",
        "end_ns",
        "row_count",
        "active",
        "size_bytes",
        "created_ns",
        "partition",
    ):
        assert field in entry, field
    assert entry["row_count"] == 2
    assert entry["size_bytes"] == (store.dataset_root / entry["path"]).stat().st_size
    assert entry["partition"] == {
        "asset_class": "crypto",
        "venue": "binance",
        "symbol": "BTC%2FUSDT",
        "interval": "1m",
        "year": "2024",
        "month": "01",
    }


def test_inactive_manifest_entry_is_excluded_from_reads_and_counts(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0)], asset_class="crypto")
    store.write_bars([_bar(5)], asset_class="crypto")

    manifest = _manifest(store.dataset_root)
    first, second = manifest["files"]
    # Hide the second Data File while leaving it on disk.
    second["active"] = False
    _write_manifest(store.dataset_root, manifest)

    assert store.count_bars(interval="1m") == 1
    table = store.read_bars(interval="1m")
    assert table.column("open_ts").to_pylist() == [
        datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    ]
    assert (store.dataset_root / second["path"]).exists()
    assert (store.dataset_root / first["path"]).exists()


def test_dataset_descriptor_records_last_flushed_ns(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    latest_ingest = datetime(2024, 1, 1, 0, 3, 7, tzinfo=UTC)
    store.write_bars([_bar(0), _bar(2, ingest_ts=latest_ingest)], asset_class="crypto")

    descriptor = _descriptor(store.dataset_root)
    assert descriptor["last_flushed_ns"] == datetime_to_ns(latest_ingest)


def test_old_manifest_without_new_fields_remains_readable(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0)], asset_class="crypto")
    store.write_bars([_bar(5)], asset_class="crypto")

    manifest = _manifest(store.dataset_root)
    for entry in manifest["files"]:
        for field in ("size_bytes", "created_ns", "partition", "max_ingest_ns"):
            entry.pop(field, None)
    _write_manifest(store.dataset_root, manifest)

    descriptor = _descriptor(store.dataset_root)
    descriptor.pop("last_flushed_ns")
    (store.dataset_root / "_dataset.json").write_text(
        json.dumps(descriptor), encoding="utf-8"
    )

    assert store.count_bars(interval="1m") == 2
    assert store.read_bars(interval="1m").num_rows == 2

    status = store.status()
    assert status["active_file_count"] == 2
    assert status["last_flushed_ts"] is not None
    assert status["median_file_size_bytes"] > 0


def test_status_reports_overall_and_per_interval_metrics(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0), _bar(1)], asset_class="crypto")
    store.write_bars([_bar(2, interval="5m")], asset_class="crypto")

    status = store.status()
    assert status["dataset"] == "bars"
    assert status["active_file_count"] == 2
    sizes = [
        (store.dataset_root / entry["path"]).stat().st_size
        for entry in _manifest(store.dataset_root)["files"]
        if entry["active"]
    ]
    assert status["median_file_size_bytes"] == statistics.median(sizes)
    assert status["last_flushed_ts"] == ns_to_iso(
        datetime_to_ns(datetime(2024, 1, 1, 0, 3, tzinfo=UTC))
    )

    assert status["intervals"]["1m"]["active_file_count"] == 1
    assert status["intervals"]["5m"]["active_file_count"] == 1
    assert status["intervals"]["1m"]["last_flushed_ts"] == ns_to_iso(
        datetime_to_ns(datetime(2024, 1, 1, 0, 2, tzinfo=UTC))
    )
    assert status["intervals"]["5m"]["last_flushed_ts"] == ns_to_iso(
        datetime_to_ns(datetime(2024, 1, 1, 0, 3, tzinfo=UTC))
    )


def test_status_on_empty_dataset_returns_nulls(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)

    status = store.status()
    assert status["active_file_count"] == 0
    assert status["median_file_size_bytes"] is None
    assert status["last_flushed_ts"] is None
    assert status["intervals"] == {}
