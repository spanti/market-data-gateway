"""Manifest Snapshot and StoreRead: selection before opening, pinned generations.

These tests use only the Store's public read seam: `open_read` returns a
context-managed `StoreRead` whose `count()` / `iter_batches()` / `read_bars()`
all observe one fixed Manifest Snapshot, and the legacy helpers delegate to the
same seam.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from mdgw.core.bar import Bar
from mdgw.core.errors import SchemaVersionMismatch
from mdgw.core.schema import BAR_SCHEMA
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import active_session_locks

UTC = timezone.utc
INSTRUMENT = "binance:BTC/USDT"
START = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)


def _bar(
    minute: int,
    *,
    instrument_id: str = INSTRUMENT,
    interval: str = "1m",
    revision: int = 0,
    base: datetime = START,
) -> Bar:
    open_ts = base + timedelta(minutes=minute)
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
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1"),
        trade_count=1,
        revision=revision,
    )


def _write(store: ParquetBarStore, bars: list[Bar]) -> None:
    store.write_bars(bars, asset_class="crypto")


def _manifest(dataset_root: Path) -> dict:
    return json.loads((dataset_root / "_manifest.json").read_text(encoding="utf-8"))


def _rows(read) -> list[dict]:
    return [row for batch in read.iter_batches() for row in batch.to_pylist()]


class _PausingLeaseStore(ParquetBarStore):
    """Pause after the lease exists but before the generation re-read."""

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.lease_acquired = threading.Event()
        self.resume = threading.Event()

    def _after_lease_acquired(self, _lease) -> None:
        self.lease_acquired.set()
        if not self.resume.wait(10):
            raise RuntimeError("test did not resume the snapshot acquisition")


def test_open_read_prunes_partitions_before_opening_any_file(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0, interval="1m")])
    _write(store, [_bar(0, interval="5m")])

    # Corrupt the unselected partition: a reader that opened every Manifest
    # entry (or globbed the Dataset root) would fail here.
    selected = [
        entry
        for entry in _manifest(store.dataset_root)["files"]
        if entry["partition"]["interval"] == "5m"
    ]
    assert len(selected) == 1
    (store.dataset_root / selected[0]["path"]).write_bytes(b"not a parquet file")
    # A stray Parquet-looking file outside the Manifest must never be opened.
    (store.dataset_root / "stray.parquet").write_bytes(b"not a parquet file")

    with store.open_read(interval="1m") as read:
        table = read.read_bars()
    assert table.num_rows == 1
    assert table.column("interval").to_pylist() == ["1m"]

    # The same selection is repeatable, while the unreferenced stray file was
    # never opened either.
    with store.open_read(interval="1m") as read:
        assert len(_rows(read)) == 1
    # Sanity: the corrupt 5m file is still on disk and the Manifest still
    # lists it as active; it was excluded by interval pruning, not by a
    # missing-file refresh.
    assert (store.dataset_root / selected[0]["path"]).exists()
    assert _manifest(store.dataset_root)["files"][1]["active"] is True


def test_open_read_selects_only_active_manifest_entries(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])
    _write(store, [_bar(1)])

    manifest = _manifest(store.dataset_root)
    manifest["files"][1]["active"] = False
    (store.dataset_root / "_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )

    with store.open_read(interval="1m") as read:
        assert read.snapshot.entries
        assert all(entry["active"] for entry in read.snapshot.entries)
        assert [row["open_ts"] for row in _rows(read)] == [START]


def test_store_read_count_and_read_share_one_snapshot(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0), _bar(1)])

    with store.open_read(interval="1m") as read:
        generation = read.snapshot.generation
        assert read.count() == 2
        # A concurrent write appends a new Data File and switches the
        # Manifest; this read must keep using its fixed Snapshot.
        _write(store, [_bar(2)])
        assert read.snapshot.generation == generation
        assert read.count() == 2
        assert [row["open_ts"] for row in _rows(read)] == [
            START,
            START + timedelta(minutes=1),
        ]

    with store.open_read(interval="1m") as read:
        assert read.snapshot.generation != generation
        assert read.count() == 3


def test_limit_zero_probe_and_read_share_one_snapshot(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])

    with store.open_read(interval="1m") as read:
        assert read.read_bars(limit=0).num_rows == 0
        _write(store, [_bar(1)])
        assert read.count() == 1
        assert read.read_bars().num_rows == 1


def test_store_read_context_releases_its_lease_on_success(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])

    with store.open_read(interval="1m") as read:
        assert [lock.session_id for lock in active_session_locks(store.dataset_root)]
        assert read.lease is not None
        assert read.count() == 1
    assert active_session_locks(store.dataset_root) == []


def test_store_read_releases_its_lease_on_exception(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])

    with pytest.raises(RuntimeError, match="boom"):
        with store.open_read(interval="1m"):
            raise RuntimeError("boom")

    assert active_session_locks(store.dataset_root) == []


def test_store_read_repeated_close_is_safe_and_releases_the_lease(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])

    read = store.open_read(interval="1m")
    assert active_session_locks(store.dataset_root)
    read.close()
    read.close()
    assert active_session_locks(store.dataset_root) == []


def test_legacy_iter_batches_closing_the_generator_releases_the_lease(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0), _bar(1)])

    iterator = store.iter_batches(interval="1m")
    batch = next(iterator)
    assert batch.num_rows
    assert active_session_locks(store.dataset_root)
    iterator.close()
    assert active_session_locks(store.dataset_root) == []


def test_legacy_helpers_delegate_to_the_same_snapshot_seam(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0), _bar(1)])

    assert store.count_bars(interval="1m") == 2
    table = store.read_bars(interval="1m")
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 2
    assert active_session_locks(store.dataset_root) == []


def test_missing_file_refreshes_the_snapshot_once_before_yielding(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])
    _write(store, [_bar(1)])

    read = store.open_read(interval="1m")
    try:
        # Simulate an external deletion that ignores the lease, followed by a
        # Manifest switch: the reader must retry against the new generation
        # instead of failing or silently dropping the deleted file's rows.
        manifest = _manifest(store.dataset_root)
        victim = manifest["files"][0]
        (store.dataset_root / victim["path"]).unlink()
        manifest["files"][0]["active"] = False
        (store.dataset_root / "_manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )

        rows = _rows(read)
        assert [row["open_ts"] for row in rows] == [
            START + timedelta(minutes=1)
        ]
        assert read.snapshot.entries
        assert all(entry["active"] for entry in read.snapshot.entries)
    finally:
        read.close()
    assert active_session_locks(store.dataset_root) == []


def test_open_read_preserves_schema_version_validation(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])
    descriptor_path = store.dataset_root / "_dataset.json"
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    descriptor["schema_version"] = 2
    descriptor_path.write_text(json.dumps(descriptor), encoding="utf-8")

    with pytest.raises(SchemaVersionMismatch):
        store.open_read(interval="1m")
    assert active_session_locks(store.dataset_root) == []


def test_manifest_change_during_lease_acquisition_retries_with_a_new_snapshot(
    tmp_path: Path,
) -> None:
    writer = ParquetBarStore(tmp_path)
    _write(writer, [_bar(0)])

    store = _PausingLeaseStore(tmp_path)
    result: dict[str, object] = {}

    def read() -> None:
        try:
            result["table"] = store.read_bars(interval="1m")
        except BaseException as exc:  # noqa: BLE001 - asserted below
            result["error"] = exc

    thread = threading.Thread(target=read, name="mdgw-lease-race-reader")
    thread.start()
    assert store.lease_acquired.wait(5)

    # The writer switches the Manifest while the reader holds a lease for the
    # generation it selected. The re-read must invalidate that lease and retry.
    _write(writer, [_bar(1)])
    store.resume.set()
    thread.join(10)
    assert not thread.is_alive()
    if "error" in result:
        raise result["error"]
    table = result["table"]
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert active_session_locks(store.dataset_root) == []
