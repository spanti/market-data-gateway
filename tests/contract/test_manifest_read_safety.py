"""Readers keep a consistent snapshot when Compaction/GC switch the Manifest.

The reader is paused with `threading.Event` after it selected the old Manifest
but before it opened any Data File. Maintenance then switches the Manifest and
deletes the old files. The reader must refresh once and return either the old or
the new snapshot, never `FileNotFoundError`.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import pyarrow as pa

from mdgw.core.bar import Bar
from mdgw.store.manifest import active_manifest_entries
from mdgw.store.parquet_dataset import ParquetBarStore

UTC = timezone.utc
INSTRUMENT = "binance:BTC/USDT"
START = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)


def _bar(minute: int) -> Bar:
    open_ts = START + timedelta(minutes=minute)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=INSTRUMENT,
        interval="1m",
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
        revision=0,
    )


class _PausingStore(ParquetBarStore):
    """Pause the first Manifest selection until the test says "continue"."""

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.snapshot_taken = threading.Event()
        self.resume = threading.Event()
        self._calls = 0

    def _active_entries(self):
        entries = super()._active_entries()
        call = self._calls
        self._calls += 1
        if call == 0:
            self.snapshot_taken.set()
            if not self.resume.wait(10):
                raise RuntimeError("test did not resume the reader")
        return entries


def _read(store: ParquetBarStore, reader: str):
    if reader == "read_bars":
        return store.read_bars(interval="1m")
    if reader == "count_bars":
        return store.count_bars(interval="1m")
    if reader == "iter_batches":
        return list(store.iter_batches(interval="1m"))
    raise AssertionError(reader)


def _run_under_switch(tmp_path: Path, reader: str, maintenance) -> tuple:
    writer = ParquetBarStore(tmp_path)
    for minute in range(3):
        writer.write_bars([_bar(minute)], asset_class="crypto")

    paused = _PausingStore(tmp_path)
    result: dict[str, object] = {}

    def read() -> None:
        try:
            result["value"] = _read(paused, reader)
        except BaseException as exc:  # noqa: BLE001 - asserted by the test
            result["error"] = exc

    thread = threading.Thread(target=read, name="mdgw-race-reader")
    thread.start()
    assert paused.snapshot_taken.wait(5)

    maintenance(writer)

    paused.resume.set()
    thread.join(10)
    assert not thread.is_alive()
    if "error" in result:
        raise result["error"]
    return result["value"], writer


def _summarize(value) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, list):
        return sum(batch.num_rows for batch in value)
    assert isinstance(value, pa.Table)
    return value.num_rows


@pytest.mark.parametrize("reader", ["read_bars", "count_bars", "iter_batches"])
def test_compaction_manifest_switch_does_not_break_a_paused_reader(
    tmp_path: Path, reader: str
) -> None:
    value, writer = _run_under_switch(
        tmp_path,
        reader,
        lambda store: store.compact(
            older_than=timedelta(seconds=0),
            now_ns=time.time_ns() + 1_000_000_000,
        ),
    )

    # The retry reads the post-Compaction snapshot, which has the same rows.
    assert _summarize(value) == 3
    assert len(active_manifest_entries(writer.dataset_root)) == 1


@pytest.mark.parametrize("reader", ["read_bars", "count_bars", "iter_batches"])
def test_gc_manifest_switch_does_not_break_a_paused_reader(
    tmp_path: Path, reader: str
) -> None:
    value, _ = _run_under_switch(
        tmp_path,
        reader,
        lambda store: store.gc(
            bars_days=90,
            apply=True,
            now=datetime(2024, 6, 15, tzinfo=UTC),
        ),
    )

    # GC removed the whole month; the refreshed Manifest is a consistent empty
    # snapshot rather than a missing-file error.
    assert _summarize(value) == 0
