"""Reusable, deterministic Bar Datasets for migration characterization.

The builder writes real Parquet Data Files through the production
`ParquetBarStore` and real `_manifest.json` files. It deliberately offers named
scenarios (empty, one row, one file, multiple files, cross-partition, inactive
Manifest entry, duplicate rows, revision history, half-open boundaries and the
Revision-heavy pagination corpus for ticket 03) so the characterization tests
and later differential tests can share one data definition.

`open_ts` is the only time field the Store can prune on; `close_ts` is derived
from the interval, exactly like the production write path.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from mdgw.core.bar import Bar
from mdgw.core.schema import INTERVALS
from mdgw.store.manifest import (
    active_manifest_entries,
    read_manifest,
    write_manifest,
)
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

BASE = datetime(2026, 1, 1, tzinfo=UTC)
BTC = "binance:BTC/USDT"
ETH = "binance:ETH/USDT"
FEED = "characterization"


def minute(offset: int, *, base: datetime = BASE) -> datetime:
    """A minute-aligned UTC instant used by every scenario."""
    return base + timedelta(minutes=offset)


def make_bar(  # ruff: ignore[too-many-arguments] - explicit fixture fields
    *,
    instrument_id: str = BTC,
    interval: str = "1m",
    open_ts: datetime = BASE,
    open: str | Decimal = "100",
    high: str | Decimal = "101",
    low: str | Decimal = "99",
    close: str | Decimal = "100.5",
    volume: str | Decimal = "1",
    trade_count: int | None = 1,
    revision: int = 0,
    seq: int | None = None,
    feed: str = FEED,
    aggregation_source: str = "external",
    ingest_ts: datetime | None = None,
) -> Bar:
    """Build one canonical Bar with exact Decimals and a derived `close_ts`."""
    close_ts = open_ts + INTERVALS[interval]
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=instrument_id,
        interval=interval,
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=ingest_ts if ingest_ts is not None else close_ts,
        feed=feed,
        seq=seq,
        aggregation_source=aggregation_source,
        open=Decimal(open) if not isinstance(open, Decimal) else open,
        high=Decimal(high) if not isinstance(high, Decimal) else high,
        low=Decimal(low) if not isinstance(low, Decimal) else low,
        close=Decimal(close) if not isinstance(close, Decimal) else close,
        volume=Decimal(volume) if not isinstance(volume, Decimal) else volume,
        trade_count=trade_count,
        revision=revision,
    )


class BarDatasetBuilder:
    """Append Bars to one Dataset root; each `add` call appends Data Files."""

    def __init__(
        self,
        root: Path,
        *,
        asset_class: str = "crypto",
        compression: str = "zstd",
    ) -> None:
        self.root = root
        self.asset_class = asset_class
        self.store = ParquetBarStore(root, compression=compression)
        self.bars: list[Bar] = []
        self.files: list[dict] = []

    def add(self, bars: Sequence[Bar]) -> BarDatasetBuilder:
        """Write one physical Data File per partition in this batch."""
        if not bars:
            return self
        self.files.extend(
            self.store.write_bars(list(bars), asset_class=self.asset_class)
        )
        self.bars.extend(bars)
        return self

    def build(self) -> BarDataset:
        return BarDataset(
            root=self.root,
            store=self.store,
            bars=tuple(self.bars),
            files=tuple(self.files),
        )


@dataclass(frozen=True)
class BarDataset:
    """A built Dataset plus the rows and Manifest entries that produced it."""

    root: Path
    store: ParquetBarStore
    bars: tuple[Bar, ...]
    files: tuple[dict, ...]

    @property
    def dataset_root(self) -> Path:
        return self.store.dataset_root

    def manifest(self) -> dict:
        return read_manifest(self.dataset_root)

    def active_entries(self) -> list[dict]:
        return active_manifest_entries(self.dataset_root)

    def deactivate(self, *, file_index: int = 0) -> None:
        """Mark one Manifest entry inactive; its file stays on disk."""
        manifest = read_manifest(self.dataset_root)
        manifest["files"][file_index]["active"] = False
        write_manifest(self.dataset_root, manifest)

    def deactivate_path(self, relative_path: str) -> None:
        manifest = read_manifest(self.dataset_root)
        for entry in manifest["files"]:
            if entry["path"] == relative_path:
                entry["active"] = False
        write_manifest(self.dataset_root, manifest)


# -- named scenarios --------------------------------------------------------


def empty_dataset(root: Path) -> BarDataset:
    """No Data Files and no `_manifest.json`; queries must return nothing."""
    return BarDatasetBuilder(root).build()


def single_row_dataset(root: Path) -> BarDataset:
    return (
        BarDatasetBuilder(root)
        .add([make_bar(open_ts=BASE, open="65000.1")])
        .build()
    )


def single_file_dataset(root: Path, *, rows: int = 3) -> BarDataset:
    """All rows live in one Data File, in ascending open_ts order."""
    return (
        BarDatasetBuilder(root)
        .add([make_bar(open_ts=minute(index), seq=index) for index in range(rows)])
        .build()
    )


def multi_file_dataset(
    root: Path,
    *,
    files: int = 3,
    rows_per_file: int = 2,
) -> BarDataset:
    """Multiple Data Files in one partition, appended in time order."""
    builder = BarDatasetBuilder(root)
    for file_index in range(files):
        start = file_index * rows_per_file
        builder.add(
            [
                make_bar(open_ts=minute(start + offset), seq=start + offset)
                for offset in range(rows_per_file)
            ]
        )
    return builder.build()


def cross_partition_dataset(root: Path) -> BarDataset:
    """Two Instruments, two Intervals and two months (four partitions)."""
    builder = BarDatasetBuilder(root)
    # BTC 1m, January.
    builder.add(
        [
            make_bar(instrument_id=BTC, interval="1m", open_ts=minute(0), seq=0),
            make_bar(instrument_id=BTC, interval="1m", open_ts=minute(1), seq=1),
        ]
    )
    # BTC 5m, January: closes at minute 5.
    builder.add(
        [make_bar(instrument_id=BTC, interval="5m", open_ts=minute(0), seq=0)]
    )
    # ETH 1m, February.
    february = datetime(2026, 2, 1, tzinfo=UTC)
    builder.add(
        [
            make_bar(
                instrument_id=ETH,
                interval="1m",
                open_ts=minute(0, base=february),
                seq=0,
            )
        ]
    )
    # BTC 1m, February (same month partition as ETH but a different symbol).
    builder.add(
        [
            make_bar(
                instrument_id=BTC,
                interval="1m",
                open_ts=minute(1, base=february),
                seq=2,
            )
        ]
    )
    return builder.build()


def inactive_entry_dataset(root: Path) -> BarDataset:
    """Two files; the second Manifest entry is inactive but its file exists."""
    builder = BarDatasetBuilder(root)
    builder.add([make_bar(open_ts=minute(0))])
    builder.add([make_bar(open_ts=minute(1))])
    dataset = builder.build()
    dataset.deactivate(file_index=1)
    return dataset


def duplicate_rows_dataset(root: Path) -> BarDataset:
    """The same content twice in one file and once more in a second file."""
    duplicate = make_bar(open_ts=minute(0), open="42.5")
    builder = BarDatasetBuilder(root)
    builder.add([duplicate, duplicate])
    builder.add([duplicate])
    return builder.build()


def revision_history_dataset(root: Path, *, revisions: int = 3) -> BarDataset:
    """One Bar identity with several revisions, each in its own Data File."""
    builder = BarDatasetBuilder(root)
    for revision in range(revisions):
        builder.add(
            [
                make_bar(
                    open_ts=minute(0),
                    revision=revision,
                    open=Decimal("100") + revision,
                )
            ]
        )
    return builder.build()


def equal_revision_ingest_tiebreak_dataset(root: Path) -> BarDataset:
    """Same identity and revision, different `ingest_ts` (latest wins)."""
    builder = BarDatasetBuilder(root)
    builder.add(
        [
            make_bar(
                open_ts=minute(0),
                revision=1,
                ingest_ts=minute(5),
                open="200",
            )
        ]
    )
    builder.add(
        [
            make_bar(
                open_ts=minute(0),
                revision=1,
                ingest_ts=minute(1),
                open="100",
            )
        ]
    )
    return builder.build()


def boundary_dataset(root: Path) -> BarDataset:
    """Rows before/at/after the half-open window and a shared timestamp.

    * `START` is included when passed as the inclusive `start`.
    * `END` is excluded when passed as the exclusive `end`.
    * `START` appears in two Data Files (different revisions) so a query that
      opens the second file cannot lose or duplicate it by file order.
    * Files are appended out of time order to model non-stable scan order.
    """
    builder = BarDatasetBuilder(root)
    builder.add([make_bar(open_ts=minute(9), seq=9)])
    builder.add(
        [
            make_bar(open_ts=minute(5), seq=5),
            make_bar(open_ts=minute(10), seq=10),
        ]
    )
    builder.add([make_bar(open_ts=minute(5), seq=5, revision=1)])
    builder.add([make_bar(open_ts=minute(15), seq=15)])
    return builder.build()


def revision_heavy_dataset(
    root: Path,
    *,
    revisions: int = 4,
    following: int = 5,
) -> BarDataset:
    """Ticket 03 corpus: many revisions of one `open_ts`, then later Bars.

    With four revisions at `minute(0)` and any page `limit < revisions`, the
    current Time Cursor returns `next_start == minute(0)`: the next request
    starts at the same inclusive timestamp and re-reads the same revisions.
    This is the existing behaviour ticket 03 owns; the fixture only prepares
    the data. Each revision lives in its own Data File, and the later Bars are
    split across two more files so the page boundary also crosses files.
    """
    builder = BarDatasetBuilder(root)
    for revision in range(revisions):
        builder.add(
            [
                make_bar(
                    open_ts=minute(0),
                    revision=revision,
                    open=Decimal("100") + revision,
                )
            ]
        )
    half = following // 2
    builder.add(
        [
            make_bar(open_ts=minute(index + 1), seq=index)
            for index in range(half)
        ]
    )
    builder.add(
        [
            make_bar(open_ts=minute(index + 1), seq=index)
            for index in range(half, following)
        ]
    )
    return builder.build()


def large_dataset(
    root: Path,
    rows: int,
    *,
    instrument_id: str = BTC,
    interval: str = "1m",
) -> BarDataset:
    """One Data File with `rows` distinct Bars, for ceilings and benchmarks."""
    builder = BarDatasetBuilder(root)
    builder.add(
        [
            make_bar(
                instrument_id=instrument_id,
                interval=interval,
                open_ts=minute(index),
                seq=index,
                open=Decimal("100") + Decimal(index % 1000) / Decimal("100"),
            )
            for index in range(rows)
        ]
    )
    return builder.build()
