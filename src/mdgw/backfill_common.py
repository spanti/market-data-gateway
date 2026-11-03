"""Shared idempotent Bar merge helpers used by live Backfill and archive import.

Both `mdgw.backfill.run_backfill` and `mdgw.archive.importer` need the same
semantics for `(instrument_id, interval, open_ts)`:

* an absent Bar is written;
* an identical stored Bar is skipped (and may still be fed to the Aggregator so
  a parent period can be rebuilt);
* a changed Bar is written again with
  `revision = max(stored_revision + 1, candidate_revision)`.

Keeping the merge here means the two entry points cannot drift apart. This
module deliberately does not write anything: callers decide when to call
`ParquetBarStore.write_bars`, so an Instrument lookup failure cannot happen
before there is something to write.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from mdgw.core.bar import Bar
from mdgw.core.schema import INTERVALS
from mdgw.core.time import datetime_to_ns, to_utc
from mdgw.store.parquet_dataset import ParquetBarStore

BASE_INTERVAL = "1m"

# Resolve the Bars already known for a `(instrument, interval, open_ts)` key.
# The live collector supplies a reader that also sees unflushed buffered Bars;
# Backfill and archive import fall back to the Parquet Dataset.
ExistingBarReader = Callable[[str, str, int, int], dict[int, Bar]]


@dataclass
class MergeResult:
    """Bars that still need writing plus the counts observed while merging."""

    written: list[Bar] = field(default_factory=list)
    skipped: int = 0
    revisions: int = 0


def interval_ns(interval: str) -> int:
    delta = INTERVALS[interval]
    return (
        delta.days * 86_400 * 1_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def read_existing_bars(
    store: ParquetBarStore,
    instrument_id: str,
    interval: str,
    start_ns: int,
    end_ns: int,
) -> dict[int, Bar]:
    """Stored Bars in the range, keeping the greatest revision per `open_ts`."""
    existing: dict[int, Bar] = {}
    for batch in store.iter_batches(
        instrument_id=instrument_id,
        interval=interval,
        start_ns=start_ns,
        end_ns=end_ns,
    ):
        for row in batch.to_pylist():
            bar = _row_to_bar(row)
            key = datetime_to_ns(bar.open_ts)
            previous = existing.get(key)
            if previous is None or bar.revision > previous.revision:
                existing[key] = bar
    return existing


def merge_bar_batch(
    *,
    store: ParquetBarStore,
    aggregator,
    instrument_id: str,
    base_bars: Sequence[Bar],
    read_start_ns: int,
    read_end_ns: int,
    existing_reader: ExistingBarReader | None = None,
) -> MergeResult:
    """Merge base-interval Bars with what is stored and rebuild their parents.

    `read_start_ns`/`read_end_ns` bound the existing-Bar lookup; `read_end_ns`
    is exclusive. Parent lookups start at the epoch-aligned boundary so a
    parent window that begins before `read_start_ns` is still found.

    `existing_reader` lets the live collector include Bars that are buffered but
    not yet flushed. Duplicates inside one batch are collapsed too, so a Feed
    Handler that repeats the same closed Bar cannot append it twice.
    """
    read_existing = existing_reader or (
        lambda target_instrument, target_interval, start_ns, end_ns: (
            read_existing_bars(
                store, target_instrument, target_interval, start_ns, end_ns
            )
        )
    )
    existing_base = read_existing(
        instrument_id, BASE_INTERVAL, read_start_ns, read_end_ns
    )

    aggregation_inputs: list[Bar] = []
    base_writes: list[Bar] = []
    staged: dict[int, Bar] = {}
    skipped = 0
    revisions = 0
    for bar in base_bars:
        key = datetime_to_ns(to_utc(bar.open_ts))
        previous = staged.get(key)
        staged_in_batch = previous is not None
        if previous is None:
            previous = existing_base.get(key)
        if previous is None:
            staged[key] = bar
            aggregation_inputs.append(bar)
            base_writes.append(bar)
        elif same_bar_content(bar, previous):
            # Feed the stored revision so a fresh Aggregator rebuilds the same
            # parents without treating the Bar as new. A duplicate already
            # staged in this batch was fed once and must not be fed again.
            if not staged_in_batch:
                aggregation_inputs.append(previous)
            skipped += 1
        else:
            corrected = with_revision(
                bar, next_revision(previous.revision, bar.revision)
            )
            staged[key] = corrected
            aggregation_inputs.append(corrected)
            base_writes.append(corrected)
            revisions += 1

    if not aggregation_inputs:
        return MergeResult()

    parent_writes: list[Bar] = []
    for interval, group in group_bars_by_interval(aggregator.add_many(aggregation_inputs)).items():
        width_ns = interval_ns(interval)
        parent_start_ns = (read_start_ns // width_ns) * width_ns
        existing_parents = read_existing(
            instrument_id, interval, parent_start_ns, read_end_ns
        )
        for bar in group:
            previous = existing_parents.get(datetime_to_ns(to_utc(bar.open_ts)))
            if previous is None:
                parent_writes.append(bar)
            elif same_bar_content(bar, previous):
                skipped += 1
            else:
                parent_writes.append(
                    with_revision(
                        bar, next_revision(previous.revision, bar.revision)
                    )
                )
                revisions += 1

    return MergeResult(
        written=base_writes + parent_writes,
        skipped=skipped,
        revisions=revisions,
    )


def same_bar_content(left: Bar, right: Bar) -> bool:
    """Compare everything except `ingest_ts` and `revision`."""
    return _content_key(left) == _content_key(right)


def _content_key(bar: Bar):
    return (
        bar.schema_version,
        bar.data_type,
        bar.instrument_id,
        bar.interval,
        datetime_to_ns(bar.open_ts),
        datetime_to_ns(bar.close_ts),
        datetime_to_ns(bar.event_ts),
        bar.feed,
        bar.seq,
        bar.aggregation_source,
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.volume,
        bar.trade_count,
    )


def next_revision(stored_revision: int, candidate_revision: int) -> int:
    return max(stored_revision + 1, candidate_revision)


def with_revision(bar: Bar, revision: int) -> Bar:
    return Bar(
        schema_version=bar.schema_version,
        data_type=bar.data_type,
        instrument_id=bar.instrument_id,
        interval=bar.interval,
        open_ts=bar.open_ts,
        close_ts=bar.close_ts,
        event_ts=bar.event_ts,
        ingest_ts=bar.ingest_ts,
        feed=bar.feed,
        seq=bar.seq,
        aggregation_source=bar.aggregation_source,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        trade_count=bar.trade_count,
        revision=revision,
    )


def group_bars_by_interval(bars: Sequence[Bar]) -> dict[str, list[Bar]]:
    grouped: dict[str, list[Bar]] = defaultdict(list)
    for bar in bars:
        grouped[bar.interval].append(bar)
    return grouped


def _row_to_bar(row: dict) -> Bar:
    return Bar(
        schema_version=int(row["schema_version"]),
        data_type=str(row["data_type"]),
        instrument_id=str(row["instrument_id"]),
        interval=str(row["interval"]),
        open_ts=to_utc(row["open_ts"]),
        close_ts=to_utc(row["close_ts"]),
        event_ts=to_utc(row["event_ts"]),
        ingest_ts=to_utc(row["ingest_ts"]),
        feed=str(row["feed"]),
        seq=row["seq"],
        aggregation_source=str(row["aggregation_source"]),
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        volume=row["volume"],
        trade_count=row["trade_count"],
        revision=int(row["revision"]),
    )


__all__ = [
    "BASE_INTERVAL",
    "ExistingBarReader",
    "MergeResult",
    "group_bars_by_interval",
    "interval_ns",
    "merge_bar_batch",
    "next_revision",
    "read_existing_bars",
    "same_bar_content",
    "with_revision",
]
