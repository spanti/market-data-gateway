"""Deep Backfill execution: one InstrumentId and one actual fetch range.

Both the automatic HWM Backfill and the Backfill Job need the same ordered
steps for one Instrument and one range: fetch `1m` history, apply the closed
window and prepare the Normalization input, Normalize in batches, merge against
stored Bars with the shared Revision/parent helper, persist the merged Bars
through an adapter owned by the caller, and collect a structured result.

The module owns that order. Entry points keep their own window selection,
chunking, cancellation, counters, gap ownership and public result mapping:

* the automatic adapter executes one whole-instrument window in one batch and
  uses the Core-owned Aggregator with one fixed ``now``;
* the Backfill Job will execute one day chunk at a time in 500-row batches with
  a per-job Aggregator and per-batch ingest timestamps.

Nothing here knows about ``CollectorCore``, Admin Job/Gap stores, write gates,
Quarantine, health or Operator Audit. Those stay in the adapters.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from mdgw.backfill_common import (
    BASE_INTERVAL,
    group_bars_by_interval,
    interval_ns,
    merge_bar_batch,
)
from mdgw.core.time import datetime_to_ns, ns_to_datetime, to_utc
from mdgw.normalize.batch import BatchNormalizationError, normalize_batch

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Sequence
    from datetime import datetime

    from mdgw.aggregate import BarAggregator
    from mdgw.backfill_common import MergeResult
    from mdgw.core.bar import Bar, RawBar
    from mdgw.ingest.feed_handler import FeedHandler
    from mdgw.normalize.batch import BatchNormalization
    from mdgw.store.parquet_dataset import ParquetBarStore


@dataclass(frozen=True)
class BackfillRange:
    """The actual fetch range for one Instrument; ``end_ns`` is exclusive."""

    instrument_id: str
    start_ns: int
    end_ns: int


@dataclass(frozen=True)
class PreparedInput:
    """Normalization input for one execution range plus execution evidence.

    ``rows`` and ``open_ns`` are parallel and already in batch order. Adapters
    that detect missing minutes or unreadable identities report them here; the
    automatic adapter has neither.
    """

    rows: tuple[RawBar, ...]
    open_ns: tuple[int, ...]
    unreadable_bars: int = 0
    gap_ranges: tuple[tuple[int, int], ...] = ()


@dataclass(frozen=True)
class RawBatch:
    """One Normalization/merge slice of a prepared range."""

    rows: tuple[RawBar, ...]
    open_ns: tuple[int, ...]


class ExecutionObserver(Protocol):
    """Progress and cooperative-cancellation points owned by the adapter.

    ``after_fetch``, ``before_batch`` and ``after_merge`` are the Job's
    cancellation boundaries inside one fetch range; raising from any hook
    aborts the execution before the next persistence call.
    """

    def after_fetch(self, raw_bars: Sequence[RawBar]) -> None: ...

    def after_prepare(self, prepared: PreparedInput) -> None: ...

    def before_batch(self, batch: RawBatch) -> None: ...

    def after_normalize(self, batch: BatchNormalization) -> None: ...

    def after_merge(self, merged: MergeResult) -> None: ...

    def after_persist(self, written: Sequence[Bar]) -> None: ...


@dataclass
class BackfillExecutionResult:
    """Structured, adapter-neutral outcome of one execution range."""

    fetched_bars: int = 0
    accepted_bars: int = 0
    rejected_bars: int = 0
    skipped_bars: int = 0
    revisions: int = 0
    written_bars: int = 0
    written_by_interval: dict[str, int] = field(default_factory=dict)
    gap_ranges: list[tuple[int, int]] = field(default_factory=list)


def prepare_closed_window(
    raw_bars: Iterable[RawBar],
    *,
    start_ns: int,
    end_ns: int,
) -> PreparedInput:
    """Keep closed `1m` Bars inside the half-open execution range.

    A Bar opening at ``t`` is forming while ``t + 1m > end_ns``; the automatic
    adapter's ``end_ns`` is the fixed ``now`` for the pass.
    """
    width_ns = interval_ns(BASE_INTERVAL)
    rows: list[RawBar] = []
    opens: list[int] = []
    for raw in raw_bars:
        if raw.interval != BASE_INTERVAL:
            continue
        open_ns = datetime_to_ns(to_utc(raw.open_ts))
        if open_ns < start_ns or open_ns >= end_ns:
            continue
        if open_ns + width_ns > end_ns:
            continue
        rows.append(raw)
        opens.append(open_ns)
    return PreparedInput(rows=tuple(rows), open_ns=tuple(opens))


def _dedupe_after_normalization(bars: Sequence[Bar]) -> list[Bar]:
    """Collapse accepted rows by `open_ts` (last wins) and sort by it."""
    by_open_ts: dict[int, Bar] = {}
    for bar in bars:
        by_open_ts[datetime_to_ns(bar.open_ts)] = bar
    return [by_open_ts[key] for key in sorted(by_open_ts)]


def _iter_batches(
    prepared: PreparedInput,
    batch_size: int | None,
) -> Iterator[RawBatch]:
    """Split prepared rows in order; ``None`` means one batch for the range."""
    if batch_size is None:
        yield RawBatch(rows=prepared.rows, open_ns=prepared.open_ns)
        return
    if batch_size <= 0:
        msg = "batch_size must be positive"
        raise ValueError(msg)
    for offset in range(0, len(prepared.rows), batch_size):
        yield RawBatch(
            rows=prepared.rows[offset : offset + batch_size],
            open_ns=prepared.open_ns[offset : offset + batch_size],
        )


class BackfillExecution:
    """Execute one Instrument range through the shared Backfill sequence."""

    def __init__(
        self,
        *,
        feed: FeedHandler,
        store: ParquetBarStore,
        aggregator: BarAggregator,
        normalize: Callable[..., BatchNormalization] = normalize_batch,
        merge: Callable[..., MergeResult] = merge_bar_batch,
    ) -> None:
        self._feed = feed
        self._store = store
        self._aggregator = aggregator
        self._normalize = normalize
        self._merge = merge

    def execute(  # ruff: ignore[too-many-arguments,too-many-branches] - adapter seam
        self,
        execution_range: BackfillRange,
        *,
        persist: Callable[[Sequence[Bar]], None],
        ingest_ts: datetime | Callable[[], datetime],
        batch_size: int | None = None,
        prepare: Callable[..., PreparedInput] = prepare_closed_window,
        handle_rejections: Callable[[BatchNormalization], None] | None = None,
        read_bounds: Callable[[RawBatch], tuple[int, int]] | None = None,
        observer: ExecutionObserver | None = None,
    ) -> BackfillExecutionResult:
        """Fetch, prepare, Normalize, merge and persist one actual range."""
        raw_bars = list(
            self._feed.fetch_history(
                execution_range.instrument_id,
                BASE_INTERVAL,
                ns_to_datetime(execution_range.start_ns),
                ns_to_datetime(execution_range.end_ns),
            )
        )
        result = BackfillExecutionResult(fetched_bars=len(raw_bars))
        if observer is not None:
            observer.after_fetch(raw_bars)
        prepared = prepare(
            raw_bars,
            start_ns=execution_range.start_ns,
            end_ns=execution_range.end_ns,
        )
        if len(prepared.rows) != len(prepared.open_ns):
            msg = "prepared rows and open_ns must be parallel"
            raise ValueError(msg)
        result.rejected_bars = prepared.unreadable_bars
        result.gap_ranges = list(prepared.gap_ranges)
        if observer is not None:
            observer.after_prepare(prepared)
        if not prepared.rows:
            return result

        for raw_batch in _iter_batches(prepared, batch_size):
            if observer is not None:
                observer.before_batch(raw_batch)
            batch_ingest_ts = ingest_ts() if callable(ingest_ts) else ingest_ts
            batch = self._normalize(
                list(raw_batch.rows),
                feed=self._feed.name,
                ingest_ts=batch_ingest_ts,
            )
            if batch.rejections:
                if handle_rejections is None:
                    raise BatchNormalizationError(batch.rejections)
                handle_rejections(batch)
            result.accepted_bars += len(batch.bars)
            result.rejected_bars += len(batch.rejections)
            if observer is not None:
                observer.after_normalize(batch)

            bars = _dedupe_after_normalization(batch.bars)
            if not bars:
                continue

            if read_bounds is None:
                read_start_ns = execution_range.start_ns
                read_end_ns = execution_range.end_ns
            else:
                read_start_ns, read_end_ns = read_bounds(raw_batch)
            merged = self._merge(
                store=self._store,
                aggregator=self._aggregator,
                instrument_id=execution_range.instrument_id,
                base_bars=bars,
                read_start_ns=read_start_ns,
                read_end_ns=read_end_ns,
            )
            result.skipped_bars += merged.skipped
            result.revisions += merged.revisions
            if observer is not None:
                observer.after_merge(merged)
            if not merged.written:
                continue

            persist(merged.written)
            result.written_bars += len(merged.written)
            for interval, group in group_bars_by_interval(
                merged.written
            ).items():
                result.written_by_interval[interval] = (
                    result.written_by_interval.get(interval, 0) + len(group)
                )
            if observer is not None:
                observer.after_persist(merged.written)
        return result


__all__ = [
    "BackfillExecution",
    "BackfillExecutionResult",
    "BackfillRange",
    "ExecutionObserver",
    "PreparedInput",
    "RawBatch",
    "prepare_closed_window",
]
