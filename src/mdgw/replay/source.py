"""Read the Bars a Replay Session needs from the Parquet Dataset.

The stored physical column is `open_ts`, while the Replay Protocol's range is
defined on `close_ts`. `ReplayBarSource` therefore widens the physical scan by
one Bar Interval: `start_ts` is inclusive and `end_ts` is exclusive.

A Replay Session queries its fixed Manifest Snapshot through DuckDB. One
query does the close-time filter, the highest-Revision choice (latest
`ingest_ts` on a tie), duplicate-subscription resolution and the five-key
total order. `iter_bars()` exposes that query as a bounded batch stream,
consumed by all three Replay Modes (realtime and step once per query
generation). There is no full materialization and no second read path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mdgw.core.bar import Bar
from mdgw.core.schema import INTERVALS
from mdgw.store.parquet_dataset import ReplayReadQuery

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from collections.abc import Iterator, Sequence

    from mdgw.replay.session import BarSubscription
    from mdgw.store.parquet_dataset import ReplayScanCancellation, StoreRead


def interval_ns(interval: str) -> int:
    """Nanoseconds in a Bar Interval, computed without floats."""
    delta = INTERVALS[interval]
    return (
        (delta.days * 86_400 + delta.seconds) * 1_000_000_000
        + delta.microseconds * 1_000
    )


def replay_read_queries(
    subscriptions: Sequence[BarSubscription],
    *,
    start_ts_ns: int,
    end_ts_ns: int,
) -> tuple[ReplayReadQuery, ...]:
    """Distinct `(instrument_id, interval)` scan windows for one Snapshot.

    Identical subscriptions are collapsed before the query is built, so the
    number of `UNION ALL` branches never depends on client duplication. The
    physical `open_ts` start is widened by one Bar Interval; the `close_ts`
    window itself stays exactly `[start_ts, end_ts)`.
    """
    seen: set[tuple[str, str]] = set()
    queries: list[ReplayReadQuery] = []
    for subscription in subscriptions:
        key = (subscription.instrument_id, subscription.interval)
        if key in seen:
            continue
        seen.add(key)
        queries.append(
            ReplayReadQuery(
                instrument_id=subscription.instrument_id,
                interval=subscription.interval,
                open_start_ns=start_ts_ns - interval_ns(subscription.interval),
                close_start_ns=start_ts_ns,
                close_end_ns=end_ts_ns,
            )
        )
    return tuple(queries)


class ReplayBarSource:
    """Turns one StoreRead's DuckDB Replay query into ordered Bar batches."""

    def iter_bars(  # ruff: ignore[no-self-use,too-many-arguments] - explicit query seam
        self,
        subscriptions: Sequence[BarSubscription],
        *,
        start_ts_ns: int,
        end_ts_ns: int,
        read: StoreRead,
        batch_size: int,
        cancellation: ReplayScanCancellation | None = None,
    ) -> Iterator[list[Bar]]:
        """Yield the ordered Bars batch by batch from the fixed Snapshot.

        This is the only Replay read: each DuckDB Arrow RecordBatch is
        converted to `Bar` values and handed to the caller immediately. The
        query does the close-window filter, Revision choice,
        duplicate-subscription resolution and five-key order; the caller never
        holds the complete result.

        Realtime passes a per-generation `cancellation` handle so a seek can
        interrupt this exact query while the Session's Snapshot and lease stay
        alive for the next generation.
        """
        queries = replay_read_queries(
            subscriptions,
            start_ts_ns=start_ts_ns,
            end_ts_ns=end_ts_ns,
        )
        batches = (
            read.iter_replay_batches(queries, batch_size=batch_size)
            if cancellation is None
            else read.iter_replay_batches(
                queries,
                batch_size=batch_size,
                cancellation=cancellation,
            )
        )
        for batch in batches:
            yield [Bar(**row) for row in batch.to_pylist()]
