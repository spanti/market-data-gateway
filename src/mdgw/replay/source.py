"""Read the Bars a Replay Session needs from the Parquet Dataset.

The stored physical column is `open_ts`, while the Replay Protocol's range is
defined on `close_ts`. `ReplayBarSource` therefore widens the physical scan by
one Bar Interval: `start_ts` is inclusive and `end_ts` is exclusive.

M5A queries the Session's fixed Manifest Snapshot through DuckDB. One query
does the close-time filter, the highest-Revision choice (latest `ingest_ts` on
a tie), duplicate-subscription resolution and the five-key total order.
`ReplayBarSource` keeps the full `list[Bar]` materialization and the push
control loops unchanged so the complete event sequence can prove query
equivalence.

The pre-DuckDB PyArrow read is kept as a private, test-only path
(`read(..., read=None)`) for migration differential tests and benchmarks.
Production Replay Sessions always pass their `StoreRead`, which selects the
DuckDB seam. There is no production engine switch.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mdgw.core.bar import Bar
from mdgw.core.schema import INTERVALS
from mdgw.core.time import datetime_to_ns
from mdgw.store.parquet_dataset import ReplayReadQuery

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from collections.abc import Iterator, Sequence

    from mdgw.replay.session import BarSubscription
    from mdgw.store.parquet_dataset import ParquetBarStore, StoreRead


def interval_ns(interval: str) -> int:
    """Nanoseconds in a Bar Interval, computed without floats."""
    delta = INTERVALS[interval]
    return (
        (delta.days * 86_400 + delta.seconds) * 1_000_000_000
        + delta.microseconds * 1_000
    )


def bar_order_key(bar: Bar) -> tuple[int, str, bool, int, str, int]:
    """Replay ordering key (deterministic across reads and processes).

    DESIGN §3.4 defines `(close_ts, instrument_id, seq)`. `seq is None` sorts
    after numeric `seq`, which the boolean flag expresses. `interval` and
    `open_ts_ns` are necessary supplements for the DESIGN key: with the same
    `close_ts` and no `seq`, Bars for the same Instrument at different
    Intervals (e.g. a 1m and a 5m Bar closing together) still need a total,
    file-read-order-independent tie-break.
    """
    seq = bar.seq
    return (
        datetime_to_ns(bar.close_ts),
        bar.instrument_id,
        seq is None,
        0 if seq is None else seq,
        bar.interval,
        datetime_to_ns(bar.open_ts),
    )


def _revision_key(bar: Bar) -> tuple[int, int]:
    """Highest `revision` wins; `ingest_ts` breaks equal revisions."""
    return (bar.revision, datetime_to_ns(bar.ingest_ts))


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
    """Reads and total-orders the Bars for a Replay Session."""

    def __init__(self, store: ParquetBarStore) -> None:
        self._store = store

    def read(
        self,
        subscriptions: Sequence[BarSubscription],
        *,
        start_ts_ns: int,
        end_ts_ns: int,
        read: StoreRead | None = None,
    ) -> list[Bar]:
        """Return the deduplicated, ordered Bars within the `close_ts` range.

        A Replay Session passes its long-lived `StoreRead`: the DuckDB query
        then reads the fixed Snapshot and Lease acquired at Session creation.
        Passing no `read` selects the private legacy PyArrow path that is only
        used by migration differential tests and benchmarks.
        """
        queries = replay_read_queries(
            subscriptions,
            start_ts_ns=start_ts_ns,
            end_ts_ns=end_ts_ns,
        )
        if read is not None:
            table = read.read_replay_bars(queries)
            # The query already produced the final order; materialization is
            # kept intact in M5A so the control loops stay unchanged.
            return [Bar(**row) for row in table.to_pylist()]
        return self._read_legacy(
            subscriptions,
            start_ts_ns=start_ts_ns,
            end_ts_ns=end_ts_ns,
        )

    def iter_bars(  # ruff: ignore[no-self-use] - overridden by migration sources
        self,
        subscriptions: Sequence[BarSubscription],
        *,
        start_ts_ns: int,
        end_ts_ns: int,
        read: StoreRead,
        batch_size: int,
    ) -> Iterator[list[Bar]]:
        """Yield the same ordered Bars batch by batch from the fixed Snapshot.

        This is the bounded M5B read: each DuckDB Arrow RecordBatch is converted
        to `Bar` values and handed to the caller immediately. The query (close
        window, Revision choice, duplicate-subscription resolution and five-key
        order) is identical to `read()`; only the materialization is removed.
        """
        queries = replay_read_queries(
            subscriptions,
            start_ts_ns=start_ts_ns,
            end_ts_ns=end_ts_ns,
        )
        for batch in read.iter_replay_batches(queries, batch_size=batch_size):
            yield [Bar(**row) for row in batch.to_pylist()]

    def _read_legacy(
        self,
        subscriptions: Sequence[BarSubscription],
        *,
        start_ts_ns: int,
        end_ts_ns: int,
    ) -> list[Bar]:
        """Pre-DuckDB PyArrow read: private migration/differential path only."""
        kept: dict[tuple[str, str, int], Bar] = {}

        for subscription in subscriptions:
            interval = subscription.interval
            # `close_ts >= start_ts` is equivalent to
            # `open_ts >= start_ts - interval`; the store can push that down.
            physical_start_ns = start_ts_ns - interval_ns(interval)
            batches = self._store.iter_batches(
                instrument_id=subscription.instrument_id,
                interval=interval,
                start_ns=physical_start_ns,
                end_ns=end_ts_ns,
            )
            for batch in batches:
                for row in batch.to_pylist():
                    close_ns = datetime_to_ns(row["close_ts"])
                    if close_ns < start_ts_ns or close_ns >= end_ts_ns:
                        continue
                    bar = Bar(**row)
                    # `(instrument_id, interval, open_ts)` is the Bar identity;
                    # only its newest revision may be replayed.
                    identity = (
                        bar.instrument_id,
                        bar.interval,
                        datetime_to_ns(bar.open_ts),
                    )
                    current = kept.get(identity)
                    if current is None or _revision_key(bar) > _revision_key(current):
                        kept[identity] = bar

        bars = list(kept.values())
        bars.sort(key=bar_order_key)
        return bars
