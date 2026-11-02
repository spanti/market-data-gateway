"""Read the Bars a Replay Session needs from the Parquet Dataset.

The Parquet read path can only push the time filter down to `open_ts` (the
partition/column actually stored), while the Replay Protocol's range is defined
on `close_ts`. `ReplayBarSource` therefore widens the physical scan by one Bar
Interval, then filters on `close_ts` in Python: `start_ts` is inclusive and
`end_ts` is exclusive.

v1 reads every matching Bar before the first message is published. Streaming
pages are a ticket-14 performance concern, not a correctness one.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from mdgw.core.bar import Bar
from mdgw.core.schema import INTERVALS
from mdgw.core.time import datetime_to_ns
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from mdgw.replay.session import BarSubscription


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


class ReplayBarSource:
    """Reads and total-orders the Bars for a Replay Session."""

    def __init__(self, store: ParquetBarStore) -> None:
        self._store = store

    def read(
        self,
        subscriptions: Sequence["BarSubscription"],
        *,
        start_ts_ns: int,
        end_ts_ns: int,
    ) -> list[Bar]:
        """Return the deduplicated, ordered Bars within the `close_ts` range."""
        kept: dict[tuple[str, str, int], Bar] = {}

        for subscription in subscriptions:
            interval = subscription.interval
            # `close_ts >= start_ts` is equivalent to
            # `open_ts >= start_ts - interval`; the store can push that down.
            physical_start_ns = start_ts_ns - interval_ns(interval)
            for batch in self._store.iter_batches(
                instrument_id=subscription.instrument_id,
                interval=interval,
                start_ns=physical_start_ns,
                end_ns=end_ts_ns,
            ):
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
