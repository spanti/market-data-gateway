"""Bar Aggregation: closed base-interval Bars -> higher-interval Bars.

Only whole parent periods are produced. A parent is emitted once every expected
base Bar has arrived; partial periods are never published. Parents are derived
from the canonical Bar stream, so prices and volumes stay exact `Decimal`s.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal

from mdgw.core.bar import Bar
from mdgw.core.schema import INTERVALS
from mdgw.core.time import datetime_to_ns, ns_to_datetime, to_utc

AGGREGATION_INTERNAL = "internal"


def _interval_ns(interval: timedelta) -> int:
    """Exact timedelta -> nanoseconds without touching floats."""
    return (
        interval.days * 86_400 * 1_000_000_000
        + interval.seconds * 1_000_000_000
        + interval.microseconds * 1_000
    )


@dataclass
class _ParentWindow:
    """Base Bars collected for one `(instrument, interval, parent open_ts)`."""

    feed: str
    children: dict[int, Bar] = field(default_factory=dict)
    last_emitted: Bar | None = None


class BarAggregator:
    """Aggregate closed base-interval Bars into configured parent intervals.

    `intervals` is the same ordered set as the runtime config; the base interval
    is accepted but never produced as a parent. Parent windows are keyed by
    InstrumentId, interval and UTC epoch-aligned `open_ts`, so out-of-order and
    interleaved input from multiple instruments is safe.
    """

    def __init__(
        self,
        *,
        intervals: Sequence[str],
        base_interval: str = "1m",
        retention_parent_periods: int = 2,
    ) -> None:
        if base_interval not in INTERVALS:
            raise ValueError(f"unsupported base Bar Interval: {base_interval!r}")
        if base_interval != "1m":
            raise ValueError(
                "v1 Bar Aggregation only supports '1m' as the base interval"
            )
        if retention_parent_periods < 1:
            raise ValueError("retention_parent_periods must be at least 1")

        unknown = [value for value in intervals if value not in INTERVALS]
        if unknown:
            raise ValueError(f"unsupported Bar Interval(s): {unknown!r}")
        if len(set(intervals)) != len(intervals):
            raise ValueError("aggregation intervals must not contain duplicates")
        if base_interval not in intervals:
            raise ValueError(
                f"aggregation intervals must contain the base interval {base_interval!r}"
            )

        self.base_interval = base_interval
        self.intervals = tuple(intervals)
        self.retention_parent_periods = retention_parent_periods
        self._base_ns = _interval_ns(INTERVALS[base_interval])
        self._parent_intervals = tuple(
            interval for interval in self.intervals if interval != base_interval
        )
        self._windows: dict[tuple[str, str], dict[int, _ParentWindow]] = {}
        self._newest_parent_ns: dict[tuple[str, str], int] = {}
        self._trimmed_before_ns: dict[tuple[str, str], int] = {}

    def add(self, bar: Bar) -> list[Bar]:
        """Feed one closed Bar and return any parents completed by it.

        Non-base intervals and Base Bars whose `open_ts` is not aligned to the
        base interval are rejected: they cannot come from the canonical stream.
        """
        if bar.interval != self.base_interval:
            raise ValueError(
                f"Bar Aggregation only accepts {self.base_interval!r} Bars, "
                f"got {bar.interval!r}"
            )

        open_ns = datetime_to_ns(to_utc(bar.open_ts))
        if open_ns % self._base_ns:
            raise ValueError(
                f"Bar open_ts must align to the {self.base_interval} boundary: "
                f"{bar.open_ts.isoformat()}"
            )

        produced: list[Bar] = []
        for interval in self._parent_intervals:
            parent = self._add_for_interval(bar, interval, open_ns)
            if parent is not None:
                produced.append(parent)
        return produced

    def add_many(self, bars: Iterable[Bar]) -> list[Bar]:
        produced: list[Bar] = []
        for bar in bars:
            produced.extend(self.add(bar))
        return produced

    # -- internals ---------------------------------------------------------

    def _add_for_interval(
        self, bar: Bar, interval: str, open_ns: int
    ) -> Bar | None:
        interval_ns = _interval_ns(INTERVALS[interval])
        parent_ns = (open_ns // interval_ns) * interval_ns
        key = (bar.instrument_id, interval)

        trimmed_before = self._trimmed_before_ns.get(key)
        if trimmed_before is not None and parent_ns < trimmed_before:
            # The retention window has moved past this period; older revisions
            # are deliberately ignored instead of resurrecting a closed parent.
            return None

        windows = self._windows.setdefault(key, {})
        window = windows.get(parent_ns)
        if window is None:
            window = _ParentWindow(feed=bar.feed)
            windows[parent_ns] = window
        elif window.feed != bar.feed:
            raise ValueError(
                "inconsistent feed within one parent Bar period: "
                f"{window.feed!r} != {bar.feed!r} "
                f"(instrument={bar.instrument_id}, interval={interval}, "
                f"open_ts={ns_to_datetime(parent_ns).isoformat()})"
            )

        index = (open_ns - parent_ns) // self._base_ns
        existing = window.children.get(index)
        if existing is not None and bar.revision <= existing.revision:
            # Duplicate or stale input: same-revision repeats must not produce
            # the parent again, nor move the retention water mark.
            return None

        window.children[index] = bar

        expected = interval_ns // self._base_ns
        if len(window.children) != expected:
            # A partial period cannot be the reference for retention: a lone
            # future Bar must not trim still-open earlier periods.
            return None

        self._advance_retention(key, parent_ns, interval_ns)

        parent = _build_parent(bar, interval, parent_ns, window, expected)
        if window.last_emitted is not None:
            previous = window.last_emitted
            if parent.revision < previous.revision:
                return None
            if parent.revision == previous.revision:
                if _same_content(parent, previous):
                    return None
                # A changed lower-revision child must still move the parent
                # forward so `(instrument, interval, open_ts, revision)` stays
                # unambiguous for readers.
                parent = _with_revision(parent, previous.revision + 1)

        window.last_emitted = parent
        return parent

    def _advance_retention(self, key: tuple[str, str], parent_ns: int, interval_ns: int) -> None:
        newest = self._newest_parent_ns.get(key)
        if newest is None or parent_ns > newest:
            newest = parent_ns
            self._newest_parent_ns[key] = newest

        cutoff = newest - self.retention_parent_periods * interval_ns
        if cutoff <= self._trimmed_before_ns.get(key, 0):
            return
        self._trimmed_before_ns[key] = cutoff

        windows = self._windows.get(key)
        if not windows:
            return
        for old_ns in [value for value in windows if value < cutoff]:
            del windows[old_ns]


def _build_parent(
    bar: Bar,
    interval: str,
    parent_ns: int,
    window: _ParentWindow,
    expected: int,
) -> Bar:
    ordered = [window.children[index] for index in range(expected)]
    open_ts = ns_to_datetime(parent_ns)
    close_ts = open_ts + INTERVALS[interval]
    trade_counts = [child.trade_count for child in ordered]
    return Bar(
        schema_version=bar.schema_version,
        data_type=bar.data_type,
        instrument_id=bar.instrument_id,
        interval=interval,
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=max(child.ingest_ts for child in ordered),
        feed=window.feed,
        seq=None,
        aggregation_source=AGGREGATION_INTERNAL,
        open=ordered[0].open,
        high=max(child.high for child in ordered),
        low=min(child.low for child in ordered),
        close=ordered[-1].close,
        volume=sum((child.volume for child in ordered), Decimal(0)),
        trade_count=None
        if all(value is None for value in trade_counts)
        else sum(value for value in trade_counts if value is not None),
        revision=max(child.revision for child in ordered),
    )


def _same_content(left: Bar, right: Bar) -> bool:
    return (
        left.schema_version,
        left.data_type,
        left.instrument_id,
        left.interval,
        left.open_ts,
        left.close_ts,
        left.event_ts,
        left.ingest_ts,
        left.feed,
        left.seq,
        left.aggregation_source,
        left.open,
        left.high,
        left.low,
        left.close,
        left.volume,
        left.trade_count,
    ) == (
        right.schema_version,
        right.data_type,
        right.instrument_id,
        right.interval,
        right.open_ts,
        right.close_ts,
        right.event_ts,
        right.ingest_ts,
        right.feed,
        right.seq,
        right.aggregation_source,
        right.open,
        right.high,
        right.low,
        right.close,
        right.volume,
        right.trade_count,
    )


def _with_revision(bar: Bar, revision: int) -> Bar:
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
