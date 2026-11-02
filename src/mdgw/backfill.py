"""Backfill: close the gap between the High-Water Mark and now.

Backfill reads from a Feed Handler's history interface, normalizes the closed
Bars, and appends only what is new or changed. Bars already stored with the same
content are skipped, so repeated Backfill calls are idempotent. A changed source
Bar at an existing `open_ts` is appended as `revision = max(stored) + 1`.

Backfilled Bars go through the same Bar Aggregator as live ingestion and are
written alongside their parents. They are intentionally not published to ZMQ:
the Streaming Interface carries live Bars, and historical consumers read the
Parquet Dataset.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from mdgw.aggregate import BarAggregator
from mdgw.core.bar import Bar, RawBar
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import INTERVALS
from mdgw.core.time import datetime_to_ns, ns_to_datetime, to_utc
from mdgw.ingest.feed_handler import FeedHandler
from mdgw.normalize.bar import normalize_bar
from mdgw.state.health import (
    GAP_BACKFILL_LIMIT,
    GAP_HISTORY_UNSUPPORTED,
    HealthTracker,
)
from mdgw.state.high_water_mark import HighWaterMarkStore
from mdgw.store.parquet_dataset import ParquetBarStore

logger = logging.getLogger(__name__)

BASE_INTERVAL = "1m"


@dataclass
class BackfillReport:
    """Observable result of one Backfill pass."""

    history_supported: bool
    instruments: int = 0
    fetched_bars: int = 0
    written_bars: int = 0
    skipped_bars: int = 0
    gap_events: int = 0
    written_by_interval: dict[str, int] = field(default_factory=dict)


def run_backfill(
    *,
    feed_handler: FeedHandler,
    store: ParquetBarStore,
    hwm: HighWaterMarkStore,
    aggregator: BarAggregator,
    health: HealthTracker,
    asset_class_for: Callable[[str], str],
    now: datetime,
    backfill_hours: float,
) -> BackfillReport:
    """Fill closed 1m Bars from the HWM (or the backfill window) up to `now`."""
    now = to_utc(now)
    now_ns = datetime_to_ns(now)
    window_start = now - timedelta(hours=backfill_hours)
    window_start_ns = datetime_to_ns(window_start)
    report = BackfillReport(history_supported=feed_handler.supports_history)

    if not feed_handler.supports_history:
        for raw_instrument_id in feed_handler.instrument_ids:
            instrument_id = InstrumentId.parse(raw_instrument_id).value
            report.instruments += 1
            hwm_ns = hwm.get(instrument_id, BASE_INTERVAL)
            from_ts = ns_to_datetime(hwm_ns) if hwm_ns is not None else window_start
            _record_gap(
                health,
                feed=feed_handler.name,
                instrument_id=instrument_id,
                from_ts=from_ts,
                to_ts=now,
                reason=GAP_HISTORY_UNSUPPORTED,
            )
            report.gap_events += 1
        return report

    for raw_instrument_id in feed_handler.instrument_ids:
        instrument_id = InstrumentId.parse(raw_instrument_id).value
        report.instruments += 1

        hwm_ns = hwm.get(instrument_id, BASE_INTERVAL)
        if hwm_ns is None:
            start_ns = window_start_ns
        elif hwm_ns < window_start_ns:
            # More history is missing than the configured window can recover.
            start_ns = window_start_ns
            _record_gap(
                health,
                feed=feed_handler.name,
                instrument_id=instrument_id,
                from_ts=ns_to_datetime(hwm_ns),
                to_ts=window_start,
                reason=GAP_BACKFILL_LIMIT,
            )
            report.gap_events += 1
        else:
            # Re-fetch the HWM Bar itself so a corrected last Bar can be fixed.
            start_ns = hwm_ns

        fetched = list(
            feed_handler.fetch_history(
                instrument_id,
                BASE_INTERVAL,
                ns_to_datetime(start_ns),
                now,
            )
        )
        report.fetched_bars += len(fetched)

        history = _normalize_history(
            fetched,
            feed=feed_handler.name,
            ingest_ts=now,
            start_ns=start_ns,
            now_ns=now_ns,
        )
        existing_1m = _read_existing(store, instrument_id, BASE_INTERVAL, start_ns, now_ns)

        aggregation_inputs: list[Bar] = []
        one_minute_writes: list[Bar] = []
        for bar in history:
            key = datetime_to_ns(bar.open_ts)
            previous = existing_1m.get(key)
            if previous is None:
                aggregation_inputs.append(bar)
                one_minute_writes.append(bar)
            elif _same_content(bar, previous):
                # Feed the stored revision so a fresh Aggregator rebuilds the
                # same parents without treating the Bar as new.
                aggregation_inputs.append(previous)
                report.skipped_bars += 1
            else:
                corrected = _with_revision(
                    bar, _next_revision(previous.revision, bar.revision)
                )
                aggregation_inputs.append(corrected)
                one_minute_writes.append(corrected)

        if not aggregation_inputs:
            continue

        parents = aggregator.add_many(aggregation_inputs)
        parent_writes: list[Bar] = []
        for interval, group in _group_by_interval(parents).items():
            interval_ns = _interval_ns(interval)
            parent_start_ns = (start_ns // interval_ns) * interval_ns
            existing_parents = _read_existing(
                store, instrument_id, interval, parent_start_ns, now_ns
            )
            for bar in group:
                previous = existing_parents.get(datetime_to_ns(bar.open_ts))
                if previous is None:
                    parent_writes.append(bar)
                elif _same_content(bar, previous):
                    report.skipped_bars += 1
                else:
                    parent_writes.append(
                        _with_revision(
                            bar, _next_revision(previous.revision, bar.revision)
                        )
                    )

        bars_to_write = one_minute_writes + parent_writes
        if not bars_to_write:
            continue

        store.write_bars(bars_to_write, asset_class=asset_class_for(instrument_id))
        report.written_bars += len(bars_to_write)

        # The HWM advances only after the Data Files and Manifest are durable.
        for interval, group in _group_by_interval(bars_to_write).items():
            hwm.set(
                instrument_id,
                interval,
                max(datetime_to_ns(bar.open_ts) for bar in group),
            )
            report.written_by_interval[interval] = (
                report.written_by_interval.get(interval, 0) + len(group)
            )

        logger.info(
            "backfill wrote %d Bar(s) for %s from %s",
            len(bars_to_write),
            instrument_id,
            ns_to_datetime(start_ns).isoformat(),
            extra={
                "event": "collector.backfill_written",
                "feed": feed_handler.name,
                "instrument_id": instrument_id,
            },
        )

    return report


def _record_gap(
    health: HealthTracker,
    *,
    feed: str,
    instrument_id: str,
    from_ts: datetime | None,
    to_ts: datetime | None,
    reason: str,
) -> None:
    health.record_gap(
        instrument_id=instrument_id,
        interval=BASE_INTERVAL,
        from_ts=from_ts,
        to_ts=to_ts,
        reason=reason,
    )
    logger.warning(
        "gap_unrecoverable instrument=%s interval=%s from=%s to=%s reason=%s",
        instrument_id,
        BASE_INTERVAL,
        None if from_ts is None else from_ts.isoformat(),
        None if to_ts is None else to_ts.isoformat(),
        reason,
        extra={
            "event": "collector.gap_unrecoverable",
            "feed": feed,
            "instrument_id": instrument_id,
        },
    )


def _normalize_history(
    raw_bars: Iterable[RawBar],
    *,
    feed: str,
    ingest_ts: datetime,
    start_ns: int,
    now_ns: int,
) -> list[Bar]:
    """Normalize, filter to the closed window, sort, and dedupe by `open_ts`."""
    interval_ns = _interval_ns(BASE_INTERVAL)
    by_open_ts: dict[int, Bar] = {}
    for raw in raw_bars:
        if raw.interval != BASE_INTERVAL:
            continue
        open_ns = datetime_to_ns(to_utc(raw.open_ts))
        if open_ns < start_ns or open_ns >= now_ns:
            continue
        if open_ns + interval_ns > now_ns:
            # The caller/reference layer rejects forming Bars.
            continue
        bar = normalize_bar(raw, feed=feed, ingest_ts=ingest_ts)
        by_open_ts[open_ns] = bar
    return [by_open_ts[key] for key in sorted(by_open_ts)]


def _read_existing(
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


def _same_content(left: Bar, right: Bar) -> bool:
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


def _next_revision(stored_revision: int, candidate_revision: int) -> int:
    return max(stored_revision + 1, candidate_revision)


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


def _group_by_interval(bars: Sequence[Bar]) -> dict[str, list[Bar]]:
    grouped: dict[str, list[Bar]] = defaultdict(list)
    for bar in bars:
        grouped[bar.interval].append(bar)
    return grouped


def _interval_ns(interval: str) -> int:
    delta = INTERVALS[interval]
    return (
        delta.days * 86_400 * 1_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )
