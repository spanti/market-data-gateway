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
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from mdgw.backfill_common import (
    BASE_INTERVAL,
    group_bars_by_interval,
    interval_ns,
    merge_bar_batch,
)
from mdgw.core.instrument import InstrumentId
from mdgw.core.time import datetime_to_ns, ns_to_datetime, to_utc
from mdgw.normalize.bar import normalize_bar
from mdgw.normalize.version import SCALAR_NORMALIZATION_RULE_VERSIONS
from mdgw.state.health import (
    GAP_BACKFILL_LIMIT,
    GAP_HISTORY_UNSUPPORTED,
    HealthTracker,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from mdgw.aggregate import BarAggregator
    from mdgw.core.bar import Bar, RawBar
    from mdgw.ingest.feed_handler import FeedHandler
    from mdgw.state.high_water_mark import HighWaterMarkStore
    from mdgw.store.parquet_dataset import ParquetBarStore

logger = logging.getLogger(__name__)


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


def run_backfill(  # ruff: ignore[too-many-arguments] -- public entry point; parameters are keyword-only
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
        result = merge_bar_batch(
            store=store,
            aggregator=aggregator,
            instrument_id=instrument_id,
            base_bars=history,
            read_start_ns=start_ns,
            read_end_ns=now_ns,
        )
        report.skipped_bars += result.skipped

        bars_to_write = result.written
        if not bars_to_write:
            continue

        store.write_bars(
            bars_to_write,
            asset_class=asset_class_for(instrument_id),
            # Backfill normalizes with the scalar reference rules, so its
            # lineage must not claim the live Polars Rule Version.
            rule_versions=SCALAR_NORMALIZATION_RULE_VERSIONS,
        )
        report.written_bars += len(bars_to_write)

        # The HWM advances only after the Data Files and Manifest are durable.
        for interval, group in group_bars_by_interval(bars_to_write).items():
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


def _record_gap(  # ruff: ignore[too-many-arguments] -- keeps gap fields explicit for logging and health
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
    width_ns = interval_ns(BASE_INTERVAL)
    by_open_ts: dict[int, Bar] = {}
    for raw in raw_bars:
        if raw.interval != BASE_INTERVAL:
            continue
        open_ns = datetime_to_ns(to_utc(raw.open_ts))
        if open_ns < start_ns or open_ns >= now_ns:
            continue
        if open_ns + width_ns > now_ns:
            # The caller/reference layer rejects forming Bars.
            continue
        bar = normalize_bar(raw, feed=feed, ingest_ts=ingest_ts)
        by_open_ts[open_ns] = bar
    return [by_open_ts[key] for key in sorted(by_open_ts)]
