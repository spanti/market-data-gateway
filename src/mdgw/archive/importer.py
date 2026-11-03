"""Import Binance `data.binance.vision` K-line archives into the Bar Dataset.

The import is deliberately offline and idempotent:

* each archive ZIP is verified with its `.CHECKSUM` before any Bar is written;
* a missing archive (404 or absent locally) is a `missing_file`, not a failure;
  a missing/mismatched CHECKSUM or a malformed file is a `failure`;
* 1m Bars are normalized through the same `normalize_bar` used by live ingest,
  then fed in chronological order to `BarAggregator` so 5m/15m/1h/1d parents
  only exist when their whole period is present;
* the shared `merge_bar_batch` helper skips identical stored Bars and re-appends
  corrected ones with an incremented `revision`;
* the data High-Water Mark is advanced only after a successful write.

Nothing is published to ZMQ: archive import is a maintenance command, not a
live feed.

Concurrency: Dataset writes are serialized only inside one process. Run archive
import while the collector is stopped (or at least staggered); this command does
not add a cross-process lock.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from mdgw.aggregate import BarAggregator
from mdgw.archive.binance import (
    ArchiveFormatError,
    ArchivePeriod,
    SUPPORTED_ARCHIVE_INTERVAL,
    archive_csv_filename,
    parse_checksum,
    parse_kline_csv,
    plan_archive_periods,
    read_zip_csv,
    timestamp_unit_for_date,
)
from mdgw.archive.fetch import ArchiveFetchError, ArchiveFetcher
from mdgw.backfill_common import (
    group_bars_by_interval,
    interval_ns,
    merge_bar_batch,
)
from mdgw.core.bar import RawBar
from mdgw.core.time import datetime_to_ns, to_utc, utc_now
from mdgw.normalize.bar import normalize_bar
from mdgw.state.high_water_mark import HighWaterMarkStore
from mdgw.store.parquet_dataset import ParquetBarStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ArchiveInstrument:
    """The archive-side identity of one Instrument.

    `symbol` is the Binance archive symbol (`BTCUSDT`), not the Gateway symbol
    (`BTC/USDT`).
    """

    instrument_id: str
    symbol: str
    asset_class: str


@dataclass
class ArchiveReport:
    """Observable result of one `mdgw backfill --archive` run."""

    instruments: int = 0
    files: int = 0
    bars_read: int = 0
    bars_written: int = 0
    skipped_existing: int = 0
    revisions: int = 0
    missing_files: int = 0
    skipped_by_retention: int = 0
    failures: int = 0

    @property
    def ok(self) -> bool:
        return self.failures == 0


def import_archive(
    *,
    instruments,
    store: ParquetBarStore,
    hwm: HighWaterMarkStore,
    aggregator: BarAggregator,
    fetcher: ArchiveFetcher,
    start: date,
    end: date,
    feed: str,
    market_type: str = "spot",
    interval: str = SUPPORTED_ARCHIVE_INTERVAL,
    granularity: str = "auto",
    retention_bars_days: int = 0,
    now: datetime | None = None,
    ingest_ts: datetime | None = None,
    today: date | None = None,
) -> ArchiveReport:
    """Import all archive files covering `[start, end]` for every Instrument."""
    if interval != SUPPORTED_ARCHIVE_INTERVAL:
        raise ValueError(
            f"v1 archive import only supports {SUPPORTED_ARCHIVE_INTERVAL!r}, "
            f"got {interval!r}"
        )
    if end < start:
        raise ValueError("archive import end date must not precede start date")

    current = to_utc(now) if now is not None else utc_now()
    ingest_time = to_utc(ingest_ts) if ingest_ts is not None else utc_now()
    planning_today = today if today is not None else current.date()
    cutoff_ns = (
        datetime_to_ns(current - timedelta(days=retention_bars_days))
        if retention_bars_days > 0
        else None
    )

    report = ArchiveReport()
    logger.info(
        "archive import started; stop the collector while importing",
        extra={"event": "archive.import_started", "feed": feed},
    )

    for instrument in sorted(instruments, key=lambda item: item.instrument_id):
        report.instruments += 1
        periods = plan_archive_periods(
            start, end, granularity, today=planning_today
        )
        for period in periods:
            _import_period(
                instrument=instrument,
                period=period,
                report=report,
                store=store,
                hwm=hwm,
                aggregator=aggregator,
                fetcher=fetcher,
                feed=feed,
                market_type=market_type,
                interval=interval,
                start=start,
                end=end,
                cutoff_ns=cutoff_ns,
                ingest_ts=ingest_time,
                requested_granularity=granularity,
            )

    logger.info(
        "archive import finished",
        extra={"event": "archive.import_completed", "feed": feed},
    )
    return report


def _import_period(
    *,
    instrument: ArchiveInstrument,
    period: ArchivePeriod,
    report: ArchiveReport,
    store: ParquetBarStore,
    hwm: HighWaterMarkStore,
    aggregator: BarAggregator,
    fetcher: ArchiveFetcher,
    feed: str,
    market_type: str,
    interval: str,
    start: date,
    end: date,
    cutoff_ns: int | None,
    ingest_ts: datetime,
    requested_granularity: str,
) -> None:
    filename = period.filename(instrument.symbol, interval)
    try:
        data = fetcher.read_zip(
            market_type=market_type,
            period=period,
            symbol=instrument.symbol,
            interval=interval,
        )
    except ArchiveFetchError as exc:
        _failure(report, instrument, feed, f"{filename}: {exc}")
        return
    if data is None:
        if requested_granularity == "auto" and period.granularity == "monthly":
            logger.warning(
                "monthly archive is missing; falling back to daily files",
                extra={
                    "event": "archive.monthly_missing_fallback_daily",
                    "feed": feed,
                    "instrument_id": instrument.instrument_id,
                },
            )
            fallback_start = max(start, period.start)
            fallback_end = min(end, period.end)
            day = fallback_start
            while day <= fallback_end:
                _import_period(
                    instrument=instrument,
                    period=ArchivePeriod("daily", day, day),
                    report=report,
                    store=store,
                    hwm=hwm,
                    aggregator=aggregator,
                    fetcher=fetcher,
                    feed=feed,
                    market_type=market_type,
                    interval=interval,
                    start=start,
                    end=end,
                    cutoff_ns=cutoff_ns,
                    ingest_ts=ingest_ts,
                    requested_granularity=requested_granularity,
                )
                day += timedelta(days=1)
            return
        report.missing_files += 1
        logger.warning(
            "archive file is missing",
            extra={
                "event": "archive.file_missing",
                "feed": feed,
                "instrument_id": instrument.instrument_id,
            },
        )
        return

    try:
        raw_bars = _load_verified_bars(
            data=data,
            instrument=instrument,
            period=period,
            interval=interval,
            market_type=market_type,
            fetcher=fetcher,
        )
    except (ArchiveFetchError, ArchiveFormatError, ValueError) as exc:
        _failure(report, instrument, feed, f"{filename}: {exc}")
        return

    report.files += 1
    report.bars_read += len(raw_bars)

    selected: list[RawBar] = []
    retention_skipped = 0
    for raw in raw_bars:
        day = raw.open_ts.date()
        if not period.contains(day):
            # A monthly archive only contains its own month, but be defensive.
            continue
        if day < start or day > end:
            continue
        open_ns = datetime_to_ns(raw.open_ts)
        if cutoff_ns is not None and open_ns < cutoff_ns:
            retention_skipped += 1
            continue
        selected.append(raw)

    if retention_skipped:
        report.skipped_by_retention += retention_skipped
        logger.info(
            "archive Bars skipped by Retention",
            extra={
                "event": "archive.retention_skipped",
                "feed": feed,
                "instrument_id": instrument.instrument_id,
            },
        )

    if not selected:
        return

    try:
        bars = [
            normalize_bar(raw, feed=feed, ingest_ts=ingest_ts)
            for raw in selected
        ]
    except ValueError as exc:
        _failure(
            report, instrument, feed, f"{filename}: normalize failed: {exc}"
        )
        return

    read_start_ns = min(datetime_to_ns(bar.open_ts) for bar in bars)
    read_end_ns = (
        max(datetime_to_ns(bar.open_ts) for bar in bars)
        + interval_ns(interval)
    )
    result = merge_bar_batch(
        store=store,
        aggregator=aggregator,
        instrument_id=instrument.instrument_id,
        base_bars=bars,
        read_start_ns=read_start_ns,
        read_end_ns=read_end_ns,
    )
    report.skipped_existing += result.skipped
    report.revisions += result.revisions

    if not result.written:
        return

    store.write_bars(result.written, asset_class=instrument.asset_class)
    report.bars_written += len(result.written)

    # The HWM advances only after Data Files and the Manifest are durable. The
    # store uses monotonic MAX semantics, so importing older history cannot
    # move it backwards.
    for bar_interval, group in group_bars_by_interval(result.written).items():
        hwm.set(
            instrument.instrument_id,
            bar_interval,
            max(datetime_to_ns(bar.open_ts) for bar in group),
        )

    logger.info(
        "archive file imported",
        extra={
            "event": "archive.file_imported",
            "feed": feed,
            "instrument_id": instrument.instrument_id,
        },
    )


def _load_verified_bars(
    *,
    data: bytes,
    instrument: ArchiveInstrument,
    period: ArchivePeriod,
    interval: str,
    market_type: str,
    fetcher: ArchiveFetcher,
) -> list[RawBar]:
    checksum_text = fetcher.read_checksum(
        market_type=market_type,
        period=period,
        symbol=instrument.symbol,
        interval=interval,
    )
    if checksum_text is None:
        raise ArchiveFormatError(
            f"missing CHECKSUM for {period.filename(instrument.symbol, interval)}"
        )
    expected = parse_checksum(checksum_text)
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected:
        raise ArchiveFormatError(
            f"SHA-256 mismatch: expected {expected}, got {actual}"
        )

    csv_data = read_zip_csv(
        data,
        expected_name=archive_csv_filename(
            instrument.symbol, interval, period
        ),
    )
    raw_bars = parse_kline_csv(
        csv_data,
        instrument_id=instrument.instrument_id,
        interval=interval,
        unit=timestamp_unit_for_date(period.start),
    )
    # Chronological, de-duplicated order is required by BarAggregator.
    by_open = {bar.open_ts: bar for bar in raw_bars}
    return [by_open[key] for key in sorted(by_open)]


def _failure(
    report: ArchiveReport,
    instrument: ArchiveInstrument,
    feed: str,
    message: str,
) -> None:
    report.failures += 1
    logger.error(
        "archive file failed: %s",
        message,
        extra={
            "event": "archive.file_failed",
            "feed": feed,
            "instrument_id": instrument.instrument_id,
        },
    )


__all__ = ["ArchiveInstrument", "ArchiveReport", "import_archive"]
