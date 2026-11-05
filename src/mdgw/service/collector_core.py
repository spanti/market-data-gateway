"""Collector pipeline shared by `GatewayRuntime` and the collector Service Role.

This module owns the Feed Handler -> normalize -> aggregate -> Parquet -> HWM
-> health pipeline. It deliberately knows nothing about HTTP or the transport:
callers publish the returned Bars however their process requires. Both the
single-process `GatewayRuntime` and the standalone `CollectorService` reuse
this class, so ingest and Backfill logic is never duplicated.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from mdgw.aggregate import BarAggregator
from mdgw.backfill import BackfillReport, run_backfill
from mdgw.backfill_common import interval_ns, merge_bar_batch
from mdgw.core.errors import UnknownInstrumentError
from mdgw.core.schema import INTERVALS
from mdgw.core.time import datetime_to_ns, ns_to_datetime, utc_now
from mdgw.normalize.bar import normalize_bar
from mdgw.reference import (
    InstrumentStore,
    export_instrument_snapshot,
    load_instrument_reference,
)
from mdgw.service.bar_sink import BarSink, ImmediateBarSink
from mdgw.state.health import GapEvent, HealthTracker
from mdgw.state.high_water_mark import HighWaterMarkStore
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from datetime import datetime

    from mdgw.config import GatewayConfig
    from mdgw.core.bar import Bar
    from mdgw.ingest.feed_handler import FeedHandler

logger = logging.getLogger(__name__)

AssetClassResolver = Callable[[str], str]

_NOT_OPEN_MESSAGE = "CollectorCore state is not open"
_OPEN_BEFORE_INGEST_MESSAGE = (
    "CollectorCore.open_state() must be called before ingest()"
)
_OPEN_BEFORE_BACKFILL_MESSAGE = (
    "CollectorCore.open_state() must be called before backfill()"
)


class CollectorCore:
    """One collector pipeline over a single (possibly composite) Feed Handler."""

    def __init__(  # ruff: ignore[too-many-arguments] - keyword-only pipeline wiring
        self,
        *,
        feed_handler: FeedHandler,
        config: GatewayConfig,
        data_root: Path | None = None,
        store: ParquetBarStore | None = None,
        aggregator: BarAggregator | None = None,
        health: HealthTracker | None = None,
        asset_class_for: AssetClassResolver | None = None,
        sink: BarSink | None = None,
        sink_factory: Callable[[ParquetBarStore], BarSink] | None = None,
    ) -> None:
        self.feed_handler = feed_handler
        self.config = config
        self.store = store or ParquetBarStore(
            data_root if data_root is not None else config.storage.root,
            compression=config.storage.compression,
            row_group_size=config.storage.row_group_size,
            format_version=config.storage.format_version,
            schema_version=config.storage.schema_version,
            duckdb=config.storage.duckdb.to_options(),
        )
        self.aggregator = aggregator or BarAggregator(
            intervals=config.aggregation.intervals,
            base_interval=config.aggregation.base_interval,
            retention_parent_periods=config.aggregation.retention_parent_periods,
        )
        self.health = health or HealthTracker(
            stale_after_s=config.collector.stale_after_s
        )
        # `GatewayRuntime` uses the default immediate sink; the collector
        # Service Role injects a buffered sink through `sink_factory`.
        if sink is None:
            sink = (
                sink_factory(self.store)
                if sink_factory is not None
                else ImmediateBarSink(self.store)
            )
        self.sink = sink
        self.sink.bind(
            on_persisted=self._on_persisted,
            asset_class_for=self.asset_class_for,
        )
        # `GatewayRuntime` passes its own resolver because tests replace the
        # runtime's Instrument Store after start.
        self._asset_class_for_callback = asset_class_for
        self.instrument_store: InstrumentStore | None = None
        self.hwm: HighWaterMarkStore | None = None
        self.bars_ingested = 0
        self.bars_written = 0
        self.last_ingest_ts: datetime | None = None

    # -- lifecycle ---------------------------------------------------------

    def open_state(self) -> None:
        """Export the Instrument Snapshot, open the HWM, and run startup Backfill."""
        reference = load_instrument_reference(Path(self.config.instruments.path))
        export = export_instrument_snapshot(
            reference, self.store.root / "instruments"
        )
        self.instrument_store = InstrumentStore(export.path)
        self.hwm = HighWaterMarkStore(
            self.store.root / self.config.collector.state_db_name
        )
        try:
            self.run_backfill(utc_now())
            self.register_expected_keys()
        except Exception:
            self.close_state()
            raise

    def close_state(self) -> None:
        # The sink flushes any buffered Bars before the HWM is closed, so
        # shutdown cannot lose a Bar that was already accepted for writing.
        try:
            self.sink.close()
        finally:
            if self.hwm is not None:
                self.hwm.close()
                self.hwm = None

    # -- collector behavior ------------------------------------------------

    def ingest(self) -> tuple[int, list[Bar]]:
        """Drain the Feed Handler once and persist Bars.

        Returns ``(base_bar_count, published_bars)``. The caller is
        responsible for publication; backfilled history is never returned.
        """
        if self.hwm is None:
            raise RuntimeError(_OPEN_BEFORE_INGEST_MESSAGE)

        ingest_ts = utc_now()
        base_bars = [
            normalize_bar(raw, feed=self.feed_handler.name, ingest_ts=ingest_ts)
            for raw in self.feed_handler.iter_bars()
        ]
        if not base_bars:
            return 0, []

        # Validate every base Bar against the Instrument Snapshot before the
        # aggregator advances any window or retention water mark. Rejecting a
        # batch must leave the Aggregation state untouched so the same Bars can
        # be retried once the reference data is fixed.
        for bar in base_bars:
            self.asset_class_for(bar.instrument_id)

        # Every persistence path uses the same `(instrument, interval,
        # open_ts)` merge: identical content is skipped, corrections get a
        # higher revision, and parents are rebuilt in the same transaction.
        # The sink contributes unflushed Bars to the lookup so buffering cannot
        # append the same Bar twice.
        by_instrument: dict[str, list[Bar]] = defaultdict(list)
        for bar in base_bars:
            by_instrument[bar.instrument_id].append(bar)

        base_interval_ns = interval_ns(self.config.aggregation.base_interval)
        written: list[Bar] = []
        for instrument_id, group in by_instrument.items():
            open_ns = [datetime_to_ns(bar.open_ts) for bar in group]
            result = merge_bar_batch(
                store=self.store,
                aggregator=self.aggregator,
                instrument_id=instrument_id,
                base_bars=group,
                read_start_ns=min(open_ns),
                read_end_ns=max(open_ns) + base_interval_ns,
                existing_reader=self.sink.existing_bars,
            )
            written.extend(result.written)

        if written:
            # HWM, health and freshness advance from the sink's
            # `_on_persisted` callback only after the Store write succeeds.
            self.sink.write_bars(written)

        self.bars_ingested += len(base_bars)
        self.last_ingest_ts = ingest_ts

        instrument_ids = sorted({bar.instrument_id for bar in base_bars})
        logger.info(
            "collector ingested %d base Bar(s), wrote %d Bar(s)",
            len(base_bars),
            len(written),
            extra={
                "event": "collector.batch_ingested",
                "feed": self.feed_handler.name,
                "instrument_id": ",".join(instrument_ids),
            },
        )
        return len(base_bars), written

    def _on_persisted(self, bars: list[Bar] | tuple[Bar, ...]) -> None:
        """Advance data HWM and freshness after Bars were durably written."""
        persisted = list(bars)
        if not persisted:
            return
        self.bars_written += len(persisted)
        self.advance_hwm(persisted)
        for bar in persisted:
            self.health.record_bar(bar)
        self.register_expected_keys()

    def backfill(self, now: datetime | None = None) -> BackfillReport:
        """Backfill closed 1m Bars from the HWM (or the configured window)."""
        if self.hwm is None:
            raise RuntimeError(_OPEN_BEFORE_BACKFILL_MESSAGE)
        return self.run_backfill(now if now is not None else utc_now())

    def run_backfill(self, now: datetime) -> BackfillReport:
        if self.hwm is None:
            raise RuntimeError(_NOT_OPEN_MESSAGE)
        return run_backfill(
            feed_handler=self.feed_handler,
            store=self.store,
            hwm=self.hwm,
            aggregator=self.aggregator,
            health=self.health,
            asset_class_for=self.asset_class_for,
            now=now,
            backfill_hours=self.config.collector.backfill_hours,
        )

    # -- collector state ---------------------------------------------------

    def register_expected_keys(self) -> None:
        """Register one freshness key per expected `(instrument, interval)`."""
        if self.hwm is None:
            return
        feed = self.feed_handler.name
        base_interval = self.config.aggregation.base_interval
        base_interval_delta = INTERVALS[base_interval]
        for instrument_id in self.feed_handler.instrument_ids:
            base_hwm_ns = self.hwm.get(instrument_id, base_interval)
            for interval in self.config.aggregation.intervals:
                own_hwm_ns = self.hwm.get(instrument_id, interval)
                if own_hwm_ns is not None:
                    event_ts = ns_to_datetime(own_hwm_ns) + INTERVALS[interval]
                elif base_hwm_ns is not None:
                    event_ts = ns_to_datetime(base_hwm_ns) + base_interval_delta
                else:
                    event_ts = None
                self.health.register(feed, instrument_id, interval, event_ts)

    def advance_hwm(self, bars: list[Bar]) -> None:
        if self.hwm is None:
            return
        grouped: dict[tuple[str, str], list[Bar]] = defaultdict(list)
        for bar in bars:
            grouped[bar.instrument_id, bar.interval].append(bar)
        for (instrument_id, interval), group in grouped.items():
            candidate = max(datetime_to_ns(bar.open_ts) for bar in group)
            # A Bar dropped by the bounded write queue may never have reached
            # disk. Cap the HWM below it so the next Backfill re-fetches that
            # hole instead of starting after it. The HWM store is monotonic, so
            # a cap cannot undo an already-persisted value.
            cap = self.sink.hwm_cap(instrument_id, interval, candidate)
            effective = candidate if cap is None or cap >= candidate else cap
            if effective < 0 and self.hwm.get(instrument_id, interval) is None:
                continue
            self.hwm.set(instrument_id, interval, effective)

    def asset_class_for(self, instrument_id: str) -> str:
        """Resolve the asset class from the Instrument Snapshot only."""
        if self._asset_class_for_callback is not None:
            return self._asset_class_for_callback(instrument_id)
        instrument = (
            None
            if self.instrument_store is None
            else self.instrument_store.get(instrument_id)
        )
        if instrument is None:
            message = f"unknown Instrument {instrument_id!r}"
            raise UnknownInstrumentError(
                message,
                details={"instrument_id": instrument_id},
            )
        return instrument.asset_class

    def high_water_mark(self, instrument_id: str, interval: str) -> int | None:
        if self.hwm is None:
            raise RuntimeError(_NOT_OPEN_MESSAGE)
        return self.hwm.get(instrument_id, interval)

    def high_water_marks(self) -> dict[tuple[str, str], int]:
        if self.hwm is None:
            raise RuntimeError(_NOT_OPEN_MESSAGE)
        return self.hwm.all()

    def stale_instruments(self, now: datetime | None = None) -> list[dict]:
        return self.health.stale(now)

    def health_snapshot(self, now: datetime | None = None) -> dict:
        return self.health.snapshot(now)

    def gap_events(self) -> tuple[GapEvent, ...]:
        return self.health.gap_events()


__all__ = ["CollectorCore"]
