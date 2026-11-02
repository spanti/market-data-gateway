"""GatewayRuntime: the single composition root used by behavior tests.

It wires Feed Handler -> normalize -> Parquet Dataset -> REST API, plus the
in-process ZMQ Streaming Interface. Ingest is synchronous so tests can drive
the whole chain deterministically; the HTTP server runs on its own event loop.
"""

from __future__ import annotations

import asyncio
import threading
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from aiohttp import web

from mdgw.aggregate import BarAggregator
from mdgw.api.server import create_app
from mdgw.backfill import BackfillReport, run_backfill
from mdgw.bus.zmq_bus import ZMQBus, ipc_to_table, table_to_ipc
from mdgw.config import GatewayConfig
from mdgw.core.bar import Bar
from mdgw.core.errors import UnknownInstrumentError
from mdgw.core.schema import INTERVALS, bars_to_logical_table
from mdgw.core.time import datetime_to_ns, ns_to_datetime, utc_now
from mdgw.ingest.feed_handler import FeedHandler
from mdgw.normalize.bar import normalize_bar
from mdgw.reference import (
    InstrumentStore,
    export_instrument_snapshot,
    load_instrument_reference,
)
from mdgw.state.health import GapEvent, HealthTracker
from mdgw.state.high_water_mark import HighWaterMarkStore
from mdgw.store.parquet_dataset import ParquetBarStore

MARKET_BAR_TOPIC_PREFIX = "market.bar."
RECEIVE_WAIT_TIMEOUT_S = 2.0


class GatewayRuntime:
    def __init__(
        self,
        *,
        feed_handler: FeedHandler,
        config: GatewayConfig,
        data_root: Path | None = None,
    ) -> None:
        self.feed_handler = feed_handler
        self.config = config
        self.store = ParquetBarStore(
            data_root if data_root is not None else config.storage.root,
            compression=config.storage.compression,
            row_group_size=config.storage.row_group_size,
            format_version=config.storage.format_version,
            schema_version=config.storage.schema_version,
        )
        self.instrument_store: InstrumentStore | None = None
        self.aggregator = BarAggregator(
            intervals=config.aggregation.intervals,
            base_interval=config.aggregation.base_interval,
            retention_parent_periods=config.aggregation.retention_parent_periods,
        )
        self._hwm: HighWaterMarkStore | None = None
        self._health = HealthTracker(stale_after_s=config.collector.stale_after_s)

        self._bus: ZMQBus | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._runner: web.AppRunner | None = None
        self._port: int | None = None
        self._started = False

        self._received_condition = threading.Condition()
        self._received_bar_count = 0
        self._expected_received_bar_count = 0

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._started:
            return
        # Instrument Reference Data is validated and exported once at startup;
        # the API and all later reads use only the Instrument Snapshot.
        reference = load_instrument_reference(Path(self.config.instruments.path))
        export = export_instrument_snapshot(
            reference, self.store.root / "instruments"
        )
        self.instrument_store = InstrumentStore(export.path)

        # Data HWM state is opened before the first write; the backfill runs
        # before the Streaming Interface comes up so historical Bars are never
        # published as live messages.
        self._hwm = HighWaterMarkStore(
            self.store.root / self.config.collector.state_db_name
        )
        bus: ZMQBus | None = None
        try:
            self._run_backfill(utc_now())
            # Register every expected freshness key before Streaming/HTTP come
            # up, so a feed that never delivers its first Bar is still visible
            # as stale instead of being absent from the health view.
            self._register_expected_keys()

            bus = ZMQBus(zmq_hwm=self.config.bus.zmq_hwm)
            endpoint = f"inproc://mdgw-{uuid4().hex}"
            bus.bind_publisher(endpoint)
            ready = bus.subscribe(
                endpoint,
                topic_prefix=MARKET_BAR_TOPIC_PREFIX,
                handler=self._on_bar_message,
            )
            bus.wait_for_subscriber(ready)
            self._start_http()
        except Exception:
            if bus is not None:
                bus.close()
            self._stop_http()
            self._close_state()
            raise
        self._bus = bus
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return
        self._stop_http()
        if self._bus is not None:
            self._bus.close()
            self._bus = None
        self._close_state()
        self._started = False

    def _close_state(self) -> None:
        if self._hwm is not None:
            self._hwm.close()
            self._hwm = None

    def _stop_http(self) -> None:
        if self._runner is not None and self._loop is not None:
            asyncio.run_coroutine_threadsafe(self._runner.cleanup(), self._loop).result(
                timeout=10
            )
            self._runner = None
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._loop_thread is not None:
            self._loop_thread.join(timeout=5)
            self._loop_thread = None
        if self._loop is not None:
            self._loop.close()
            self._loop = None

    def __enter__(self) -> "GatewayRuntime":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.stop()

    # -- collector behavior ------------------------------------------------

    def ingest(self) -> int:
        """Drain the Feed Handler once, aggregate, persist, and publish Bars.

        Returns the number of base-interval (1m) Bars ingested this call;
        aggregated parents are written and published alongside them.
        """
        if not self._started:
            raise RuntimeError("GatewayRuntime.start() must be called before ingest()")

        ingest_ts = utc_now()
        base_bars = [
            normalize_bar(raw, feed=self.feed_handler.name, ingest_ts=ingest_ts)
            for raw in self.feed_handler.iter_bars()
        ]
        if not base_bars:
            return 0

        # Validate every base Bar against the Instrument Snapshot before the
        # aggregator advances any window or retention water mark. Rejecting a
        # batch must leave the Aggregation state untouched so the same Bars can
        # be retried once the reference data is fixed.
        asset_classes: dict[str, str] = {}
        for bar in base_bars:
            if bar.instrument_id not in asset_classes:
                asset_classes[bar.instrument_id] = self._asset_class_for(
                    bar.instrument_id
                )

        aggregated = self.aggregator.add_many(base_bars)
        bars = base_bars + aggregated

        by_asset_class: dict[str, list[Bar]] = defaultdict(list)
        for bar in bars:
            # Aggregated parents inherit their children's instrument_id, so the
            # validated mapping already covers them.
            by_asset_class[asset_classes[bar.instrument_id]].append(bar)
        for asset_class, group in sorted(by_asset_class.items()):
            self.store.write_bars(group, asset_class=asset_class)

        # HWM and freshness are updated only after the write succeeded, so a
        # failed flush cannot make a Bar look persisted.
        self._advance_hwm(bars)
        for bar in bars:
            self._health.record_bar(bar)
        # A newly persisted 1m Bar is also the conservative baseline for parent
        # intervals that have not closed yet.
        self._register_expected_keys()

        target = self._publish_bars(bars)
        self._wait_for_received(target)
        return len(base_bars)

    # -- collector state ---------------------------------------------------

    def _register_expected_keys(self) -> None:
        """Register one freshness key per expected `(instrument, interval)`.

        The starting event timestamp comes from the interval's own data HWM
        (`open_ts + interval`, i.e. its close/event timestamp). An interval
        with no HWM of its own falls back to the base-interval (1m) HWM close as
        a conservative baseline, so a parent period is not reported stale just
        because it has not closed since startup. With no 1m HWM either the key
        is registered as "never received" and is stale until a Bar arrives.
        Forward-only registration means a later recorded event is never lost.
        """
        if self._hwm is None:
            return
        feed = self.feed_handler.name
        base_interval = self.config.aggregation.base_interval
        base_interval_delta = INTERVALS[base_interval]
        for instrument_id in self.feed_handler.instrument_ids:
            base_hwm_ns = self._hwm.get(instrument_id, base_interval)
            for interval in self.config.aggregation.intervals:
                own_hwm_ns = self._hwm.get(instrument_id, interval)
                if own_hwm_ns is not None:
                    event_ts = ns_to_datetime(own_hwm_ns) + INTERVALS[interval]
                elif base_hwm_ns is not None:
                    event_ts = ns_to_datetime(base_hwm_ns) + base_interval_delta
                else:
                    event_ts = None
                self._health.register(feed, instrument_id, interval, event_ts)

    def _advance_hwm(self, bars: list[Bar]) -> None:
        if self._hwm is None:
            return
        grouped: dict[tuple[str, str], list[Bar]] = defaultdict(list)
        for bar in bars:
            grouped[(bar.instrument_id, bar.interval)].append(bar)
        for (instrument_id, interval), group in grouped.items():
            self._hwm.set(
                instrument_id,
                interval,
                max(datetime_to_ns(bar.open_ts) for bar in group),
            )

    def _run_backfill(self, now: datetime) -> BackfillReport:
        if self._hwm is None:
            raise RuntimeError("GatewayRuntime state is not open")
        return run_backfill(
            feed_handler=self.feed_handler,
            store=self.store,
            hwm=self._hwm,
            aggregator=self.aggregator,
            health=self._health,
            asset_class_for=self._asset_class_for,
            now=now,
            backfill_hours=self.config.collector.backfill_hours,
        )

    def backfill(self, now: datetime | None = None) -> BackfillReport:
        """Backfill closed 1m Bars from the HWM (or the configured window).

        Historical Bars are written and aggregated but never published.
        """
        if self._hwm is None:
            raise RuntimeError(
                "GatewayRuntime.start() must be called before backfill()"
            )
        return self._run_backfill(now if now is not None else utc_now())

    def high_water_mark(self, instrument_id: str, interval: str) -> int | None:
        """Return the persisted data HWM in UTC nanoseconds, if any."""
        if self._hwm is None:
            raise RuntimeError(
                "GatewayRuntime.start() must be called before high_water_mark()"
            )
        return self._hwm.get(instrument_id, interval)

    def high_water_marks(self) -> dict[tuple[str, str], int]:
        if self._hwm is None:
            raise RuntimeError(
                "GatewayRuntime.start() must be called before high_water_marks()"
            )
        return self._hwm.all()

    def stale_instruments(self, now: datetime | None = None) -> list[dict]:
        """Return stale `(feed, instrument, interval)` entries only."""
        return self._health.stale(now)

    def health(self, now: datetime | None = None) -> dict:
        """Assertable collector health snapshot (stale + gap_unrecoverable)."""
        return self._health.snapshot(now)

    def gap_events(self) -> tuple[GapEvent, ...]:
        return self._health.gap_events()

    def _asset_class_for(self, instrument_id: str) -> str:
        """Resolve the asset class from the Instrument Snapshot only.

        Unknown Instruments are rejected; there is no constructor fallback.
        """
        instrument = (
            None
            if self.instrument_store is None
            else self.instrument_store.get(instrument_id)
        )
        if instrument is None:
            raise UnknownInstrumentError(
                f"unknown Instrument {instrument_id!r}",
                details={"instrument_id": instrument_id},
            )
        return instrument.asset_class

    def _publish_bars(self, bars: list[Bar]) -> int:
        groups: dict[tuple[str, str], list[Bar]] = defaultdict(list)
        for bar in bars:
            groups[(bar.interval, bar.instrument_id)].append(bar)

        with self._received_condition:
            self._expected_received_bar_count += len(bars)
            target = self._expected_received_bar_count

        assert self._bus is not None
        for (interval, instrument_id), group in groups.items():
            topic = f"{MARKET_BAR_TOPIC_PREFIX}{interval}.{instrument_id}"
            self._bus.publish(topic, table_to_ipc(bars_to_logical_table(group)))
        return target

    def _on_bar_message(self, topic: str, payload: bytes) -> None:
        table = ipc_to_table(payload)
        with self._received_condition:
            self._received_bar_count += table.num_rows
            self._received_condition.notify_all()

    def _wait_for_received(self, target: int) -> None:
        with self._received_condition:
            received = self._received_condition.wait_for(
                lambda: self._received_bar_count >= target,
                timeout=RECEIVE_WAIT_TIMEOUT_S,
            )
        if not received:
            raise TimeoutError(
                f"api received {self._received_bar_count} of {target} Bars over ZMQ"
            )

    @property
    def received_bar_count(self) -> int:
        with self._received_condition:
            return self._received_bar_count

    # -- HTTP --------------------------------------------------------------

    def _start_http(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._run_event_loop,
            name="mdgw-http",
            daemon=True,
        )
        self._loop_thread.start()
        future = asyncio.run_coroutine_threadsafe(
            self._start_http_on_loop(), self._loop
        )
        self._runner = future.result(timeout=10)

    def _run_event_loop(self) -> None:
        assert self._loop is not None
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _start_http_on_loop(self) -> web.AppRunner:
        app = create_app(self.store, instrument_store=self.instrument_store)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(
            runner,
            host=self.config.server.host,
            port=self.config.server.http_port,
        )
        await site.start()
        assert site._server is not None
        self._port = site._server.sockets[0].getsockname()[1]
        return runner

    @property
    def base_url(self) -> str:
        if self._port is None:
            raise RuntimeError("GatewayRuntime.start() must be called first")
        host = self.config.server.host
        if host in {"0.0.0.0", "::"}:
            host = "127.0.0.1"
        return f"http://{host}:{self._port}"
