"""GatewayRuntime: the single composition root used by behavior tests.

It wires Feed Handler -> normalize -> Parquet Dataset -> REST API, plus the
in-process ZMQ Streaming Interface. Ingest is synchronous so tests can drive
the whole chain deterministically; the HTTP server runs on its own event loop.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections import defaultdict
from contextlib import suppress
from typing import TYPE_CHECKING, Self
from uuid import uuid4

from aiohttp import web

from mdgw.aggregate import BarAggregator
from mdgw.api.server import create_app
from mdgw.api.ws_hub import WebSocketHub
from mdgw.bus.zmq_bus import ZMQBus, ipc_to_table, table_to_ipc
from mdgw.core.envelope import table_to_messages
from mdgw.core.errors import UnknownInstrumentError
from mdgw.core.schema import bars_to_logical_table
from mdgw.core.time import utc_now
from mdgw.replay.service import ReplayService
from mdgw.service.collector_core import CollectorCore
from mdgw.state.health import GapEvent, HealthTracker
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from datetime import datetime
    from pathlib import Path

    from mdgw.backfill import BackfillReport
    from mdgw.config import GatewayConfig
    from mdgw.core.bar import Bar
    from mdgw.ingest.feed_handler import FeedHandler
    from mdgw.reference.snapshot import InstrumentStore
    from mdgw.replay.session import Publish, ReplaySessionSpec
    from mdgw.state.high_water_mark import HighWaterMarkStore

MARKET_BAR_TOPIC_PREFIX = "market.bar."
RECEIVE_WAIT_TIMEOUT_S = 2.0

logger = logging.getLogger(__name__)


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
        # The collector pipeline is shared with the standalone collector
        # Service Role. `asset_class_for` deliberately points back at this
        # runtime so tests that replace `instrument_store` keep working.
        self._core = CollectorCore(
            feed_handler=feed_handler,
            config=config,
            store=self.store,
            aggregator=self.aggregator,
            health=self._health,
            asset_class_for=self._asset_class_for,
        )

        self._bus: ZMQBus | None = None
        self._hub: WebSocketHub | None = None
        # Replay is created lazily: most runtime tests never replay, and a
        # ReplayService costs nothing until its first Session. No ZMQ server is
        # started here, so no port is bound unless the caller asks for replay.
        self._replay_service: ReplayService | None = None
        self._stream_endpoint: str | None = None
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
        # the API and all later reads use only the Instrument Snapshot. The
        # shared core also opens the data HWM and runs startup Backfill before
        # the Streaming Interface comes up, so historical Bars are never
        # published as live messages.
        self._core.open_state()
        self.instrument_store = self._core.instrument_store
        self._hwm = self._core.hwm
        bus: ZMQBus | None = None
        try:
            bus = self._bind_streaming_bus()
            self._start_http()
        except Exception:
            if bus is not None:
                bus.close()
            self._stream_endpoint = None
            self._stop_http()
            self._close_feed_handler()
            self._close_state()
            raise
        self._bus = bus
        self._started = True

    def _bind_streaming_bus(self) -> ZMQBus:
        bus = ZMQBus(zmq_hwm=self.config.bus.zmq_hwm)
        endpoint = f"inproc://mdgw-{uuid4().hex}"
        # One PUB socket feeds the internal api subscriber over inproc and
        # external ZMQ consumers over TCP (port 0 resolves to a free port).
        tcp_endpoint = (
            f"tcp://{self.config.bus.stream_bind_host}:"
            f"{self.config.bus.stream_pub_port}"
        )
        try:
            resolved = bus.bind_publisher([endpoint, tcp_endpoint])
            self._stream_endpoint = resolved[-1]
            ready = bus.subscribe(
                endpoint,
                topic_prefix=MARKET_BAR_TOPIC_PREFIX,
                handler=self._on_bar_message,
            )
            bus.wait_for_subscriber(ready)
        except Exception:
            bus.close()
            raise
        return bus

    def stop(self) -> None:
        if not self._started:
            return
        self._close_replay()
        self._stop_http()
        if self._bus is not None:
            self._bus.close()
            self._bus = None
        self._stream_endpoint = None
        self._close_feed_handler()
        self._close_state()
        self._started = False

    def _close_replay(self) -> None:
        """Stop every Replay Session created through the runtime seam."""
        service = self._replay_service
        self._replay_service = None
        if service is None:
            return
        try:
            service.close()
        except Exception:  # pragma: no cover - defensive shutdown path
            logger.exception(
                "error closing Replay Sessions",
                extra={"event": "runtime.replay_close_failed"},
            )

    def _close_feed_handler(self) -> None:
        """Release venue resources owned by the Feed Handler, if any.

        HWM shutdown stays in `CollectorCore.close_state()`, so this never
        duplicates state cleanup.
        """
        close = getattr(self.feed_handler, "close", None)
        if close is None:
            return
        try:
            close()
        except Exception:  # pragma: no cover - defensive shutdown path
            logger.exception(
                "error closing Feed Handler",
                extra={"event": "runtime.feed_close_failed"},
            )

    def _close_state(self) -> None:
        self._core.close_state()
        self._hwm = None

    def _stop_http(self) -> None:
        loop = self._loop
        if loop is not None and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                self._shutdown_http_on_loop(), loop
            )
            try:
                future.result(timeout=10)
            finally:
                loop.call_soon_threadsafe(loop.stop)
        if self._loop_thread is not None:
            self._loop_thread.join(timeout=5)
            self._loop_thread = None
        if loop is not None:
            if not loop.is_running():
                loop.close()
            self._loop = None

    async def _shutdown_http_on_loop(self) -> None:
        hub = self._hub
        self._hub = None
        if hub is not None:
            await hub.close()
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    def __enter__(self) -> Self:
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
            msg = "GatewayRuntime.start() must be called before ingest()"
            raise RuntimeError(msg)

        base_count, bars = self._core.ingest()
        if not bars:
            return base_count

        target = self._publish_bars(bars)
        self._wait_for_received(target)
        return base_count

    # -- collector state ---------------------------------------------------

    def backfill(self, now: datetime | None = None) -> BackfillReport:
        """Backfill closed 1m Bars from the HWM (or the configured window).

        Historical Bars are written and aggregated but never published.
        """
        if self._hwm is None:
            msg = "GatewayRuntime.start() must be called before backfill()"
            raise RuntimeError(msg)
        return self._core.backfill(now if now is not None else utc_now())

    def high_water_mark(self, instrument_id: str, interval: str) -> int | None:
        """Return the persisted data HWM in UTC nanoseconds, if any."""
        if self._hwm is None:
            msg = "GatewayRuntime.start() must be called before high_water_mark()"
            raise RuntimeError(msg)
        return self._core.high_water_mark(instrument_id, interval)

    def high_water_marks(self) -> dict[tuple[str, str], int]:
        if self._hwm is None:
            msg = "GatewayRuntime.start() must be called before high_water_marks()"
            raise RuntimeError(msg)
        return self._core.high_water_marks()

    def stale_instruments(self, now: datetime | None = None) -> list[dict]:
        """Return stale `(feed, instrument, interval)` entries only."""
        return self._core.stale_instruments(now)

    def health(self, now: datetime | None = None) -> dict:
        """Assertable collector health status (stale + gap_unrecoverable)."""
        return self._core.health_snapshot(now)

    def gap_events(self) -> tuple[GapEvent, ...]:
        return self._core.gap_events()

    # -- replay ------------------------------------------------------------

    @property
    def replay_service(self) -> ReplayService:
        """The lazily created in-process Replay Service (no ZMQ server bound).

        It reuses this runtime's Parquet Store and Instrument Snapshot, so a
        Replay Session observes the same Bars the collector just wrote. The
        first access creates it; `stop()` closes it and every Session.
        """
        if not self._started:
            msg = "GatewayRuntime.start() must be called before replay_service"
            raise RuntimeError(msg)
        if self._replay_service is None:
            self._replay_service = ReplayService(
                self.store,
                max_sessions=self.config.replay.max_sessions,
                heartbeat_s=self.config.replay.heartbeat_s,
                instrument_store=self.instrument_store,
            )
        return self._replay_service

    def create_replay_session(
        self,
        spec: ReplaySessionSpec,
        *,
        publish: Publish | None = None,
    ) -> str:
        """Create a Replay Session through the runtime seam and return its id.

        `publish` replaces the runtime Replay Service's message sink, which
        receives the same `[topic][Arrow IPC]` frames a `ReplayServer` would
        send. That lets a test observe replay events without binding ZMQ.
        """
        service = self.replay_service
        if publish is not None:
            service.set_publish(publish)
        return service.create_session(spec)

    def _asset_class_for(self, instrument_id: str) -> str:
        """Resolve the asset class from the Instrument Snapshot only.

        Unknown Instruments are rejected; there is no constructor fallback.
        During startup Backfill the core's freshly exported Snapshot is used
        until `start()` publishes it on `self.instrument_store`; tests may then
        replace that attribute and the replacement takes precedence.
        """
        instrument_store = self.instrument_store
        if instrument_store is None:
            instrument_store = self._core.instrument_store
        instrument = (
            None
            if instrument_store is None
            else instrument_store.get(instrument_id)
        )
        if instrument is None:
            msg = f"unknown Instrument {instrument_id!r}"
            raise UnknownInstrumentError(
                msg,
                details={"instrument_id": instrument_id},
            )
        return instrument.asset_class

    def _publish_bars(self, bars: list[Bar]) -> int:
        groups: dict[tuple[str, str], list[Bar]] = defaultdict(list)
        for bar in bars:
            groups[bar.interval, bar.instrument_id].append(bar)

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

        # This runs on the ZMQ subscriber thread. Hand JSON messages to the
        # event-loop hub via call_soon_threadsafe; never touch aiohttp objects
        # from here. The external ZMQ consumer already received the same frames
        # straight from the PUB socket.
        hub = self._hub
        loop = self._loop
        if hub is None or loop is None:
            return
        messages = table_to_messages(table)
        with suppress(RuntimeError):
            # The event loop is already shutting down.
            loop.call_soon_threadsafe(hub.publish, topic, messages)

    def _wait_for_received(self, target: int) -> None:
        with self._received_condition:
            received = self._received_condition.wait_for(
                lambda: self._received_bar_count >= target,
                timeout=RECEIVE_WAIT_TIMEOUT_S,
            )
        if not received:
            msg = f"api received {self._received_bar_count} of {target} Bars over ZMQ"
            raise TimeoutError(msg)

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
        # The hub belongs to the HTTP event loop; the runtime passes it into
        # the app so ZMQ fan-out and the WebSocket route share one registry.
        self._hub = WebSocketHub(
            queue_size=self.config.streaming.queue_size,
            lag_notice_interval_ms=self.config.streaming.lag_notice_interval_ms,
        )
        app = create_app(
            self.store,
            instrument_store=self.instrument_store,
            ws_hub=self._hub,
            ws_path=self.config.server.ws_path,
        )
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
    def stream_endpoint(self) -> str:
        """Resolved external ZMQ PUB endpoint (`tcp://host:port`)."""
        if self._stream_endpoint is None:
            msg = "GatewayRuntime.start() must be called first"
            raise RuntimeError(msg)
        return self._stream_endpoint

    @property
    def base_url(self) -> str:
        if self._port is None:
            msg = "GatewayRuntime.start() must be called first"
            raise RuntimeError(msg)
        host = self.config.server.host
        if host in {"0.0.0.0", "::"}:  # ruff: ignore[hardcoded-bind-all-interfaces] - compares the configured host; no socket bind
            host = "127.0.0.1"
        return f"http://{host}:{self._port}"
