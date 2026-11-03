"""GatewayRuntime: the single composition root used by behavior tests.

It wires Feed Handler -> normalize -> Parquet Dataset -> REST API, plus the
in-process ZMQ Streaming Interface. Ingest is synchronous so tests can drive
the whole chain deterministically; the HTTP server runs on its own event loop.
"""

from __future__ import annotations

import logging
import threading
from collections import defaultdict
from contextlib import suppress
from typing import TYPE_CHECKING, Self
from uuid import uuid4

from mdgw.aggregate import BarAggregator
from mdgw.api.http_server import HttpServer
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
    from collections.abc import Mapping
    from datetime import datetime
    from pathlib import Path
    from typing import Any

    from mdgw.backfill import BackfillReport
    from mdgw.config import GatewayConfig
    from mdgw.core.bar import Bar
    from mdgw.ingest.feed_handler import FeedHandler
    from mdgw.reference.snapshot import InstrumentStore
    from mdgw.replay.service import PublishBar, PublishEvent
    from mdgw.replay.session import ReplaySessionSpec
    from mdgw.state.high_water_mark import HighWaterMarkStore

MARKET_BAR_TOPIC_PREFIX = "market.bar."
RECEIVE_WAIT_TIMEOUT_S = 2.0

logger = logging.getLogger(__name__)


def _drop_bar(bar: Bar) -> None:
    """Default runtime replay Bar sink (the session id is irrelevant here)."""


def _drop_event(event: Mapping[str, Any]) -> None:
    """Default runtime replay event sink."""


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
        self._http = HttpServer()
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
        self._http.stop()
        self._hub = None

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
        publish_bar: PublishBar | None = None,
        publish_event: PublishEvent | None = None,
    ) -> str:
        """Create a Replay Session through the runtime seam and return its id.

        `publish_bar` and `publish_event` are typed, single-argument callbacks
        that receive the same Bars and End of Session mapping a `ReplayServer`
        would encode for the wire. They let a test observe replay events
        without binding ZMQ. Either callback may be omitted.
        """
        service = self.replay_service
        bar_sink = publish_bar if publish_bar is not None else _drop_bar
        event_sink = (
            publish_event if publish_event is not None else _drop_event
        )
        # The Service seam is session-id aware because one ReplayServer PUB
        # multiplexes every Session; the runtime has no transport, so the
        # wrappers drop the id and keep the test-facing callbacks typed.
        service.set_publish(
            lambda _session_id, bar: bar_sink(bar),
            lambda _session_id, event: event_sink(event),
        )
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
        if hub is None:
            return
        messages = table_to_messages(table)
        with suppress(RuntimeError):
            # The HttpServer event loop is already shutting down.
            self._http.call_soon_threadsafe(hub.publish, topic, messages)

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
        # Building the hub and the app does not need a running event loop, so
        # both happen synchronously before the `HttpServer` owns the lifecycle.
        # The runtime keeps ownership of the hub and passes it into the app so
        # ZMQ fan-out and the WebSocket route share one registry.
        hub = WebSocketHub(
            queue_size=self.config.streaming.queue_size,
            lag_notice_interval_ms=self.config.streaming.lag_notice_interval_ms,
        )
        self._hub = hub
        app = create_app(
            self.store,
            instrument_store=self.instrument_store,
            ws_hub=hub,
            ws_path=self.config.server.ws_path,
        )
        self._http.start(
            app,
            host=self.config.server.host,
            port=self.config.server.http_port,
            thread_name="mdgw-http",
            on_shutdown=hub.close,
        )

    @property
    def stream_endpoint(self) -> str:
        """Resolved external ZMQ PUB endpoint (`tcp://host:port`)."""
        if self._stream_endpoint is None:
            msg = "GatewayRuntime.start() must be called first"
            raise RuntimeError(msg)
        return self._stream_endpoint

    @property
    def base_url(self) -> str:
        try:
            return self._http.base_url
        except RuntimeError:
            msg = "GatewayRuntime.start() must be called first"
            raise RuntimeError(msg) from None
