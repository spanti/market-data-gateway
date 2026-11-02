"""Collector Service Role: ingest, persist, aggregate, and publish market Bars."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from mdgw.bus.zmq_bus import READY_TOPIC, ZMQBus, table_to_ipc
from mdgw.config import GatewayConfig
from mdgw.core.bar import Bar
from mdgw.core.schema import bars_to_logical_table
from mdgw.ingest.composite import build_configured_feed_handler
from mdgw.ingest.feed_handler import FeedHandler
from mdgw.metrics import MetricsRegistry
from mdgw.service.collector_core import CollectorCore
from mdgw.service.common import ns_iso, update_dataset_metrics
from mdgw.service.health_server import ServiceHealthServer

logger = logging.getLogger(__name__)

MARKET_BAR_TOPIC_PREFIX = "market.bar."
READY_INTERVAL_S = 0.05
STATUS_INTERVAL_S = 1.0
POLL_INTERVAL_S = 0.05
ERROR_BACKOFF_S = 1.0


class CollectorService:
    """Owns the collector pipeline and its ZMQ PUB endpoint."""

    def __init__(
        self,
        config: GatewayConfig,
        *,
        data_root: Path | None = None,
        feed_handler: FeedHandler | None = None,
    ) -> None:
        self.config = config
        self._feed_handler = feed_handler or build_configured_feed_handler(
            config.feeds
        )
        self.core = CollectorCore(
            feed_handler=self._feed_handler,
            config=config,
            data_root=data_root,
        )
        self.metrics = MetricsRegistry()

        self._bus: ZMQBus | None = None
        self._health_server: ServiceHealthServer | None = None
        self._publish_endpoint: str | None = None

        self._stop = threading.Event()
        self._ready = threading.Event()
        self._started = False
        self._started_at: float | None = None
        self._last_bar_ts: datetime | None = None

        self._ingest_thread: threading.Thread | None = None
        self._ready_thread: threading.Thread | None = None
        self._status_thread: threading.Thread | None = None

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._started:
            return
        self.core.open_state()
        try:
            self._bus = ZMQBus(zmq_hwm=self.config.bus.zmq_hwm)
            resolved = self._bus.bind_publisher(
                self.config.bus.collector_pub_bind
            )
            self._publish_endpoint = resolved[0]

            self._health_server = ServiceHealthServer(
                service="collector",
                host=self.config.service.health_host,
                port=self.config.service.collector_health_port,
                ready=self._ready_payload,
                metrics_render=self.render_metrics,
                status=self.status_payload,
            )
            self._health_server.start()

            self.metrics.inc(
                "mdgw_collector_bars_ingested_total",
                amount=0,
                help="Closed 1m Bars ingested by the collector",
            )
            self.metrics.inc(
                "mdgw_collector_bars_written_total",
                amount=0,
                help="Bars written to the Dataset (including aggregated parents)",
            )
            self.metrics.inc(
                "mdgw_collector_zmq_publishes_total",
                amount=0,
                help="Bar messages published on the collector PUB",
            )

            self._stop.clear()
            self._started = True
            self._started_at = time.monotonic()
            self._ready.set()

            self._ready_thread = threading.Thread(
                target=self._run_ready_loop,
                name="mdgw-collector-ready",
                daemon=True,
            )
            self._status_thread = threading.Thread(
                target=self._run_status_loop,
                name="mdgw-collector-status",
                daemon=True,
            )
            self._ingest_thread = threading.Thread(
                target=self._run_ingest_loop,
                name="mdgw-collector-ingest",
                daemon=True,
            )
            self._ready_thread.start()
            self._status_thread.start()
            self._ingest_thread.start()
        except Exception:
            self.stop()
            raise

    def stop(self) -> None:
        self._ready.clear()
        self._stop.set()

        for thread in (
            self._ingest_thread,
            self._ready_thread,
            self._status_thread,
        ):
            if thread is not None:
                thread.join(timeout=10)
        self._ingest_thread = None
        self._ready_thread = None
        self._status_thread = None

        close = getattr(self._feed_handler, "close", None)
        if close is not None:
            try:
                close()
            except Exception:  # pragma: no cover - defensive shutdown path
                logger.exception("error closing Feed Handler")

        if self._bus is not None:
            self._bus.close()
            self._bus = None
        self._publish_endpoint = None
        if self._health_server is not None:
            self._health_server.stop()
            self._health_server = None
        self.core.close_state()
        self._started = False
        self._started_at = None

    # -- properties --------------------------------------------------------

    @property
    def publish_endpoint(self) -> str:
        if self._publish_endpoint is None:
            raise RuntimeError("CollectorService.start() must be called first")
        return self._publish_endpoint

    @property
    def health_url(self) -> str:
        if self._health_server is None:
            raise RuntimeError("CollectorService.start() must be called first")
        return self._health_server.base_url

    # -- ingest loop -------------------------------------------------------

    def _run_ingest_loop(self) -> None:
        # Give the api SUB time to finish subscription forwarding before the
        # first Bar is published (PUB/SUB slow-joiner, DESIGN §2.2).
        if self._stop.wait(self.config.service.startup_delay_s):
            return
        while not self._stop.is_set():
            try:
                count, bars = self.core.ingest()
            except Exception:
                logger.exception(
                    "collector ingest failed",
                    extra={
                        "event": "collector.ingest_failed",
                        "feed": getattr(self._feed_handler, "name", None),
                    },
                )
                self._stop.wait(ERROR_BACKOFF_S)
                continue

            if bars:
                self._publish_bars(bars)
                self.metrics.inc(
                    "mdgw_collector_bars_ingested_total", amount=count
                )
                self.metrics.inc(
                    "mdgw_collector_bars_written_total", amount=len(bars)
                )
            if count == 0:
                self._stop.wait(POLL_INTERVAL_S)

    def _publish_bars(self, bars: list[Bar]) -> None:
        bus = self._bus
        if bus is None:
            return
        groups: dict[tuple[str, str], list[Bar]] = defaultdict(list)
        for bar in bars:
            groups[(bar.interval, bar.instrument_id)].append(bar)

        for (interval, instrument_id), group in groups.items():
            topic = f"{MARKET_BAR_TOPIC_PREFIX}{interval}.{instrument_id}"
            bus.publish(topic, table_to_ipc(bars_to_logical_table(group)))
            self.metrics.inc("mdgw_collector_zmq_publishes_total")

        last = max(bar.close_ts for bar in bars)
        self._last_bar_ts = last

    # -- readiness / status ------------------------------------------------

    def _run_ready_loop(self) -> None:
        while not self._stop.is_set():
            bus = self._bus
            if bus is not None:
                try:
                    bus.publish(READY_TOPIC, b"")
                except RuntimeError:
                    pass
            self._stop.wait(READY_INTERVAL_S)

    def _run_status_loop(self) -> None:
        while not self._stop.is_set():
            self._publish_status()
            self._stop.wait(STATUS_INTERVAL_S)

    def _publish_status(self) -> None:
        bus = self._bus
        if bus is None:
            return
        try:
            payload = self.status_payload()
            bus.publish(
                "_internal.status",
                json.dumps(payload, ensure_ascii=False, default=str).encode(
                    "utf-8"
                ),
            )
        except Exception:  # pragma: no cover - defensive status path
            logger.debug("collector status publish failed", exc_info=True)

    def _ready_payload(self) -> tuple[bool, str]:
        if self._started and self._ready.is_set():
            return True, "collector ready"
        return False, "collector starting"

    def status_payload(self) -> dict:
        health = self.core.health_snapshot()
        return {
            "service": "collector",
            "role": "collector",
            "ready": self._ready.is_set(),
            "uptime_s": self._uptime_s(),
            "feed": getattr(self._feed_handler, "name", None),
            "last_ingest_ts": ns_iso(self.core.last_ingest_ts),
            "last_bar_ts": ns_iso(self._last_bar_ts),
            "bars_ingested": self.core.bars_ingested,
            "bars_written": self.core.bars_written,
            "stale_count": health.get("stale_count", 0),
            "gap_unrecoverable_count": health.get("gap_unrecoverable_count", 0),
            "health": health,
        }

    # -- metrics -----------------------------------------------------------

    def render_metrics(self) -> str:
        self.metrics.set(
            "mdgw_up", 1 if self._started else 0, help="Service process is up"
        )
        ready = self._started and self._ready.is_set()
        self.metrics.set(
            "mdgw_ready",
            1 if ready else 0,
            help="Service process is ready",
        )
        health = self.core.health_snapshot()
        self.metrics.set(
            "mdgw_collector_stale_count",
            health.get("stale_count", 0),
            help="Stale (feed, instrument, interval) keys",
        )
        self.metrics.set(
            "mdgw_collector_gap_unrecoverable_count",
            health.get("gap_unrecoverable_count", 0),
            help="Unrecoverable gap events",
        )
        update_dataset_metrics(self.metrics, self.core.store)
        return self.metrics.render()

    def _uptime_s(self) -> float:
        if self._started_at is None:
            return 0.0
        return max(0.0, time.monotonic() - self._started_at)


__all__ = ["CollectorService"]
