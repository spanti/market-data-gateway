"""Collector Service Role: ingest, persist, aggregate, and publish market Bars."""

from __future__ import annotations

import logging
import math
import threading
import time
from typing import TYPE_CHECKING

from mdgw.aggregate.bar_aggregator import AGGREGATION_INTERNAL
from mdgw.core.time import datetime_to_ns
from mdgw.ingest.composite import (
    attach_quarantine,
    build_configured_feed_handler,
)
from mdgw.ingest.feed_status import (
    FEED_ERROR_CODES,
    FEED_STATES,
    FeedConnectionStatus,
    handler_feed_statuses,
)
from mdgw.metrics import MetricsRegistry
from mdgw.service.bar_sink import BufferedBarSink
from mdgw.service.collector_core import CollectorCore
from mdgw.service.common import (
    advance_counter,
    iso_to_seconds,
    ns_iso,
    update_dataset_metrics,
)
from mdgw.service.health_server import ServiceHealthServer
from mdgw.store.composition import StorageComposition
from mdgw.streaming import StreamingInterface

if TYPE_CHECKING:
    from datetime import datetime
    from pathlib import Path

    from mdgw.config import GatewayConfig
    from mdgw.core.bar import Bar
    from mdgw.ingest.feed_handler import FeedHandler

logger = logging.getLogger(__name__)

STATUS_INTERVAL_S = 1.0
POLL_INTERVAL_S = 0.05
ERROR_BACKOFF_S = 1.0
_NOT_STARTED_MESSAGE = "CollectorService.start() must be called first"

#: Ingestion Delay spans sub-second to Backfill-scale minutes. The help text
#: names source clock skew: event time comes from the Venue clock. The Gateway
#: has no protocol RTT measurement, so no metric is named Network Latency and
#: this delay must never be mislabelled as one.
INGESTION_DELAY_BUCKETS: tuple[float, ...] = (
    0.1,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
    600.0,
    math.inf,
)

_INGESTION_DELAY_HELP = (
    "Ingestion Delay: seconds from the Bar event time (close_ts) to the "
    "collector ingest time (ingest_ts); includes source clock skew; this is "
    "not a protocol RTT or Network Latency measurement"
)
_STORAGE_BYTES_HELP = (
    "Bytes written to Parquet Data Files by the collector write buffer, as "
    "reported by the Store write result (includes aggregated parent Bars and "
    "buffered flushes; queue depth stays a separate metric)"
)


class CollectorService:
    """Owns the collector pipeline and its ZMQ PUB endpoint."""

    def __init__(
        self,
        config: GatewayConfig,
        *,
        data_root: Path | None = None,
        feed_handler: FeedHandler | None = None,
        wait_for_api: bool | None = None,
    ) -> None:
        self.config = config
        # One joint composition owns both Store mappings and the effective
        # Normalization Rule Version. The Collector owns the Quarantine
        # Dataset; the pipeline and every ccxt Feed Handler it builds share
        # this exact instance, so counters and the physical Dataset agree.
        composition = StorageComposition(config, root=data_root)
        bundle = composition.bars_and_quarantine()
        quarantine = bundle.quarantine
        self._feed_handler = feed_handler or build_configured_feed_handler(
            config.feeds,
            quarantine=quarantine,
            rule_version=composition.rule_version,
        )
        attach_quarantine(
            self._feed_handler,
            quarantine,
            rule_version=composition.rule_version,
        )
        self.core = CollectorCore(
            feed_handler=self._feed_handler,
            config=config,
            data_root=data_root,
            store=bundle.bars,
            sink_factory=lambda store: BufferedBarSink(
                store,
                queue_size=config.collector.write_queue_size,
                flush_max_bytes=config.storage.flush_max_bytes,
                flush_max_age_s=config.storage.flush_max_age_s,
            ),
            quarantine=quarantine,
        )
        self.metrics = MetricsRegistry()
        # `bars_written` now counts Bars that actually reached disk (including
        # buffered writer flushes), so the process metric is advanced by delta.
        self._last_written_metric = 0.0
        self._written_metric_lock = threading.Lock()
        # Quarantine counters advance by delta from `QuarantineStore.stats()`;
        # CollectorCore never depends on the MetricsRegistry.
        self._last_quarantine_counts: dict[tuple[str, str], int] = {}
        self._last_quarantine_failures = 0
        self._quarantine_metric_lock = threading.Lock()
        # Feed connection gauges/counters are refreshed from the real handler
        # lifecycle; cumulative reconnect counters advance by source delta.
        self._feed_metric_lock = threading.Lock()
        self._last_feed_reconnects: dict[tuple[str, str], float] = {}
        # Storage bytes are cumulative Store write results; advance by delta.
        self._storage_metric_lock = threading.Lock()
        self._last_storage_bytes_metric = 0.0

        self._streaming: StreamingInterface | None = None
        self._health_server: ServiceHealthServer | None = None

        # `None` means "use [service].wait_for_api_ready". The CLI overrides it
        # with --wait-for-api/--no-wait-for-api.
        self._wait_for_api = (
            config.service.wait_for_api_ready
            if wait_for_api is None
            else wait_for_api
        )

        self._stop = threading.Event()
        self._ready = threading.Event()
        self._started = False
        self._started_at: float | None = None
        self._last_bar_ts: datetime | None = None

        self._ingest_thread: threading.Thread | None = None
        self._status_thread: threading.Thread | None = None

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._started:
            return
        self.core.open_state()
        try:
            self._start_service()
        except Exception:
            self.stop()
            raise

    def _start_service(self) -> None:
        streaming = StreamingInterface(
            bind=self.config.bus.collector_pub_bind,
            connect=self.config.bus.api_pub_connect,
            zmq_hwm=self.config.bus.zmq_hwm,
        )
        self._streaming = streaming
        streaming.start()
        # The module owns the 50 ms READY heartbeat from here until stop().
        streaming.mark_ready()

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
        self._status_thread.start()
        self._ingest_thread.start()
        logger.info(
            "collector service started",
            extra={
                "event": "collector.started",
                "publish_endpoint": streaming.endpoints[0],
            },
        )

    def stop(self) -> None:
        self._ready.clear()
        self._stop.set()

        # Stop both publishing threads before the Streaming Interface: they
        # must not observe a closed socket (publish is a no-op after stop, but
        # joining first keeps the shutdown deterministic).
        for thread in (self._ingest_thread, self._status_thread):
            if thread is not None:
                thread.join(timeout=10)
        self._ingest_thread = None
        self._status_thread = None

        close = getattr(self._feed_handler, "close", None)
        if close is not None:
            try:
                close()
            except Exception:  # pragma: no cover - defensive shutdown path
                logger.exception("error closing Feed Handler")

        if self._streaming is not None:
            self._streaming.stop()
            self._streaming = None
        if self._health_server is not None:
            self._health_server.stop()
            self._health_server = None
        self.core.close_state()
        # The CollectorService owns this Store (CollectorCore created it).
        self.core.store.close()
        self._started = False
        self._started_at = None

    # -- properties --------------------------------------------------------

    @property
    def publish_endpoint(self) -> str:
        streaming = self._streaming
        if streaming is None or not streaming.endpoints:
            raise RuntimeError(_NOT_STARTED_MESSAGE)
        return streaming.endpoints[0]

    @property
    def health_url(self) -> str:
        if self._health_server is None:
            raise RuntimeError(_NOT_STARTED_MESSAGE)
        return self._health_server.base_url

    # -- ingest loop -------------------------------------------------------

    def _run_ingest_loop(self) -> None:
        # When enabled, wait for the api to prove its SUB forwarding is live
        # before the first Bar is published (PUB/SUB slow-joiner, DESIGN §2.2).
        # A timeout only warns: standalone collectors and split deployments
        # must still ingest.
        self._wait_for_api_ready()
        # Keep the configured settle delay as an extra cushion after the
        # handshake (and as the only delay when not waiting for the api).
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
                # Only live base Bars carry a meaningful ingest/event pair;
                # aggregated parents reuse child ingest times.
                self._record_ingestion_delay(bars)
                self._publish_bars(bars)
                self.metrics.inc(
                    "mdgw_collector_bars_ingested_total", amount=count
                )
            if count == 0:
                self._stop.wait(POLL_INTERVAL_S)

    def _publish_bars(self, bars: list[Bar]) -> None:
        streaming = self._streaming
        if streaming is None:
            return
        # `publish_bars` groups internally; the counter delta gives the exact
        # number of Bar frames actually sent without re-implementing grouping.
        before = streaming.published_frames
        streaming.publish_bars(bars)
        published = streaming.published_frames - before
        if published:
            self.metrics.inc(
                "mdgw_collector_zmq_publishes_total", amount=published
            )

        last = max(bar.close_ts for bar in bars)
        self._last_bar_ts = last

    # -- readiness / status ------------------------------------------------

    def _wait_for_api_ready(self) -> bool:
        """Wait for the api READY handshake when configured.

        Returns ``True`` immediately when waiting is disabled or a peer READY
        was already observed. Returns ``False`` after `api_ready_timeout_s` and
        logs `collector.api_ready_timeout` so a split deployment can proceed.
        """
        if not self._wait_for_api:
            return True

        streaming = self._streaming
        if streaming is None:
            return False

        timeout = self.config.service.api_ready_timeout_s
        deadline = time.monotonic() + timeout
        while not streaming.peer_ready:
            if self._stop.is_set():
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if streaming.wait_until_peer_ready(min(remaining, 0.1)):
                return True

        if streaming.peer_ready:
            return True
        logger.warning(
            "api did not signal readiness before the ingest deadline",
            extra={
                "event": "collector.api_ready_timeout",
                "timeout_s": timeout,
            },
        )
        return False

    def _run_status_loop(self) -> None:
        while not self._stop.is_set():
            self._publish_status()
            self._stop.wait(STATUS_INTERVAL_S)

    def _publish_status(self) -> None:
        streaming = self._streaming
        if streaming is None:
            return
        try:
            streaming.publish_status(self.status_payload())
        except Exception:  # pragma: no cover - defensive status path
            logger.debug("collector status publish failed", exc_info=True)

    def _ready_payload(self) -> tuple[bool, str]:
        if self._started and self._ready.is_set():
            return True, "collector ready"
        return False, "collector starting"

    def status_payload(self) -> dict:
        health = self.core.health_snapshot()
        sink = self.core.sink.stats()
        feed_statuses = handler_feed_statuses(self._feed_handler)
        return {
            "service": "collector",
            "role": "collector",
            "ready": self._ready.is_set(),
            "uptime_s": self._uptime_s(),
            "feed": getattr(self._feed_handler, "name", None),
            # Truthful upstream lifecycle. Venues/feeds come from the
            # configured/runtime handlers, never from a hardcoded list.
            "feeds": [status.to_dict() for status in feed_statuses],
            "venues": sorted(
                {
                    status.venue
                    for status in feed_statuses
                    if status.venue
                }
            ),
            "last_ingest_ts": ns_iso(self.core.last_ingest_ts),
            "last_bar_ts": ns_iso(self._last_bar_ts),
            "bars_ingested": self.core.bars_ingested,
            "bars_written": self.core.bars_written,
            "stale_count": health.get("stale_count", 0),
            "gap_unrecoverable_count": health.get("gap_unrecoverable_count", 0),
            "write_queue_depth": sink.get("queue_depth", 0),
            "write_dropped_bars": sink.get("dropped_bars", 0),
            "write_lag_bars": sink.get("lag_bars", 0),
            "write_last_flush_ts": sink.get("last_flush_ts"),
            "write_last_flush_duration_s": sink.get("last_flush_duration_s"),
            "write_sink": sink,
            "quarantine": self.core.quarantine.stats(),
            "health": health,
        }

    # -- metrics -----------------------------------------------------------

    def _record_ingestion_delay(self, bars: list[Bar]) -> None:
        """Observe ``ingest_ts - event_ts`` for live base Bars only."""
        for bar in bars:
            if bar.aggregation_source == AGGREGATION_INTERNAL:
                continue
            delay_s = (bar.ingest_ts - bar.event_ts).total_seconds()
            self.metrics.observe(
                "mdgw_collector_ingestion_delay_seconds",
                delay_s,
                labels={"feed": bar.feed},
                help=_INGESTION_DELAY_HELP,
                buckets=INGESTION_DELAY_BUCKETS,
            )

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
        sink = self.core.sink.stats()
        with self._written_metric_lock:
            written = float(self.core.bars_written)
            if written > self._last_written_metric:
                self.metrics.inc(
                    "mdgw_collector_bars_written_total",
                    amount=written - self._last_written_metric,
                    help=(
                        "Bars persisted to the Dataset "
                        "(including aggregated parents)"
                    ),
                )
                self._last_written_metric = written
        self.metrics.set(
            "mdgw_collector_write_queue_depth",
            sink.get("queue_depth", 0),
            help="Bars waiting in the collector write queue",
        )
        self.metrics.set(
            "mdgw_collector_write_dropped_total",
            sink.get("dropped_bars", 0),
            help="Bars dropped because the write queue was full",
        )
        self.metrics.set(
            "mdgw_collector_write_lag_total",
            sink.get("lag_bars", 0),
            help="Cumulative write-queue lag (dropped Bars)",
        )
        self.metrics.set(
            "mdgw_collector_last_flush_ts_seconds",
            iso_to_seconds(sink.get("last_flush_ts")),
            help="Last successful write-buffer flush timestamp",
        )
        self.metrics.set(
            "mdgw_collector_last_flush_duration_seconds",
            sink.get("last_flush_duration_s") or 0.0,
            help="Duration of the last successful write-buffer flush",
        )
        with self._storage_metric_lock:
            if (
                self.metrics.get(
                    "mdgw_collector_storage_bytes_written_total"
                )
                is None
            ):
                # Expose an explicit zero series before the first write.
                self.metrics.inc(
                    "mdgw_collector_storage_bytes_written_total",
                    amount=0,
                    help=_STORAGE_BYTES_HELP,
                )
            self._last_storage_bytes_metric = advance_counter(
                self.metrics,
                "mdgw_collector_storage_bytes_written_total",
                sink.get("bytes_written", 0) or 0.0,
                self._last_storage_bytes_metric,
                help=_STORAGE_BYTES_HELP,
            )
        self._update_quarantine_metrics()
        self._update_feed_metrics(handler_feed_statuses(self._feed_handler))
        update_dataset_metrics(self.metrics, self.core.store)
        return self.metrics.render()

    def _update_feed_metrics(
        self, statuses: tuple[FeedConnectionStatus, ...]
    ) -> None:
        """Expose upstream lifecycle with bounded ``feed``/``venue`` labels."""
        with self._feed_metric_lock:
            for status in statuses:
                labels = {"feed": status.feed, "venue": status.venue}
                for state in FEED_STATES:
                    self.metrics.set(
                        "mdgw_collector_feed_connection_state",
                        1 if status.state == state else 0,
                        labels={**labels, "state": state},
                        help=(
                            "Feed Handler connection state from real lifecycle "
                            "transitions, never inferred from Feed Freshness "
                            "(1 for the current state)"
                        ),
                    )
                self.metrics.set(
                    "mdgw_collector_feed_state_since_seconds",
                    _timestamp_seconds(status.state_since),
                    labels=labels,
                    help=(
                        "Unix time when the current Feed Handler connection "
                        "state began"
                    ),
                )
                self.metrics.set(
                    "mdgw_collector_feed_last_event_ts_seconds",
                    _timestamp_seconds(status.last_event_ts),
                    labels=labels,
                    help=(
                        "Unix time when the last live watch event arrived "
                        "from the Feed Handler (event arrival; not a protocol "
                        "heartbeat)"
                    ),
                )
                for code in FEED_ERROR_CODES:
                    self.metrics.set(
                        "mdgw_collector_feed_last_error_ts_seconds",
                        _timestamp_seconds(status.last_error_at)
                        if status.last_error_code == code
                        else 0.0,
                        labels={**labels, "error_code": code},
                        help=(
                            "Unix time of the last sanitized Feed Handler "
                            "error by bounded category (0 = no such error)"
                        ),
                    )
                key = (status.feed, status.venue)
                if key not in self._last_feed_reconnects:
                    # Expose an explicit zero series before the first failure.
                    self.metrics.inc(
                        "mdgw_collector_feed_reconnects_total",
                        amount=0,
                        labels=labels,
                        help=(
                            "Feed Handler reconnect attempts after upstream "
                            "watch failures"
                        ),
                    )
                self._last_feed_reconnects[key] = advance_counter(
                    self.metrics,
                    "mdgw_collector_feed_reconnects_total",
                    status.reconnect_count,
                    self._last_feed_reconnects.get(key, 0.0),
                    labels=labels,
                    help=(
                        "Feed Handler reconnect attempts after upstream watch "
                        "failures"
                    ),
                )

    def _update_quarantine_metrics(self) -> None:
        """Expose Quarantine records and write failures by stable labels."""
        stats = self.core.quarantine.stats()
        with self._quarantine_metric_lock:
            for entry in stats["records_by_stage_and_reason"]:
                key = (entry["source_stage"], entry["reason_code"])
                previous = self._last_quarantine_counts.get(key, 0)
                delta = int(entry["count"]) - previous
                if delta > 0:
                    self.metrics.inc(
                        "mdgw_collector_quarantine_records_total",
                        labels={
                            "source_stage": key[0],
                            "reason_code": key[1],
                        },
                        amount=delta,
                        help="Rejected inputs written to the Quarantine Dataset",
                    )
                self._last_quarantine_counts[key] = int(entry["count"])

            failures = int(stats["write_failures"])
            self.metrics.inc(
                "mdgw_collector_quarantine_write_failures_total",
                amount=max(0, failures - self._last_quarantine_failures),
                help=(
                    "Quarantine writes that failed; the corresponding input "
                    "was not acknowledged (fail-closed)"
                ),
            )
            self._last_quarantine_failures = failures

        # Capacity/retention visibility: record volume grows until an operator
        # runs `mdgw quarantine-gc`. Only stable labels and plain gauges are
        # used (no paths, request ids or exception text).
        self.metrics.set(
            "mdgw_collector_quarantine_active_files",
            int(stats["active_file_count"]),
            help="Active Quarantine Data Files",
        )
        self.metrics.set(
            "mdgw_collector_quarantine_active_bytes",
            int(stats["active_bytes"]),
            help="Bytes held by active Quarantine Data Files",
        )
        self.metrics.set(
            "mdgw_collector_quarantine_retention_days",
            int(stats["retention_days"]),
            help="Quarantine Retention in days (0 = keep forever)",
        )

    def _uptime_s(self) -> float:
        if self._started_at is None:
            return 0.0
        return max(0.0, time.monotonic() - self._started_at)


def _timestamp_seconds(value: datetime | None) -> float:
    if value is None:
        return 0.0
    return datetime_to_ns(value) / 1_000_000_000.0


__all__ = ["CollectorService"]
