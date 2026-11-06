"""API Service Role: REST/WS plus ZMQ fan-out from the collector PUB."""

from __future__ import annotations

import contextlib
import logging
import threading
import time
from typing import TYPE_CHECKING

from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.api.ws_hub import WebSocketHub
from mdgw.core.envelope import table_to_messages
from mdgw.metrics import MetricsRegistry
from mdgw.reference import load_instrument_store
from mdgw.service.common import ns_iso, update_dataset_metrics
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.streaming import StreamingInterface

if TYPE_CHECKING:
    from datetime import datetime
    from pathlib import Path

    import pyarrow as pa

    from mdgw.config import GatewayConfig

logger = logging.getLogger(__name__)

_NOT_STARTED_MESSAGE = "ApiService.start() must be called first"


class ApiService:
    """REST/WS server consuming collector Bars and re-publishing them."""

    def __init__(
        self,
        config: GatewayConfig,
        *,
        data_root: Path | None = None,
    ) -> None:
        self.config = config
        self.store = ParquetBarStore(
            data_root if data_root is not None else config.storage.root,
            compression=config.storage.compression,
            row_group_size=config.storage.row_group_size,
            format_version=config.storage.format_version,
            schema_version=config.storage.schema_version,
            duckdb=config.storage.duckdb.to_options(),
        )
        self.instrument_store = load_instrument_store(self.store.root)
        self.metrics = MetricsRegistry()

        self._streaming: StreamingInterface | None = None

        self._hub: WebSocketHub | None = None
        self._http = AsgiServer()

        self._stop = threading.Event()
        self._started = False
        self._started_at: float | None = None

        self._collector_status: dict | None = None
        self._last_bar_ts: datetime | None = None
        self._messages_received = 0
        self._messages_forwarded = 0

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._started:
            return
        try:
            self._start_service()
        except Exception:
            self.stop()
            raise

    def _start_service(self) -> None:
        self._stop.clear()
        streaming = StreamingInterface(
            bind=self.config.bus.api_pub_bind,
            connect=self.config.bus.collector_pub_connect,
            zmq_hwm=self.config.bus.zmq_hwm,
        )
        self._streaming = streaming
        streaming.start()

        self.metrics.inc(
            "mdgw_api_messages_received_total",
            amount=0,
            help="Bar messages received from the collector",
        )
        self.metrics.inc(
            "mdgw_api_messages_forwarded_total",
            amount=0,
            help="Bar messages re-published to external ZMQ consumers",
        )

        # `forward=True` re-publishes the collector's original frame bytes on
        # this api's PUB before the handler runs; external consumers therefore
        # do not need the collector's address and no re-encoding happens.
        streaming.subscribe_bars(self._on_bar, forward=True)
        streaming.subscribe_status(self._on_status)

        self._start_http()
        self._started = True
        self._started_at = time.monotonic()
        logger.info(
            "api service started",
            extra={
                "event": "api.started",
                "http": self.base_url,
                "publish_endpoint": streaming.endpoints[0],
            },
        )

    def stop(self) -> None:
        self._stop.set()
        # Stop accepting HTTP/WS traffic before tearing down the streaming
        # fan-out the handlers feed.
        self._stop_http()
        streaming = self._streaming
        self._streaming = None
        self._started = False
        self._started_at = None
        if streaming is not None:
            streaming.stop()
        # ApiService owns its Store; stop() is idempotent.
        self.store.close()

    # -- properties --------------------------------------------------------

    def _started_http(self) -> AsgiServer:
        """Return the started `AsgiServer`, or raise this service's error."""
        try:
            _ = self._http.bound_port
        except RuntimeError as exc:
            raise RuntimeError(_NOT_STARTED_MESSAGE) from exc
        return self._http

    @property
    def base_url(self) -> str:
        return self._started_http().base_url

    @property
    def http_port(self) -> int:
        return self._started_http().bound_port

    @property
    def publish_endpoint(self) -> str:
        streaming = self._streaming
        if streaming is None or not streaming.endpoints:
            raise RuntimeError(_NOT_STARTED_MESSAGE)
        return streaming.endpoints[0]

    # -- collector subscription -------------------------------------------

    def _on_bar(self, topic: str, table: pa.Table) -> None:
        """Runs on the streaming subscriber thread; never touches server objects."""
        self._messages_received += 1
        self.metrics.inc("mdgw_api_messages_received_total")
        # The module already forwarded the raw frame before calling us.
        self._messages_forwarded += 1
        self.metrics.inc("mdgw_api_messages_forwarded_total")

        if table.num_rows:
            with contextlib.suppress(KeyError, ValueError):  # pragma: no cover
                self._last_bar_ts = max(
                    row for row in table.column("close_ts").to_pylist()
                )

        self._mark_ready_when_collector_ready()

        hub = self._hub
        if hub is None:
            return
        messages = table_to_messages(table)
        with contextlib.suppress(RuntimeError):  # pragma: no cover - stopped server
            self._http.call_soon_threadsafe(hub.publish, topic, messages)

    def _on_status(self, status: dict) -> None:
        """Runs on the streaming subscriber thread; never touches server objects."""
        self._collector_status = status
        self._mark_ready_when_collector_ready()

    # -- readiness / status ------------------------------------------------

    def _mark_ready_when_collector_ready(self) -> None:
        """Advertise READY once the collector handshake was observed.

        That proves this api's SUB forwarding is live, so a collector waiting
        on this READY can safely publish the first Bars. The heartbeat is
        idempotent; status frames arrive every second and the collector's
        READY heartbeat sets `peer_ready` immediately, so either handler can
        perform the hand-off on a threading caller.
        """
        if self._stop.is_set():
            return
        if not self._collector_is_ready():
            return
        streaming = self._streaming
        if streaming is None:
            return
        with contextlib.suppress(RuntimeError):  # pragma: no cover - shutdown race
            streaming.mark_ready()

    def _collector_is_ready(self) -> bool:
        streaming = self._streaming
        if streaming is None:
            return False
        return streaming.peer_ready or streaming.received_frames > 0

    def ready_payload(self) -> dict:
        collector_ready = self._collector_is_ready()
        if not self._started:
            return {
                "ready": False,
                "reason": "api starting",
                "collector_ready": collector_ready,
            }
        if collector_ready:
            return {
                "ready": True,
                "reason": "collector subscription ready",
                "collector_ready": True,
            }
        if not self.config.service.collector_required:
            return {
                "ready": True,
                "reason": "collector optional and not connected",
                "collector_ready": False,
            }
        return {
            "ready": False,
            "reason": "waiting for collector subscription",
            "collector_ready": False,
        }

    def status_payload(self) -> dict:
        try:
            http_port = self._http.bound_port
        except RuntimeError:
            http_port = None
        streaming = self._streaming
        publish_endpoint = (
            streaming.endpoints[0]
            if streaming is not None and streaming.endpoints
            else None
        )
        return {
            "service": "api",
            "role": "api",
            "uptime_s": self._uptime_s(),
            "http": {"host": self.config.server.host, "port": http_port},
            "zmq": {
                "publish_endpoint": publish_endpoint,
                "collector_endpoint": self.config.bus.collector_pub_connect,
            },
            "messages_received": self._messages_received,
            "messages_forwarded": self._messages_forwarded,
            "collector": {
                "ready": self._collector_is_ready(),
                "last_bar_ts": ns_iso(self._last_bar_ts),
                "status": self._collector_status,
            },
        }

    # -- metrics -----------------------------------------------------------

    def render_metrics(self) -> str:
        self.metrics.set(
            "mdgw_up", 1 if self._started else 0, help="Service process is up"
        )
        ready = self.ready_payload().get("ready", False)
        self.metrics.set(
            "mdgw_ready", 1 if ready else 0, help="Service process is ready"
        )
        hub = self._hub
        self.metrics.set(
            "mdgw_api_ws_sessions",
            0 if hub is None else hub.session_count,
            help="Open WebSocket sessions",
        )
        self.metrics.set(
            "mdgw_api_ws_dropped_total",
            0 if hub is None else hub.dropped_total,
            help="WebSocket messages dropped for backpressure",
        )
        update_dataset_metrics(self.metrics, self.store)
        return self.metrics.render()

    def _uptime_s(self) -> float:
        if self._started_at is None:
            return 0.0
        return max(0.0, time.monotonic() - self._started_at)

    # -- HTTP --------------------------------------------------------------

    def _start_http(self) -> None:
        self._hub = WebSocketHub(
            queue_size=self.config.streaming.queue_size,
            lag_notice_interval_ms=self.config.streaming.lag_notice_interval_ms,
        )
        app = create_app(
            self.store,
            instrument_store=self.instrument_store,
            ws_hub=self._hub,
            ws_path=self.config.server.ws_path,
            metrics=self.metrics,
            metrics_render=self.render_metrics,
            status_provider=self.status_payload,
            ready_provider=self.ready_payload,
            service_role="api",
            rate_limit=self.config.rate_limit,
        )
        self._http.start(
            app,
            host=self.config.server.host,
            port=self.config.server.http_port,
            thread_name="mdgw-api-http",
            on_shutdown=self._hub.close,
        )

    def _stop_http(self) -> None:
        self._http.stop()
        self._hub = None


__all__ = ["ApiService"]
