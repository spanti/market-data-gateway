"""API Service Role: REST/WS plus ZMQ fan-out from the collector PUB."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import threading
import time
from typing import TYPE_CHECKING

from aiohttp import web

from mdgw.api.server import create_app
from mdgw.api.ws_hub import WebSocketHub
from mdgw.bus.zmq_bus import READY_TOPIC, ZMQBus, ipc_to_table
from mdgw.core.envelope import table_to_messages
from mdgw.metrics import MetricsRegistry
from mdgw.reference import load_instrument_store
from mdgw.service.common import ns_iso, update_dataset_metrics
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from datetime import datetime
    from pathlib import Path

    from mdgw.config import GatewayConfig

logger = logging.getLogger(__name__)

MARKET_BAR_TOPIC_PREFIX = "market.bar."
INTERNAL_STATUS_TOPIC = "_internal.status"
READY_INTERVAL_S = 0.05
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
        )
        self.instrument_store = load_instrument_store(self.store.root)
        self.metrics = MetricsRegistry()

        self._bus: ZMQBus | None = None
        self._publish_endpoint: str | None = None
        self._ready_event: threading.Event | None = None

        self._hub: WebSocketHub | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._runner: web.AppRunner | None = None
        self._port: int | None = None

        self._stop = threading.Event()
        self._ready_thread: threading.Thread | None = None
        self._started = False
        self._started_at: float | None = None

        self._collector_ready = False
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
        self._bus = ZMQBus(zmq_hwm=self.config.bus.zmq_hwm)
        resolved = self._bus.bind_publisher(self.config.bus.api_pub_bind)
        self._publish_endpoint = resolved[0]
        self._ready_event = self._bus.subscribe(
            self.config.bus.collector_pub_connect,
            topic_prefix=MARKET_BAR_TOPIC_PREFIX,
            extra_prefixes=(INTERNAL_STATUS_TOPIC,),
            handler=self._on_message,
        )

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

        self._start_http()
        self._stop.clear()
        self._started = True
        self._started_at = time.monotonic()
        self._ready_thread = threading.Thread(
            target=self._run_ready_loop,
            name="mdgw-api-ready",
            daemon=True,
        )
        self._ready_thread.start()
        logger.info(
            "api service started",
            extra={
                "event": "api.started",
                "http": self.base_url,
                "publish_endpoint": self._publish_endpoint,
            },
        )

    def stop(self) -> None:
        self._stop.set()
        if self._ready_thread is not None:
            self._ready_thread.join(timeout=5)
            self._ready_thread = None
        self._stop_http()
        if self._bus is not None:
            self._bus.close()
            self._bus = None
        self._publish_endpoint = None
        self._ready_event = None
        self._started = False
        self._started_at = None

    # -- properties --------------------------------------------------------

    @property
    def base_url(self) -> str:
        if self._port is None:
            raise RuntimeError(_NOT_STARTED_MESSAGE)
        host = self.config.server.host
        if host in {"0.0.0.0", "::"}:  # ruff: ignore[hardcoded-bind-all-interfaces] - compares the configured host; no socket bind
            host = "127.0.0.1"
        return f"http://{host}:{self._port}"

    @property
    def http_port(self) -> int:
        if self._port is None:
            raise RuntimeError(_NOT_STARTED_MESSAGE)
        return self._port

    @property
    def publish_endpoint(self) -> str:
        if self._publish_endpoint is None:
            raise RuntimeError(_NOT_STARTED_MESSAGE)
        return self._publish_endpoint

    # -- collector subscription -------------------------------------------

    def _on_message(self, topic: str, payload: bytes) -> None:
        """Runs on the ZMQ subscriber thread; never touches aiohttp objects."""
        self._collector_ready = True
        if topic == INTERNAL_STATUS_TOPIC:
            try:
                status = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, ValueError):
                return
            if isinstance(status, dict):
                self._collector_status = status
            return
        if not topic.startswith(MARKET_BAR_TOPIC_PREFIX):
            return

        try:
            table = ipc_to_table(payload)
        except Exception:  # ruff: ignore[blind-except]  # pragma: no cover - malformed external frame
            logger.warning(
                "discarding malformed collector Bar frame",
                extra={"event": "api.malformed_frame"},
            )
            return

        self._messages_received += 1
        self.metrics.inc("mdgw_api_messages_received_total")
        bus = self._bus
        if bus is not None:
            try:
                bus.publish(topic, payload)
                self._messages_forwarded += 1
                self.metrics.inc("mdgw_api_messages_forwarded_total")
            except RuntimeError:  # pragma: no cover - shutdown race
                pass
        if table.num_rows:
            with contextlib.suppress(KeyError, ValueError):  # pragma: no cover
                self._last_bar_ts = max(
                    row for row in table.column("close_ts").to_pylist()
                )

        hub = self._hub
        loop = self._loop
        if hub is None or loop is None:
            return
        messages = table_to_messages(table)
        with contextlib.suppress(RuntimeError):  # pragma: no cover - closed loop
            loop.call_soon_threadsafe(hub.publish, topic, messages)

    def _run_ready_loop(self) -> None:
        # Only advertise READY once the collector handshake was observed. That
        # proves this api's SUB forwarding is live, so a collector waiting on
        # this READY can safely start publishing the first Bars. An api without
        # a collector never publishes READY (it has no live stream to offer).
        while not self._stop.is_set():
            bus = self._bus
            if bus is not None and self._collector_is_ready():
                with contextlib.suppress(RuntimeError):
                    bus.publish(READY_TOPIC, b"")
            self._stop.wait(READY_INTERVAL_S)

    # -- readiness / status ------------------------------------------------

    def _collector_is_ready(self) -> bool:
        if self._collector_ready:
            return True
        ready_event = self._ready_event
        return ready_event is not None and ready_event.is_set()

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
        return {
            "service": "api",
            "role": "api",
            "uptime_s": self._uptime_s(),
            "http": {"host": self.config.server.host, "port": self._port},
            "zmq": {
                "publish_endpoint": self._publish_endpoint,
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
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._run_event_loop,
            name="mdgw-api-http",
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


__all__ = ["ApiService"]
