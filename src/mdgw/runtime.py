"""GatewayRuntime: the single composition root used by behavior tests.

It wires Feed Handler -> normalize -> Parquet Dataset -> REST API, plus the
in-process ZMQ Streaming Interface. Ingest is synchronous so tests can drive
the whole chain deterministically; the HTTP server runs on its own event loop.
"""

from __future__ import annotations

import asyncio
import threading
from collections import defaultdict
from pathlib import Path
from uuid import uuid4

from aiohttp import web

from mdgw.api.server import create_app
from mdgw.bus.zmq_bus import ZMQBus, ipc_to_table, table_to_ipc
from mdgw.config import GatewayConfig
from mdgw.core.bar import Bar
from mdgw.core.schema import bars_to_logical_table
from mdgw.core.time import utc_now
from mdgw.ingest.feed_handler import FeedHandler
from mdgw.normalize.bar import normalize_bar
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
        asset_class: str = "crypto",
    ) -> None:
        self.feed_handler = feed_handler
        self.config = config
        # Placeholder until Instrument Reference Data provides asset_class.
        self.asset_class = asset_class
        self.store = ParquetBarStore(
            data_root if data_root is not None else config.storage.root,
            compression=config.storage.compression,
            row_group_size=config.storage.row_group_size,
            format_version=config.storage.format_version,
            schema_version=config.storage.schema_version,
        )

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
        bus = ZMQBus(zmq_hwm=self.config.bus.zmq_hwm)
        try:
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
            bus.close()
            self._stop_http()
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
        self._started = False

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
        """Drain the Feed Handler once, persist Bars, and publish them."""
        if not self._started:
            raise RuntimeError("GatewayRuntime.start() must be called before ingest()")

        ingest_ts = utc_now()
        bars = [
            normalize_bar(raw, feed=self.feed_handler.name, ingest_ts=ingest_ts)
            for raw in self.feed_handler.iter_bars()
        ]
        if not bars:
            return 0

        self.store.write_bars(bars, asset_class=self.asset_class)
        target = self._publish_bars(bars)
        self._wait_for_received(target)
        return len(bars)

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
        app = create_app(self.store)
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
