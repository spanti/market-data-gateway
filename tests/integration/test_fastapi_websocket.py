# ruff: file-ignore[implicit-namespace-package]
"""End-to-end tests for the FastAPI WebSocket route.

The app is built with a real `WebSocketHub` and served through `AsgiServer` on
an ephemeral socket. Clients use the `websockets` library, so assertions see
wire behaviour only. Bars are written by `GatewayRuntime` before the FastAPI
app is built; the hub is fanned out to with `call_soon_threadsafe`, exactly as
the production runtime dispatches from its ZMQ subscriber thread.
"""

from __future__ import annotations

import asyncio
import json
import re
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import websockets

from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.api.ws_hub import WebSocketHub
from mdgw.config import load_config
from mdgw.core.envelope import table_to_messages
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.runtime import GatewayRuntime

if TYPE_CHECKING:
    from collections.abc import Generator

    from mdgw.store.parquet_dataset import ParquetBarStore

PROJECT_ROOT = Path(__file__).resolve().parents[2]
HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-fastapi-ws"
BTC = "binance:BTC/USDT"
TOPIC = f"market.bar.1m.{BTC}"
NS_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z")

ENVELOPE_KEYS = frozenset(
    {
        "schema_version",
        "data_type",
        "instrument_id",
        "event_ts",
        "ingest_ts",
        "feed",
        "seq",
        "payload",
    }
)


# -- fixtures and helpers ---------------------------------------------------


def _config():
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    return config.model_copy(update={"server": server})


def _bar_input(minute: int) -> BarInput:
    return BarInput(
        open_ts=datetime(2024, 1, 1, 0, minute, tzinfo=UTC),
        open="65000.1",
        high="65010.0",
        low="64990.0",
        close="65005.5",
        volume="12.34",
        trade_count=128,
    )


def _seed(
    tmp_path: Path, bars: list[BarInput]
) -> tuple[ParquetBarStore, list[dict[str, Any]]]:
    """Persist Bars through the runtime and return the REST-equivalent messages."""
    feed = FakeFeedHandler(instrument_id=BTC, bars=bars)
    runtime = GatewayRuntime(
        feed_handler=feed,
        config=_config(),
        data_root=tmp_path / "data",
    )
    runtime.start()
    try:
        runtime.ingest()
        table = runtime.store.read_bars(instrument_id=BTC, interval="1m")
    finally:
        runtime.stop()
    return runtime.store, table_to_messages(table)


def _app_and_hub(
    store: ParquetBarStore,
    *,
    queue_size: int = 1000,
    lag_notice_interval_ms: int = 1000,
) -> tuple[Any, WebSocketHub]:
    hub = WebSocketHub(
        queue_size=queue_size,
        lag_notice_interval_ms=lag_notice_interval_ms,
    )
    app = create_app(store, ws_hub=hub)
    return app, hub


@contextmanager
def _serve(app: Any, hub: WebSocketHub) -> Generator[AsgiServer]:
    server = AsgiServer()
    try:
        server.start(
            app,
            host=HOST,
            port=0,
            thread_name=THREAD_NAME,
            on_shutdown=hub.close,
        )
        yield server
    finally:
        server.stop()

    assert _threads_named(THREAD_NAME) == []


def _threads_named(name: str) -> list[threading.Thread]:
    return [thread for thread in threading.enumerate() if thread.name == name]


def _wait_until(predicate: Any, *, timeout_s: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def _ws_url(server: AsgiServer) -> str:
    return server.base_url.replace("http://", "ws://", 1) + "/ws"


async def _recv_op(ws: Any, op: str, *, timeout_s: float = 3.0) -> dict[str, Any]:
    async def _await_op() -> dict[str, Any]:
        while True:
            payload = json.loads(await ws.recv())
            if payload.get("op") == op:
                return payload

    return await asyncio.wait_for(_await_op(), timeout_s)


async def _ping(ws: Any) -> None:
    await ws.send(json.dumps({"op": "ping"}))
    assert await _recv_op(ws, "pong") == {"op": "pong"}


# -- tests ------------------------------------------------------------------


def test_subscribe_pong_and_event_envelope_match_rest_json(
    tmp_path: Path,
) -> None:
    store, messages = _seed(tmp_path, [_bar_input(0), _bar_input(1)])
    app, hub = _app_and_hub(store)
    assert len(messages) == 2

    async def scenario() -> tuple[dict[str, Any], int]:
        async with websockets.connect(_ws_url(server)) as ws:
            await ws.send(json.dumps({"op": "subscribe", "topics": [TOPIC]}))
            # Pong proves the subscribe op was processed before fan-out.
            await _ping(ws)
            server.call_soon_threadsafe(hub.publish, TOPIC, messages)
            event = await _recv_op(ws, "event")
            return event, hub.session_count

    with _serve(app, hub) as server:
        event, session_count = asyncio.run(scenario())

    assert session_count == 1
    assert set(event) == {"op", "topic", "data"}
    assert event["op"] == "event"
    assert event["topic"] == TOPIC
    # The WS Message Envelope is exactly the REST JSON form.
    assert event["data"] == messages[0]
    assert set(event["data"]) == ENVELOPE_KEYS
    assert event["data"]["schema_version"] == 1
    assert event["data"]["instrument_id"] == BTC
    assert event["data"]["payload"]["open"] == "65000.1"
    assert event["data"]["payload"]["open_ts"] == (
        "2024-01-01T00:00:00.000000000Z"
    )
    assert NS_TIMESTAMP.fullmatch(event["data"]["ingest_ts"]) is not None


def test_ping_pong_works_without_any_subscription(tmp_path: Path) -> None:
    store, _ = _seed(tmp_path, [_bar_input(0)])
    app, hub = _app_and_hub(store)

    async def scenario() -> None:
        async with websockets.connect(_ws_url(server)) as ws:
            await _ping(ws)
            await _ping(ws)

    with _serve(app, hub) as server:
        asyncio.run(scenario())


def test_unsubscribe_stops_events_and_connection_stays_usable(
    tmp_path: Path,
) -> None:
    store, messages = _seed(tmp_path, [_bar_input(0), _bar_input(1)])
    app, hub = _app_and_hub(store)

    async def scenario() -> None:
        async with websockets.connect(_ws_url(server)) as ws:
            await ws.send(json.dumps({"op": "subscribe", "topics": [TOPIC]}))
            await _ping(ws)
            server.call_soon_threadsafe(hub.publish, TOPIC, messages)
            # Drain both published events before unsubscribing so a late
            # in-flight write cannot be mistaken for a post-unsubscribe event.
            await _recv_op(ws, "event")
            await _recv_op(ws, "event")

            await ws.send(json.dumps({"op": "unsubscribe", "topics": [TOPIC]}))
            # Pong proves the unsubscribe op was processed.
            await _ping(ws)
            server.call_soon_threadsafe(hub.publish, TOPIC, messages)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(_recv_op(ws, "event"), 0.5)
            # The connection is still usable after unsubscribing.
            await _ping(ws)

    with _serve(app, hub) as server:
        asyncio.run(scenario())


def test_slow_consumer_gets_lag_notice_and_stays_connected(
    tmp_path: Path,
) -> None:
    bars = [_bar_input(minute) for minute in range(30)]
    store, messages = _seed(tmp_path, bars)
    app, hub = _app_and_hub(
        store,
        queue_size=1,
        lag_notice_interval_ms=0,
    )

    async def scenario() -> tuple[dict[str, Any], dict[str, Any]]:
        async with websockets.connect(_ws_url(server)) as ws:
            await ws.send(json.dumps({"op": "subscribe", "topics": [TOPIC]}))
            await _ping(ws)
            # One publish call enqueues all Bars on the server loop, so the
            # writer cannot drain a queue of size 1 in between.
            server.call_soon_threadsafe(hub.publish, TOPIC, messages)
            notice = await _recv_op(ws, "lag_notice")
            event = await _recv_op(ws, "event")
            # Still connected after the drop burst.
            await _ping(ws)
            return notice, event

    with _serve(app, hub) as server:
        notice, event = asyncio.run(scenario())

    assert set(notice) == {"op", "dropped", "since"}
    assert notice["op"] == "lag_notice"
    assert notice["dropped"] == len(messages) - 1
    assert NS_TIMESTAMP.fullmatch(notice["since"]) is not None
    assert event["topic"] == TOPIC
    assert event["data"] == messages[-1]


def test_disconnect_unregisters_the_session(tmp_path: Path) -> None:
    store, _ = _seed(tmp_path, [_bar_input(0)])
    app, hub = _app_and_hub(store)
    registered = False

    async def scenario() -> None:
        nonlocal registered
        async with websockets.connect(_ws_url(server)) as ws:
            await _ping(ws)
            registered = _wait_until(lambda: hub.session_count == 1)
        assert _wait_until(lambda: hub.session_count == 0)

    with _serve(app, hub) as server:
        asyncio.run(scenario())

    assert registered
    assert hub.session_count == 0
