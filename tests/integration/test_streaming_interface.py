"""Behavior tests for the external Streaming Interface (ZMQ + WebSocket).

Seams under test:
- `GatewayRuntime.stream_endpoint`: a raw ZMQ SUB connected to the PUB port
  receives `[topic][Arrow IPC stream]` frames.
- The `/ws` route: subscribe/unsubscribe/ping and the JSON Message Envelope,
  which must match the REST JSON form produced by `table_to_messages`.
- A small WS queue: a slow consumer gets `lag_notice` without disconnecting.
"""

from __future__ import annotations

import asyncio
import json
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import aiohttp
import pyarrow as pa
import pytest
import zmq

from mdgw.bus.zmq_bus import ipc_to_table
from mdgw.config import GatewayConfig, load_config
from mdgw.core.bar import RawBar
from mdgw.core.schema import BAR_SCHEMA
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.runtime import GatewayRuntime

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc

BTC = "binance:BTC/USDT"
ETH = "binance:ETH/USDT"

INSTRUMENT_YAML = """\
- instrument_id: "binance:BTC/USDT"
  venue: binance
  symbol: "BTC/USDT"
  asset_class: crypto
  instrument_type: spot
  base: BTC
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.00001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
- instrument_id: "binance:ETH/USDT"
  venue: binance
  symbol: "ETH/USDT"
  asset_class: crypto
  instrument_type: spot
  base: ETH
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.0001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
"""


class TickingFeedHandler:
    """Emit one fresh closed 1m Bar per instrument on every `iter_bars()`.

    Used by the slow-joiner tests: the subscription may not have propagated on
    the first publish, so the test retries ingestion until a frame arrives.
    """

    name = "ticking"
    supports_history = False

    def __init__(self, instrument_ids: tuple[str, ...], start: datetime) -> None:
        self.instrument_ids = instrument_ids
        self._next = start

    def iter_bars(self):
        open_ts = self._next
        self._next += timedelta(minutes=1)
        for instrument_id in self.instrument_ids:
            yield RawBar(
                instrument_id=instrument_id,
                interval="1m",
                open_ts=open_ts,
                open=Decimal("100.5"),
                high=Decimal("101.5"),
                low=Decimal("99.5"),
                close=Decimal("100.9"),
                volume=Decimal("3.25"),
                trade_count=7,
                revision=0,
            )

    def fetch_history(self, instrument_id, interval, start, end):
        return ()


def _instrument_dir(tmp_path: Path, *, two: bool = False) -> Path:
    path = tmp_path / "instruments"
    path.mkdir(parents=True, exist_ok=True)
    text = INSTRUMENT_YAML if two else INSTRUMENT_YAML.split('- instrument_id: "binance:ETH')[0]
    (path / "venues.yaml").write_text(text, encoding="utf-8")
    return path


def _config(
    tmp_path: Path,
    *,
    two_instruments: bool = False,
    queue_size: int = 1000,
    lag_notice_interval_ms: int = 1000,
) -> GatewayConfig:
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    streaming = config.streaming.model_copy(
        update={
            "queue_size": queue_size,
            "lag_notice_interval_ms": lag_notice_interval_ms,
        }
    )
    instruments = config.instruments.model_copy(
        update={"path": _instrument_dir(tmp_path, two=two_instruments)}
    )
    return config.model_copy(
        update={
            "server": server,
            "streaming": streaming,
            "instruments": instruments,
        }
    )


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


def _drain_frames(socket: zmq.Socket) -> list[list[bytes]]:
    frames: list[list[bytes]] = []
    while True:
        try:
            frames.append(socket.recv_multipart(zmq.NOBLOCK))
        except zmq.Again:
            return frames


def _collect_zmq_until(
    runtime: GatewayRuntime,
    socket: zmq.Socket,
    predicate,
    *,
    timeout_s: float = 5.0,
) -> list[tuple[str, pa.Table]]:
    """Retry ingestion until `predicate(received)` is true (slow-joiner safe).

    Retrying a real publish is the robust alternative to a fixed sleep: the
    first publishes may be silently dropped while the SUB subscription is still
    propagating.
    """
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    received: list[tuple[str, pa.Table]] = []
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        runtime.ingest()
        if dict(poller.poll(50)).get(socket) == zmq.POLLIN:
            for frames in _drain_frames(socket):
                topic = frames[0].decode("utf-8")
                received.append((topic, ipc_to_table(frames[1])))
        if predicate(received):
            return received
    raise AssertionError(f"no matching ZMQ frames after {timeout_s}s: {received!r}")


def test_external_zmq_sub_gets_arrow_ipc_for_exact_topic(tmp_path: Path) -> None:
    feed = FakeFeedHandler(instrument_id=BTC, bars=[_bar_input(0)])
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.SUBSCRIBE, f"market.bar.1m.{BTC}".encode())
    try:
        with GatewayRuntime(
            feed_handler=feed,
            config=_config(tmp_path / "single"),
            data_root=tmp_path / "single-data",
        ) as runtime:
            runtime.start()
            assert runtime.stream_endpoint.startswith("tcp://127.0.0.1:")
            port = int(runtime.stream_endpoint.rsplit(":", 1)[1])
            assert 0 < port < 65536
            subscriber.connect(runtime.stream_endpoint)
            received = _collect_zmq_until(
                runtime,
                subscriber,
                lambda items: any(topic == f"market.bar.1m.{BTC}" for topic, _ in items),
            )
    finally:
        subscriber.close()
        context.term()

    topic, table = next(
        (topic, table) for topic, table in received if topic == f"market.bar.1m.{BTC}"
    )
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 1
    assert table.column("instrument_id").to_pylist() == [BTC]
    assert table.column("open").to_pylist() == [Decimal("65000.1")]
    assert table.column("event_ts").to_pylist() == [
        datetime(2024, 1, 1, 0, 1, tzinfo=UTC)
    ]


def test_external_zmq_prefix_sub_gets_multiple_instruments(tmp_path: Path) -> None:
    config = _config(tmp_path, two_instruments=True)
    feed = TickingFeedHandler((BTC, ETH), start=datetime(2024, 1, 1, tzinfo=UTC))
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    # A bare prefix matches every market.bar topic, like a ZMQ subscription.
    subscriber.setsockopt(zmq.SUBSCRIBE, b"market.bar.")
    try:
        with GatewayRuntime(
            feed_handler=feed, config=config, data_root=tmp_path / "data"
        ) as runtime:
            runtime.start()
            subscriber.connect(runtime.stream_endpoint)
            received = _collect_zmq_until(
                runtime,
                subscriber,
                lambda items: {
                    topic.split("market.bar.1m.")[-1]
                    for topic, _ in items
                    if topic.startswith("market.bar.1m.")
                }
                >= {BTC, ETH},
            )
    finally:
        subscriber.close()
        context.term()

    one_minute = {
        topic.split("market.bar.1m.")[-1]: table
        for topic, table in received
        if topic.startswith("market.bar.1m.")
    }
    assert set(one_minute) >= {BTC, ETH}
    for instrument_id in (BTC, ETH):
        table = one_minute[instrument_id]
        assert table.schema == BAR_SCHEMA
        assert table.column("instrument_id").to_pylist() == [instrument_id]
        assert table.column("interval").to_pylist() == ["1m"]


async def _connect_ws(runtime: GatewayRuntime) -> tuple[aiohttp.ClientSession, Any]:
    session = aiohttp.ClientSession()
    url = runtime.base_url.replace("http://", "ws://", 1) + "/ws"
    ws = await session.ws_connect(url)
    return session, ws


async def _ping(ws) -> None:
    await ws.send_json({"op": "ping"})
    await _await_op(ws, "pong")


async def _await_op(ws, op: str, *, timeout: float = 3.0) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise AssertionError(f"timed out waiting for WS op {op!r}")
        message = await asyncio.wait_for(ws.receive(), remaining)
        assert message.type == aiohttp.WSMsgType.TEXT, message
        payload = json.loads(message.data)
        if payload.get("op") == op:
            return payload


def test_websocket_subscribe_receives_rest_equivalent_envelope(tmp_path: Path) -> None:
    feed = FakeFeedHandler(instrument_id=BTC, bars=[_bar_input(0)])
    config = _config(tmp_path)

    async def scenario() -> tuple[dict, dict]:
        session, ws = await _connect_ws(runtime)
        try:
            await ws.send_json(
                {"op": "subscribe", "topics": [f"market.bar.1m.{BTC}"]}
            )
            # Pong proves the subscribe op was processed before ingestion.
            await _ping(ws)
            runtime.ingest()
            event = await _await_op(ws, "event")
        finally:
            await ws.close()
            await session.close()

        with urllib.request.urlopen(
            f"{runtime.base_url}/api/v1/bars?instrument={BTC}&interval=1m"
        ) as response:
            rest = json.load(response)["bars"]
        return event, rest[0]

    with GatewayRuntime(
        feed_handler=feed, config=config, data_root=tmp_path / "data"
    ) as runtime:
        runtime.start()
        event, rest_message = asyncio.run(scenario())

    assert event["topic"] == f"market.bar.1m.{BTC}"
    assert event["data"] == rest_message
    envelope = event["data"]
    assert envelope["schema_version"] == 1
    assert envelope["data_type"] == "bar"
    assert envelope["instrument_id"] == BTC
    assert envelope["event_ts"] == "2024-01-01T00:01:00.000000000Z"
    assert envelope["ingest_ts"].endswith("Z")
    assert envelope["feed"] == "fake"
    assert envelope["payload"]["open"] == "65000.1"
    assert envelope["payload"]["volume"] == "12.34"


def test_websocket_ping_and_unsubscribe_stop_events(tmp_path: Path) -> None:
    feed = FakeFeedHandler(
        instrument_id=BTC, bars=[_bar_input(0), _bar_input(1)]
    )
    config = _config(tmp_path)

    async def scenario() -> None:
        session, ws = await _connect_ws(runtime)
        try:
            topic = f"market.bar.1m.{BTC}"
            await ws.send_json({"op": "subscribe", "topics": [topic]})
            await _ping(ws)
            runtime.ingest()
            first = await _await_op(ws, "event")
            assert first["topic"] == topic

            await ws.send_json({"op": "unsubscribe", "topics": [topic]})
            await _ping(ws)
            runtime.ingest()
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(_await_op(ws, "event"), 0.5)
            # The connection is still usable after unsubscribing.
            await _ping(ws)
        finally:
            await ws.close()
            await session.close()

    with GatewayRuntime(
        feed_handler=feed, config=config, data_root=tmp_path / "data"
    ) as runtime:
        runtime.start()
        asyncio.run(scenario())


def test_websocket_prefix_subscription_matches_multiple_topics(tmp_path: Path) -> None:
    config = _config(tmp_path, two_instruments=True)
    # TickingFeedHandler carries two instruments and publishes both per call.
    feed = TickingFeedHandler((BTC, ETH), start=datetime(2024, 1, 1, tzinfo=UTC))

    async def scenario() -> set[str]:
        session, ws = await _connect_ws(runtime)
        try:
            await ws.send_json({"op": "subscribe", "topics": ["market.bar.1m."]})
            await _ping(ws)
            runtime.ingest()
            topics: set[str] = set()
            while topics < {f"market.bar.1m.{BTC}", f"market.bar.1m.{ETH}"}:
                event = await _await_op(ws, "event")
                topics.add(event["topic"])
            return topics
        finally:
            await ws.close()
            await session.close()

    with GatewayRuntime(
        feed_handler=feed, config=config, data_root=tmp_path / "data"
    ) as runtime:
        runtime.start()
        topics = asyncio.run(scenario())

    assert topics == {f"market.bar.1m.{BTC}", f"market.bar.1m.{ETH}"}


def test_slow_websocket_consumer_gets_lag_notice_without_disconnect(
    tmp_path: Path,
) -> None:
    bars = [_bar_input(minute) for minute in range(30)]
    feed = FakeFeedHandler(instrument_id=BTC, bars=bars)
    config = _config(tmp_path, queue_size=1, lag_notice_interval_ms=0)

    async def scenario() -> tuple[dict, set[str]]:
        session, ws = await _connect_ws(runtime)
        try:
            topic = f"market.bar.1m.{BTC}"
            await ws.send_json({"op": "subscribe", "topics": [topic]})
            await _ping(ws)
            # One ingestion publishes all 30 Bars in a single hub fan-out, so
            # the writer cannot drain a queue of size 1 in between.
            runtime.ingest()
            notice = await _await_op(ws, "lag_notice")
            seen: set[str] = set()
            while topic not in seen:
                event = await _await_op(ws, "event")
                seen.add(event["topic"])
            # Still connected after the drop burst.
            await _ping(ws)
            return notice, seen
        finally:
            await ws.close()
            await session.close()

    with GatewayRuntime(
        feed_handler=feed, config=config, data_root=tmp_path / "data"
    ) as runtime:
        runtime.start()
        notice, seen = asyncio.run(scenario())

    assert notice["dropped"] == len(bars) - 1
    assert notice["since"].endswith("Z")
    assert seen == {f"market.bar.1m.{BTC}"}


def test_websocket_ignores_empty_and_invalid_topics_and_messages(
    tmp_path: Path,
) -> None:
    feed = FakeFeedHandler(instrument_id=BTC, bars=[_bar_input(0)])
    config = _config(tmp_path)

    async def scenario() -> dict:
        session, ws = await _connect_ws(runtime)
        try:
            await ws.send_bytes(b"{not json")
            await ws.send_json({"op": "unknown"})
            await ws.send_json(
                {"op": "subscribe", "topics": ["", "   ", None, 42, f"market.bar.1m.{BTC}"]}
            )
            await ws.send_json({"op": "subscribe"})
            await ws.send_json({"op": "unsubscribe", "topics": "not-a-list"})
            await _ping(ws)
            runtime.ingest()
            return await _await_op(ws, "event")
        finally:
            await ws.close()
            await session.close()

    with GatewayRuntime(
        feed_handler=feed, config=config, data_root=tmp_path / "data"
    ) as runtime:
        runtime.start()
        event = asyncio.run(scenario())

    assert event["topic"] == f"market.bar.1m.{BTC}"


def test_websocket_path_comes_from_config(tmp_path: Path) -> None:
    config = _config(tmp_path)
    server = config.server.model_copy(update={"ws_path": "/stream"})
    config = config.model_copy(update={"server": server})
    feed = FakeFeedHandler(instrument_id=BTC, bars=[])

    async def scenario() -> None:
        session = aiohttp.ClientSession()
        try:
            url = runtime.base_url.replace("http://", "ws://", 1) + "/stream"
            ws = await session.ws_connect(url)
            await _ping(ws)
            await ws.close()
        finally:
            await session.close()

    with GatewayRuntime(
        feed_handler=feed, config=config, data_root=tmp_path / "data"
    ) as runtime:
        runtime.start()
        asyncio.run(scenario())


def test_streaming_config_defaults_and_repository_toml() -> None:
    config = GatewayConfig()
    assert config.bus.stream_bind_host == "127.0.0.1"
    assert config.bus.stream_pub_port == 0
    assert config.streaming.queue_size == 1000
    assert config.streaming.lag_notice_interval_ms == 1000

    with (PROJECT_ROOT / "config" / "gateway.toml").open("rb") as handle:
        import tomllib

        raw = tomllib.load(handle)
    assert raw["bus"]["stream_bind_host"] == "127.0.0.1"
    assert raw["bus"]["stream_pub_port"] == 0
    assert raw["streaming"]["queue_size"] == 1000
    assert raw["streaming"]["lag_notice_interval_ms"] == 1000


@pytest.mark.parametrize(
    "payload",
    [
        {"streaming": {"queue_size": 0}},
        {"streaming": {"lag_notice_interval_ms": -1}},
        {"bus": {"stream_pub_port": 70_000}},
    ],
)
def test_streaming_config_rejects_invalid_values(payload: dict) -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        GatewayConfig.model_validate(payload)
