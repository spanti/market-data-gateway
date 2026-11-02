"""WebSocket auto-reconnect: drop the first connection, serve one event next.

Uses ``websockets.sync.server`` as a local stub so the test does not need the
gateway at all.
"""

from __future__ import annotations

import json
import threading
from contextlib import suppress
from decimal import Decimal

from websockets.sync.server import serve

from mdgw_client import GatewayClient
from mdgw_client.schema import BAR_SCHEMA

BTC = "binance:BTC/USDT"
TOPIC = f"market.bar.1m.{BTC}"

ENVELOPE = {
    "schema_version": 1,
    "data_type": "bar",
    "instrument_id": BTC,
    "event_ts": "2024-01-01T00:01:00.000000000Z",
    "ingest_ts": "2024-01-01T00:01:00.000000000Z",
    "feed": "fake",
    "seq": None,
    "payload": {
        "interval": "1m",
        "open_ts": "2024-01-01T00:00:00.000000000Z",
        "close_ts": "2024-01-01T00:01:00.000000000Z",
        "aggregation_source": "external",
        "open": "1.5",
        "high": "2",
        "low": "1",
        "close": "1.75",
        "volume": "3",
        "trade_count": 1,
        "revision": 0,
    },
}


def test_websocket_reconnects_resubscribes_and_delivers_an_event() -> None:
    connections = 0
    subscriptions: list[list[str]] = []
    lock = threading.Lock()

    def handler(ws) -> None:
        nonlocal connections
        with lock:
            index = connections
            connections += 1
        if index == 0:
            # Simulate a dropped connection before the handshake completes.
            ws.close()
            return
        with suppress(Exception):
            while True:
                payload = json.loads(ws.recv())
                op = payload.get("op")
                if op == "subscribe":
                    with lock:
                        subscriptions.append(list(payload["topics"]))
                elif op == "ping":
                    ws.send(json.dumps({"op": "pong"}))
                    ws.send(
                        json.dumps(
                            {"op": "event", "topic": TOPIC, "data": ENVELOPE}
                        )
                    )

    server = serve(handler, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.socket.getsockname()[1]
        with GatewayClient(
            "http://127.0.0.1:1",
            ws_url=f"ws://127.0.0.1:{port}",
            timeout_s=2.0,
            reconnect_backoff_s=(0.01, 0.02),
        ) as client:
            with client.subscribe([TOPIC], transport="ws") as subscription:
                event = subscription.next(timeout=5)

                assert connections >= 2
                assert subscription.reconnect_count >= 1
                assert subscriptions and subscriptions[-1] == [TOPIC]
    finally:
        server.shutdown()
        thread.join(timeout=2)
        with suppress(OSError):
            server.socket.close()

    assert event.kind == "bar"
    assert event.topic == TOPIC
    assert event.table.schema == BAR_SCHEMA
    assert event.table.column("close").to_pylist() == [Decimal("1.75")]
    assert event.table.column("instrument_id").to_pylist() == [BTC]
