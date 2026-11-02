"""A thin, framework-neutral client SDK for the Market Data Gateway.

``mdgw_client`` depends only on ``pyarrow``, ``pyzmq`` and the standard
library. WebSocket support is an optional extra (``mdgw-client[ws]``). It does
not import any ``mdgw`` module.

Typical use::

    from mdgw_client import GatewayClient

    with GatewayClient(
        "http://127.0.0.1:8080",
        stream_endpoint="tcp://127.0.0.1:5555",
        replay_publish_endpoint="tcp://127.0.0.1:5556",
        replay_control_endpoint="tcp://127.0.0.1:5557",
    ) as client:
        table = client.history("1m", instrument="binance:BTC/USDT")
        frame = table.to_pandas()  # decimals are Decimal objects

        with client.subscribe(["market.bar.1m.binance:BTC/USDT"]) as live:
            for event in live:
                print(event.topic, event.table.num_rows)

        session = client.replay.create_session(
            subscriptions=[
                {
                    "data_type": "bar",
                    "instrument_id": "binance:BTC/USDT",
                    "interval": "1m",
                }
            ],
            start_ts="2026-10-30T00:00:00Z",
            end_ts="2026-10-30T01:00:00Z",
            replay_mode="asap",
        )
        for event in session:
            print(event.table.num_rows)
        print(session.end_of_session)
"""

from __future__ import annotations

from mdgw_client.client import GatewayClient
from mdgw_client.errors import ClientError
from mdgw_client.live import LiveSubscription, StreamEvent
from mdgw_client.replay import ReplayClient, ReplaySession
from mdgw_client.schema import BAR_SCHEMA

__all__ = [
    "BAR_SCHEMA",
    "ClientError",
    "GatewayClient",
    "LiveSubscription",
    "ReplayClient",
    "ReplaySession",
    "StreamEvent",
]
