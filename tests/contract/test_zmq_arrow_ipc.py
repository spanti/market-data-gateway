from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow as pa

from mdgw.bus.zmq_bus import ZMQBus, ipc_to_table, table_to_ipc
from mdgw.core.schema import BAR_SCHEMA, bars_to_logical_table
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.normalize.bar import normalize_bar

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_zmq_inproc_carries_arrow_ipc_bars() -> None:
    feed = FakeFeedHandler(
        instrument_id="binance:BTC/USDT",
        bars=[
            BarInput(
                open_ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
                open="65000.1",
                high="65010.0",
                low="64990.0",
                close="65005.5",
                volume="12.34",
                trade_count=128,
            )
        ],
    )
    bar = normalize_bar(
        next(feed.iter_bars()),
        feed=feed.name,
        ingest_ts=datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc),
    )

    received: list[tuple[str, pa.Table]] = []
    bus = ZMQBus()
    endpoint = f"inproc://mdgw-test-{uuid4().hex}"
    try:
        bus.bind_publisher(endpoint)
        bus.subscribe(
            endpoint,
            topic_prefix="market.bar.",
            handler=lambda topic, payload: received.append(
                (topic, ipc_to_table(payload))
            ),
        )
        bus.wait_for_subscriber()
        bus.publish(
            "market.bar.1m.binance:BTC/USDT",
            table_to_ipc(bars_to_logical_table([bar])),
        )

        deadline = time.monotonic() + 2.0
        while not received and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        bus.close()

    assert len(received) == 1
    topic, table = received[0]
    assert topic == "market.bar.1m.binance:BTC/USDT"
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 1
    assert table.column("open").to_pylist() == [bar.open]
    assert table.column("event_ts").to_pylist() == [bar.event_ts]
