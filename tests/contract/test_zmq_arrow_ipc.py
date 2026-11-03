from __future__ import annotations

import time
from datetime import datetime, timezone
from uuid import uuid4

import pyarrow as pa

from mdgw.core.schema import BAR_SCHEMA
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.normalize.bar import normalize_bar
from mdgw.streaming import StreamingInterface


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
    endpoint = f"inproc://mdgw-test-{uuid4().hex}"
    interface = StreamingInterface(bind=endpoint, connect=endpoint)
    interface.start()
    try:
        interface.subscribe_bars(
            lambda topic, table: received.append((topic, table)),
            await_ready=True,
        )
        interface.publish_bars([bar])

        deadline = time.monotonic() + 2.0
        while not received and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        interface.stop()

    assert len(received) == 1
    topic, table = received[0]
    assert topic == "market.bar.1m.binance:BTC/USDT"
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 1
    assert table.column("open").to_pylist() == [bar.open]
    assert table.column("event_ts").to_pylist() == [bar.event_ts]
