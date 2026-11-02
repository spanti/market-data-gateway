from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from mdgw.config import FeedConfig
from mdgw.core.time import utc_now
from mdgw.ingest.ccxt_feed import build_feed_handler

pytestmark = pytest.mark.network


def test_binance_spot_history_returns_only_closed_decimal_bars() -> None:
    feed_config = FeedConfig(
        name="binance_spot_network",
        venue="binance",
        symbols=("BTC/USDT",),
    )
    handler = build_feed_handler(feed_config)
    try:
        end = utc_now()
        start = end - timedelta(minutes=30)
        bars = list(handler.fetch_history("binance:BTC/USDT", "1m", start, end))
    finally:
        handler.close()

    assert bars, "expected Binance to return recent 1m K-lines"
    assert all(isinstance(bar.open, Decimal) for bar in bars)
    assert all(bar.open_ts + timedelta(minutes=1) <= end for bar in bars)
    assert all(bar.interval == "1m" for bar in bars)
