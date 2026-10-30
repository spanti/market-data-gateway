from __future__ import annotations

from datetime import datetime, timezone

import pytest

from mdgw.core.bar import RawBar
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.normalize.bar import normalize_bar


def _raw(*, open_ts: datetime, open_value="1.0") -> RawBar:
    return RawBar(
        instrument_id="binance:BTC/USDT",
        interval="1m",
        open_ts=open_ts,
        open=open_value,
        high="2.0",
        low="0.5",
        close="1.5",
        volume="10.0",
    )


def test_normalize_rejects_unclosed_bar() -> None:
    raw = _raw(open_ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc))
    with pytest.raises(ValueError, match="not closed"):
        normalize_bar(
            raw,
            feed="fake",
            ingest_ts=datetime(2024, 1, 1, 0, 0, 30, tzinfo=timezone.utc),
        )


def test_normalize_rejects_float_prices() -> None:
    raw = _raw(
        open_ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        open_value=1.0,  # deliberately wrong type: floats are forbidden
    )
    with pytest.raises(TypeError, match="floats are forbidden"):
        normalize_bar(
            raw,
            feed="fake",
            ingest_ts=datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc),
        )


def test_fake_feed_handler_stamps_configured_instrument_id() -> None:
    feed = FakeFeedHandler(
        instrument_id="binance:ETH/USDT",
        bars=[
            BarInput(
                open_ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
                open="1",
                high="2",
                low="0.5",
                close="1.5",
                volume="3",
            )
        ],
    )
    raw = next(feed.iter_bars())
    assert raw.instrument_id == "binance:ETH/USDT"
    bar = normalize_bar(
        raw,
        feed=feed.name,
        ingest_ts=datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc),
    )
    assert bar.event_ts == bar.close_ts
    assert bar.aggregation_source == "external"
    assert bar.data_type == "bar"
    assert bar.schema_version == 1
