from __future__ import annotations

from datetime import datetime, timezone

import pytest

from mdgw.ingest.fake import BarInput, FakeFeedHandler

UTC = timezone.utc
INSTRUMENT = "binance:BTC/USDT"


def _bar(minute: int, *, close: str = "100") -> BarInput:
    return BarInput(
        open_ts=datetime(2024, 1, 1, 0, minute, tzinfo=UTC),
        open="100",
        high="101",
        low="99",
        close=close,
        volume="1",
    )


def test_fake_feed_handler_defaults_to_no_history() -> None:
    feed = FakeFeedHandler(instrument_id=INSTRUMENT, bars=[_bar(0)])

    assert feed.instrument_ids == (INSTRUMENT,)
    assert feed.supports_history is False
    with pytest.raises(NotImplementedError):
        list(feed.fetch_history(INSTRUMENT, "1m", _bar(0).open_ts, _bar(1).open_ts))


def test_fake_feed_handler_history_filters_instrument_interval_and_range() -> None:
    feed = FakeFeedHandler(
        instrument_id=INSTRUMENT,
        bars=[],
        history=[_bar(0), _bar(1), _bar(2)],
    )

    assert feed.supports_history is True
    fetched = list(
        feed.fetch_history(
            INSTRUMENT,
            "1m",
            datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
            datetime(2024, 1, 1, 0, 3, tzinfo=UTC),
        )
    )
    assert [bar.open_ts.minute for bar in fetched] == [1, 2]
    assert all(bar.instrument_id == INSTRUMENT for bar in fetched)
    assert all(bar.interval == "1m" for bar in fetched)

    assert (
        list(
            feed.fetch_history(
                "binance:ETH/USDT",
                "1m",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 1, 1, tzinfo=UTC),
            )
        )
        == []
    )
    assert (
        list(
            feed.fetch_history(
                INSTRUMENT,
                "5m",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 1, 1, tzinfo=UTC),
            )
        )
        == []
    )


def test_fake_feed_handler_iter_bars_is_unchanged() -> None:
    feed = FakeFeedHandler(instrument_id=INSTRUMENT, bars=[_bar(0), _bar(1)])
    assert [bar.open_ts.minute for bar in feed.iter_bars()] == [0, 1]
