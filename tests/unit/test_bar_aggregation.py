from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mdgw.aggregate import BarAggregator
from mdgw.core.bar import Bar, RawBar
from mdgw.core.schema import INTERVALS
from mdgw.normalize.bar import normalize_bar

UTC = timezone.utc
DAY = datetime(2024, 1, 1, tzinfo=UTC)


def _bar_at(
    open_ts: datetime,
    *,
    open: str = "100",
    high: str = "110",
    low: str = "90",
    close: str = "105",
    volume: str = "1",
    trade_count: int | None = 10,
    revision: int = 0,
    feed: str = "fake",
    ingest_ts: datetime | None = None,
    instrument_id: str = "binance:BTC/USDT",
    interval: str = "1m",
) -> Bar:
    closed_at = open_ts + INTERVALS[interval]
    return normalize_bar(
        RawBar(
            instrument_id=instrument_id,
            interval=interval,
            open_ts=open_ts,
            open=Decimal(open),
            high=Decimal(high),
            low=Decimal(low),
            close=Decimal(close),
            volume=Decimal(volume),
            trade_count=trade_count,
            revision=revision,
        ),
        feed=feed,
        ingest_ts=ingest_ts or closed_at + timedelta(seconds=1),
    )


def _minute(index: int, *, day: datetime = DAY, **overrides) -> Bar:
    return _bar_at(day + timedelta(minutes=index), **overrides)


def _aggregator(*intervals: str, **kwargs) -> BarAggregator:
    return BarAggregator(
        intervals=intervals or ("1m", "5m", "15m", "1h", "1d"),
        base_interval="1m",
        **kwargs,
    )


def test_five_closed_one_minute_bars_produce_one_complete_five_minute_bar() -> None:
    aggregator = _aggregator("1m", "5m")

    produced: list[Bar] = []
    for index in range(5):
        produced.extend(
            aggregator.add(
                _minute(
                    index,
                    open=str(100 + index),
                    high=str(110 + index),
                    low=str(90 + index),
                    close=str(105 + index),
                    volume="2",
                    trade_count=10,
                )
            )
        )

    assert len(produced) == 1
    bar = produced[0]
    assert bar.interval == "5m"
    assert bar.open_ts == DAY
    assert bar.close_ts == DAY + timedelta(minutes=5)
    assert bar.event_ts == DAY + timedelta(minutes=5)
    assert bar.open == Decimal("100")
    assert bar.high == Decimal("114")
    assert bar.low == Decimal("90")
    assert bar.close == Decimal("109")
    assert bar.volume == Decimal("10")
    assert bar.trade_count == 50
    assert bar.aggregation_source == "internal"
    assert bar.feed == "fake"
    assert bar.seq is None
    assert bar.revision == 0
    assert bar.ingest_ts == DAY + timedelta(minutes=5, seconds=1)


def test_incomplete_period_never_produces_a_partial_parent() -> None:
    aggregator = _aggregator("1m", "5m")

    produced = [aggregator.add(_minute(index)) for index in range(4)]

    assert produced == [[], [], [], []]


def test_a_missing_one_minute_bar_keeps_the_parent_unpublished() -> None:
    aggregator = _aggregator("1m", "5m")

    produced: list[Bar] = []
    for index in (0, 1, 2, 4, 5):
        produced.extend(aggregator.add(_minute(index)))

    assert produced == []


def test_utc_day_boundary_produces_one_daily_bar() -> None:
    aggregator = _aggregator("1m", "1d")
    start = datetime(2023, 12, 31, tzinfo=UTC)

    produced: list[Bar] = []
    for index in range(1440):
        produced.extend(aggregator.add(_minute(index, day=start)))

    assert len(produced) == 1
    daily = produced[0]
    assert daily.interval == "1d"
    assert daily.open_ts == datetime(2023, 12, 31, tzinfo=UTC)
    assert daily.close_ts == datetime(2024, 1, 1, tzinfo=UTC)
    assert daily.event_ts == datetime(2024, 1, 1, tzinfo=UTC)


def test_complete_hour_produces_one_hourly_bar() -> None:
    aggregator = _aggregator("1m", "1h")

    produced: list[Bar] = []
    for index in range(60):
        produced.extend(aggregator.add(_minute(index, volume="2", trade_count=3)))

    assert len(produced) == 1
    hourly = produced[0]
    assert hourly.interval == "1h"
    assert hourly.open_ts == DAY
    assert hourly.close_ts == DAY + timedelta(hours=1)
    assert hourly.volume == Decimal("120")
    assert hourly.trade_count == 180


def test_parent_metadata_is_derived_from_all_children() -> None:
    aggregator = _aggregator("1m", "5m")
    revisions = [0, 0, 2, 0, 1]

    produced: list[Bar] = []
    for index, revision in enumerate(revisions):
        produced.extend(
            aggregator.add(
                _minute(
                    index,
                    revision=revision,
                    ingest_ts=DAY + timedelta(minutes=index + 2),
                )
            )
        )

    assert len(produced) == 1
    parent = produced[0]
    assert parent.revision == 2
    assert parent.feed == "fake"
    assert parent.seq is None
    assert parent.ingest_ts == DAY + timedelta(minutes=6)
    assert parent.aggregation_source == "internal"


def test_non_base_interval_is_rejected() -> None:
    aggregator = _aggregator("1m", "5m")
    five_minute_bar = _bar_at(DAY, interval="5m")

    with pytest.raises(ValueError, match="1m"):
        aggregator.add(five_minute_bar)


def test_unaligned_one_minute_open_ts_is_rejected() -> None:
    aggregator = _aggregator("1m", "5m")

    with pytest.raises(ValueError, match="align"):
        aggregator.add(_bar_at(DAY + timedelta(seconds=30)))


def test_out_of_order_arrival_produces_the_parent_once_complete() -> None:
    aggregator = _aggregator("1m", "5m")

    produced: list[Bar] = []
    for index in (4, 1, 3, 0, 2):
        produced.extend(aggregator.add(_minute(index)))

    assert len(produced) == 1
    assert produced[0].open_ts == DAY
    assert produced[0].open == Decimal("100")
    assert produced[0].close == Decimal("105")


def test_same_revision_duplicate_input_does_not_reproduce_the_parent() -> None:
    aggregator = _aggregator("1m", "5m")
    bars = [_minute(index) for index in range(5)]
    produced = [aggregator.add(bar) for bar in bars]
    assert len(produced[-1]) == 1

    assert aggregator.add(bars[0]) == []
    assert aggregator.add(bars[4]) == []
    assert aggregator.add(bars[2]) == []


def test_child_revision_increase_recomputes_and_republishes_the_parent() -> None:
    aggregator = _aggregator("1m", "5m")
    initial = [
        _minute(
            index,
            high="110",
            volume="1",
            trade_count=10,
        )
        for index in range(5)
    ]
    produced: list[Bar] = []
    for bar in initial:
        produced.extend(aggregator.add(bar))

    assert len(produced) == 1
    first = produced[0]
    assert first.revision == 0
    assert first.high == Decimal("110")
    assert first.volume == Decimal("5")

    corrected = _minute(
        4,
        high="999",
        close="998",
        volume="100",
        trade_count=20,
        revision=1,
        ingest_ts=DAY + timedelta(minutes=10),
    )
    republished = aggregator.add(corrected)

    assert len(republished) == 1
    second = republished[0]
    assert second.revision == 1
    assert second.open_ts == first.open_ts
    assert second.high == Decimal("999")
    assert second.low == Decimal("90")
    assert second.close == Decimal("998")
    assert second.volume == Decimal("104")
    assert second.trade_count == 60
    assert second.ingest_ts == DAY + timedelta(minutes=10)

    assert aggregator.add(corrected) == []


def test_lower_revision_child_correction_still_moves_parent_revision_forward() -> None:
    aggregator = _aggregator("1m", "5m")
    revisions = [0, 0, 2, 0, 0]
    for index, revision in enumerate(revisions):
        produced = aggregator.add(_minute(index, revision=revision))
    assert len(produced) == 1
    assert produced[0].revision == 2

    corrected = _minute(0, high="999", revision=1)
    republished = aggregator.add(corrected)

    assert len(republished) == 1
    assert republished[0].high == Decimal("999")
    assert republished[0].revision > 2


def test_multiple_instruments_are_aggregated_independently() -> None:
    aggregator = _aggregator("1m", "5m")
    produced: list[Bar] = []

    for index in range(5):
        produced.extend(aggregator.add(_minute(index, instrument_id="binance:BTC/USDT")))
        produced.extend(aggregator.add(_minute(index, instrument_id="binance:ETH/USDT")))

    assert sorted((bar.instrument_id, bar.open_ts) for bar in produced) == [
        ("binance:BTC/USDT", DAY),
        ("binance:ETH/USDT", DAY),
    ]


def test_inconsistent_feed_within_one_parent_is_rejected() -> None:
    aggregator = _aggregator("1m", "5m")

    aggregator.add(_minute(0, feed="venue_a"))

    with pytest.raises(ValueError, match="inconsistent feed"):
        aggregator.add(_minute(1, feed="venue_b"))


def test_retention_window_ignores_revisions_for_trimmed_periods() -> None:
    aggregator = _aggregator("1m", "5m", retention_parent_periods=1)
    produced: list[Bar] = []
    for index in range(15):
        produced.extend(aggregator.add(_minute(index)))

    assert [bar.open_ts for bar in produced] == [
        DAY,
        DAY + timedelta(minutes=5),
        DAY + timedelta(minutes=10),
    ]

    # The first period is outside the retention window; a late correction for
    # it must be ignored rather than resurrecting a pruned parent.
    late = _minute(0, high="999", revision=1)
    assert aggregator.add(late) == []
    assert aggregator.add(_minute(4)) == []


def test_all_none_trade_counts_produce_none_in_the_parent() -> None:
    aggregator = _aggregator("1m", "5m")
    produced: list[Bar] = []
    for index in range(5):
        produced.extend(aggregator.add(_minute(index, trade_count=None)))

    assert len(produced) == 1
    assert produced[0].trade_count is None


def test_only_configured_parent_intervals_are_produced() -> None:
    aggregator = _aggregator("1m", "15m")

    produced: list[Bar] = []
    for index in range(15):
        produced.extend(aggregator.add(_minute(index)))

    assert [bar.interval for bar in produced] == ["15m"]
