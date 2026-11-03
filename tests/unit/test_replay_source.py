"""Replay read path: close_ts range, latest revision, deterministic ordering."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mdgw.core.bar import Bar
from mdgw.core.schema import INTERVALS
from mdgw.core.time import datetime_to_ns
from mdgw.replay.session import BarSubscription
from mdgw.replay.source import ReplayBarSource, bar_order_key
from mdgw.store.parquet_dataset import ParquetBarStore

UTC = timezone.utc
BASE = datetime(2026, 10, 30, 0, 0, tzinfo=UTC)
BTC = "binance:BTC/USDT"
ETH = "binance:ETH/USDT"


def _bar(
    *,
    instrument_id: str = BTC,
    interval: str = "1m",
    open_minute: int = 0,
    seq: int | None = None,
    revision: int = 0,
    ingest_minute: int = 0,
    price: str = "100",
) -> Bar:
    open_ts = BASE + timedelta(minutes=open_minute)
    close_ts = open_ts + INTERVALS[interval]
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=instrument_id,
        interval=interval,
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=BASE + timedelta(minutes=ingest_minute),
        feed="fake",
        seq=seq,
        aggregation_source="external",
        open=Decimal(price),
        high=Decimal(price),
        low=Decimal(price),
        close=Decimal(price),
        volume=Decimal("1"),
        trade_count=1,
        revision=revision,
    )


def _read(
    store: ParquetBarStore,
    subscriptions: list[BarSubscription],
    *,
    start_minute: int,
    end_minute: int,
) -> list[Bar]:
    return ReplayBarSource(store).read(
        subscriptions,
        start_ts_ns=datetime_to_ns(BASE + timedelta(minutes=start_minute)),
        end_ts_ns=datetime_to_ns(BASE + timedelta(minutes=end_minute)),
    )


def test_range_filter_uses_close_ts_start_inclusive_end_exclusive(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    # close_ts runs from 00:00 through 00:06.
    store.write_bars(
        [_bar(open_minute=minute) for minute in range(-1, 6)],
        asset_class="crypto",
    )

    bars = _read(
        store,
        [BarSubscription(BTC, "1m")],
        start_minute=2,
        end_minute=5,
    )

    assert [bar.close_ts for bar in bars] == [
        BASE + timedelta(minutes=2),
        BASE + timedelta(minutes=3),
        BASE + timedelta(minutes=4),
    ]


def test_only_highest_revision_is_replayed(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [
            _bar(open_minute=0, revision=0, price="100"),
            _bar(open_minute=0, revision=2, price="200"),
            _bar(open_minute=0, revision=1, price="150"),
        ],
        asset_class="crypto",
    )

    bars = _read(store, [BarSubscription(BTC, "1m")], start_minute=0, end_minute=2)

    assert len(bars) == 1
    assert bars[0].revision == 2
    assert bars[0].close == Decimal("200")


def test_equal_revision_falls_back_to_latest_ingest_ts(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [
            _bar(open_minute=0, revision=1, ingest_minute=5, price="200"),
            _bar(open_minute=0, revision=1, ingest_minute=1, price="100"),
        ],
        asset_class="crypto",
    )

    bars = _read(store, [BarSubscription(BTC, "1m")], start_minute=0, end_minute=2)

    assert len(bars) == 1
    assert bars[0].revision == 1
    assert bars[0].close == Decimal("200")


def test_order_key_is_close_ts_instrument_seq_then_interval() -> None:
    # Sort-key level test: two Bars with the same (instrument, interval,
    # open_ts) are revisions, not distinct events, so seq ordering is asserted
    # directly on the total-order key.
    base = _bar(open_minute=0)
    bars = [
        # Same close_ts (00:01): cross-instrument and seq ties.
        _bar(instrument_id=ETH, open_minute=0, seq=0),
        replace(base, instrument_id=BTC, seq=None),
        replace(base, instrument_id=BTC, seq=3),
        replace(base, instrument_id=BTC, seq=1),
        # A 5m Bar also closing at 00:01; interval breaks the tie.
        _bar(instrument_id=BTC, interval="5m", open_minute=-4, seq=None),
        # A strictly later close_ts (00:02) always sorts last.
        _bar(instrument_id="binance:AAA/USDT", open_minute=1, seq=0),
    ]

    ordered = sorted(bars, key=bar_order_key)

    assert [(bar.instrument_id, bar.interval, bar.seq) for bar in ordered] == [
        (BTC, "1m", 1),
        (BTC, "1m", 3),
        (BTC, "1m", None),
        (BTC, "5m", None),
        (ETH, "1m", 0),
        ("binance:AAA/USDT", "1m", 0),
    ]


def test_ordering_puts_earlier_close_ts_first_regardless_of_instrument(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [
            _bar(instrument_id="binance:AAA/USDT", open_minute=1),
            _bar(instrument_id=ETH, open_minute=0),
        ],
        asset_class="crypto",
    )

    bars = _read(
        store,
        [BarSubscription("binance:AAA/USDT", "1m"), BarSubscription(ETH, "1m")],
        start_minute=0,
        end_minute=5,
    )

    assert [(bar.instrument_id, bar.close_ts) for bar in bars] == [
        (ETH, BASE + timedelta(minutes=1)),
        ("binance:AAA/USDT", BASE + timedelta(minutes=2)),
    ]


def test_duplicate_subscriptions_do_not_duplicate_bars(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(open_minute=0)], asset_class="crypto")

    bars = _read(
        store,
        [BarSubscription(BTC, "1m"), BarSubscription(BTC, "1m")],
        start_minute=0,
        end_minute=2,
    )

    assert len(bars) == 1
