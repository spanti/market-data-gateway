"""Replay read path: close_ts range, latest revision, deterministic ordering."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mdgw.core.bar import Bar
from mdgw.core.schema import BAR_SCHEMA, INTERVALS, empty_logical_table
from mdgw.core.time import datetime_to_ns
from mdgw.replay.session import BarSubscription
from mdgw.replay.source import ReplayBarSource, bar_order_key, interval_ns
from mdgw.store.parquet_dataset import (
    BarReadQuery,
    ParquetBarStore,
    ReplayReadQuery,
)

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


# -- DuckDB replay query -----------------------------------------------------


def _duckdb_read(
    store: ParquetBarStore,
    subscriptions: list[BarSubscription],
    *,
    start_minute: int,
    end_minute: int,
) -> list[Bar]:
    """Read through the production DuckDB seam on one fixed Snapshot."""
    start_ns = datetime_to_ns(BASE + timedelta(minutes=start_minute))
    end_ns = datetime_to_ns(BASE + timedelta(minutes=end_minute))
    queries = tuple(
        BarReadQuery(
            instrument_id=subscription.instrument_id,
            interval=subscription.interval,
            start_ns=start_ns - interval_ns(subscription.interval),
            end_ns=end_ns,
        )
        for subscription in subscriptions
    )
    with store.open_read(queries=queries, lease_id="unit-replay") as read:
        return ReplayBarSource(store).read(
            subscriptions,
            start_ts_ns=start_ns,
            end_ts_ns=end_ns,
            read=read,
        )


def _snapshot_query(
    *,
    instrument_id: str = BTC,
    interval: str = "1m",
    start_minute: int,
    end_minute: int,
) -> BarReadQuery:
    start_ns = datetime_to_ns(BASE + timedelta(minutes=start_minute))
    return BarReadQuery(
        instrument_id=instrument_id,
        interval=interval,
        start_ns=start_ns - interval_ns(interval),
        end_ns=datetime_to_ns(BASE + timedelta(minutes=end_minute)),
    )


def _replay_query(
    *,
    instrument_id: str = BTC,
    interval: str = "1m",
    start_minute: int,
    end_minute: int,
) -> ReplayReadQuery:
    start_ns = datetime_to_ns(BASE + timedelta(minutes=start_minute))
    return ReplayReadQuery(
        instrument_id=instrument_id,
        interval=interval,
        open_start_ns=start_ns - interval_ns(interval),
        close_start_ns=start_ns,
        close_end_ns=datetime_to_ns(BASE + timedelta(minutes=end_minute)),
    )


def _signature(bar: Bar) -> tuple:
    return (
        bar.instrument_id,
        bar.interval,
        datetime_to_ns(bar.open_ts),
        datetime_to_ns(bar.close_ts),
        bar.seq,
        bar.revision,
        bar.open,
    )


def test_store_read_replay_query_filters_dedupes_and_orders(
    tmp_path: Path,
) -> None:
    """One DuckDB query does close-window, Revision choice and five-key order.

    The query lives on `StoreRead`, so the Session's fixed Snapshot entries are
    its only input (never a Dataset-root glob).
    """
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [
            # Overlapping Data Files for the same identity: highest revision.
            _bar(open_minute=1, revision=0, price="100"),
            _bar(open_minute=2, revision=1, price="200"),
            _bar(open_minute=3, seq=3, price="100"),
            _bar(open_minute=4, seq=None, price="100"),
        ],
        asset_class="crypto",
    )
    store.write_bars(
        [
            _bar(open_minute=1, revision=2, price="300"),
            _bar(open_minute=2, revision=1, ingest_minute=5, price="250"),
            # A 5m Bar opening before the window closes inside it.
            _bar(interval="5m", open_minute=-3, price="100"),
            _bar(open_minute=0, price="100"),
        ],
        asset_class="crypto",
    )
    store.write_bars(
        [
            _bar(instrument_id=ETH, open_minute=1, seq=None, price="100"),
        ],
        asset_class="crypto",
    )

    replay_queries = (
        _replay_query(start_minute=2, end_minute=5),
        _replay_query(
            interval="5m", start_minute=2, end_minute=5
        ),
        _replay_query(
            instrument_id=ETH, start_minute=2, end_minute=5
        ),
    )
    snapshot_queries = (
        _snapshot_query(start_minute=2, end_minute=5),
        _snapshot_query(interval="5m", start_minute=2, end_minute=5),
        _snapshot_query(
            instrument_id=ETH, start_minute=2, end_minute=5
        ),
    )
    with store.open_read(
        queries=snapshot_queries, lease_id="unit-replay"
    ) as read:
        table = read.read_replay_bars(replay_queries)

    assert table.schema.equals(BAR_SCHEMA, check_metadata=False)
    assert [_signature(Bar(**row)) for row in table.to_pylist()] == [
        # close_ts 2m: BTC 1m (seq NULL) before BTC 5m (interval tie-break).
        (BTC, "1m", datetime_to_ns(BASE + timedelta(minutes=1)),
         datetime_to_ns(BASE + timedelta(minutes=2)), None, 2, Decimal("300")),
        (BTC, "5m", datetime_to_ns(BASE - timedelta(minutes=3)),
         datetime_to_ns(BASE + timedelta(minutes=2)), None, 0, Decimal("100")),
        (ETH, "1m", datetime_to_ns(BASE + timedelta(minutes=1)),
         datetime_to_ns(BASE + timedelta(minutes=2)), None, 0, Decimal("100")),
        # close_ts 3m: equal Revision, latest ingest_ts wins.
        (BTC, "1m", datetime_to_ns(BASE + timedelta(minutes=2)),
         datetime_to_ns(BASE + timedelta(minutes=3)), None, 1, Decimal("250")),
        # Numeric seq sorts before NULL seq at the same close_ts ordering key.
        (BTC, "1m", datetime_to_ns(BASE + timedelta(minutes=3)),
         datetime_to_ns(BASE + timedelta(minutes=4)), 3, 0, Decimal("100")),
        # close_ts 5m is the exclusive end (open 4m closes at 5m): absent.
    ]


def test_duckdb_source_matches_legacy_for_the_full_bar_sequence(
    tmp_path: Path,
) -> None:
    """The production DuckDB read reproduces every legacy Bar exactly."""
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [
            _bar(open_minute=0, revision=1, price="100"),
            _bar(open_minute=1, seq=None, price="100"),
            _bar(open_minute=2, seq=2, price="100"),
            _bar(open_minute=3, seq=None, price="100"),
        ],
        asset_class="crypto",
    )
    store.write_bars(
        [
            _bar(open_minute=0, revision=2, price="200"),
            _bar(open_minute=1, seq=None, revision=1, ingest_minute=5),
            _bar(instrument_id=ETH, open_minute=1, seq=None),
        ],
        asset_class="crypto",
    )
    store.write_bars(
        [
            _bar(instrument_id=ETH, open_minute=1, seq=None),
            _bar(interval="5m", open_minute=0, seq=None),
            _bar(open_minute=1, seq=None, revision=1, ingest_minute=1),
        ],
        asset_class="crypto",
    )
    subscriptions = [
        BarSubscription(BTC, "1m"),
        BarSubscription(BTC, "5m"),
        BarSubscription(ETH, "1m"),
    ]

    legacy = _read(
        store, subscriptions, start_minute=0, end_minute=5
    )
    duckdb = _duckdb_read(
        store, subscriptions, start_minute=0, end_minute=5
    )

    assert duckdb == legacy
    assert [bar.revision for bar in duckdb] == [2, 1, 0, 0, 0]


def test_duckdb_source_dedupes_identical_subscriptions_before_querying(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(open_minute=0)], asset_class="crypto")

    class _RecordingRead:
        queries: tuple[ReplayReadQuery, ...] | None = None

        def read_replay_bars(self, queries):
            self.queries = tuple(queries)
            return empty_logical_table()

    read = _RecordingRead()
    ReplayBarSource(store).read(
        [
            BarSubscription(BTC, "1m"),
            BarSubscription(BTC, "1m"),
            BarSubscription(BTC, "5m"),
            BarSubscription(ETH, "1m"),
        ],
        start_ts_ns=datetime_to_ns(BASE),
        end_ts_ns=datetime_to_ns(BASE + timedelta(minutes=2)),
        read=read,  # type: ignore[arg-type] - dedicated query seam
    )

    assert read.queries is not None
    assert [(query.instrument_id, query.interval) for query in read.queries] == [
        (BTC, "1m"),
        (BTC, "5m"),
        (ETH, "1m"),
    ]


def test_store_read_replay_query_returns_empty_contract_table(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(open_minute=0)], asset_class="crypto")

    with store.open_read(
        queries=(_snapshot_query(start_minute=5, end_minute=8),),
        lease_id="unit-replay",
    ) as read:
        table = read.read_replay_bars(
            (_replay_query(start_minute=5, end_minute=8),)
        )

    assert table.num_rows == 0
    assert table.schema.equals(BAR_SCHEMA, check_metadata=False)


def test_store_read_replay_query_fails_on_a_missing_data_file(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(open_minute=0)], asset_class="crypto")
    entries = store._active_entries()
    (store.dataset_root / entries[0]["path"]).unlink()

    with store.open_read(
        queries=(_snapshot_query(start_minute=0, end_minute=2),),
        lease_id="unit-replay",
    ) as read:
        try:
            read.read_replay_bars(
                (_replay_query(start_minute=0, end_minute=2),)
            )
        except FileNotFoundError:
            pass
        else:  # pragma: no cover - explicit failure path
            msg = "a missing Data File must fail the read"
            raise AssertionError(msg)
