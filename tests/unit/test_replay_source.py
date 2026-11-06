"""Replay read path: close_ts range, latest revision, deterministic ordering."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from mdgw.core.bar import Bar
from mdgw.core.schema import BAR_SCHEMA, INTERVALS
from mdgw.core.time import datetime_to_ns
from mdgw.replay.session import BarSubscription
from mdgw.replay.source import ReplayBarSource, interval_ns
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
    """Collect the production streaming read on one fixed Snapshot."""
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
        return [
            bar
            for batch in ReplayBarSource().iter_bars(
                subscriptions,
                start_ts_ns=start_ns,
                end_ts_ns=end_ns,
                read=read,
                batch_size=7,
            )
            for bar in batch
        ]


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


def test_order_is_close_ts_instrument_seq_then_interval(tmp_path: Path) -> None:
    """The streaming query's five-key total order, observed end to end."""
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [
            # Same close_ts (00:01): Instrument, then numeric seq.
            _bar(instrument_id=ETH, open_minute=0, seq=0),
            _bar(instrument_id=BTC, open_minute=0, seq=3),
            _bar(instrument_id=BTC, interval="5m", open_minute=-4, seq=1),
            # Same close_ts (00:02), same Instrument, NULL seq: interval.
            _bar(instrument_id=BTC, open_minute=1),
            _bar(instrument_id=BTC, interval="5m", open_minute=-3),
        ],
        asset_class="crypto",
    )

    bars = _read(
        store,
        [
            BarSubscription(BTC, "1m"),
            BarSubscription(BTC, "5m"),
            BarSubscription(ETH, "1m"),
        ],
        start_minute=0,
        end_minute=3,
    )

    assert [
        (bar.instrument_id, bar.interval, bar.open_ts, bar.seq)
        for bar in bars
    ] == [
        (BTC, "5m", BASE - timedelta(minutes=4), 1),
        (BTC, "1m", BASE, 3),
        (ETH, "1m", BASE, 0),
        (BTC, "1m", BASE + timedelta(minutes=1), None),
        (BTC, "5m", BASE - timedelta(minutes=3), None),
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
        batches = list(
            read.iter_replay_batches(replay_queries, batch_size=2)
        )

    assert batches
    for batch in batches:
        assert batch.schema.equals(BAR_SCHEMA, check_metadata=False)
    rows = [row for batch in batches for row in batch.to_pylist()]
    assert [_signature(Bar(**row)) for row in rows] == [
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


def test_source_selects_every_revision_identity_in_five_key_order(
    tmp_path: Path,
) -> None:
    """One row per (instrument, interval, open_ts), newest revision, ordered."""
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

    bars = _read(store, subscriptions, start_minute=0, end_minute=5)

    assert [
        (bar.instrument_id, bar.interval, bar.open_ts, bar.close_ts,
         bar.seq, bar.revision, bar.open)
        for bar in bars
    ] == [
        # Highest revision wins for the open-0 identity.
        (BTC, "1m", BASE, BASE + timedelta(minutes=1), None, 2, Decimal("200")),
        # Equal revision: the latest ingest_ts wins for the open-1 identity.
        (BTC, "1m", BASE + timedelta(minutes=1),
         BASE + timedelta(minutes=2), None, 1, Decimal("100")),
        (ETH, "1m", BASE + timedelta(minutes=1),
         BASE + timedelta(minutes=2), None, 0, Decimal("100")),
        (BTC, "1m", BASE + timedelta(minutes=2),
         BASE + timedelta(minutes=3), 2, 0, Decimal("100")),
        (BTC, "1m", BASE + timedelta(minutes=3),
         BASE + timedelta(minutes=4), None, 0, Decimal("100")),
    ]


def test_duckdb_source_dedupes_identical_subscriptions_before_querying(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(open_minute=0)], asset_class="crypto")

    class _RecordingRead:
        queries: tuple[ReplayReadQuery, ...] | None = None

        def iter_replay_batches(  # ruff: ignore[unused-method-argument]
            self, queries, *, batch_size=None, cancellation=None
        ):
            self.queries = tuple(queries)
            return iter(())

    read = _RecordingRead()
    bars = list(
        ReplayBarSource().iter_bars(
            [
                BarSubscription(BTC, "1m"),
                BarSubscription(BTC, "1m"),
                BarSubscription(BTC, "5m"),
                BarSubscription(ETH, "1m"),
            ],
            start_ts_ns=datetime_to_ns(BASE),
            end_ts_ns=datetime_to_ns(BASE + timedelta(minutes=2)),
            read=read,  # type: ignore[arg-type] - dedicated query seam
            batch_size=2,
        )
    )
    assert bars == []

    assert read.queries is not None
    assert [(query.instrument_id, query.interval) for query in read.queries] == [
        (BTC, "1m"),
        (BTC, "5m"),
        (ETH, "1m"),
    ]


def test_store_read_iter_replay_batches_streams_the_ordered_contract_batches(
    tmp_path: Path,
) -> None:
    """The streaming seam yields the ordered Bars split into batches."""
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [
            _bar(open_minute=0, revision=0, price="100"),
            _bar(open_minute=1, seq=3, price="100"),
            _bar(open_minute=2, seq=None, price="100"),
            _bar(open_minute=3, seq=1, price="100"),
            _bar(open_minute=4, seq=None, price="100"),
        ],
        asset_class="crypto",
    )
    store.write_bars(
        [_bar(open_minute=1, revision=2, price="200")],
        asset_class="crypto",
    )
    query = _replay_query(start_minute=0, end_minute=10)
    with store.open_read(
        queries=(_snapshot_query(start_minute=0, end_minute=10),),
        lease_id="unit-stream",
    ) as read:
        batches = list(
            read.iter_replay_batches((query,), batch_size=2)
        )

    assert [batch.num_rows for batch in batches] == [2, 2, 1]
    for batch in batches:
        assert batch.schema.equals(BAR_SCHEMA, check_metadata=False)
    streamed = pa.Table.from_batches(batches, schema=BAR_SCHEMA)
    assert [_signature(Bar(**row)) for row in streamed.to_pylist()] == [
        (BTC, "1m", datetime_to_ns(BASE), datetime_to_ns(BASE + timedelta(minutes=1)),
         None, 0, Decimal("100")),
        (BTC, "1m", datetime_to_ns(BASE + timedelta(minutes=1)),
         datetime_to_ns(BASE + timedelta(minutes=2)), None, 2, Decimal("200")),
        (BTC, "1m", datetime_to_ns(BASE + timedelta(minutes=2)),
         datetime_to_ns(BASE + timedelta(minutes=3)), None, 0, Decimal("100")),
        (BTC, "1m", datetime_to_ns(BASE + timedelta(minutes=3)),
         datetime_to_ns(BASE + timedelta(minutes=4)), 1, 0, Decimal("100")),
        (BTC, "1m", datetime_to_ns(BASE + timedelta(minutes=4)),
         datetime_to_ns(BASE + timedelta(minutes=5)), None, 0, Decimal("100")),
    ]


def test_store_read_iter_replay_batches_is_lazy_until_consumed(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(open_minute=0)], asset_class="crypto")
    with store.open_read(
        queries=(_snapshot_query(start_minute=0, end_minute=2),),
        lease_id="unit-stream-lazy",
    ) as read:
        batches = read.iter_replay_batches(
            (_replay_query(start_minute=0, end_minute=2),),
            batch_size=1,
        )
        # A closed StoreRead must fail when iteration actually starts; a
        # materializing call would already have produced (or refused) here.
        read.close()
        with pytest.raises(RuntimeError, match="closed"):
            next(iter(batches))


def test_store_read_iter_replay_batches_returns_nothing_without_files(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(open_minute=0)], asset_class="crypto")
    with store.open_read(
        queries=(_snapshot_query(start_minute=5, end_minute=8),),
        lease_id="unit-stream-empty",
    ) as read:
        batches = list(
            read.iter_replay_batches(
                (_replay_query(start_minute=5, end_minute=8),),
                batch_size=2,
            )
        )
    assert batches == []


def test_store_read_replay_query_returns_empty_contract_table(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(open_minute=0)], asset_class="crypto")

    with store.open_read(
        queries=(_snapshot_query(start_minute=5, end_minute=8),),
        lease_id="unit-replay",
    ) as read:
        batches = list(
            read.iter_replay_batches(
                (_replay_query(start_minute=5, end_minute=8),)
            )
        )

    table = pa.Table.from_batches(batches, schema=BAR_SCHEMA)
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
            list(
                read.iter_replay_batches(
                    (_replay_query(start_minute=0, end_minute=2),)
                )
            )
        except FileNotFoundError:
            pass
        else:  # pragma: no cover - explicit failure path
            msg = "a missing Data File must fail the read"
            raise AssertionError(msg)
