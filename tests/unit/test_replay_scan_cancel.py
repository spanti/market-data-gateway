"""Cancellation of one Replay query generation on one shared `StoreRead`.

A realtime seek or stop must interrupt the active DuckDB Reader and connection
without releasing the Session's fixed Manifest Snapshot or Dataset Read Lease,
so the next query generation can read the same Snapshot. These tests pin that
seam against a real Store and real DuckDB.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from mdgw.core.bar import Bar
from mdgw.core.time import datetime_to_ns
from mdgw.store.parquet_dataset import (
    BarReadQuery,
    ParquetBarStore,
    ReplayReadQuery,
    ReplayScanCancellation,
)

UTC = timezone.utc
BASE = datetime(2026, 10, 30, tzinfo=UTC)
INSTRUMENT = "binance:BTC/USDT"


def _bar(minute_offset: int) -> Bar:
    open_ts = BASE + timedelta(minutes=minute_offset)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=INSTRUMENT,
        interval="1m",
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=minute_offset,
        aggregation_source="external",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1"),
        trade_count=1,
        revision=0,
    )


def _snapshot_query() -> BarReadQuery:
    return BarReadQuery(
        instrument_id=INSTRUMENT,
        interval="1m",
        start_ns=0,
        end_ns=None,
    )


def _replay_query(
    *, start_minute: int = 0, end_minute: int = 10
) -> ReplayReadQuery:
    return ReplayReadQuery(
        instrument_id=INSTRUMENT,
        interval="1m",
        open_start_ns=datetime_to_ns(
            BASE + timedelta(minutes=start_minute - 1)
        ),
        close_start_ns=datetime_to_ns(BASE + timedelta(minutes=start_minute)),
        close_end_ns=datetime_to_ns(BASE + timedelta(minutes=end_minute)),
    )


def test_a_cancelled_generation_never_starts_its_query(tmp_path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0), _bar(1)], asset_class="crypto")

    with store.open_read(
        queries=(_snapshot_query(),), lease_id="unit-cancel-before"
    ) as read:
        cancellation = ReplayScanCancellation()
        cancellation.cancel()

        stream = read.iter_replay_batches(
            (_replay_query(),),
            batch_size=2,
            cancellation=cancellation,
        )
        assert list(stream) == []

        # The fixed Snapshot and lease are still usable by the next generation.
        batches = list(
            read.iter_replay_batches((_replay_query(),), batch_size=2)
        )
        assert sum(batch.num_rows for batch in batches) == 2


def test_a_cancelled_generation_leaves_the_snapshot_read_usable(tmp_path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(index) for index in range(4)], asset_class="crypto")

    with store.open_read(
        queries=(_snapshot_query(),), lease_id="unit-cancel-reuse"
    ) as read:
        cancellation = ReplayScanCancellation()
        stream = read.iter_replay_batches(
            (_replay_query(),),
            batch_size=1,
            cancellation=cancellation,
        )
        # Consume one batch, then cancel the generation while the Reader is
        # still open. Closing the generator afterwards is the same path the
        # Replay prefetcher uses after a seek.
        assert next(stream).num_rows == 1
        cancellation.cancel()
        stream.close()
        assert cancellation.cancelled

        # A new generation on the same StoreRead reads the same Snapshot.
        batches = list(
            read.iter_replay_batches((_replay_query(),), batch_size=3)
        )
        assert sum(batch.num_rows for batch in batches) == 4


def test_repeated_cancellation_is_harmless() -> None:
    cancellation = ReplayScanCancellation()
    cancellation.cancel()
    cancellation.cancel()
    assert cancellation.cancelled
