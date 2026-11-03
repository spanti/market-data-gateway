from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

from mdgw.core.bar import Bar
from mdgw.core.schema import BAR_SCHEMA
from mdgw.core.time import datetime_to_ns
from mdgw.store.parquet_dataset import ParquetBarStore

UTC = timezone.utc


def _bar(minute: int) -> Bar:
    open_ts = datetime(2024, 1, 1, 0, 0, tzinfo=UTC) + timedelta(minutes=minute)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id="binance:BTC/USDT",
        interval="1m",
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=None,
        aggregation_source="external",
        open=Decimal("65000.1"),
        high=Decimal("65010.0"),
        low=Decimal("64990.0"),
        close=Decimal("65005.5"),
        volume=Decimal("12.34"),
        trade_count=128,
        revision=0,
    )


def test_iter_batches_streams_multiple_batches_without_losing_rows(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    bars = [_bar(minute) for minute in range(5)]
    store.write_bars(bars, asset_class="crypto")

    batches = list(store.iter_batches(interval="1m", batch_size=2))
    combined = pa.Table.from_batches(batches, schema=BAR_SCHEMA)

    assert len(batches) > 1
    assert combined.num_rows == 5
    assert combined.equals(store.read_bars(interval="1m"))


def test_iter_batches_drops_bars_outside_open_ts_range(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(minute) for minute in range(5)], asset_class="crypto")

    def ns(minute: int) -> int:
        return datetime_to_ns(datetime(2024, 1, 1, 0, minute, tzinfo=UTC))

    batches = list(store.iter_batches(interval="1m", start_ns=ns(1), end_ns=ns(4)))
    table = pa.Table.from_batches(batches, schema=BAR_SCHEMA)

    assert [value.hour * 60 + value.minute for value in table.column("open_ts").to_pylist()] == [
        1,
        2,
        3,
    ]
