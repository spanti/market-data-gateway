"""Shared deterministic Bar generation for the benchmark scripts.

Not part of the gateway package. `bench_*.py` scripts import this module from
their own directory, so running `python benchmarks/bench_ingest.py` needs no
installation beyond the gateway's normal dependencies.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from mdgw.core.bar import Bar
from mdgw.core.time import datetime_to_ns

UTC = timezone.utc
BASE_OPEN_TS = datetime(2026, 1, 1, tzinfo=UTC)
BASE_PRICE = Decimal("100")
TICK = Decimal("0.01")
FEED = "bench"


def make_bars(
    count: int,
    *,
    instrument_id: str = "binance:BTC/USDT",
    interval: str = "1m",
    start: datetime = BASE_OPEN_TS,
) -> list[Bar]:
    """Return `count` deterministic closed 1m Bars.

    Prices stay exact Decimals; every revision 0. The values vary by index so
    the compiler cannot fold the workload away.
    """
    step = _interval_delta(interval)
    bars: list[Bar] = []
    for index in range(count):
        open_ts = start + step * index
        close_ts = open_ts + step
        price = BASE_PRICE + TICK * (index % 1_000)
        bars.append(
            Bar(
                schema_version=1,
                data_type="bar",
                instrument_id=instrument_id,
                interval=interval,
                open_ts=open_ts,
                close_ts=close_ts,
                event_ts=close_ts,
                ingest_ts=close_ts + timedelta(milliseconds=100),
                feed=FEED,
                seq=index,
                aggregation_source="external",
                open=price,
                high=price + TICK,
                low=price - TICK,
                close=price + TICK,
                volume=Decimal("1") + Decimal(index % 100) / Decimal("100"),
                trade_count=1 + index % 100,
                revision=0,
            )
        )
    return bars


def _interval_delta(interval: str) -> timedelta:
    from mdgw.core.schema import INTERVALS

    return INTERVALS[interval]


def bar_range_ns(bars: list[Bar]) -> tuple[int, int]:
    """Wide-enough Replay range covering every Bar's close_ts."""
    if not bars:
        return 0, 1
    start_ns = min(datetime_to_ns(bar.open_ts) for bar in bars)
    end_ns = max(datetime_to_ns(bar.close_ts) for bar in bars) + 1
    return start_ns, end_ns


def format_bytes(value: float) -> str:
    units = ("B", "KiB", "MiB", "GiB")
    amount = float(value)
    for unit in units:
        if amount < 1024.0 or unit == units[-1]:
            return f"{amount:.2f} {unit}"
        amount /= 1024.0
    return f"{amount:.2f} GiB"


def format_rate(rows: int, seconds: float) -> str:
    if seconds <= 0:
        return "n/a"
    return f"{rows / seconds:,.0f} rows/s"
