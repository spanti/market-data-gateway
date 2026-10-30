"""Normalization: closed Feed Handler output -> canonical Bar."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from mdgw.core.bar import SCHEMA_VERSION, DATA_TYPE, Bar, RawBar
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import INTERVALS
from mdgw.core.time import to_utc

MAX_SCALE = 18


def as_decimal(value: Decimal | str | int, field: str) -> Decimal:
    """Coerce exact input to Decimal and reject anything float-shaped."""
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, str):
        result = Decimal(value)
    elif isinstance(value, int) and not isinstance(value, bool):
        result = Decimal(value)
    else:
        raise TypeError(
            f"{field} must be Decimal, str or int; floats are forbidden: {value!r}"
        )
    if result.as_tuple().exponent < -MAX_SCALE:
        raise ValueError(f"{field} has more than {MAX_SCALE} decimal places: {value!r}")
    return result


def normalize_bar(
    raw: RawBar,
    *,
    feed: str,
    ingest_ts: datetime,
) -> Bar:
    """Stamp envelope fields and enforce the closed-Bar rule."""
    instrument = InstrumentId.parse(raw.instrument_id)
    if raw.interval not in INTERVALS:
        raise ValueError(f"invalid Bar Interval: {raw.interval!r}")

    open_ts = to_utc(raw.open_ts)
    close_ts = open_ts + INTERVALS[raw.interval]
    ingest_ts = to_utc(ingest_ts)
    if close_ts > ingest_ts:
        raise ValueError(
            f"Bar is not closed: close_ts={close_ts.isoformat()} > ingest_ts={ingest_ts.isoformat()}"
        )

    return Bar(
        schema_version=SCHEMA_VERSION,
        data_type=DATA_TYPE,
        instrument_id=instrument.value,
        interval=raw.interval,
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=ingest_ts,
        feed=feed,
        seq=None,
        aggregation_source="external",
        open=as_decimal(raw.open, "open"),
        high=as_decimal(raw.high, "high"),
        low=as_decimal(raw.low, "low"),
        close=as_decimal(raw.close, "close"),
        volume=as_decimal(raw.volume, "volume"),
        trade_count=raw.trade_count,
        revision=raw.revision,
    )
