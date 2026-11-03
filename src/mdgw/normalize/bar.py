"""Normalization: closed Feed Handler output -> canonical Bar."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from mdgw.core.bar import DATA_TYPE, SCHEMA_VERSION, Bar, RawBar
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import INTERVALS
from mdgw.core.time import to_utc

if TYPE_CHECKING:
    from datetime import datetime

MAX_SCALE = 18


def as_decimal(value: Decimal | str | int, field: str) -> Decimal:
    """Coerce exact input to Decimal and reject anything float-shaped."""
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, str) or (
        isinstance(value, int) and not isinstance(value, bool)
    ):
        result = Decimal(value)
    else:
        msg = f"{field} must be Decimal, str or int; floats are forbidden: {value!r}"
        raise TypeError(msg)
    if result.as_tuple().exponent < -MAX_SCALE:
        msg = f"{field} has more than {MAX_SCALE} decimal places: {value!r}"
        raise ValueError(msg)
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
        msg = f"invalid Bar Interval: {raw.interval!r}"
        raise ValueError(msg)

    open_ts = to_utc(raw.open_ts)
    close_ts = open_ts + INTERVALS[raw.interval]
    ingest_ts = to_utc(ingest_ts)
    if close_ts > ingest_ts:
        msg = (
            "Bar is not closed: "
            f"close_ts={close_ts.isoformat()} > ingest_ts={ingest_ts.isoformat()}"
        )
        raise ValueError(msg)

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
