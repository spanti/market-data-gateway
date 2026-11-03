"""Bar value objects: raw Feed Handler output and the canonical Bar."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime
    from decimal import Decimal

SCHEMA_VERSION = 1
DATA_TYPE = "bar"


@dataclass(frozen=True, slots=True)
class RawBar:
    """A closed Bar as produced by a Feed Handler, before normalization."""

    instrument_id: str
    interval: str
    open_ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trade_count: int | None = None
    revision: int = 0


@dataclass(frozen=True, slots=True)
class Bar:
    """Canonical Bar row. Field order mirrors the Data Contract schema."""

    schema_version: int
    data_type: str
    instrument_id: str
    interval: str
    open_ts: datetime
    close_ts: datetime
    event_ts: datetime
    ingest_ts: datetime
    feed: str
    seq: int | None
    aggregation_source: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trade_count: int | None
    revision: int
