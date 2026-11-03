"""Binance `data.binance.vision` K-line archive format (v1: spot only).

This module owns everything specific to the public archive:

* URL and file-name construction for daily and monthly ZIPs;
* SHA-256 `.CHECKSUM` parsing;
* safe ZIP reading (one `.csv`, validated name, never extracted to disk);
* CSV parsing with exact `Decimal` prices and the archive `close_time` check;
* the 2025-01-01 millisecond -> microsecond timestamp switch.

Prices and quantities never pass through `float`. Timestamps are converted to
UTC nanoseconds and handed back as timezone-aware `datetime` objects, matching
the rest of the Gateway.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

from mdgw.core.bar import RawBar
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import INTERVALS
from mdgw.core.time import ns_to_datetime

DEFAULT_BASE_URL = "https://data.binance.vision"
SUPPORTED_GRANULARITIES = ("auto", "daily", "monthly")
SUPPORTED_ARCHIVE_INTERVAL = "1m"

# Binance spot switched its archive timestamps from milliseconds to
# microseconds on 2025-01-01. The unit is chosen from the archive file's date,
# then cross-checked against each value's magnitude.
MICROSECOND_EPOCH = date(2025, 1, 1)
UNIT_MS = "ms"
UNIT_US = "us"

_MS_MIN, _MS_MAX = 10**11, 10**14
_US_MIN, _US_MAX = 10**14, 10**17
_CHECKSUM_TOKEN_RE = re.compile(r"^[0-9a-fA-F]{64}$")


class ArchiveFormatError(ValueError):
    """An archive file or its CHECKSUM does not match the expected format."""


@dataclass(frozen=True, slots=True)
class ArchivePeriod:
    """One requested archive file: a single day or a complete calendar month."""

    granularity: str
    start: date
    end: date

    def __post_init__(self) -> None:
        if self.granularity not in ("daily", "monthly"):
            raise ValueError(f"unsupported archive granularity: {self.granularity!r}")
        if self.end < self.start:
            raise ValueError("archive period end must not precede its start")

    @property
    def unit(self) -> str:
        return timestamp_unit_for_date(self.start)

    def filename(self, symbol: str, interval: str) -> str:
        return archive_filename(symbol, interval, self)

    def csv_filename(self, symbol: str, interval: str) -> str:
        return archive_csv_filename(symbol, interval, self)

    def contains(self, day: date) -> bool:
        return self.start <= day <= self.end


def market_symbol(instrument_id: str) -> str:
    """Map an InstrumentId to the Binance archive symbol.

    v1 only supports Binance spot, whose archive symbol is the InstrumentId
    symbol with `/` removed: `binance:BTC/USDT` -> `BTCUSDT`. Instruments whose
    venue-native symbol differs from this rule are out of scope for v1.
    """
    return InstrumentId.parse(instrument_id).symbol.replace("/", "")


def timestamp_unit_for_date(day: date) -> str:
    """Archive dates on/after 2025-01-01 are microseconds, earlier are milliseconds."""
    return UNIT_US if day >= MICROSECOND_EPOCH else UNIT_MS


def infer_timestamp_unit(value: int) -> str:
    """Infer ms/µs from the epoch value's magnitude, independent of the date."""
    if _US_MIN <= value < _US_MAX:
        return UNIT_US
    if _MS_MIN <= value < _MS_MAX:
        return UNIT_MS
    raise ArchiveFormatError(
        f"unrecognized archive timestamp magnitude: {value!r} "
        "(expected milliseconds ~1e12 or microseconds ~1e15)"
    )


def raw_timestamp_to_ns(value: int, unit: str) -> int:
    if unit == UNIT_MS:
        return value * 1_000_000
    if unit == UNIT_US:
        return value * 1_000
    raise ArchiveFormatError(f"unsupported archive timestamp unit: {unit!r}")


def timestamp_to_datetime(value: int, unit: str):
    return ns_to_datetime(raw_timestamp_to_ns(value, unit))


def archive_filename(symbol: str, interval: str, period: ArchivePeriod) -> str:
    if period.granularity == "daily":
        stamp = f"{period.start:%Y-%m-%d}"
    else:
        stamp = f"{period.start:%Y-%m}"
    return f"{symbol}-{interval}-{stamp}.zip"


def archive_csv_filename(symbol: str, interval: str, period: ArchivePeriod) -> str:
    return archive_filename(symbol, interval, period)[: -len(".zip")] + ".csv"


def archive_url(
    base_url: str,
    market_type: str,
    symbol: str,
    interval: str,
    period: ArchivePeriod,
) -> str:
    if market_type != "spot":
        raise ArchiveFormatError(
            f"v1 archive only supports market_type 'spot', got {market_type!r}"
        )
    filename = archive_filename(symbol, interval, period)
    return (
        f"{base_url.rstrip('/')}/{market_type}/{period.granularity}/klines/"
        f"{symbol}/{interval}/{filename}"
    )


def plan_archive_periods(
    start: date,
    end: date,
    granularity: str,
    *,
    today: date | None = None,
) -> list[ArchivePeriod]:
    """Choose daily/monthly archive files for the inclusive UTC date range.

    * `daily`: one file per day, capped at yesterday (never the current day).
    * `monthly`: one file per intersecting calendar month, but only months that
      have ended (never the current month).
    * `auto`: complete calendar months inside the range use monthly files;
      partial edge months and the current month use daily files.

    Bars are still filtered to `[start, end]` after parsing, so a monthly file
    that overlaps a partial edge is safe to read.
    """
    if granularity not in SUPPORTED_GRANULARITIES:
        raise ValueError(f"unsupported archive granularity: {granularity!r}")
    if end < start:
        return []
    today = today or date.today()
    yesterday = today - timedelta(days=1)

    if granularity == "daily":
        periods: list[ArchivePeriod] = []
        day = start
        last = min(end, yesterday)
        while day <= last:
            periods.append(ArchivePeriod("daily", day, day))
            day += timedelta(days=1)
        return periods

    if granularity == "monthly":
        return [
            ArchivePeriod("monthly", month_first, month_last)
            for month_first, month_last in _months_between(start, end)
            if month_last <= yesterday
        ]

    periods = []
    for month_first, month_last in _months_between(start, end):
        if start <= month_first and month_last <= end and month_last <= yesterday:
            periods.append(ArchivePeriod("monthly", month_first, month_last))
            continue
        day = max(start, month_first)
        last = min(end, month_last, yesterday)
        while day <= last:
            periods.append(ArchivePeriod("daily", day, day))
            day += timedelta(days=1)
    return periods


def _months_between(start: date, end: date):
    cursor = start.replace(day=1)
    while cursor <= end:
        month_last = _month_last(cursor)
        yield cursor, month_last
        cursor = month_last + timedelta(days=1)


def _month_last(day: date) -> date:
    if day.month == 12:
        following = date(day.year + 1, 1, 1)
    else:
        following = date(day.year, day.month + 1, 1)
    return following - timedelta(days=1)


def parse_checksum(text: str) -> str:
    """Extract the SHA-256 hex digest from a `.CHECKSUM` file."""
    stripped = text.strip()
    if not stripped:
        raise ArchiveFormatError("empty CHECKSUM file")
    token = stripped.split()[0].lstrip("*")
    if not _CHECKSUM_TOKEN_RE.match(token):
        raise ArchiveFormatError(f"invalid CHECKSUM digest: {stripped.split()[0]!r}")
    return token.lower()


def read_zip_csv(data: bytes, *, expected_name: str) -> bytes:
    """Return the single CSV inside a ZIP after validating its file name.

    Nothing is written to disk, so a malicious path inside the ZIP (zip slip)
    has no effect.
    """
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as exc:
        raise ArchiveFormatError(f"not a valid ZIP archive: {exc}") from exc

    with archive:
        csv_names = [
            name
            for name in archive.namelist()
            if name.lower().endswith(".csv") and not name.endswith("/")
        ]
        if len(csv_names) != 1:
            raise ArchiveFormatError(
                f"ZIP must contain exactly one .csv file, found {len(csv_names)}"
            )
        name = csv_names[0]
        if Path(name).name != expected_name:
            raise ArchiveFormatError(
                f"unexpected CSV file name {Path(name).name!r}; "
                f"expected {expected_name!r}"
            )
        try:
            return archive.read(name)
        except (KeyError, OSError, RuntimeError) as exc:
            raise ArchiveFormatError(f"cannot read {name!r} from ZIP: {exc}") from exc


def parse_kline_csv(
    data: bytes,
    *,
    instrument_id: str,
    interval: str,
    unit: str,
) -> list[RawBar]:
    """Parse a Binance K-line CSV into 1m `RawBar`s.

    Supports files with or without a header, skips blank lines, handles a UTF-8
    BOM, and validates `close_time` against `open_time + interval - 1` (a
    one-unit drift is tolerated, as in the archives).
    """
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ArchiveFormatError(f"CSV is not valid UTF-8: {exc}") from exc

    interval_units = _interval_units(interval, unit)
    bars: list[RawBar] = []
    first_row = True
    for row in csv.reader(io.StringIO(text)):
        if not row or all(not cell.strip() for cell in row):
            continue
        if first_row:
            first_row = False
            if row[0].strip().lower() == "open_time":
                continue
        if len(row) < 9:
            raise ArchiveFormatError(
                f"K-line row has {len(row)} column(s), expected at least 9"
            )

        open_raw = _parse_int(row[0], "open_time")
        close_raw = _parse_int(row[6], "close_time")
        inferred = infer_timestamp_unit(open_raw)
        if inferred != unit:
            raise ArchiveFormatError(
                f"open_time {open_raw!r} looks like {inferred} but the archive "
                f"date requires {unit}"
            )
        expected_close = open_raw + interval_units - 1
        if abs(close_raw - expected_close) > 1:
            raise ArchiveFormatError(
                f"close_time {close_raw!r} does not match open_time + "
                f"interval - 1 ({expected_close!r})"
            )

        bars.append(
            RawBar(
                instrument_id=instrument_id,
                interval=interval,
                open_ts=timestamp_to_datetime(open_raw, unit),
                open=_parse_decimal(row[1], "open"),
                high=_parse_decimal(row[2], "high"),
                low=_parse_decimal(row[3], "low"),
                close=_parse_decimal(row[4], "close"),
                volume=_parse_decimal(row[5], "volume"),
                trade_count=_parse_trade_count(row[8]),
                revision=0,
            )
        )
    return bars


def _interval_units(interval: str, unit: str) -> int:
    if interval not in INTERVALS:
        raise ArchiveFormatError(f"unsupported archive interval: {interval!r}")
    seconds = int(INTERVALS[interval].total_seconds())
    return seconds * (1_000 if unit == UNIT_MS else 1_000_000)


def _parse_int(value: str, field: str) -> int:
    text = value.strip()
    if not text:
        raise ArchiveFormatError(f"empty {field} column")
    try:
        return int(text)
    except ValueError as exc:
        raise ArchiveFormatError(f"invalid {field}: {value!r}") from exc


def _parse_decimal(value: str, field: str) -> Decimal:
    text = value.strip()
    if not text:
        raise ArchiveFormatError(f"empty {field} column")
    try:
        number = Decimal(text)
    except InvalidOperation as exc:
        raise ArchiveFormatError(f"invalid {field}: {value!r}") from exc
    if not number.is_finite():
        raise ArchiveFormatError(f"{field} must be finite: {value!r}")
    return number


def _parse_trade_count(value: str) -> int | None:
    text = value.strip()
    if not text:
        return None
    try:
        count = int(text)
    except ValueError as exc:
        raise ArchiveFormatError(
            f"invalid number_of_trades: {value!r}"
        ) from exc
    if not 0 <= count <= 0xFFFFFFFF:
        raise ArchiveFormatError(
            f"number_of_trades does not fit uint32: {count!r}"
        )
    return count


__all__ = [
    "ArchiveFormatError",
    "ArchivePeriod",
    "DEFAULT_BASE_URL",
    "SUPPORTED_ARCHIVE_INTERVAL",
    "SUPPORTED_GRANULARITIES",
    "UNIT_MS",
    "UNIT_US",
    "archive_csv_filename",
    "archive_filename",
    "archive_url",
    "infer_timestamp_unit",
    "market_symbol",
    "parse_checksum",
    "parse_kline_csv",
    "plan_archive_periods",
    "raw_timestamp_to_ns",
    "read_zip_csv",
    "timestamp_to_datetime",
    "timestamp_unit_for_date",
]
