"""Binance archive format: naming, CSV parsing, CHECKSUM, ms/µs handling."""

from __future__ import annotations

import hashlib
import io
import zipfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mdgw.archive.binance import (
    ArchiveFormatError,
    ArchivePeriod,
    UNIT_MS,
    UNIT_US,
    archive_csv_filename,
    archive_filename,
    archive_url,
    infer_timestamp_unit,
    market_symbol,
    parse_checksum,
    parse_kline_csv,
    plan_archive_periods,
    raw_timestamp_to_ns,
    read_zip_csv,
    timestamp_to_datetime,
    timestamp_unit_for_date,
)
from mdgw.core.time import datetime_to_ns

UTC = timezone.utc
SYMBOL = "BTCUSDT"
INSTRUMENT = "binance:BTC/USDT"
DAY = date(2024, 1, 1)


def _raw_timestamp(value: datetime, unit: str) -> int:
    ns = datetime_to_ns(value)
    return ns // 1_000_000 if unit == UNIT_MS else ns // 1_000


def _csv_row(
    open_ts: datetime,
    *,
    unit: str,
    price: str = "100",
    trade_count: str = "10",
    close_offset: int = 0,
) -> str:
    interval_units = 60_000 if unit == UNIT_MS else 60_000_000
    open_raw = _raw_timestamp(open_ts, unit)
    close_raw = open_raw + interval_units - 1 + close_offset
    value = Decimal(price)
    return ",".join(
        [
            str(open_raw),
            price,
            str(value + 1),
            str(value - 1),
            price,
            "1.5",
            str(close_raw),
            "150",
            trade_count,
            "0.75",
            "75",
            "",
        ]
    )


def _csv(*rows: str, header: bool = False) -> bytes:
    lines = ["open_time,open,high,low,close,volume,close_time,quote_volume,"
             "number_of_trades,taker_buy_base_volume,taker_buy_quote_volume,ignore"]
    if not header:
        lines = []
    lines.extend(rows)
    return ("\n".join(lines) + "\n").encode("utf-8")


def _zip(csv_name: str, csv_data: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(csv_name, csv_data)
    return buffer.getvalue()


def test_market_symbol_removes_the_slash() -> None:
    assert market_symbol("binance:BTC/USDT") == "BTCUSDT"
    assert market_symbol("binance:ETH/BTC") == "ETHBTC"


def test_daily_and_monthly_urls_and_filenames() -> None:
    daily = ArchivePeriod("daily", DAY, DAY)
    monthly = ArchivePeriod("monthly", date(2024, 1, 1), date(2024, 1, 31))

    assert archive_filename(SYMBOL, "1m", daily) == "BTCUSDT-1m-2024-01-01.zip"
    assert archive_filename(SYMBOL, "1m", monthly) == "BTCUSDT-1m-2024-01.zip"
    assert archive_csv_filename(SYMBOL, "1m", daily) == (
        "BTCUSDT-1m-2024-01-01.csv"
    )
    assert archive_url("https://data.binance.vision", "spot", SYMBOL, "1m", daily) == (
        "https://data.binance.vision/spot/daily/klines/BTCUSDT/1m/"
        "BTCUSDT-1m-2024-01-01.zip"
    )
    assert archive_url("https://data.binance.vision/", "spot", SYMBOL, "1m", monthly) == (
        "https://data.binance.vision/spot/monthly/klines/BTCUSDT/1m/"
        "BTCUSDT-1m-2024-01.zip"
    )


def test_plan_daily_never_requests_the_current_or_future_day() -> None:
    periods = plan_archive_periods(
        date(2024, 4, 1),
        date(2024, 4, 10),
        "daily",
        today=date(2024, 4, 5),
    )
    assert periods == [
        ArchivePeriod("daily", date(2024, 4, 1), date(2024, 4, 1)),
        ArchivePeriod("daily", date(2024, 4, 2), date(2024, 4, 2)),
        ArchivePeriod("daily", date(2024, 4, 3), date(2024, 4, 3)),
        ArchivePeriod("daily", date(2024, 4, 4), date(2024, 4, 4)),
    ]


def test_plan_monthly_never_requests_the_current_month() -> None:
    periods = plan_archive_periods(
        date(2024, 3, 1),
        date(2024, 5, 31),
        "monthly",
        today=date(2024, 5, 10),
    )
    assert periods == [
        ArchivePeriod("monthly", date(2024, 3, 1), date(2024, 3, 31)),
        ArchivePeriod("monthly", date(2024, 4, 1), date(2024, 4, 30)),
    ]


def test_plan_auto_uses_monthly_for_complete_months_and_daily_for_edges() -> None:
    periods = plan_archive_periods(
        date(2024, 1, 15),
        date(2024, 4, 10),
        "auto",
        today=date(2024, 4, 15),
    )
    assert [period.granularity for period in periods[:17]] == ["daily"] * 17
    assert periods[0] == ArchivePeriod("daily", date(2024, 1, 15), date(2024, 1, 15))
    assert periods[16] == ArchivePeriod("daily", date(2024, 1, 31), date(2024, 1, 31))
    assert periods[17] == ArchivePeriod(
        "monthly", date(2024, 2, 1), date(2024, 2, 29)
    )
    assert periods[18] == ArchivePeriod(
        "monthly", date(2024, 3, 1), date(2024, 3, 31)
    )
    assert [period.granularity for period in periods[19:]] == ["daily"] * 10
    assert periods[-1] == ArchivePeriod("daily", date(2024, 4, 10), date(2024, 4, 10))


def test_plan_auto_uses_daily_for_the_unfinished_current_month() -> None:
    periods = plan_archive_periods(
        date(2024, 4, 1),
        date(2024, 4, 30),
        "auto",
        today=date(2024, 4, 5),
    )
    assert len(periods) == 4
    assert all(period.granularity == "daily" for period in periods)
    assert periods[-1].start == date(2024, 4, 4)


def test_csv_with_and_without_a_header_produce_the_same_bars() -> None:
    open_ts = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    row = _csv_row(open_ts, unit=UNIT_MS)

    without = parse_kline_csv(
        _csv(row), instrument_id=INSTRUMENT, interval="1m", unit=UNIT_MS
    )
    with_header = parse_kline_csv(
        _csv(row, header=True),
        instrument_id=INSTRUMENT,
        interval="1m",
        unit=UNIT_MS,
    )

    assert without == with_header
    assert len(without) == 1
    assert without[0].open_ts == open_ts


def test_decimal_prices_and_volume_are_exact_and_never_floats() -> None:
    price = "65000.123456789012345678"
    open_ts = datetime(2024, 1, 1, tzinfo=UTC)
    bars = parse_kline_csv(
        _csv(_csv_row(open_ts, unit=UNIT_MS, price=price)),
        instrument_id=INSTRUMENT,
        interval="1m",
        unit=UNIT_MS,
    )

    bar = bars[0]
    assert isinstance(bar.open, Decimal)
    assert isinstance(bar.volume, Decimal)
    assert bar.open == Decimal(price)
    assert str(bar.open) == price


def test_trade_count_empty_is_none_and_uint32_is_enforced() -> None:
    open_ts = datetime(2024, 1, 1, tzinfo=UTC)
    empty = parse_kline_csv(
        _csv(_csv_row(open_ts, unit=UNIT_MS, trade_count="")),
        instrument_id=INSTRUMENT,
        interval="1m",
        unit=UNIT_MS,
    )
    assert empty[0].trade_count is None

    too_large = _csv(_csv_row(open_ts, unit=UNIT_MS, trade_count=str(2**32)))
    with pytest.raises(ArchiveFormatError, match="uint32"):
        parse_kline_csv(
            too_large, instrument_id=INSTRUMENT, interval="1m", unit=UNIT_MS
        )


def test_close_time_tolerates_one_unit_and_rejects_more() -> None:
    open_ts = datetime(2024, 1, 1, tzinfo=UTC)
    parse_kline_csv(
        _csv(_csv_row(open_ts, unit=UNIT_MS, close_offset=1)),
        instrument_id=INSTRUMENT,
        interval="1m",
        unit=UNIT_MS,
    )

    with pytest.raises(ArchiveFormatError, match="close_time"):
        parse_kline_csv(
            _csv(_csv_row(open_ts, unit=UNIT_MS, close_offset=2)),
            instrument_id=INSTRUMENT,
            interval="1m",
            unit=UNIT_MS,
        )


def test_blank_lines_and_utf8_bom_are_handled() -> None:
    open_ts = datetime(2024, 1, 1, tzinfo=UTC)
    text = "\n\n" + _csv_row(open_ts, unit=UNIT_MS) + "\n\n"
    bars = parse_kline_csv(
        text.encode("utf-8-sig"),
        instrument_id=INSTRUMENT,
        interval="1m",
        unit=UNIT_MS,
    )
    assert len(bars) == 1


def test_ms_and_us_representations_of_the_same_instant_match() -> None:
    instant = datetime(2024, 12, 31, 12, 0, tzinfo=UTC)
    ms_value = _raw_timestamp(instant, UNIT_MS)
    us_value = _raw_timestamp(instant, UNIT_US)

    assert infer_timestamp_unit(ms_value) == UNIT_MS
    assert infer_timestamp_unit(us_value) == UNIT_US
    assert raw_timestamp_to_ns(us_value, UNIT_US) == raw_timestamp_to_ns(
        ms_value, UNIT_MS
    )
    assert timestamp_to_datetime(ms_value, UNIT_MS) == instant
    assert timestamp_to_datetime(us_value, UNIT_US) == instant


def test_timestamp_unit_follows_the_2025_boundary() -> None:
    assert timestamp_unit_for_date(date(2024, 12, 31)) == UNIT_MS
    assert timestamp_unit_for_date(date(2025, 1, 1)) == UNIT_US


def test_timestamp_unit_and_magnitude_must_agree_with_the_archive_date() -> None:
    open_ts = datetime(2025, 1, 1, tzinfo=UTC)
    us_value = _raw_timestamp(open_ts, UNIT_US)
    # A microseconds archive carrying a milliseconds-sized value is rejected.
    row = _csv_row(open_ts, unit=UNIT_MS)
    with pytest.raises(ArchiveFormatError, match="looks like ms"):
        parse_kline_csv(
            _csv(row), instrument_id=INSTRUMENT, interval="1m", unit=UNIT_US
        )

    # And a milliseconds archive carrying a microseconds-sized value.
    us_row = _csv_row(open_ts, unit=UNIT_US)
    with pytest.raises(ArchiveFormatError, match="looks like us"):
        parse_kline_csv(
            _csv(us_row), instrument_id=INSTRUMENT, interval="1m", unit=UNIT_MS
        )


def test_parse_checksum_accepts_sha256sum_format_and_rejects_junk() -> None:
    digest = hashlib.sha256(b"data").hexdigest()
    assert parse_checksum(f"{digest}  BTCUSDT-1m-2024-01-01.zip\n") == digest
    assert parse_checksum(f"{digest.upper()}  file.zip") == digest
    with pytest.raises(ArchiveFormatError):
        parse_checksum("not-a-digest  file.zip")
    with pytest.raises(ArchiveFormatError):
        parse_checksum("")


def test_zip_requires_exactly_one_expected_csv() -> None:
    expected = "BTCUSDT-1m-2024-01-01.csv"
    assert read_zip_csv(_zip(expected, b"a"), expected_name=expected) == b"a"

    with pytest.raises(ArchiveFormatError, match="valid ZIP"):
        read_zip_csv(b"not a zip", expected_name=expected)

    with pytest.raises(ArchiveFormatError, match="unexpected CSV"):
        read_zip_csv(_zip("other.csv", b"a"), expected_name=expected)
