"""Batch Normalization is field-equivalent to the scalar implementation.

The scalar ``normalize_bar`` remains the reference used by Backfill and by the
differential tests. The live Collector boundary uses ``normalize_batch``,
which validates and stamps the same fields through typed Arrow columns and
Polars. Rejections are classified with the stable Quarantine vocabulary.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mdgw.core.bar import RawBar
from mdgw.normalize.bar import normalize_bar
from mdgw.normalize.batch import BatchNormalizationError, normalize_batch
from mdgw.quarantine import (
    REASON_DECIMAL_SCALE_EXCEEDED,
    REASON_FLOAT_VALUE,
    REASON_INVALID_INSTRUMENT,
    REASON_INVALID_INTERVAL,
    REASON_MALFORMED_ROW,
    REASON_UNCLOSED_BAR,
    SOURCE_STAGE_LIVE_NORMALIZE,
)

UTC = timezone.utc
START = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
INGEST = datetime(2024, 1, 2, 0, 0, tzinfo=UTC)


def _raw(
    minute: int = 0,
    *,
    instrument_id: str = "binance:BTC/USDT",
    interval: str = "1m",
    open_ts: datetime | None = None,
    values: tuple[object, object, object, object, object] | None = None,
    trade_count: int | None = 7,
    revision: int = 0,
) -> RawBar:
    open_value, high, low, close, volume = values or (
        Decimal("100.5"),
        Decimal("101.25"),
        Decimal("99.75"),
        Decimal("100.125"),
        Decimal("12.5"),
    )
    return RawBar(
        instrument_id=instrument_id,
        interval=interval,
        open_ts=open_ts if open_ts is not None else START + timedelta(minutes=minute),
        open=open_value,  # type: ignore[arg-type]
        high=high,  # type: ignore[arg-type]
        low=low,  # type: ignore[arg-type]
        close=close,  # type: ignore[arg-type]
        volume=volume,  # type: ignore[arg-type]
        trade_count=trade_count,
        revision=revision,
    )


def _assert_same_bars(actual: list, expected: list) -> None:
    assert len(actual) == len(expected)
    for got, want in zip(actual, expected):
        assert got == want


@pytest.mark.parametrize("count", [0, 1, 25])
def test_valid_batches_match_scalar_normalization(count: int) -> None:
    raws = [
        _raw(
            index % 7,
            instrument_id="binance:BTC/USDT"
            if index % 2 == 0
            else "binance:ETH/USDT",
            trade_count=None if index % 3 == 0 else index,
            revision=index % 2,
        )
        for index in range(count)
    ]

    batch = normalize_batch(raws, feed="fake", ingest_ts=INGEST)

    expected = [normalize_bar(raw, feed="fake", ingest_ts=INGEST) for raw in raws]
    assert batch.rejections == ()
    _assert_same_bars(batch.bars, expected)


def test_timezone_conversion_and_envelope_match_scalar() -> None:
    shanghai = timezone(timedelta(hours=8))
    open_ts = datetime(2024, 1, 1, 8, 0, tzinfo=shanghai)
    raw = _raw(open_ts=open_ts)

    batch = normalize_batch([raw], feed="binance_spot", ingest_ts=INGEST)

    (normalized,) = batch.bars
    expected = normalize_bar(raw, feed="binance_spot", ingest_ts=INGEST)
    assert normalized == expected
    assert normalized.open_ts == datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    assert normalized.close_ts == datetime(2024, 1, 1, 0, 1, tzinfo=UTC)
    assert normalized.event_ts == normalized.close_ts
    assert normalized.ingest_ts == INGEST
    assert normalized.schema_version == 1
    assert normalized.data_type == "bar"
    assert normalized.seq is None
    assert normalized.aggregation_source == "external"
    assert normalized.feed == "binance_spot"


def test_decimal_precision_is_not_rounded_through_polars() -> None:
    exact = Decimal("12345678901234567890.123456789012345678")
    raw = _raw(
        values=(
            exact,
            Decimal("-99999999999999999999.999999999999999999"),
            Decimal("-0.000000000000000001"),
            Decimal("0.000000000000000001"),
            Decimal("99999999999999999999.999999999999999999"),
        )
    )

    batch = normalize_batch([raw], feed="fake", ingest_ts=INGEST)

    (bar,) = batch.bars
    expected = normalize_bar(raw, feed="fake", ingest_ts=INGEST)
    assert bar == expected
    assert bar.open == exact
    assert str(bar.open) == str(exact)
    assert bar.high == Decimal("-99999999999999999999.999999999999999999")
    assert bar.low == Decimal("-0.000000000000000001")
    assert bar.close == Decimal("0.000000000000000001")
    assert bar.volume == Decimal("99999999999999999999.999999999999999999")


def test_float_shaped_value_is_rejected_not_converted() -> None:
    batch = normalize_batch(
        [_raw(values=(1.0, Decimal("2"), Decimal("1"), Decimal("1.5"), Decimal("3")))],
        feed="fake",
        ingest_ts=INGEST,
    )

    assert batch.bars == []
    (rejection,) = batch.rejections
    assert rejection.reason_code == REASON_FLOAT_VALUE
    assert rejection.source_stage == SOURCE_STAGE_LIVE_NORMALIZE
    assert rejection.row_index == 0
    assert rejection.details["field"] == "open"
    assert rejection.instrument_id == "binance:BTC/USDT"
    assert rejection.interval == "1m"


def test_unclosed_bar_is_rejected() -> None:
    raw = _raw(open_ts=START)
    batch = normalize_batch(
        [raw], feed="fake", ingest_ts=START + timedelta(seconds=30)
    )

    assert batch.bars == []
    (rejection,) = batch.rejections
    assert rejection.reason_code == REASON_UNCLOSED_BAR
    assert rejection.source_stage == SOURCE_STAGE_LIVE_NORMALIZE
    assert rejection.details["open_ts"].startswith("2024-01-01T00:00:00")
    assert rejection.details["close_ts"].startswith("2024-01-01T00:01:00")


def test_invalid_interval_is_rejected() -> None:
    batch = normalize_batch(
        [_raw(interval="2m")], feed="fake", ingest_ts=INGEST
    )

    assert batch.bars == []
    (rejection,) = batch.rejections
    assert rejection.reason_code == REASON_INVALID_INTERVAL
    assert rejection.details["interval"] == "2m"


@pytest.mark.parametrize(
    "value",
    [
        Decimal("0.0000000000000000001"),  # 19 fractional digits
        Decimal("1.1234567890123456789"),  # 19 fractional digits
        Decimal("123456789012345678901"),  # 21 integer digits
    ],
)
def test_over_scale_decimal_is_rejected_without_rescaling(
    value: Decimal,
) -> None:
    batch = normalize_batch(
        [_raw(values=(value, Decimal("2"), Decimal("1"), Decimal("1.5"), Decimal("3")))],
        feed="fake",
        ingest_ts=INGEST,
    )

    assert batch.bars == []
    (rejection,) = batch.rejections
    assert rejection.reason_code == REASON_DECIMAL_SCALE_EXCEEDED
    assert rejection.source_stage == SOURCE_STAGE_LIVE_NORMALIZE
    assert rejection.details["field"] == "open"


@pytest.mark.parametrize("instrument_id", ["binance", "binance:", ":BTC/USDT"])
def test_invalid_instrument_id_is_rejected(instrument_id: str) -> None:
    batch = normalize_batch(
        [_raw(instrument_id=instrument_id)], feed="fake", ingest_ts=INGEST
    )

    assert batch.bars == []
    (rejection,) = batch.rejections
    assert rejection.reason_code == REASON_INVALID_INSTRUMENT
    assert rejection.instrument_id == instrument_id


def test_naive_timestamp_is_a_malformed_row() -> None:
    raw = _raw(open_ts=datetime(2024, 1, 1, 0, 0))
    batch = normalize_batch([raw], feed="fake", ingest_ts=INGEST)

    assert batch.bars == []
    (rejection,) = batch.rejections
    assert rejection.reason_code == REASON_MALFORMED_ROW
    assert rejection.details["field"] == "open_ts"


def test_mixed_batch_reports_every_rejection_and_keeps_valid_rows() -> None:
    raws = [
        _raw(0),
        _raw(1, values=(1.5, Decimal("2"), Decimal("1"), Decimal("1.5"), Decimal("3"))),
        _raw(2, interval="7m"),
        _raw(3, instrument_id="nocolon"),
        _raw(4, open_ts=INGEST),
        _raw(5),
    ]

    batch = normalize_batch(raws, feed="fake", ingest_ts=INGEST)

    assert [row.row_index for row in batch.rows] == [0, 5]
    assert [row.bar.open_ts.minute for row in batch.rows] == [0, 5]
    assert {(rejection.row_index, rejection.reason_code) for rejection in batch.rejections} == {
        (1, REASON_FLOAT_VALUE),
        (2, REASON_INVALID_INTERVAL),
        (3, REASON_INVALID_INSTRUMENT),
        (4, REASON_UNCLOSED_BAR),
    }


def test_batch_error_carries_stable_reason_codes() -> None:
    batch = normalize_batch(
        [_raw(interval="bad")], feed="fake", ingest_ts=INGEST
    )

    error = BatchNormalizationError(batch.rejections)

    assert isinstance(error, ValueError)
    assert error.reason_codes == (REASON_INVALID_INTERVAL,)
    assert "invalid Bar Interval" in str(error)
