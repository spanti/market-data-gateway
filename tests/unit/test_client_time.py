"""UTC nanosecond normalization used by history, seek and Replay create."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from mdgw_client.time import (
    datetime_to_ns,
    iso_to_ns,
    ns_to_datetime,
    ns_to_iso,
    normalize_ts,
    to_ns,
)

UTC = timezone.utc
NS_2024 = 1_704_067_200_000_000_000  # 2024-01-01T00:00:00Z


def test_aware_datetime_is_converted_to_utc_nanoseconds() -> None:
    value = datetime(2024, 1, 1, 8, 0, 0, 123456, tzinfo=timezone(timedelta(hours=8)))
    assert to_ns(value) == NS_2024 + 123_456_000


def test_naive_datetime_is_interpreted_as_utc() -> None:
    naive = datetime(2024, 1, 1, 0, 0, 0)
    assert to_ns(naive) == NS_2024
    assert datetime_to_ns(naive) == NS_2024


def test_integer_nanoseconds_pass_through_unchanged() -> None:
    assert to_ns(NS_2024) == NS_2024
    assert normalize_ts(123) == 123


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2024-01-01T00:00:00Z", NS_2024),
        ("2024-01-01T00:00:00.123456789Z", NS_2024 + 123_456_789),
        ("2024-01-01T00:00:00.5Z", NS_2024 + 500_000_000),
        ("2024-01-01T08:00:00+08:00", NS_2024),
        ("2024-01-01T07:59:59.999999999+08:00", NS_2024 - 1),
        ("2024-01-01 00:00:00", NS_2024),  # space separator, naive means UTC
    ],
)
def test_iso_strings_accept_offsets_fractions_and_naive_values(
    text: str, expected: int
) -> None:
    assert iso_to_ns(text) == expected
    assert to_ns(text) == expected


def test_nanoseconds_render_with_exactly_nine_digits() -> None:
    value = NS_2024 + 123_456_789
    assert ns_to_iso(value) == "2024-01-01T00:00:00.123456789Z"
    assert ns_to_iso(NS_2024) == "2024-01-01T00:00:00.000000000Z"
    assert iso_to_ns(ns_to_iso(value)) == value


def test_ns_to_datetime_keeps_microsecond_precision() -> None:
    assert ns_to_datetime(NS_2024 + 123_456_789) == datetime(
        2024, 1, 1, 0, 0, 0, 123456, tzinfo=UTC
    )


def test_invalid_timestamps_are_rejected() -> None:
    with pytest.raises(ValueError):
        iso_to_ns("not-a-timestamp")
    with pytest.raises(TypeError):
        to_ns(True)
    with pytest.raises(TypeError):
        to_ns(1.5)  # floats are never accepted: they would lose precision
