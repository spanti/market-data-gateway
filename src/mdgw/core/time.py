"""UTC nanosecond time helpers.

`datetime` only carries microsecond precision. Bars are minute-aligned, so the
value is exact today; conversion to and from int nanoseconds is explicit so a
future Data Type with finer timestamps never has to go through floats.
"""

from __future__ import annotations

from datetime import datetime, timezone

UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
NANOS_PER_SECOND = 1_000_000_000


def to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("naive datetime is not allowed; timestamps must be UTC")
    return value.astimezone(UTC)


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def datetime_to_ns(value: datetime) -> int:
    delta = to_utc(value) - EPOCH
    return (
        delta.days * 86_400 * NANOS_PER_SECOND
        + delta.seconds * NANOS_PER_SECOND
        + delta.microseconds * 1_000
    )


def ns_to_datetime(value: int) -> datetime:
    seconds, nanos = divmod(value, NANOS_PER_SECOND)
    return datetime.fromtimestamp(seconds, tz=UTC).replace(
        microsecond=nanos // 1_000
    )


def ns_to_iso(value: int) -> str:
    """ISO-8601 with exactly nine fractional digits and a `Z` suffix."""
    seconds, nanos = divmod(value, NANOS_PER_SECOND)
    base = datetime.fromtimestamp(seconds, tz=UTC)
    return f"{base:%Y-%m-%dT%H:%M:%S}.{nanos:09d}Z"
