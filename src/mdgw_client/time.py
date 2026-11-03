"""UTC nanosecond helpers shared by every client transport.

The Data Contract carries ``timestamp[ns, tz=UTC]``. Python ``datetime`` only
has microsecond precision, so public arguments may be ``datetime``, UTC
nanosecond ``int`` or an ISO-8601 string; all are normalized without going
through ``float``. Naive datetimes are interpreted as UTC by the SDK.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
NANOS_PER_SECOND = 1_000_000_000
NANOS_PER_MICROSECOND = 1_000

_ISO_RE = re.compile(
    r"^(?P<base>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<frac>\d{1,9}))?"
    r"(?P<tz>Z|[+-]\d{2}:?\d{2})?$"
)

Timestamp = datetime | int | str


def datetime_to_ns(value: datetime) -> int:
    """Convert a datetime to UTC nanoseconds. Naive values are treated as UTC."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    delta = value - EPOCH
    return (
        delta.days * 86_400 * NANOS_PER_SECOND
        + delta.seconds * NANOS_PER_SECOND
        + delta.microseconds * NANOS_PER_MICROSECOND
    )


def iso_to_ns(value: str) -> int:
    """Parse ISO-8601 into UTC nanoseconds, preserving up to 9 fractional digits."""
    match = _ISO_RE.match(value.strip())
    if match is None:
        raise ValueError(f"invalid ISO-8601 timestamp: {value!r}")

    base = datetime.strptime(
        match.group("base").replace(" ", "T"), "%Y-%m-%dT%H:%M:%S"
    ).replace(tzinfo=UTC)

    tz = match.group("tz")
    if tz is not None and tz != "Z":
        sign = 1 if tz[0] == "+" else -1
        digits = tz[1:].replace(":", "")
        base -= sign * timedelta(hours=int(digits[:2]), minutes=int(digits[2:]))

    fraction = match.group("frac") or ""
    nanos = int(fraction.ljust(9, "0")) if fraction else 0
    return datetime_to_ns(base) + nanos


def ns_to_datetime(value: int) -> datetime:
    """Convert nanoseconds to a UTC datetime (microsecond precision)."""
    seconds, nanos = divmod(value, NANOS_PER_SECOND)
    return datetime.fromtimestamp(seconds, tz=UTC).replace(
        microsecond=nanos // NANOS_PER_MICROSECOND
    )


def ns_to_iso(value: int) -> str:
    """Render nanoseconds as ISO-8601 with exactly nine fractional digits."""
    seconds, nanos = divmod(value, NANOS_PER_SECOND)
    base = datetime.fromtimestamp(seconds, tz=UTC)
    return f"{base:%Y-%m-%dT%H:%M:%S}.{nanos:09d}Z"


def to_ns(value: Timestamp) -> int:
    """Normalize a public timestamp argument to UTC nanoseconds.

    Accepts ``datetime`` (naive means UTC), ``int`` nanoseconds and ISO-8601
    strings with an optional offset and 1-9 fractional digits.
    """
    if isinstance(value, bool):
        raise TypeError("bool is not a timestamp")
    if isinstance(value, int):
        return value
    if isinstance(value, datetime):
        return datetime_to_ns(value)
    if isinstance(value, str):
        return iso_to_ns(value)
    raise TypeError(
        "timestamp must be datetime, int nanoseconds or an ISO-8601 string, "
        f"not {type(value).__name__}"
    )


def normalize_ts(value: Timestamp) -> int:
    """Alias kept for callers that prefer the verb used in the docs."""
    return to_ns(value)
