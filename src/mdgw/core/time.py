"""UTC nanosecond time helpers.

`datetime` only carries microsecond precision. Bars are minute-aligned, so the
value is exact today; conversion to and from int nanoseconds is explicit so a
future Data Type with finer timestamps never has to go through floats.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
NANOS_PER_SECOND = 1_000_000_000

_ISO_RE = re.compile(
    r"^(?P<base>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<frac>\d{1,9}))?"
    r"(?P<tz>Z|[+-]\d{2}:?\d{2})?$"
)


def to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        msg = "naive datetime is not allowed; timestamps must be UTC"
        raise ValueError(msg)
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


def iso_to_ns(value: str) -> int:
    """Parse ISO-8601 into UTC nanoseconds without going through a float.

    Accepts an optional 1-9 digit fraction and an optional offset (`Z`,
    `+08:00`, `+0800`). A value without an offset is interpreted as UTC.
    """
    match = _ISO_RE.match(value.strip())
    if match is None:
        msg = f"invalid ISO-8601 timestamp: {value!r}"
        raise ValueError(msg)

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
