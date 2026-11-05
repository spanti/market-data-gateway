"""Polars batch Normalization at the Collector ingest boundary.

Raw Bars are validated and normalized once per ``CollectorCore.ingest()``
batch, never one DataFrame per message. Values enter Polars through typed
Arrow columns (``decimal128(38,18)`` and ``timestamp[ns, tz=UTC]``), because
both Arrow-typed and untyped decimal paths silently rescale over-scale input.
This module therefore rejects float-shaped values and over-scale decimals
before they can reach a Polars Decimal Series, and lets Polars compute the
Instrument, Interval and closed-Bar rules for the whole batch.

This is the single production Normalization rule source: live Collector
batches, Backfill and archive import all call ``normalize_batch``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

import polars as pl
import pyarrow as pa

from mdgw.core.bar import DATA_TYPE, SCHEMA_VERSION, Bar
from mdgw.core.schema import INTERVALS
from mdgw.core.time import datetime_to_ns, ns_to_datetime, ns_to_iso, to_utc
from mdgw.quarantine import (
    REASON_DECIMAL_SCALE_EXCEEDED,
    REASON_FLOAT_VALUE,
    REASON_INVALID_INSTRUMENT,
    REASON_INVALID_INTERVAL,
    REASON_MALFORMED_ROW,
    REASON_UNCLOSED_BAR,
    SOURCE_STAGE_LIVE_NORMALIZE,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mdgw.core.bar import RawBar

# Data Contract price/quantity scale: decimal128(38, 18).
MAX_SCALE = 18
MAX_INTEGER_DIGITS = 38 - MAX_SCALE
DECIMAL_FIELDS: tuple[str, ...] = ("open", "high", "low", "close", "volume")
_INSTRUMENT_PATTERN = r"(?s)^[^:]+:.+$"
_INTERVAL_NS = {
    interval: (
        delta.days * 86_400 * 1_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )
    for interval, delta in INTERVALS.items()
}


@dataclass(frozen=True, slots=True)
class BatchRejection:
    """One Raw Bar rejected during Normalization."""

    row_index: int
    source_stage: str
    reason_code: str
    details: dict[str, Any]
    raw: RawBar
    instrument_id: str | None
    interval: str | None
    open_ts: datetime | None


@dataclass(frozen=True, slots=True)
class NormalizedRow:
    """One accepted batch row and the Bar it became."""

    row_index: int
    raw: RawBar
    bar: Bar


@dataclass(frozen=True, slots=True)
class BatchNormalization:
    """Result of one batch Normalization pass."""

    rows: tuple[NormalizedRow, ...]
    rejections: tuple[BatchRejection, ...]

    @property
    def bars(self) -> list[Bar]:
        return [row.bar for row in self.rows]


class BatchNormalizationError(ValueError):
    """The batch contained at least one Normalization rejection."""

    def __init__(self, rejections: Sequence[BatchRejection]) -> None:
        self.rejections = tuple(rejections)
        self.reason_codes = tuple(
            sorted({rejection.reason_code for rejection in self.rejections})
        )
        messages = [_rejection_message(rejection) for rejection in self.rejections]
        if len(messages) == 1:
            msg = messages[0]
        else:
            reasons = ", ".join(self.reason_codes) or "unknown"
            msg = (
                f"batch normalization rejected {len(self.rejections)} row(s): "
                f"{reasons}. First error: {messages[0] if messages else 'unknown'}"
            )
        super().__init__(msg)


def _rejection_message(rejection: BatchRejection) -> str:
    """Legacy-readable message; the Reason Code stays the stable contract."""
    details = rejection.details
    field = details.get("field", "value")
    if rejection.reason_code == REASON_UNCLOSED_BAR:
        message = (
            "Bar is not closed: "
            f"close_ts={details.get('close_ts')} > "
            f"ingest_ts={details.get('ingest_ts')}"
        )
    elif rejection.reason_code == REASON_FLOAT_VALUE:
        message = f"{field} must be Decimal, str or int; floats are forbidden"
    elif rejection.reason_code == REASON_DECIMAL_SCALE_EXCEEDED:
        if "scale" in details:
            message = (
                f"{field} has more than {details.get('max_scale')} decimal places"
            )
        else:
            message = (
                f"{field} has more than {details.get('max_integer_digits')} "
                "integer digits"
            )
    elif rejection.reason_code == REASON_INVALID_INTERVAL:
        message = f"invalid Bar Interval: {details.get('interval')!r}"
    elif rejection.reason_code == REASON_INVALID_INSTRUMENT:
        message = f"invalid InstrumentId: {details.get('instrument_id')!r}"
    else:
        message = f"malformed row: field {details.get('field', 'unknown')}"
    return message


@dataclass(slots=True)
class _RowIssues:
    """Type-level problems found while building the typed Arrow columns."""

    instrument: tuple[str, dict[str, Any]] | None = None
    interval: tuple[str, dict[str, Any]] | None = None
    timestamp: tuple[str, dict[str, Any]] | None = None
    decimal: tuple[str, dict[str, Any]] | None = None
    other: tuple[str, dict[str, Any]] | None = None


def _supplied_text(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _readable_datetime(value: object) -> datetime | None:
    """Return a UTC-convertible datetime, or None when it is not readable."""
    if not isinstance(value, datetime):
        return None
    try:
        return to_utc(value)
    except (ValueError, OverflowError, OSError):
        return None


def _coerce_decimal(
    value: object,
) -> tuple[Decimal | None, tuple[str, dict[str, Any]] | None]:
    """Coerce exact input to Decimal, rejecting float-shaped values first."""
    if isinstance(value, bool):
        return None, (
            REASON_MALFORMED_ROW,
            {"field": "value", "problem": "boolean is not a decimal"},
        )
    if isinstance(value, Decimal):
        return value, None
    if isinstance(value, (str, int)):
        try:
            return Decimal(value), None
        except (InvalidOperation, ValueError, TypeError):
            return None, (
                REASON_MALFORMED_ROW,
                {"field": "value", "problem": "not a decimal"},
            )
    if isinstance(value, float):
        # Arrow would happily turn this into a rounded Decimal; the scalar
        # rule forbids floats outright, so reject before building the column.
        return None, (
            REASON_FLOAT_VALUE,
            {"field": "value", "problem": "float-shaped value"},
        )
    return None, (
        REASON_MALFORMED_ROW,
        {"field": "value", "problem": f"unsupported type {type(value).__name__}"},
    )


def _decimal_issue(
    value: object,
) -> tuple[Decimal | None, tuple[str, dict[str, Any]] | None]:
    """Validate one decimal field without ever creating a float or rounding."""
    result, issue = _coerce_decimal(value)
    if issue is not None:
        return None, issue
    assert result is not None
    if not result.is_finite():
        return None, (
            REASON_MALFORMED_ROW,
            {"field": "value", "problem": "non-finite decimal"},
        )
    exponent = result.as_tuple().exponent
    if exponent < -MAX_SCALE:
        return None, (
            REASON_DECIMAL_SCALE_EXCEEDED,
            {
                "field": "value",
                "scale": -int(exponent),
                "max_scale": MAX_SCALE,
            },
        )
    if result != 0 and result.adjusted() >= MAX_INTEGER_DIGITS:
        return None, (
            REASON_DECIMAL_SCALE_EXCEEDED,
            {
                "field": "value",
                "integer_digits": result.adjusted() + 1,
                "max_integer_digits": MAX_INTEGER_DIGITS,
            },
        )
    return result, None


def _optional_integer(
    value: object, field: str
) -> tuple[int | None, tuple[str, dict[str, Any]] | None]:
    if value is None:
        return None, None
    if isinstance(value, int):
        result = int(value)
        if -(2**63) <= result < 2**63:
            return result, None
    return None, (
        REASON_MALFORMED_ROW,
        {"field": field, "problem": "not an int64"},
    )


def _classify_row(raw: RawBar) -> tuple[dict[str, Any], _RowIssues]:
    """Build typed, Arrow-compatible column values plus type-level issues."""
    issues = _RowIssues()

    instrument_value = _supplied_text(raw.instrument_id)
    if instrument_value is None:
        issues.instrument = (
            REASON_MALFORMED_ROW,
            {"field": "instrument_id", "problem": "not a string"},
        )

    interval_value = _supplied_text(raw.interval)
    if interval_value is None:
        # The scalar rule reports every non-Interval value as invalid_interval.
        issues.interval = (
            REASON_INVALID_INTERVAL,
            {"interval": str(raw.interval)[:128]},
        )

    open_ns: int | None = None
    if isinstance(raw.open_ts, datetime):
        try:
            open_ns = datetime_to_ns(to_utc(raw.open_ts))
        except (ValueError, OverflowError, OSError):
            issues.timestamp = (
                REASON_MALFORMED_ROW,
                {"field": "open_ts", "problem": "timestamp is not UTC-convertible"},
            )
    else:
        issues.timestamp = (
            REASON_MALFORMED_ROW,
            {"field": "open_ts", "problem": "not a datetime"},
        )

    decimals: dict[str, Decimal | None] = {}
    for field in DECIMAL_FIELDS:
        value, issue = _decimal_issue(getattr(raw, field))
        if issue is not None:
            reason, details = issue
            details = {**details, "field": field}
            if issues.decimal is None:
                issues.decimal = (reason, details)
            decimals[field] = None
        else:
            decimals[field] = value

    trade_count, trade_issue = _optional_integer(raw.trade_count, "trade_count")
    revision, revision_issue = _optional_integer(raw.revision, "revision")
    if issues.other is None:
        if trade_issue is not None:
            issues.other = trade_issue
        elif revision_issue is not None:
            issues.other = revision_issue

    row = {
        "instrument_id": instrument_value,
        "interval": interval_value,
        "open_ts": open_ns,
        "open": decimals["open"],
        "high": decimals["high"],
        "low": decimals["low"],
        "close": decimals["close"],
        "volume": decimals["volume"],
        "trade_count": trade_count,
        "revision": revision,
    }
    return row, issues


def _typed_table(rows: Sequence[dict[str, Any]]) -> pa.Table:
    """Build the typed columns the Polars rules actually read.

    Decimals are validated row-by-row before this point and are not used by
    any Polars expression, so they never enter a Decimal Series. Keeping this
    table narrow is what makes batch Normalization cheaper than the deleted
    scalar path for large Backfill/archive batches.
    """
    return pa.table(
        {
            "instrument_id": pa.array(
                [row["instrument_id"] for row in rows], type=pa.string()
            ),
            "interval": pa.array(
                [row["interval"] for row in rows], type=pa.string()
            ),
            "open_ns": pa.array(
                [row["open_ts"] for row in rows], type=pa.int64()
            ),
        }
    )


def _rejection(
    row_index: int,
    raw: RawBar,
    reason_code: str,
    details: dict[str, Any],
) -> BatchRejection:
    return BatchRejection(
        row_index=row_index,
        source_stage=SOURCE_STAGE_LIVE_NORMALIZE,
        reason_code=reason_code,
        details=details,
        raw=raw,
        instrument_id=_supplied_text(raw.instrument_id),
        interval=_supplied_text(raw.interval),
        open_ts=_readable_datetime(raw.open_ts),
    )


def _unclosed_details(open_ns: int, close_ns: int, ingest_ns: int) -> dict[str, Any]:
    return {
        "open_ts": ns_to_iso(open_ns),
        "close_ts": ns_to_iso(close_ns),
        "ingest_ts": ns_to_iso(ingest_ns),
    }


def normalize_batch(  # ruff: ignore[too-many-locals] - one explicit pass
    raws: Sequence[RawBar],
    *,
    feed: str,
    ingest_ts: datetime,
) -> BatchNormalization:
    """Normalize one Collector batch through a typed Arrow/Polars pipeline.

    Valid rows become Bars; every rejected row carries a stable Quarantine
    Reason Code and Source Stage. Batch atomicity is the caller's decision
    (``CollectorCore`` rejects the whole batch when any rejection exists).
    """
    if not raws:
        return BatchNormalization(rows=(), rejections=())

    ingest_utc = to_utc(ingest_ts)
    ingest_ns = datetime_to_ns(ingest_utc)

    column_rows: list[dict[str, Any]] = []
    issues_by_row: list[_RowIssues] = []
    for raw in raws:
        row, issues = _classify_row(raw)
        column_rows.append(row)
        issues_by_row.append(issues)

    frame = (
        pl.from_arrow(_typed_table(column_rows))
        .with_columns(
            pl.col("instrument_id")
            .str.contains(_INSTRUMENT_PATTERN)
            .fill_null(False)
            .alias("_instrument_ok"),
            pl.col("interval")
            .is_in(list(INTERVALS))
            .fill_null(False)
            .alias("_interval_ok"),
            pl.col("open_ns").alias("_open_ns"),
            pl.col("interval")
            .replace_strict(_INTERVAL_NS, default=None, return_dtype=pl.Int64)
            .alias("_interval_ns"),
        )
        .with_columns(
            (pl.col("_open_ns") + pl.col("_interval_ns")).alias("_close_ns")
        )
        .with_columns(
            (pl.col("_close_ns") <= ingest_ns)
            .fill_null(False)
            .alias("_closed")
        )
    )
    instrument_ok = frame.get_column("_instrument_ok").to_list()
    interval_ok = frame.get_column("_interval_ok").to_list()
    open_ns_values = frame.get_column("_open_ns").to_list()
    close_ns_values = frame.get_column("_close_ns").to_list()
    closed_values = frame.get_column("_closed").to_list()

    normalized: list[NormalizedRow] = []
    rejections: list[BatchRejection] = []
    for row_index, (values, issues) in enumerate(
        zip(column_rows, issues_by_row, strict=True)
    ):
        raw = raws[row_index]
        reason_code: str | None = None
        details: dict[str, Any] = {}

        if issues.instrument is not None:
            reason_code, details = issues.instrument
        elif values["instrument_id"] is not None and not instrument_ok[row_index]:
            reason_code = REASON_INVALID_INSTRUMENT
            details = {"instrument_id": values["instrument_id"][:128]}
        elif issues.interval is not None:
            reason_code, details = issues.interval
        elif values["interval"] is not None and not interval_ok[row_index]:
            reason_code = REASON_INVALID_INTERVAL
            details = {"interval": values["interval"][:128]}
        elif issues.timestamp is not None:
            reason_code, details = issues.timestamp
        elif not closed_values[row_index]:
            reason_code = REASON_UNCLOSED_BAR
            details = _unclosed_details(
                int(open_ns_values[row_index]),
                int(close_ns_values[row_index]),
                ingest_ns,
            )
        elif issues.decimal is not None:
            reason_code, details = issues.decimal
        elif issues.other is not None:
            reason_code, details = issues.other

        if reason_code is not None:
            rejections.append(
                _rejection(row_index, raw, reason_code, details)
            )
            continue

        open_ts = ns_to_datetime(int(open_ns_values[row_index]))
        close_ts = ns_to_datetime(int(close_ns_values[row_index]))
        bar = Bar(
            schema_version=SCHEMA_VERSION,
            data_type=DATA_TYPE,
            instrument_id=str(values["instrument_id"]),
            interval=str(values["interval"]),
            open_ts=open_ts,
            close_ts=close_ts,
            event_ts=close_ts,
            ingest_ts=ingest_utc,
            feed=feed,
            seq=None,
            aggregation_source="external",
            open=values["open"],
            high=values["high"],
            low=values["low"],
            close=values["close"],
            volume=values["volume"],
            trade_count=values["trade_count"],
            revision=int(values["revision"]),
        )
        normalized.append(NormalizedRow(row_index=row_index, raw=raw, bar=bar))

    return BatchNormalization(
        rows=tuple(normalized), rejections=tuple(rejections)
    )


__all__ = [
    "BatchNormalization",
    "BatchNormalizationError",
    "BatchRejection",
    "NormalizedRow",
    "normalize_batch",
]
