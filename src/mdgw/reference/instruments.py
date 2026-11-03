"""Instrument Reference Data: the YAML truth source and its validation.

`config/instruments/*.yaml` is human-maintained reference data. It is never
read by the API or replay at request time; the Gateway validates it once and
exports an Instrument Snapshot (see `mdgw.reference.snapshot`).

Validation happens before any Parquet is written. Errors name the file and the
entry so a bad reviewable diff fails loudly instead of quietly producing wrong
metadata.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml

from mdgw.core.instrument import Instrument, InstrumentId
from mdgw.core.time import iso_to_ns

REQUIRED_FIELDS: tuple[str, ...] = (
    "instrument_id",
    "venue",
    "symbol",
    "asset_class",
    "instrument_type",
    "base",
    "quote",
    "tick_size",
    "lot_size",
    "multiplier",
    "timezone",
    "listing_ts",
)
OPTIONAL_FIELDS: tuple[str, ...] = ("delisted_ts",)
KNOWN_FIELDS: frozenset[str] = frozenset(REQUIRED_FIELDS + OPTIONAL_FIELDS)
DECIMAL_FIELDS: tuple[str, ...] = ("tick_size", "lot_size", "multiplier")
STRING_FIELDS: tuple[str, ...] = (
    "instrument_id",
    "venue",
    "symbol",
    "asset_class",
    "instrument_type",
    "base",
    "quote",
    "timezone",
)
DECIMAL_SCALE = 18
DECIMAL_INTEGER_DIGITS = 20


class InstrumentReferenceError(ValueError):
    """Instrument Reference Data failed validation or snapshot export."""


@dataclass(frozen=True, slots=True)
class InstrumentReference:
    """Validated Instrument Reference Data plus the hash of its YAML content."""

    instruments: tuple[Instrument, ...]
    config_hash: str


def load_instrument_reference(directory: Path) -> InstrumentReference:
    """Load and validate every `*.yaml` file in `directory`.

    Files are sorted by name, entries by `instrument_id`, so the same content
    always yields the same Instrument Snapshot hash.
    """
    directory = Path(directory)
    files = _reference_files(directory)

    instruments: list[Instrument] = []
    seen: dict[str, str] = {}
    for path in files:
        for position, raw in enumerate(_read_yaml_list(path), start=1):
            location = f"{path.name}: entry {position}"
            instrument = _validate_entry(raw, location)
            previous = seen.get(instrument.instrument_id)
            if previous is not None:
                message = (
                    f"{location}: duplicate instrument_id "
                    f"{instrument.instrument_id!r} (already defined at {previous})"
                )
                raise InstrumentReferenceError(message)
            seen[instrument.instrument_id] = location
            instruments.append(instrument)

    instruments.sort(key=lambda item: item.instrument_id)
    return InstrumentReference(
        instruments=tuple(instruments),
        config_hash=_config_hash(files),
    )


def _reference_files(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    if not directory.is_dir():
        message = f"Instrument Reference Data path is not a directory: {directory}"
        raise InstrumentReferenceError(message)
    return sorted(
        (path for path in directory.iterdir() if path.suffix in {".yaml", ".yml"}),
        key=lambda path: path.name,
    )


def _config_hash(files: list[Path]) -> str:
    """Deterministic sha256 over sorted file names and their exact bytes."""
    digest = hashlib.sha256()
    for path in files:
        data = path.read_bytes()
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(len(data)).encode("ascii"))
        digest.update(b"\0")
        digest.update(data)
        digest.update(b"\0")
    return digest.hexdigest()


def _read_yaml_list(path: Path) -> list[Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        message = f"{path.name}: cannot read file: {exc}"
        raise InstrumentReferenceError(message) from exc

    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        message = f"{path.name}: invalid YAML: {exc}"
        raise InstrumentReferenceError(message) from exc

    if not isinstance(document, list):
        message = f"{path.name}: document must be a list of Instrument mappings"
        raise InstrumentReferenceError(message)
    return document


def _validate_entry(raw: Any, location: str) -> Instrument:
    if not isinstance(raw, Mapping):
        message = f"{location}: entry must be an Instrument mapping"
        raise InstrumentReferenceError(message)
    entry = dict(raw)

    unknown = sorted(set(entry) - KNOWN_FIELDS)
    if unknown:
        message = f"{location}: unknown field(s): {', '.join(unknown)}"
        raise InstrumentReferenceError(message)
    missing = [field for field in REQUIRED_FIELDS if field not in entry]
    if missing:
        if len(missing) == 1:
            detail = f"missing required field {missing[0]!r}"
        else:
            detail = f"missing required field(s): {', '.join(missing)}"
        message = f"{location}: {detail}"
        raise InstrumentReferenceError(message)

    strings = {
        field: _require_string(entry[field], field, location)
        for field in STRING_FIELDS
    }
    instrument_id = strings["instrument_id"]
    try:
        parsed = InstrumentId.parse(instrument_id)
    except ValueError as exc:
        message = (
            f"{location}: instrument_id {instrument_id!r} is not a valid "
            "InstrumentId (<venue>:<symbol>)"
        )
        raise InstrumentReferenceError(message) from exc

    expected_id = f"{strings['venue']}:{strings['symbol']}"
    if instrument_id != expected_id:
        message = (
            f"{location}: instrument_id {instrument_id!r} does not match "
            f"venue:symbol {expected_id!r}"
        )
        raise InstrumentReferenceError(message)
    if parsed.venue != strings["venue"] or parsed.symbol != strings["symbol"]:
        message = (
            f"{location}: instrument_id {instrument_id!r} does not match "
            f"venue:symbol {expected_id!r}"
        )
        raise InstrumentReferenceError(message)

    decimals = {
        field: _parse_decimal(entry[field], field, location)
        for field in DECIMAL_FIELDS
    }
    listing_ts_ns = _parse_timestamp(entry["listing_ts"], "listing_ts", location)

    delisted_ts_ns: int | None = None
    if entry.get("delisted_ts") is not None:
        delisted_ts_ns = _parse_timestamp(
            entry["delisted_ts"], "delisted_ts", location
        )
        if delisted_ts_ns < listing_ts_ns:
            message = f"{location}: delisted_ts must not be before listing_ts"
            raise InstrumentReferenceError(message)

    return Instrument(
        instrument_id=instrument_id,
        venue=strings["venue"],
        symbol=strings["symbol"],
        asset_class=strings["asset_class"],
        instrument_type=strings["instrument_type"],
        base=strings["base"],
        quote=strings["quote"],
        tick_size=decimals["tick_size"],
        lot_size=decimals["lot_size"],
        multiplier=decimals["multiplier"],
        timezone=strings["timezone"],
        listing_ts_ns=listing_ts_ns,
        delisted_ts_ns=delisted_ts_ns,
    )


def _require_string(value: Any, field: str, location: str) -> str:
    if not isinstance(value, str) or not value.strip():
        message = f"{location}: field {field!r} must be a non-empty string"
        raise InstrumentReferenceError(message)
    return value


def _parse_decimal(value: Any, field: str, location: str) -> Decimal:
    """Parse a decimal field without ever routing the value through a float."""
    if isinstance(value, (bool, float)):
        message = (
            f"{location}: field {field!r} must not be a float; quote it as a string"
        )
        raise InstrumentReferenceError(message)
    if isinstance(value, int):
        text = str(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            message = (
                f"{location}: field {field!r} must be a non-empty decimal string"
            )
            raise InstrumentReferenceError(message)
    else:
        message = (
            f"{location}: field {field!r} must be a string or integer, "
            f"not {type(value).__name__}"
        )
        raise InstrumentReferenceError(message)

    try:
        number = Decimal(text)
    except InvalidOperation as exc:
        message = f"{location}: field {field!r} is not a valid decimal: {value!r}"
        raise InstrumentReferenceError(message) from exc
    if not number.is_finite():
        message = f"{location}: field {field!r} must be a finite decimal"
        raise InstrumentReferenceError(message)

    exponent = number.as_tuple().exponent
    integer_digits = len(number.as_tuple().digits) + int(exponent)
    if int(exponent) < -DECIMAL_SCALE or integer_digits > DECIMAL_INTEGER_DIGITS:
        message = (
            f"{location}: field {field!r} does not fit decimal128(38, 18): {value!r}"
        )
        raise InstrumentReferenceError(message)
    return number


def _parse_timestamp(value: Any, field: str, location: str) -> int:
    if not isinstance(value, str) or isinstance(value, bool):
        message = f"{location}: field {field!r} must be an ISO-8601 string"
        raise InstrumentReferenceError(message)
    try:
        return iso_to_ns(value)
    except ValueError as exc:
        message = (
            f"{location}: field {field!r} is not a valid ISO-8601 timestamp: "
            f"{value!r}"
        )
        raise InstrumentReferenceError(message) from exc
