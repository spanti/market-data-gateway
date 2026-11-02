"""Instrument Reference Data validation (YAML truth source)."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from mdgw.reference import InstrumentReferenceError, load_instrument_reference

VALID = """
- instrument_id: "binance:BTC/USDT"
  venue: binance
  symbol: "BTC/USDT"
  asset_class: crypto
  instrument_type: spot
  base: BTC
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.00001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
"""


def _write(directory: Path, name: str, text: str) -> Path:
    path = directory / name
    path.write_text(text, encoding="utf-8")
    return path


def test_loads_valid_file_and_parses_decimal_and_time(tmp_path: Path) -> None:
    _write(tmp_path, "binance.yaml", VALID)

    reference = load_instrument_reference(tmp_path)

    assert len(reference.instruments) == 1
    instrument = reference.instruments[0]
    assert instrument.instrument_id == "binance:BTC/USDT"
    assert instrument.tick_size == Decimal("0.01")
    assert instrument.lot_size == Decimal("0.00001")
    assert instrument.multiplier == Decimal("1")
    assert instrument.listing_ts_ns == 1_502_942_400_000_000_000
    assert instrument.delisted_ts_ns is None
    assert len(reference.config_hash) == 64


def test_loads_multiple_files_in_sorted_order(tmp_path: Path) -> None:
    second = VALID.replace("binance:BTC/USDT", "binance:ETH/USDT").replace(
        "symbol: \"BTC/USDT\"", "symbol: \"ETH/USDT\""
    ).replace("base: BTC", "base: ETH")
    _write(tmp_path, "b.yaml", second)
    _write(tmp_path, "a.yaml", VALID)

    reference = load_instrument_reference(tmp_path)

    assert [item.instrument_id for item in reference.instruments] == [
        "binance:BTC/USDT",
        "binance:ETH/USDT",
    ]


def test_integers_are_accepted_for_decimal_fields(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "binance.yaml",
        VALID.replace('multiplier: "1"', "multiplier: 1"),
    )

    reference = load_instrument_reference(tmp_path)

    assert reference.instruments[0].multiplier == Decimal("1")


def test_duplicate_instrument_id_across_files_is_rejected(tmp_path: Path) -> None:
    _write(tmp_path, "a.yaml", VALID)
    _write(tmp_path, "b.yaml", VALID)

    with pytest.raises(InstrumentReferenceError, match="duplicate instrument_id"):
        load_instrument_reference(tmp_path)


def test_duplicate_instrument_id_within_a_file_is_rejected(tmp_path: Path) -> None:
    _write(tmp_path, "a.yaml", VALID + VALID)

    with pytest.raises(InstrumentReferenceError, match="duplicate instrument_id"):
        load_instrument_reference(tmp_path)


@pytest.mark.parametrize(
    "field",
    [
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
    ],
)
def test_missing_required_field_is_rejected(tmp_path: Path, field: str) -> None:
    entry = yaml.safe_load(VALID)[0]
    del entry[field]
    _write(tmp_path, "binance.yaml", yaml.safe_dump([entry], sort_keys=False))

    with pytest.raises(InstrumentReferenceError, match=f"missing required field '{field}'"):
        load_instrument_reference(tmp_path)


def test_float_decimal_field_is_rejected(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "binance.yaml",
        VALID.replace('tick_size: "0.01"', "tick_size: 0.01"),
    )

    with pytest.raises(
        InstrumentReferenceError, match="must not be a float"
    ):
        load_instrument_reference(tmp_path)


def test_invalid_timestamp_is_rejected(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "binance.yaml",
        VALID.replace('listing_ts: "2017-08-17T04:00:00Z"', 'listing_ts: "yesterday"'),
    )

    with pytest.raises(InstrumentReferenceError, match="listing_ts"):
        load_instrument_reference(tmp_path)


def test_instrument_id_must_match_venue_and_symbol(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "binance.yaml",
        VALID.replace('instrument_id: "binance:BTC/USDT"', 'instrument_id: "binance:ETH/USDT"'),
    )

    with pytest.raises(
        InstrumentReferenceError, match="does not match venue:symbol"
    ):
        load_instrument_reference(tmp_path)


def test_instrument_id_must_parse(tmp_path: Path) -> None:
    text = VALID.replace('instrument_id: "binance:BTC/USDT"', 'instrument_id: "BTCUSDT"')
    _write(tmp_path, "binance.yaml", text)

    with pytest.raises(InstrumentReferenceError, match="instrument_id"):
        load_instrument_reference(tmp_path)


def test_delisted_ts_before_listing_ts_is_rejected(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "binance.yaml",
        VALID + '  delisted_ts: "2016-01-01T00:00:00Z"\n',
    )

    with pytest.raises(
        InstrumentReferenceError, match="delisted_ts must not be before"
    ):
        load_instrument_reference(tmp_path)


def test_delisted_ts_equal_to_listing_ts_is_accepted(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "binance.yaml",
        VALID + '  delisted_ts: "2017-08-17T04:00:00Z"\n',
    )

    reference = load_instrument_reference(tmp_path)

    assert reference.instruments[0].delisted_ts_ns is not None


def test_empty_file_is_rejected(tmp_path: Path) -> None:
    _write(tmp_path, "empty.yaml", "")

    with pytest.raises(InstrumentReferenceError, match="must be a list"):
        load_instrument_reference(tmp_path)


def test_non_list_document_is_rejected(tmp_path: Path) -> None:
    _write(tmp_path, "bad.yaml", "instrument_id: binance:BTC/USDT\n")

    with pytest.raises(InstrumentReferenceError, match="must be a list"):
        load_instrument_reference(tmp_path)


def test_non_string_field_is_rejected(tmp_path: Path) -> None:
    _write(tmp_path, "binance.yaml", VALID.replace("venue: binance", "venue: 42"))

    with pytest.raises(InstrumentReferenceError, match="venue"):
        load_instrument_reference(tmp_path)


def test_empty_instrument_directory_is_allowed(tmp_path: Path) -> None:
    reference = load_instrument_reference(tmp_path)

    assert reference.instruments == ()
    assert len(reference.config_hash) == 64


def test_yaml_change_changes_the_hash(tmp_path: Path) -> None:
    _write(tmp_path, "binance.yaml", VALID)
    first = load_instrument_reference(tmp_path).config_hash

    _write(
        tmp_path,
        "binance.yaml",
        VALID.replace('tick_size: "0.01"', 'tick_size: "0.001"'),
    )
    second = load_instrument_reference(tmp_path).config_hash

    assert first != second
