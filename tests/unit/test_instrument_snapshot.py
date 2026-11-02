"""Instrument Snapshot export and the read-only InstrumentStore."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mdgw.core.instrument import Instrument
from mdgw.core.schema import INSTRUMENT_SCHEMA
from mdgw.reference import (
    InstrumentSnapshotError,
    InstrumentStore,
    export_instrument_snapshot,
    load_instrument_reference,
)

YAML = """
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
- instrument_id: "binance:ETH/USDT"
  venue: binance
  symbol: "ETH/USDT"
  asset_class: crypto
  instrument_type: spot
  base: ETH
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.0001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
- instrument_id: "binance:OLD/USDT"
  venue: binance
  symbol: "OLD/USDT"
  asset_class: crypto
  instrument_type: spot
  base: OLD
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.0001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
  delisted_ts: "2020-01-01T00:00:00Z"
""".strip()


def _write_yaml(directory: Path, text: str = YAML) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "binance.yaml").write_text(text + "\n", encoding="utf-8")
    return directory


def _reference(tmp_path: Path):
    return load_instrument_reference(_write_yaml(tmp_path / "instruments"))


def test_snapshot_path_is_derived_from_the_content_hash(tmp_path: Path) -> None:
    reference = _reference(tmp_path)

    export = export_instrument_snapshot(reference, tmp_path / "data" / "instruments")

    assert export.created is True
    assert export.path.name == f"snapshot-{reference.config_hash}.parquet"
    assert export.path.parent == tmp_path / "data" / "instruments"


def test_snapshot_uses_the_instrument_contract_and_nanosecond_timestamps(
    tmp_path: Path,
) -> None:
    reference = _reference(tmp_path)

    export = export_instrument_snapshot(reference, tmp_path / "instruments")

    table = pq.read_table(export.path)
    assert table.schema == INSTRUMENT_SCHEMA
    assert table.num_rows == 3
    assert [row["instrument_id"] for row in table.to_pylist()] == [
        "binance:BTC/USDT",
        "binance:ETH/USDT",
        "binance:OLD/USDT",
    ]
    assert table.column("tick_size").to_pylist()[0] == Decimal("0.010000000000000000")

    metadata = pq.ParquetFile(export.path).metadata
    assert metadata.format_version == "2.6"
    column = metadata.row_group(0).column(0)
    assert column.compression == "ZSTD"


def test_snapshot_is_written_atomically_without_leftover_temp_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reference = _reference(tmp_path)
    directory = tmp_path / "instruments"

    def boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(pq, "write_table", boom)
    with pytest.raises(InstrumentSnapshotError, match="failed to write"):
        export_instrument_snapshot(reference, directory)

    assert list(directory.glob("*.parquet")) == []
    assert list(directory.glob("*.tmp")) == []


def test_successful_export_leaves_only_the_snapshot(tmp_path: Path) -> None:
    export = export_instrument_snapshot(_reference(tmp_path), tmp_path / "data")

    assert [path.name for path in export.path.parent.iterdir()] == [export.path.name]


def test_same_hash_is_reused_without_rewriting(tmp_path: Path) -> None:
    reference = _reference(tmp_path)
    directory = tmp_path / "instruments"

    first = export_instrument_snapshot(reference, directory)
    before = first.path.stat().st_mtime_ns
    second = export_instrument_snapshot(reference, directory)

    assert second.path == first.path
    assert second.created is False
    assert second.path.stat().st_mtime_ns == before


def test_changed_yaml_produces_a_new_snapshot(tmp_path: Path) -> None:
    source = _write_yaml(tmp_path / "instruments")
    first = export_instrument_snapshot(
        load_instrument_reference(source), tmp_path / "data"
    )

    source = _write_yaml(
        source, YAML.replace('tick_size: "0.01"', 'tick_size: "0.001"')
    )
    second = export_instrument_snapshot(
        load_instrument_reference(source), tmp_path / "data"
    )

    assert first.config_hash != second.config_hash
    assert second.created is True
    assert first.path.exists() and second.path.exists()


def test_empty_reference_exports_an_empty_snapshot(tmp_path: Path) -> None:
    (tmp_path / "instruments").mkdir()
    reference = load_instrument_reference(tmp_path / "instruments")

    export = export_instrument_snapshot(reference, tmp_path / "data")

    table = pq.read_table(export.path)
    assert table.schema == INSTRUMENT_SCHEMA
    assert table.num_rows == 0


def test_store_lists_and_filters_instruments(tmp_path: Path) -> None:
    export = export_instrument_snapshot(_reference(tmp_path), tmp_path / "data")
    store = InstrumentStore(export.path)

    assert len(store) == 3
    assert [item.instrument_id for item in store.list()] == [
        "binance:BTC/USDT",
        "binance:ETH/USDT"
    ]

    assert [item.instrument_id for item in store.list(venue="binance")] == [
        "binance:BTC/USDT",
        "binance:ETH/USDT",
    ]
    assert store.list(venue="ctp") == []
    assert [item.instrument_id for item in store.list(asset_class="crypto")] == [
        "binance:BTC/USDT",
        "binance:ETH/USDT",
    ]
    assert store.list(asset_class="equity") == []


def test_store_hides_delisted_instruments_by_default(tmp_path: Path) -> None:
    export = export_instrument_snapshot(_reference(tmp_path), tmp_path / "data")
    store = InstrumentStore(export.path)

    assert "binance:OLD/USDT" not in [
        item.instrument_id for item in store.list()
    ]
    assert "binance:OLD/USDT" in [
        item.instrument_id for item in store.list(include_delisted=True)
    ]


def test_store_gets_delisted_and_unknown_instruments(tmp_path: Path) -> None:
    export = export_instrument_snapshot(_reference(tmp_path), tmp_path / "data")
    store = InstrumentStore(export.path)

    delisted = store.get("binance:OLD/USDT")
    assert delisted is not None
    assert delisted.is_delisted is True
    assert delisted.delisted_ts_ns == 1_577_836_800_000_000_000
    assert store.get("binance:MISSING") is None


def test_store_rejects_a_snapshot_with_the_wrong_schema(tmp_path: Path) -> None:
    path = tmp_path / "snapshot-wrong.parquet"
    pq.write_table(
        pa.table({"instrument_id": pa.array(["binance:BTC/USDT"])}), path
    )

    with pytest.raises(InstrumentSnapshotError, match="schema"):
        InstrumentStore(path)


def test_store_reads_an_empty_snapshot(tmp_path: Path) -> None:
    (tmp_path / "instruments").mkdir()
    reference = load_instrument_reference(tmp_path / "instruments")
    export = export_instrument_snapshot(reference, tmp_path / "data")

    store = InstrumentStore(export.path)

    assert len(store) == 0
    assert store.list() == []
    assert store.get("binance:BTC/USDT") is None
