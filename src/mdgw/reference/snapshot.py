"""Instrument Snapshot export and the read-only InstrumentStore.

The YAML truth source is validated once (`mdgw.reference.instruments`) and
materialized as a deterministic, immutable Parquet Instrument Snapshot. Every
Service Role and the REST API read the snapshot; they never assemble business
data straight from YAML.

The snapshot name is `snapshot-<config_hash>.parquet`; the same YAML content
always reuses the same file, and a YAML change produces a new file instead of
mutating the old one.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from mdgw.core.envelope import decimal_to_str
from mdgw.core.instrument import Instrument
from mdgw.core.schema import INSTRUMENT_SCHEMA
from mdgw.core.time import ns_to_iso
from mdgw.reference.instruments import InstrumentReference, InstrumentReferenceError

SNAPSHOT_PREFIX = "snapshot-"
SNAPSHOT_SUFFIX = ".parquet"
TIMESTAMP_FIELDS = ("listing_ts", "delisted_ts")


class InstrumentSnapshotError(InstrumentReferenceError):
    """The Instrument Snapshot could not be written or read safely."""


@dataclass(frozen=True, slots=True)
class SnapshotExport:
    """Result of `export_instrument_snapshot`."""

    path: Path
    config_hash: str
    created: bool


def instruments_to_table(instruments: tuple[Instrument, ...]) -> pa.Table:
    """Build the canonical Instrument table, never passing decimals via float."""
    rows = [
        {
            "instrument_id": instrument.instrument_id,
            "venue": instrument.venue,
            "symbol": instrument.symbol,
            "asset_class": instrument.asset_class,
            "instrument_type": instrument.instrument_type,
            "base": instrument.base,
            "quote": instrument.quote,
            "tick_size": instrument.tick_size,
            "lot_size": instrument.lot_size,
            "multiplier": instrument.multiplier,
            "timezone": instrument.timezone,
            "listing_ts": instrument.listing_ts_ns,
            "delisted_ts": instrument.delisted_ts_ns,
        }
        for instrument in instruments
    ]
    return pa.Table.from_pylist(rows, schema=INSTRUMENT_SCHEMA)


def export_instrument_snapshot(
    reference: InstrumentReference, directory: Path
) -> SnapshotExport:
    """Write `snapshot-<config_hash>.parquet`, reusing an identical snapshot.

    The Parquet file is written to a temporary file in the same directory and
    atomically replaced into place, so readers never observe a partial file.
    """
    directory = Path(directory)
    path = directory / f"{SNAPSHOT_PREFIX}{reference.config_hash}{SNAPSHOT_SUFFIX}"

    if path.exists():
        _validate_snapshot_schema(path)
        return SnapshotExport(
            path=path, config_hash=reference.config_hash, created=False
        )

    try:
        directory.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        message = f"cannot create Instrument Snapshot directory {directory}: {exc}"
        raise InstrumentSnapshotError(message) from exc

    temporary = directory / f".{path.name}.{uuid4().hex}.tmp"
    try:
        pq.write_table(
            instruments_to_table(reference.instruments),
            temporary,
            version="2.6",
            use_deprecated_int96_timestamps=False,
            compression="zstd",
        )
        temporary.replace(path)
    except Exception as exc:
        temporary.unlink(missing_ok=True)
        message = f"failed to write Instrument Snapshot {path}: {exc}"
        raise InstrumentSnapshotError(message) from exc

    return SnapshotExport(path=path, config_hash=reference.config_hash, created=True)


def find_instrument_snapshot(data_root: Path) -> Path | None:
    """Return the newest Instrument Snapshot under `<data_root>/instruments`.

    Every process reads the same exported snapshot directory; `None` means the
    collector has not exported one yet. Snapshots are immutable and named by
    config hash, so the newest mtime is the one the collector last published.
    """
    directory = Path(data_root) / "instruments"
    if not directory.exists():
        return None
    candidates = sorted(
        directory.glob(f"{SNAPSHOT_PREFIX}*{SNAPSHOT_SUFFIX}"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def load_instrument_store(data_root: Path) -> InstrumentStore | None:
    """Open the newest Instrument Snapshot, or `None` when none exists.

    A malformed snapshot raises `InstrumentSnapshotError` rather than being
    silently ignored: readers must not fall back to unvalidated Instruments.
    """
    path = find_instrument_snapshot(data_root)
    return None if path is None else InstrumentStore(path)


class InstrumentStore:
    """Read-only view over one Instrument Snapshot."""

    def __init__(self, snapshot_path: Path) -> None:
        self.snapshot_path = Path(snapshot_path)
        self._instruments = _read_snapshot(self.snapshot_path)
        self._by_id = {
            instrument.instrument_id: instrument for instrument in self._instruments
        }

    def __len__(self) -> int:
        return len(self._instruments)

    def list(
        self,
        venue: str | None = None,
        asset_class: str | None = None,
        include_delisted: bool = False,
    ) -> list[Instrument]:
        """Instruments matching the filters, ordered by InstrumentId."""
        return [
            instrument
            for instrument in self._instruments
            if (venue is None or instrument.venue == venue)
            and (asset_class is None or instrument.asset_class == asset_class)
            and (include_delisted or not instrument.is_delisted)
        ]

    def get(self, instrument_id: str) -> Instrument | None:
        """Look up one Instrument, including Delisted ones."""
        return self._by_id.get(instrument_id)


def instrument_to_json(instrument: Instrument) -> dict[str, Any]:
    """JSON form used by the REST API: decimals and timestamps as strings."""
    return {
        "instrument_id": instrument.instrument_id,
        "venue": instrument.venue,
        "symbol": instrument.symbol,
        "asset_class": instrument.asset_class,
        "instrument_type": instrument.instrument_type,
        "base": instrument.base,
        "quote": instrument.quote,
        "tick_size": decimal_to_str(instrument.tick_size),
        "lot_size": decimal_to_str(instrument.lot_size),
        "multiplier": decimal_to_str(instrument.multiplier),
        "timezone": instrument.timezone,
        "listing_ts": ns_to_iso(instrument.listing_ts_ns),
        "delisted_ts": (
            ns_to_iso(instrument.delisted_ts_ns)
            if instrument.delisted_ts_ns is not None
            else None
        ),
    }


def _read_snapshot(path: Path) -> list[Instrument]:
    if not path.exists():
        message = f"Instrument Snapshot does not exist: {path}"
        raise InstrumentSnapshotError(message)
    try:
        table = pq.read_table(path)
    except Exception as exc:
        message = f"cannot read Instrument Snapshot {path}: {exc}"
        raise InstrumentSnapshotError(message) from exc

    if table.schema != INSTRUMENT_SCHEMA:
        message = (
            f"Instrument Snapshot {path} schema does not match the Instrument "
            f"Data Contract: {table.schema}"
        )
        raise InstrumentSnapshotError(message)

    listing_ns = table.column("listing_ts").cast(pa.int64()).to_pylist()
    delisted_ns = table.column("delisted_ts").cast(pa.int64()).to_pylist()
    instruments: list[Instrument] = []
    for index, row in enumerate(table.to_pylist()):
        instruments.append(
            Instrument(
                instrument_id=row["instrument_id"],
                venue=row["venue"],
                symbol=row["symbol"],
                asset_class=row["asset_class"],
                instrument_type=row["instrument_type"],
                base=row["base"],
                quote=row["quote"],
                tick_size=_as_decimal(row["tick_size"]),
                lot_size=_as_decimal(row["lot_size"]),
                multiplier=_as_decimal(row["multiplier"]),
                timezone=row["timezone"],
                listing_ts_ns=int(listing_ns[index]),
                delisted_ts_ns=(
                    int(delisted_ns[index])
                    if delisted_ns[index] is not None
                    else None
                ),
            )
        )
    instruments.sort(key=lambda item: item.instrument_id)
    return instruments


def _as_decimal(value: Any) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _validate_snapshot_schema(path: Path) -> None:
    try:
        schema = pq.read_schema(path)
    except Exception as exc:
        message = f"cannot read existing Instrument Snapshot {path}: {exc}"
        raise InstrumentSnapshotError(message) from exc
    if schema != INSTRUMENT_SCHEMA:
        message = (
            f"existing Instrument Snapshot {path} schema does not match the "
            f"Instrument Data Contract: {schema}"
        )
        raise InstrumentSnapshotError(message)
