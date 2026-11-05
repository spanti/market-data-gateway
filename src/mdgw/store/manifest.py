"""Manifest and Dataset descriptor helpers for the Parquet Dataset layout.

`_manifest.json` is the authority for which Data Files are readable. Entries
written by earlier tickets may lack the operational fields added in this
ticket, so every entry is normalized from the file system when it is read.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import pyarrow.parquet as pq

DATASET_NAME = "bars"
MANIFEST_NAME = "_manifest.json"
DATASET_DESCRIPTOR_NAME = "_dataset.json"
PARTITION_FIELDS: tuple[str, ...] = (
    "asset_class",
    "venue",
    "symbol",
    "interval",
    "year",
    "month",
)

_FILE_NAME_RE = re.compile(
    r"^data-(?P<start>-?\d+)-(?P<end>-?\d+)-(?P<uuid>[0-9a-fA-F-]+)\.parquet$"
)


def parse_partition(relative_path: str | Path) -> dict[str, str]:
    """Extract Hive `key=value` segments from a Dataset-relative path."""
    partition: dict[str, str] = {}
    for segment in Path(relative_path).parts:
        key, separator, value = segment.partition("=")
        if separator and key in PARTITION_FIELDS:
            partition[key] = value
    return partition


def partition_label(partition: dict[str, Any]) -> str:
    """Human-readable label used in maintenance reports."""
    symbol = unquote(str(partition.get("symbol", "")))
    venue = partition.get("venue", "?")
    interval = partition.get("interval", "?")
    year = partition.get("year", "?")
    month = partition.get("month", "?")
    return f"{venue}:{symbol}:{interval}:{year}-{month}"


def partition_key(partition: dict[str, Any]) -> tuple[str, ...]:
    return tuple(str(partition.get(field, "")) for field in PARTITION_FIELDS)


def instrument_id_for_partition(partition: dict[str, Any]) -> str:
    """Reconstruct the InstrumentId from the encoded `symbol` path segment."""
    return f"{partition.get('venue', '')}:{unquote(str(partition.get('symbol', '')))}"


def _coerce_partition_value(field: str, value: Any) -> str:
    if field == "year":
        try:
            return f"{int(value):04d}"
        except (TypeError, ValueError):
            return str(value)
    if field == "month":
        try:
            return f"{int(value):02d}"
        except (TypeError, ValueError):
            return str(value)
    return str(value)


def normalize_manifest_entry(
    entry: dict[str, Any], dataset_root: Path
) -> dict[str, Any]:
    """Return a Manifest entry with all fields required by this ticket.

    Missing fields from ticket 01/02 Manifests are derived from the file name
    and from `stat`; this is a read-path compatibility rule, not an error.
    """
    dataset_root = Path(dataset_root)
    path_value = str(entry["path"])
    absolute = dataset_root / path_value

    partition = entry.get("partition") or parse_partition(path_value)
    partition = {
        field: _coerce_partition_value(field, partition[field])
        for field in PARTITION_FIELDS
        if field in partition
    }

    filename_match = _FILE_NAME_RE.match(Path(path_value).name)
    start_ns = entry.get("start_ns")
    end_ns = entry.get("end_ns")
    if start_ns is None and filename_match is not None:
        start_ns = int(filename_match.group("start"))
    if end_ns is None and filename_match is not None:
        end_ns = int(filename_match.group("end"))

    size_bytes = entry.get("size_bytes")
    created_ns = entry.get("created_ns")
    if absolute.exists():
        stat = absolute.stat()
        if size_bytes is None:
            size_bytes = stat.st_size
        if created_ns is None:
            created_ns = stat.st_mtime_ns

    row_count = entry.get("row_count")
    if row_count is None and absolute.exists():
        try:
            row_count = pq.ParquetFile(absolute).metadata.num_rows
        except (OSError, ValueError):
            row_count = 0

    normalized = dict(entry)
    normalized.update(
        {
            "path": path_value,
            "start_ns": int(start_ns or 0),
            "end_ns": int(end_ns or 0),
            "row_count": int(row_count or 0),
            "active": bool(entry.get("active", True)),
            "size_bytes": int(size_bytes or 0),
            "created_ns": int(created_ns or 0),
            "partition": partition,
            "max_ingest_ns": int(entry.get("max_ingest_ns") or end_ns or 0),
        }
    )
    return normalized


def read_manifest(dataset_root: Path) -> dict[str, Any]:
    path = Path(dataset_root) / MANIFEST_NAME
    if not path.exists():
        return {"schema_version": 1, "data_type": "bar", "files": []}
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest.setdefault("files", [])
    return manifest


def write_manifest(dataset_root: Path, manifest: dict[str, Any]) -> None:
    write_json_atomic(Path(dataset_root) / MANIFEST_NAME, manifest)


MISSING_MANIFEST_GENERATION = "missing"


def manifest_generation(dataset_root: Path) -> str:
    """Stable identity of the current Manifest bytes.

    A content hash (rather than size+mtime) cannot collide when a writer
    replaces the Manifest with identical-size content inside one timestamp
    tick. Two reads with the same generation see the same active file set.
    """
    path = Path(dataset_root) / MANIFEST_NAME
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return MISSING_MANIFEST_GENERATION
    return hashlib.sha256(raw).hexdigest()


def read_dataset_descriptor(dataset_root: Path) -> dict[str, Any]:
    path = Path(dataset_root) / DATASET_DESCRIPTOR_NAME
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def write_dataset_descriptor(dataset_root: Path, descriptor: dict[str, Any]) -> None:
    write_json_atomic(Path(dataset_root) / DATASET_DESCRIPTOR_NAME, descriptor)


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def active_manifest_entries(
    dataset_root: Path, *, existing_only: bool = True
) -> list[dict[str, Any]]:
    """Normalized active entries, skipping inactive and missing Data Files."""
    dataset_root = Path(dataset_root)
    entries: list[dict[str, Any]] = []
    for raw in read_manifest(dataset_root).get("files", []):
        entry = normalize_manifest_entry(raw, dataset_root)
        if not entry["active"]:
            continue
        if existing_only and not (dataset_root / entry["path"]).exists():
            continue
        entries.append(entry)
    return entries
