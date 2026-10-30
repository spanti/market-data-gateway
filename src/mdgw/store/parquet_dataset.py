"""Parquet Dataset storage for Bars (v1 Data Contract layout)."""

from __future__ import annotations

import json
import threading
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.parquet as pq

from mdgw.core.bar import Bar
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import (
    PARTITION_COLUMNS,
    PHYSICAL_SCHEMA,
    bars_to_physical_table,
    empty_logical_table,
    schema_json,
)
from mdgw.core.time import datetime_to_ns

DATASET_NAME = "bars"
DATASET_SCHEMA_VERSION = 1
MANIFEST_NAME = "_manifest.json"
DATASET_DESCRIPTOR_NAME = "_dataset.json"
FILE_NAMING = "data-<start_ns>-<end_ns>-<uuid>.parquet"


class ParquetBarStore:
    """Writes immutable Data Files and reads them back through pyarrow.dataset."""

    def __init__(
        self,
        root: Path,
        *,
        compression: str = "zstd",
        row_group_size: int = 1_000_000,
        format_version: str = "2.6",
        schema_version: int = DATASET_SCHEMA_VERSION,
    ) -> None:
        self.root = Path(root)
        self.dataset_root = self.root / DATASET_NAME
        self.compression = compression
        self.row_group_size = row_group_size
        self.format_version = format_version
        self.schema_version = schema_version
        self._lock = threading.Lock()

    def write_bars(self, bars: Sequence[Bar], *, asset_class: str) -> list[dict]:
        groups: dict[tuple[str, str, str, str, int, int], list[Bar]] = defaultdict(list)
        for bar in bars:
            instrument = InstrumentId.parse(bar.instrument_id)
            groups[
                (
                    asset_class,
                    instrument.venue,
                    instrument.file_symbol,
                    bar.interval,
                    bar.open_ts.year,
                    bar.open_ts.month,
                )
            ].append(bar)

        written: list[dict] = []
        for key, group in sorted(groups.items()):
            written.append(self._write_partition(key, group))

        if written:
            with self._lock:
                self._ensure_dataset_descriptor()
                self._append_manifest(written)
        return written

    def _write_partition(
        self,
        key: tuple[str, str, str, str, int, int],
        bars: Sequence[Bar],
    ) -> dict:
        asset_class, venue, file_symbol, interval, year, month = key
        directory = (
            self.dataset_root
            / f"asset_class={asset_class}"
            / f"venue={venue}"
            / f"symbol={file_symbol}"
            / f"interval={interval}"
            / f"year={year:04d}"
            / f"month={month:02d}"
        )
        directory.mkdir(parents=True, exist_ok=True)

        start_ns = min(datetime_to_ns(bar.open_ts) for bar in bars)
        end_ns = max(datetime_to_ns(bar.close_ts) for bar in bars)
        filename = f"data-{start_ns}-{end_ns}-{uuid4()}.parquet"
        path = directory / filename

        pq.write_table(
            bars_to_physical_table(bars),
            path,
            version=self.format_version,
            use_deprecated_int96_timestamps=False,
            compression=self.compression,
            row_group_size=self.row_group_size,
        )

        return {
            "path": path.relative_to(self.dataset_root).as_posix(),
            "start_ns": start_ns,
            "end_ns": end_ns,
            "row_count": len(bars),
            "active": True,
        }

    def _ensure_dataset_descriptor(self) -> None:
        descriptor = {
            "schema_version": self.schema_version,
            "data_type": "bar",
            "arrow_schema": schema_json(),
            "partitioning": list(PARTITION_COLUMNS),
            "partitioning_flavor": "hive",
            "file_naming": FILE_NAMING,
        }
        _write_json(self.dataset_root / DATASET_DESCRIPTOR_NAME, descriptor)

    def _append_manifest(self, files: Sequence[dict]) -> None:
        path = self.dataset_root / MANIFEST_NAME
        if path.exists():
            manifest = json.loads(path.read_text(encoding="utf-8"))
        else:
            manifest = {
                "schema_version": self.schema_version,
                "data_type": "bar",
                "files": [],
            }
        manifest["files"].extend(files)
        _write_json(path, manifest)

    def read_bars(
        self,
        *,
        instrument_id: str | None = None,
        interval: str | None = None,
    ) -> pa.Table:
        if not self._has_data_files():
            return empty_logical_table()

        dataset = pds.dataset(self.dataset_root, format="parquet", partitioning="hive")
        expression = None
        if instrument_id is not None:
            expression = pds.field("instrument_id") == instrument_id
        if interval is not None:
            interval_expression = pds.field("interval") == interval
            expression = (
                interval_expression
                if expression is None
                else expression & interval_expression
            )

        columns = list(PHYSICAL_SCHEMA.names) + ["interval"]
        return dataset.to_table(columns=columns, filter=expression)

    def _has_data_files(self) -> bool:
        manifest_path = self.dataset_root / MANIFEST_NAME
        if not manifest_path.exists():
            return False
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        return any(entry.get("active", True) for entry in manifest.get("files", []))


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    temporary.replace(path)
