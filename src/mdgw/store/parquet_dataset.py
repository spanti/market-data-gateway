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
from mdgw.core.errors import SchemaVersionMismatch
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import (
    BAR_SCHEMA,
    PARTITION_COLUMNS,
    UTC_TS,
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
DEFAULT_BATCH_SIZE = 65_536


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

    def validate_schema_version(self) -> None:
        """Reject a Dataset written under a different Data Contract version."""
        descriptor_path = self.dataset_root / DATASET_DESCRIPTOR_NAME
        if not descriptor_path.exists():
            return
        try:
            descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SchemaVersionMismatch(
                f"cannot read Dataset descriptor {descriptor_path}: {exc}"
            ) from exc

        found = descriptor.get("schema_version")
        if found != self.schema_version:
            raise SchemaVersionMismatch(
                f"Dataset schema_version {found!r} does not match "
                f"the expected {self.schema_version}"
            )

    def iter_batches(
        self,
        *,
        instrument_id: str | None = None,
        interval: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
        limit: int | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        """Yield Bar RecordBatches without materializing the whole Dataset.

        `start_ns` is inclusive and `end_ns` is exclusive, both applied to
        `open_ts`. `limit` caps the total number of rows yielded.
        """
        self.validate_schema_version()
        if not self._has_data_files():
            return

        dataset = pds.dataset(
            self.dataset_root, format="parquet", partitioning="hive"
        )
        scanner = dataset.scanner(
            columns=list(BAR_SCHEMA.names),
            filter=self._filter_expression(
                instrument_id=instrument_id,
                interval=interval,
                start_ns=start_ns,
                end_ns=end_ns,
            ),
            batch_size=batch_size,
        )

        remaining = limit
        for batch in scanner.to_batches():
            if remaining is not None:
                if remaining <= 0:
                    break
                if batch.num_rows > remaining:
                    batch = batch.slice(0, remaining)
                remaining -= batch.num_rows
            yield pa.RecordBatch.from_arrays(
                [batch.column(name) for name in BAR_SCHEMA.names],
                schema=BAR_SCHEMA,
            )

    def count_bars(
        self,
        *,
        instrument_id: str | None = None,
        interval: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
    ) -> int:
        """Count matching Bars, used to enforce the JSON row ceiling."""
        self.validate_schema_version()
        if not self._has_data_files():
            return 0

        dataset = pds.dataset(
            self.dataset_root, format="parquet", partitioning="hive"
        )
        return int(
            dataset.count_rows(
                filter=self._filter_expression(
                    instrument_id=instrument_id,
                    interval=interval,
                    start_ns=start_ns,
                    end_ns=end_ns,
                )
            )
        )

    def read_bars(
        self,
        *,
        instrument_id: str | None = None,
        interval: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
        limit: int | None = None,
    ) -> pa.Table:
        batches = list(
            self.iter_batches(
                instrument_id=instrument_id,
                interval=interval,
                start_ns=start_ns,
                end_ns=end_ns,
                limit=limit,
            )
        )
        if not batches:
            return empty_logical_table()
        return pa.Table.from_batches(batches, schema=BAR_SCHEMA)

    @staticmethod
    def _filter_expression(
        *,
        instrument_id: str | None,
        interval: str | None,
        start_ns: int | None,
        end_ns: int | None,
    ):
        expression = None

        def combine(clause):
            nonlocal expression
            expression = clause if expression is None else expression & clause

        if instrument_id is not None:
            combine(pds.field("instrument_id") == instrument_id)
        if interval is not None:
            combine(pds.field("interval") == interval)
        if start_ns is not None:
            combine(pds.field("open_ts") >= pa.scalar(start_ns, type=UTC_TS))
        if end_ns is not None:
            combine(pds.field("open_ts") < pa.scalar(end_ns, type=UTC_TS))
        return expression

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
