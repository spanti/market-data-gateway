r"""Canonical CSV rendering for Bar RecordBatches.

The public ``GET /api/v1/bars?format=csv`` stream and the Admin Export Job
executor render through this one module, so a saved export is byte-identical
to the streaming response for the same rows: ``BAR_SCHEMA`` column order,
timestamps as UTC ISO-8601 with nine fractional digits, decimals as exact
text (never through ``float``), ``\\n`` line terminators and Python's ``csv``
quoting rules.

This module was extracted from ``mdgw.api.routes.bars`` without changing the
bytes it produces; the CSV characterization tests pin that behaviour.
"""

from __future__ import annotations

import csv
import io
from typing import Any

import pyarrow as pa

from mdgw.core.envelope import decimal_to_str
from mdgw.core.schema import BAR_SCHEMA
from mdgw.core.time import ns_to_iso

TIMESTAMP_COLUMNS = ("open_ts", "close_ts", "event_ts", "ingest_ts")
DECIMAL_COLUMNS = ("open", "high", "low", "close", "volume")

#: The CSV header line (no trailing newline); one writer for every surface.
CSV_HEADER = ",".join(BAR_SCHEMA.names)


def header_bytes() -> bytes:
    """The exact UTF-8 header bytes a CSV response/file starts with."""
    return (CSV_HEADER + "\n").encode("utf-8")


def batch_to_csv(batch: pa.RecordBatch) -> str:
    """Render one batch as CSV without ever routing a price through float."""
    columns: dict[str, list[Any]] = {}
    for name in TIMESTAMP_COLUMNS:
        columns[name] = [
            ns_to_iso(value)
            for value in batch.column(name).cast(pa.int64()).to_pylist()
        ]
    for name in DECIMAL_COLUMNS:
        columns[name] = [
            decimal_to_str(value) for value in batch.column(name).to_pylist()
        ]
    for name in BAR_SCHEMA.names:
        if name not in columns:
            columns[name] = batch.column(name).to_pylist()

    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for index in range(batch.num_rows):
        writer.writerow([columns[name][index] for name in BAR_SCHEMA.names])
    return buffer.getvalue()


__all__ = [
    "CSV_HEADER",
    "DECIMAL_COLUMNS",
    "TIMESTAMP_COLUMNS",
    "batch_to_csv",
    "header_bytes",
]
