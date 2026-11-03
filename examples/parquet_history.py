#!/usr/bin/env python3
"""Read Bar history directly from the Parquet Dataset with pyarrow only.

This example deliberately imports *nothing* from the gateway packages. It walks
the Hive-partitioned `{data_root}/bars` tree with `pyarrow.dataset`, which is
the same physical layout the gateway writes:

    {data_root}/bars/asset_class=crypto/venue=binance/symbol=BTC%2FUSDT/
        interval=1m/year=YYYY/month=MM/data-<start_ns>-<end_ns>-<uuid>.parquet

For a production consumer the authoritative read path is `_manifest.json`
(only active Data Files are readable). A raw `pyarrow.dataset` scan sees every
`.parquet` file still on disk, which is fine for inspection but may include
files a Compaction has already marked inactive until Garbage Collection runs.
Use the REST API or the ParquetBarStore reader when that distinction matters.

Examples:
    python examples/parquet_history.py --data-root ./data-demo --interval 1m
    python examples/parquet_history.py --data-root ./data-demo \\
        --instrument binance:BTC/USDT --start 2026-11-01T00:00:00Z
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as pds

UTC = timezone.utc


def parse_iso_utc(value: str) -> datetime:
    """Parse an ISO-8601 string; a value without an offset is interpreted as UTC."""
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def build_filter(args: argparse.Namespace):
    expression = None

    def combine(clause) -> None:
        nonlocal expression
        expression = clause if expression is None else expression & clause

    if args.instrument:
        combine(pds.field("instrument_id") == args.instrument)
    # `interval` is a Hive partition column, not a column inside the file.
    if args.interval:
        combine(pds.field("interval") == args.interval)
    if args.start:
        combine(
            pds.field("open_ts")
            >= pa.scalar(parse_iso_utc(args.start), type=pa.timestamp("ns", tz="UTC"))
        )
    if args.end:
        combine(
            pds.field("open_ts")
            < pa.scalar(parse_iso_utc(args.end), type=pa.timestamp("ns", tz="UTC"))
        )
    return expression


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="parquet_history.py",
        description=(
            "Read Bars directly from {data_root}/bars with pyarrow.dataset. "
            "No gateway code is imported."
        ),
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("./data"),
        help="Dataset parent directory containing bars/ (default: ./data)",
    )
    parser.add_argument(
        "--instrument",
        metavar="INSTRUMENT_ID",
        help="filter one Instrument, e.g. binance:BTC/USDT",
    )
    parser.add_argument(
        "--interval",
        metavar="INTERVAL",
        help="filter one Bar Interval, e.g. 1m, 5m, 1h, 1d",
    )
    parser.add_argument(
        "--start",
        metavar="ISO8601",
        help="inclusive open_ts lower bound, e.g. 2026-11-01T00:00:00Z",
    )
    parser.add_argument(
        "--end",
        metavar="ISO8601",
        help="exclusive open_ts upper bound",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="maximum rows to print; 0 prints every row (default: 20)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.limit < 0:
        parser.error("--limit must be non-negative")

    dataset_root = args.data_root / "bars"
    if not dataset_root.is_dir():
        print(
            f"error: Dataset root does not exist: {dataset_root}",
            file=sys.stderr,
        )
        return 2

    dataset = pds.dataset(
        str(dataset_root), format="parquet", partitioning="hive"
    )
    expression = build_filter(args)
    table = dataset.to_table(filter=expression) if expression is not None else dataset.to_table()

    # File order is not part of the contract; sort for readable output.
    if table.num_rows:
        table = table.sort_by("open_ts")

    print(f"Dataset root: {dataset_root}")
    print("Schema (Arrow):")
    print(table.schema)

    limit = args.limit or table.num_rows
    shown = table.slice(0, limit)
    print(f"\nRows: {table.num_rows} matching, showing {shown.num_rows}")
    for row in shown.to_pylist():
        print(row)

    print("\nRead with pyarrow only; the mdgw package was not imported.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
