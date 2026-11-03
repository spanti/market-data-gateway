#!/usr/bin/env python3
"""Ingestion benchmark: write deterministic Bars through `ParquetBarStore`.

Measures the write path itself (Arrow construction, Parquet encoding,
compression, Data File + Manifest update). Generation and temporary-directory
setup happen outside the timed section.

Examples:
    python benchmarks/bench_ingest.py
    python benchmarks/bench_ingest.py --rows 200000 --repeat 3 --aggregate
"""

from __future__ import annotations

import argparse
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _bench_common import format_bytes, format_rate, make_bars  # noqa: E402

from mdgw.aggregate import BarAggregator  # noqa: E402
from mdgw.store.parquet_dataset import ParquetBarStore  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bench_ingest.py",
        description=(
            "Write deterministic 1m Bars to a temporary Parquet Dataset and "
            "report rows/s and bytes written."
        ),
    )
    parser.add_argument(
        "--rows", type=int, default=50_000, help="Bars per pass (default: 50000)"
    )
    parser.add_argument(
        "--repeat", type=int, default=3, help="timed passes (default: 3)"
    )
    parser.add_argument(
        "--aggregate",
        action="store_true",
        help="also run Bars through BarAggregator and write produced parents",
    )
    parser.add_argument(
        "--compression",
        default="zstd",
        help="Parquet compression codec (default: zstd)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.rows <= 0:
        parser.error("--rows must be positive")
    if args.repeat <= 0:
        parser.error("--repeat must be positive")

    bars = make_bars(args.rows)
    with tempfile.TemporaryDirectory(prefix="mdgw-bench-ingest-") as tmp:
        store = ParquetBarStore(Path(tmp), compression=args.compression)
        durations: list[float] = []
        written_rows: list[int] = []
        bytes_per_pass: list[int] = []

        for _ in range(args.repeat):
            written = bars
            if args.aggregate:
                # A fresh Aggregator per pass keeps every pass independent.
                aggregator = BarAggregator(
                    intervals=("1m", "5m", "15m", "1h", "1d"),
                    base_interval="1m",
                    retention_parent_periods=2,
                )
                written = bars + aggregator.add_many(bars)

            started = time.perf_counter()
            files = store.write_bars(written, asset_class="crypto")
            durations.append(time.perf_counter() - started)
            written_rows.append(len(written))
            bytes_per_pass.append(sum(int(item["size_bytes"]) for item in files))

        best = min(durations)
        median = statistics.median(durations)
        rows = written_rows[-1]
        print(f"rows per pass: {rows:,} (input Bars: {args.rows:,})")
        print(f"passes: {args.repeat}  compression: {args.compression}")
        print(
            f"written: {format_bytes(statistics.mean(bytes_per_pass))} per pass"
        )
        print(f"best:   {best:.3f}s  {format_rate(rows, best)}")
        print(f"median: {median:.3f}s  {format_rate(rows, median)}")
        print(f"files per pass: {len(files)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
