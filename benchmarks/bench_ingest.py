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

import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

import click
import typer

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _bench_common import format_bytes, format_rate, make_bars  # noqa: E402

from mdgw.aggregate import BarAggregator  # noqa: E402
from mdgw.store.parquet_dataset import ParquetBarStore  # noqa: E402

app = typer.Typer(
    add_completion=False,
    help=(
        "Write deterministic 1m Bars to a temporary Parquet Dataset and "
        "report rows/s and bytes written."
    ),
)


@app.command()
def ingest(
    rows: int = typer.Option(
        50_000, "--rows", help="Bars per pass (default: 50000)"
    ),
    repeat: int = typer.Option(3, "--repeat", help="timed passes (default: 3)"),
    aggregate: bool = typer.Option(
        False,
        "--aggregate",
        help="also run Bars through BarAggregator and write produced parents",
    ),
    compression: str = typer.Option(
        "zstd",
        "--compression",
        help="Parquet compression codec (default: zstd)",
    ),
) -> int:
    if rows <= 0:
        raise typer.BadParameter("--rows must be positive", param_hint="--rows")
    if repeat <= 0:
        raise typer.BadParameter(
            "--repeat must be positive", param_hint="--repeat"
        )

    bars = make_bars(rows)
    with tempfile.TemporaryDirectory(prefix="mdgw-bench-ingest-") as tmp:
        store = ParquetBarStore(Path(tmp), compression=compression)
        durations: list[float] = []
        written_rows: list[int] = []
        bytes_per_pass: list[int] = []

        for _ in range(repeat):
            written = bars
            if aggregate:
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
        written_count = written_rows[-1]
        typer.echo(f"rows per pass: {written_count:,} (input Bars: {rows:,})")
        typer.echo(f"passes: {repeat}  compression: {compression}")
        typer.echo(
            f"written: {format_bytes(statistics.mean(bytes_per_pass))} per pass"
        )
        typer.echo(f"best:   {best:.3f}s  {format_rate(written_count, best)}")
        typer.echo(f"median: {median:.3f}s  {format_rate(written_count, median)}")
        typer.echo(f"files per pass: {len(files)}")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    try:
        result = app(args=argv, standalone_mode=False)
    except click.exceptions.ClickException as exc:
        exc.show()
        raise SystemExit(exc.exit_code) from None
    except click.exceptions.Abort:
        click.echo("Aborted!", err=True)
        return 1
    return 0 if result is None else int(result)


if __name__ == "__main__":
    raise SystemExit(main())
