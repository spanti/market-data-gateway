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

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import click
import pyarrow as pa
import pyarrow.dataset as pds
import typer

UTC = timezone.utc

app = typer.Typer(
    name="parquet_history.py",
    add_completion=False,
    help=(
        "Read Bars directly from {data_root}/bars with pyarrow.dataset. "
        "No gateway code is imported."
    ),
)


def parse_iso_utc(value: str) -> datetime:
    """Parse an ISO-8601 string; a value without an offset is interpreted as UTC."""
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_iso_option(value: Optional[str], param_hint: str) -> Optional[datetime]:
    if value is None:
        return None
    try:
        return parse_iso_utc(value)
    except ValueError as exc:
        raise typer.BadParameter(
            f"invalid ISO-8601 timestamp {value!r}", param_hint=param_hint
        ) from exc


def build_filter(
    *,
    instrument: Optional[str],
    interval: Optional[str],
    start: Optional[datetime],
    end: Optional[datetime],
):
    expression = None

    def combine(clause) -> None:
        nonlocal expression
        expression = clause if expression is None else expression & clause

    if instrument:
        combine(pds.field("instrument_id") == instrument)
    # `interval` is a Hive partition column, not a column inside the file.
    if interval:
        combine(pds.field("interval") == interval)
    if start is not None:
        combine(
            pds.field("open_ts")
            >= pa.scalar(start, type=pa.timestamp("ns", tz="UTC"))
        )
    if end is not None:
        combine(
            pds.field("open_ts")
            < pa.scalar(end, type=pa.timestamp("ns", tz="UTC"))
        )
    return expression


@app.command()
def history(
    data_root: Path = typer.Option(
        Path("./data"),
        "--data-root",
        help="Dataset parent directory containing bars/ (default: ./data)",
    ),
    instrument: Optional[str] = typer.Option(
        None,
        "--instrument",
        metavar="INSTRUMENT_ID",
        help="filter one Instrument, e.g. binance:BTC/USDT",
    ),
    interval: Optional[str] = typer.Option(
        None,
        "--interval",
        metavar="INTERVAL",
        help="filter one Bar Interval, e.g. 1m, 5m, 1h, 1d",
    ),
    start: Optional[str] = typer.Option(
        None,
        "--start",
        metavar="ISO8601",
        help="inclusive open_ts lower bound, e.g. 2026-11-01T00:00:00Z",
    ),
    end: Optional[str] = typer.Option(
        None,
        "--end",
        metavar="ISO8601",
        help="exclusive open_ts upper bound",
    ),
    limit: int = typer.Option(
        20,
        "--limit",
        help="maximum rows to print; 0 prints every row (default: 20)",
    ),
) -> int:
    if limit < 0:
        raise typer.BadParameter("--limit must be non-negative", param_hint="--limit")

    dataset_root = data_root / "bars"
    if not dataset_root.is_dir():
        typer.echo(
            f"error: Dataset root does not exist: {dataset_root}", err=True
        )
        raise typer.Exit(code=2)

    dataset = pds.dataset(
        str(dataset_root), format="parquet", partitioning="hive"
    )
    expression = build_filter(
        instrument=instrument,
        interval=interval,
        start=_parse_iso_option(start, "--start"),
        end=_parse_iso_option(end, "--end"),
    )
    table = dataset.to_table(filter=expression) if expression is not None else dataset.to_table()

    # File order is not part of the contract; sort for readable output. A raw
    # scan can also contain several revisions of one Bar, so collapse them to
    # the greatest revision per identity (Data Contract §9).
    table = latest_revisions(table)

    typer.echo(f"Dataset root: {dataset_root}")
    typer.echo("Schema (Arrow):")
    typer.echo(table.schema)

    shown_limit = limit or table.num_rows
    shown = table.slice(0, shown_limit)
    typer.echo(f"\nRows: {table.num_rows} matching, showing {shown.num_rows}")
    for row in shown.to_pylist():
        typer.echo(row)

    typer.echo("\nRead with pyarrow only; the mdgw package was not imported.")
    return 0


def latest_revisions(table: pa.Table) -> pa.Table:
    """Keep the greatest revision per (instrument_id, interval, open_ts).

    The Data Contract makes those three fields the Bar identity; a source
    correction appends a new row with a higher revision. Raw PyArrow scans see
    every row, so a semantic consumer must deduplicate before using the series.
    """
    if table.num_rows == 0:
        return table
    latest: dict[tuple, dict] = {}
    for row in table.to_pylist():
        key = (row["instrument_id"], row["interval"], row["open_ts"])
        previous = latest.get(key)
        if previous is None or row["revision"] > previous["revision"]:
            latest[key] = row
    rows = sorted(
        latest.values(),
        key=lambda row: (row["instrument_id"], row["interval"], row["open_ts"]),
    )
    return pa.Table.from_pylist(rows, schema=table.schema)


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
