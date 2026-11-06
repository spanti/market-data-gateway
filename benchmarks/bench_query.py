#!/usr/bin/env python3
"""Query benchmark: direct `ParquetBarStore` reads or REST Arrow/JSON.

Default (offline) mode seeds a temporary Dataset with deterministic Bars, then
times `store.read_bars` and `store.iter_batches`. Pass `--base-url` to time the
gateway REST API instead; that is the only mode that touches the network.

Examples:
    python benchmarks/bench_query.py
    python benchmarks/bench_query.py --rows 200000 --repeat 5
    python benchmarks/bench_query.py --base-url http://127.0.0.1:8080 \\
        --instrument binance:BTC/USDT
"""

from __future__ import annotations

import json
import statistics
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional

import click
import typer

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _bench_common import format_rate, make_bars  # noqa: E402

from mdgw.store.parquet_dataset import ParquetBarStore  # noqa: E402
from mdgw_client import GatewayClient  # noqa: E402

app = typer.Typer(
    add_completion=False,
    help="Measure Bar query throughput through ParquetBarStore or the REST API.",
)


def _best_median(durations: list[float]) -> tuple[float, float]:
    return min(durations), statistics.median(durations)


def run_direct(
    *,
    rows: int,
    repeat: int,
    instrument: str,
    interval: str,
) -> int:
    bars = make_bars(rows, instrument_id=instrument, interval=interval)
    with tempfile.TemporaryDirectory(prefix="mdgw-bench-query-") as tmp:
        store = ParquetBarStore(Path(tmp))
        store.write_bars(bars, asset_class="crypto")

        # Warm-up so file handles/caches do not skew the first timed pass.
        store.read_bars(instrument_id=instrument, interval=interval)

        read_durations: list[float] = []
        read_rows: list[int] = []
        for _ in range(repeat):
            started = time.perf_counter()
            table = store.read_bars(
                instrument_id=instrument, interval=interval
            )
            read_durations.append(time.perf_counter() - started)
            read_rows.append(table.num_rows)

        iter_durations: list[float] = []
        iter_rows: list[int] = []
        for _ in range(repeat):
            started = time.perf_counter()
            count = sum(
                batch.num_rows
                for batch in store.iter_batches(
                    instrument_id=instrument, interval=interval
                )
            )
            iter_durations.append(time.perf_counter() - started)
            iter_rows.append(count)

        count_durations: list[float] = []
        count_rows: list[int] = []
        for _ in range(repeat):
            started = time.perf_counter()
            counted = store.count_bars(
                instrument_id=instrument, interval=interval
            )
            count_durations.append(time.perf_counter() - started)
            count_rows.append(counted)

    read_best, read_median = _best_median(read_durations)
    iter_best, iter_median = _best_median(iter_durations)
    count_best, count_median = _best_median(count_durations)
    total_rows = read_rows[-1]
    print(f"mode: direct ParquetBarStore  rows: {total_rows:,}")
    print(
        f"read_bars:      best {read_best:.3f}s "
        f"({format_rate(total_rows, read_best)})  "
        f"median {read_median:.3f}s ({format_rate(total_rows, read_median)})"
    )
    print(
        f"iter_batches:   best {iter_best:.3f}s "
        f"({format_rate(total_rows, iter_best)})  "
        f"median {iter_median:.3f}s ({format_rate(total_rows, iter_median)})"
    )
    print(
        f"count_bars:     best {count_best:.3f}s "
        f"({format_rate(count_rows[-1], count_best)})  "
        f"median {count_median:.3f}s "
        f"({format_rate(count_rows[-1], count_median)})"
    )
    return 0


def run_rest(
    *,
    base_url: str,
    timeout_s: float,
    instrument: str,
    interval: str,
    repeat: int,
) -> int:
    query = urllib.parse.urlencode(
        {"interval": interval, "instrument": instrument, "format": "json"}
    )
    url = f"{base_url.rstrip('/')}/api/v1/bars?{query}"

    arrow_durations: list[float] = []
    arrow_rows: list[int] = []
    json_durations: list[float] = []
    json_rows: list[int] = []

    with GatewayClient(base_url, timeout_s=timeout_s) as client:
        # Warm-up both paths once.
        client.history(interval, instrument=instrument)
        urllib.request.urlopen(url, timeout=timeout_s).read()

        for _ in range(repeat):
            started = time.perf_counter()
            table = client.history(interval, instrument=instrument)
            arrow_durations.append(time.perf_counter() - started)
            arrow_rows.append(table.num_rows)

        for _ in range(repeat):
            started = time.perf_counter()
            with urllib.request.urlopen(url, timeout=timeout_s) as response:
                payload = json.loads(response.read())
            json_durations.append(time.perf_counter() - started)
            json_rows.append(len(payload.get("bars", ())))

    for label, durations, counts in (
        ("REST arrow", arrow_durations, arrow_rows),
        ("REST json ", json_durations, json_rows),
    ):
        best, median = _best_median(durations)
        rows = counts[-1]
        print(
            f"{label}: rows {rows:,}  best {best:.3f}s "
            f"({format_rate(rows, best)})  median {median:.3f}s "
            f"({format_rate(rows, median)})"
        )
    return 0


@app.command()
def query(
    rows: int = typer.Option(
        50_000, "--rows", help="Bars to seed for direct mode (default: 50000)"
    ),
    repeat: int = typer.Option(5, "--repeat", help="timed passes (default: 5)"),
    instrument: str = typer.Option(
        "binance:BTC/USDT",
        "--instrument",
        help="Instrument to query (default: binance:BTC/USDT)",
    ),
    interval: str = typer.Option(
        "1m", "--interval", help="Bar Interval (default: 1m)"
    ),
    base_url: Optional[str] = typer.Option(
        None,
        "--base-url",
        metavar="URL",
        help="benchmark the REST API at this URL instead of a local Dataset",
    ),
    timeout_s: float = typer.Option(
        30.0,
        "--timeout-s",
        help="REST timeout in seconds (default: 30)",
    ),
) -> int:
    if repeat <= 0:
        raise typer.BadParameter(
            "--repeat must be positive", param_hint="--repeat"
        )
    if base_url:
        return run_rest(
            base_url=base_url,
            timeout_s=timeout_s,
            instrument=instrument,
            interval=interval,
            repeat=repeat,
        )
    if rows <= 0:
        raise typer.BadParameter("--rows must be positive", param_hint="--rows")
    return run_direct(
        rows=rows,
        repeat=repeat,
        instrument=instrument,
        interval=interval,
    )


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
