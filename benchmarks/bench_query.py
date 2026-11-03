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

import argparse
import json
import statistics
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _bench_common import format_rate, make_bars  # noqa: E402

from mdgw.store.parquet_dataset import ParquetBarStore  # noqa: E402
from mdgw_client import GatewayClient  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bench_query.py",
        description=(
            "Measure Bar query throughput through ParquetBarStore or the REST API."
        ),
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=50_000,
        help="Bars to seed for direct mode (default: 50000)",
    )
    parser.add_argument(
        "--repeat", type=int, default=5, help="timed passes (default: 5)"
    )
    parser.add_argument(
        "--instrument",
        default="binance:BTC/USDT",
        help="Instrument to query (default: binance:BTC/USDT)",
    )
    parser.add_argument(
        "--interval", default="1m", help="Bar Interval (default: 1m)"
    )
    parser.add_argument(
        "--base-url",
        metavar="URL",
        help="benchmark the REST API at this URL instead of a local Dataset",
    )
    parser.add_argument(
        "--timeout-s",
        type=float,
        default=30.0,
        help="REST timeout in seconds (default: 30)",
    )
    return parser


def _best_median(durations: list[float]) -> tuple[float, float]:
    return min(durations), statistics.median(durations)


def run_direct(args: argparse.Namespace) -> int:
    bars = make_bars(args.rows, instrument_id=args.instrument, interval=args.interval)
    with tempfile.TemporaryDirectory(prefix="mdgw-bench-query-") as tmp:
        store = ParquetBarStore(Path(tmp))
        store.write_bars(bars, asset_class="crypto")

        # Warm-up so file handles/caches do not skew the first timed pass.
        store.read_bars(instrument_id=args.instrument, interval=args.interval)

        read_durations: list[float] = []
        read_rows: list[int] = []
        for _ in range(args.repeat):
            started = time.perf_counter()
            table = store.read_bars(
                instrument_id=args.instrument, interval=args.interval
            )
            read_durations.append(time.perf_counter() - started)
            read_rows.append(table.num_rows)

        iter_durations: list[float] = []
        iter_rows: list[int] = []
        for _ in range(args.repeat):
            started = time.perf_counter()
            count = sum(
                batch.num_rows
                for batch in store.iter_batches(
                    instrument_id=args.instrument, interval=args.interval
                )
            )
            iter_durations.append(time.perf_counter() - started)
            iter_rows.append(count)

    read_best, read_median = _best_median(read_durations)
    iter_best, iter_median = _best_median(iter_durations)
    rows = read_rows[-1]
    print(f"mode: direct ParquetBarStore  rows: {rows:,}")
    print(
        f"read_bars:      best {read_best:.3f}s "
        f"({format_rate(rows, read_best)})  "
        f"median {read_median:.3f}s ({format_rate(rows, read_median)})"
    )
    print(
        f"iter_batches:   best {iter_best:.3f}s "
        f"({format_rate(rows, iter_best)})  "
        f"median {iter_median:.3f}s ({format_rate(rows, iter_median)})"
    )
    return 0


def run_rest(args: argparse.Namespace) -> int:
    query = urllib.parse.urlencode(
        {"interval": args.interval, "instrument": args.instrument, "format": "json"}
    )
    url = f"{args.base_url.rstrip('/')}/api/v1/bars?{query}"

    arrow_durations: list[float] = []
    arrow_rows: list[int] = []
    json_durations: list[float] = []
    json_rows: list[int] = []

    with GatewayClient(args.base_url, timeout_s=args.timeout_s) as client:
        # Warm-up both paths once.
        client.history(args.interval, instrument=args.instrument)
        urllib.request.urlopen(url, timeout=args.timeout_s).read()

        for _ in range(args.repeat):
            started = time.perf_counter()
            table = client.history(args.interval, instrument=args.instrument)
            arrow_durations.append(time.perf_counter() - started)
            arrow_rows.append(table.num_rows)

        for _ in range(args.repeat):
            started = time.perf_counter()
            with urllib.request.urlopen(url, timeout=args.timeout_s) as response:
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


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.repeat <= 0:
        parser.error("--repeat must be positive")
    if args.base_url:
        return run_rest(args)
    if args.rows <= 0:
        parser.error("--rows must be positive")
    return run_direct(args)


if __name__ == "__main__":
    raise SystemExit(main())
