#!/usr/bin/env python3
"""Replay benchmark: push a seeded Dataset through `ReplayService` in asap mode.

Measures the full Replay Session worker path: read + deduplicate + order the
Bars, encode each as a one-row Arrow IPC stream, and call a no-op publisher.
Transport fan-out is excluded on purpose so the number reflects the gateway's
own work.

Examples:
    python benchmarks/bench_replay.py
    python benchmarks/bench_replay.py --rows 200000 --repeat 3
"""

from __future__ import annotations

import argparse
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _bench_common import bar_range_ns, format_rate, make_bars  # noqa: E402

from mdgw.replay.service import ReplayService  # noqa: E402
from mdgw.replay.session import BarSubscription, ReplaySessionSpec  # noqa: E402
from mdgw.store.parquet_dataset import ParquetBarStore  # noqa: E402


def _drop(topic: str, payload: bytes) -> None:
    """No-op market sink: the Replay Session still encodes the Arrow IPC frame."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bench_replay.py",
        description=(
            "Run asap Replay Sessions over a seeded Dataset and report Bars/s."
        ),
    )
    parser.add_argument(
        "--rows", type=int, default=50_000, help="Bars to seed (default: 50000)"
    )
    parser.add_argument(
        "--repeat", type=int, default=3, help="timed sessions (default: 3)"
    )
    parser.add_argument(
        "--instrument",
        default="binance:BTC/USDT",
        help="Instrument to replay (default: binance:BTC/USDT)",
    )
    parser.add_argument(
        "--interval", default="1m", help="Bar Interval (default: 1m)"
    )
    parser.add_argument(
        "--timeout-s",
        type=float,
        default=120.0,
        help="abort a session after this many seconds (default: 120)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.rows <= 0:
        parser.error("--rows must be positive")
    if args.repeat <= 0:
        parser.error("--repeat must be positive")

    bars = make_bars(args.rows, instrument_id=args.instrument, interval=args.interval)
    start_ns, end_ns = bar_range_ns(bars)
    subscription = BarSubscription(
        instrument_id=args.instrument, interval=args.interval
    )

    durations: list[float] = []
    sent_counts: list[int] = []
    with tempfile.TemporaryDirectory(prefix="mdgw-bench-replay-") as tmp:
        store = ParquetBarStore(Path(tmp))
        store.write_bars(bars, asset_class="crypto")

        for index in range(args.repeat):
            service = ReplayService(store, publish=_drop, max_sessions=4)
            spec = ReplaySessionSpec(
                subscriptions=(subscription,),
                start_ts_ns=start_ns,
                end_ts_ns=end_ns,
                replay_mode="asap",
                speed=1.0,
            )
            started = time.perf_counter()
            session_id = service.create_session(spec)
            finished = service.wait_session(session_id, timeout=args.timeout_s)
            durations.append(time.perf_counter() - started)
            sent_counts.append(service.session_bars_sent(session_id))
            if not finished:
                print(
                    f"error: session {index + 1} did not finish within "
                    f"{args.timeout_s}s",
                    file=sys.stderr,
                )
                service.close()
                return 1
            service.close()

    best = min(durations)
    median = statistics.median(durations)
    rows = sent_counts[-1]
    print(f"mode: asap replay  rows: {rows:,}  passes: {args.repeat}")
    print(f"best:   {best:.3f}s  {format_rate(rows, best)}")
    print(f"median: {median:.3f}s  {format_rate(rows, median)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
