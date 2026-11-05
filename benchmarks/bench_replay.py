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

import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

import click
import typer

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _bench_common import bar_range_ns, format_rate, make_bars  # noqa: E402

from mdgw.core.bar import Bar  # noqa: E402
from mdgw.core.schema import bars_to_logical_table  # noqa: E402
from mdgw.replay.service import ReplayService  # noqa: E402
from mdgw.replay.session import BarSubscription, ReplaySessionSpec  # noqa: E402
from mdgw.replay.source import ReplayBarSource, interval_ns  # noqa: E402
from mdgw.store.parquet_dataset import (  # noqa: E402
    BarReadQuery,
    ParquetBarStore,
)
from mdgw.streaming import table_to_ipc  # noqa: E402

app = typer.Typer(
    add_completion=False,
    help="Run asap Replay Sessions over a seeded Dataset and report Bars/s.",
)


def _drop(_session_id: str, bar: Bar) -> None:
    """Encode each Bar as Arrow IPC and discard it, like the Replay adapter."""
    table_to_ipc(bars_to_logical_table([bar]))


@app.command()
def replay(
    rows: int = typer.Option(
        50_000, "--rows", help="Bars to seed (default: 50000)"
    ),
    repeat: int = typer.Option(
        3, "--repeat", help="timed sessions (default: 3)"
    ),
    instrument: str = typer.Option(
        "binance:BTC/USDT",
        "--instrument",
        help="Instrument to replay (default: binance:BTC/USDT)",
    ),
    interval: str = typer.Option(
        "1m", "--interval", help="Bar Interval (default: 1m)"
    ),
    timeout_s: float = typer.Option(
        120.0,
        "--timeout-s",
        help="abort a session after this many seconds (default: 120)",
    ),
) -> int:
    if rows <= 0:
        raise typer.BadParameter("--rows must be positive", param_hint="--rows")
    if repeat <= 0:
        raise typer.BadParameter(
            "--repeat must be positive", param_hint="--repeat"
        )

    bars = make_bars(rows, instrument_id=instrument, interval=interval)
    start_ns, end_ns = bar_range_ns(bars)
    subscription = BarSubscription(
        instrument_id=instrument, interval=interval
    )

    durations: list[float] = []
    sent_counts: list[int] = []
    query_durations: list[float] = []
    query_counts: list[int] = []
    legacy_query_durations: list[float] = []
    with tempfile.TemporaryDirectory(prefix="mdgw-bench-replay-") as tmp:
        store = ParquetBarStore(Path(tmp))
        store.write_bars(bars, asset_class="crypto")

        # Replay query preparation alone (Snapshot + lease acquisition,
        # DuckDB scan, close_ts filter, Revision dedupe and five-key sort),
        # before any Arrow IPC / publish cost. This is the production path.
        source = ReplayBarSource(store)
        snapshot_query = BarReadQuery(
            instrument_id=subscription.instrument_id,
            interval=subscription.interval,
            start_ns=start_ns - interval_ns(subscription.interval),
            end_ns=end_ns,
        )
        for _ in range(repeat):
            started = time.perf_counter()
            with store.open_read(
                queries=(snapshot_query,), lease_id="bench-replay"
            ) as read:
                prepared = source.read(
                    [subscription],
                    start_ts_ns=start_ns,
                    end_ts_ns=end_ns,
                    read=read,
                )
            query_durations.append(time.perf_counter() - started)
            query_counts.append(len(prepared))

        # The pre-DuckDB PyArrow path is measured only for comparison; it is
        # not reachable from a production Replay Session.
        for _ in range(repeat):
            started = time.perf_counter()
            source.read([subscription], start_ts_ns=start_ns, end_ts_ns=end_ns)
            legacy_query_durations.append(time.perf_counter() - started)

        for index in range(repeat):
            service = ReplayService(
                store, publish_bar=_drop, max_sessions=4
            )
            spec = ReplaySessionSpec(
                subscriptions=(subscription,),
                start_ts_ns=start_ns,
                end_ts_ns=end_ns,
                replay_mode="asap",
                speed=1.0,
            )
            started = time.perf_counter()
            session_id = service.create_session(spec)
            finished = service.wait_session(session_id, timeout=timeout_s)
            durations.append(time.perf_counter() - started)
            sent_counts.append(service.session_bars_sent(session_id))
            if not finished:
                typer.echo(
                    f"error: session {index + 1} did not finish within "
                    f"{timeout_s}s",
                    err=True,
                )
                service.close()
                raise typer.Exit(code=1)
            service.close()

    best = min(durations)
    median = statistics.median(durations)
    sent = sent_counts[-1]
    query_best = min(query_durations)
    query_median = statistics.median(query_durations)
    query_rows = query_counts[-1]
    legacy_best = min(legacy_query_durations)
    legacy_median = statistics.median(legacy_query_durations)
    typer.echo(f"mode: asap replay  rows: {sent:,}  passes: {repeat}")
    typer.echo(
        f"query prep (duckdb): best {query_best:.3f}s "
        f"({format_rate(query_rows, query_best)})  "
        f"median {query_median:.3f}s "
        f"({format_rate(query_rows, query_median)})"
    )
    typer.echo(
        f"query prep (legacy): best {legacy_best:.3f}s "
        f"({format_rate(query_rows, legacy_best)})  "
        f"median {legacy_median:.3f}s "
        f"({format_rate(query_rows, legacy_median)})"
    )
    typer.echo(f"best:   {best:.3f}s  {format_rate(sent, best)}")
    typer.echo(f"median: {median:.3f}s  {format_rate(sent, median)}")
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
