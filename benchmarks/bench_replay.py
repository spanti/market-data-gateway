#!/usr/bin/env python3
"""Replay benchmark: push a seeded Dataset through `ReplayService`.

Measures the full Replay Session worker path: read + deduplicate + order the
Bars, encode each as a one-row Arrow IPC stream, and call a no-op publisher.
Transport fan-out is excluded on purpose so the number reflects the gateway's
own work. `--mode realtime` runs the same bounded stream paced by the Replay
Clock; use a high `--speed` so the benchmark measures throughput rather than
the Clock.

Examples:
    python benchmarks/bench_replay.py
    python benchmarks/bench_replay.py --rows 200000 --repeat 3
    python benchmarks/bench_replay.py --mode realtime --memory
"""

from __future__ import annotations

import gc
import statistics
import sys
import tempfile
import threading
import time
import tracemalloc
from pathlib import Path
from typing import Optional

import click
import typer

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _bench_common import (  # noqa: E402
    bar_range_ns,
    format_bytes,
    format_rate,
    make_bars,
)

from mdgw.core.bar import Bar  # noqa: E402
from mdgw.core.schema import bars_to_logical_table  # noqa: E402
from mdgw.replay.service import ReplayService  # noqa: E402
from mdgw.replay.session import BarSubscription, ReplaySessionSpec  # noqa: E402
from mdgw.replay.source import interval_ns  # noqa: E402
from mdgw.store.duckdb_reader import DuckDBOptions  # noqa: E402
from mdgw.store.parquet_dataset import (  # noqa: E402
    BarReadQuery,
    ParquetBarStore,
    ReplayReadQuery,
)
from mdgw.streaming import table_to_ipc  # noqa: E402

app = typer.Typer(
    add_completion=False,
    help="Run asap Replay Sessions over a seeded Dataset and report Bars/s.",
)


def _drop(_session_id: str, bar: Bar) -> None:
    """Encode each Bar as Arrow IPC and discard it, like the Replay adapter."""
    table_to_ipc(bars_to_logical_table([bar]))


def _count(_session_id: str, bar: Bar) -> None:
    """Record nothing; the memory pass measures the read side, not IPC."""


def _psutil_process():
    """Optional RSS sampler; psutil is not a required dependency."""
    try:
        import psutil  # ruff: ignore[import-outside-top-level] - optional
    except ImportError:  # pragma: no cover - environment dependent
        return None
    return psutil.Process()


class _ArrowPeakStore:
    """Store proxy recording Arrow native bytes at every batch hand-off."""

    def __init__(self, inner: ParquetBarStore, peaks: dict[str, int]) -> None:
        self._inner = inner
        self._peaks = peaks

    @property
    def dataset_root(self) -> Path:
        return self._inner.dataset_root

    def open_read(self, **kwargs):
        return _ArrowPeakRead(self._inner.open_read(**kwargs), self._peaks)


class _ArrowPeakRead:
    def __init__(self, inner, peaks: dict[str, int]) -> None:
        self._inner = inner
        self._peaks = peaks

    def iter_replay_batches(
        self, queries, *, batch_size=None, cancellation=None
    ):
        for batch in self._inner.iter_replay_batches(
            queries, batch_size=batch_size, cancellation=cancellation
        ):
            # The batch is alive here, before the Session converts it to
            # Python Bar values. `nbytes` covers DuckDB-exported Arrow
            # buffers, which `pa.total_allocated_bytes()` cannot see because
            # DuckDB links its own Arrow allocator.
            self._peaks["arrow"] = max(self._peaks["arrow"], batch.nbytes)
            yield batch

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _parse_row_counts(raw: str) -> list[int]:
    counts: list[int] = []
    for part in raw.split(","):
        cleaned = part.strip()
        if not cleaned:
            continue
        try:
            value = int(cleaned)
        except ValueError:
            raise typer.BadParameter(
                f"not a row count: {cleaned!r}", param_hint="--memory-rows"
            ) from None
        if value <= 0:
            raise typer.BadParameter(
                "row counts must be positive", param_hint="--memory-rows"
            )
        counts.append(value)
    if not counts:
        raise typer.BadParameter(
            "at least one row count is required", param_hint="--memory-rows"
        )
    return counts


def _run_memory_pass(
    row_counts: list[int],
    *,
    mode: str,
    speed: float,
    memory_limit: str,
    batch_size: int,
    prefetch_batches: int,
    timeout_s: float,
) -> int:
    """Prove the read-side process memory is bounded while result size grows.

    One Session runs per row count with the same query and a DuckDB memory
    limit low enough to force an external sort at the larger sizes. Python
    (`tracemalloc`), Arrow native (largest handed-over RecordBatch) and RSS
    (sampled, optional psutil) peaks are reported next to the sampled DuckDB
    temp-directory peak. A flat Python/Arrow bound plus growing temp disk is
    the bounded-prefetch proof; temp files are never a second result copy.
    """
    typer.echo(
        f"memory scaling, mode={mode} (same query, increasing result size; "
        f"DuckDB memory_limit={memory_limit}, threads=1)"
    )
    typer.echo(
        f"{'rows':>9}  {'replay s':>8}  {'bars sent':>10}  "
        f"{'tracemalloc peak':>17}  {'Arrow native peak':>19}  "
        f"{'RSS delta':>10}  {'temp disk peak':>15}"
    )
    for rows in row_counts:
        bars = make_bars(rows)
        start_ns, end_ns = bar_range_ns(bars)
        with tempfile.TemporaryDirectory(
            prefix=f"mdgw-bench-replay-mem-{rows}-"
        ) as tmp:
            root = Path(tmp)
            temp_dir = root / "duckdb-temp"
            temp_dir.mkdir()
            store = ParquetBarStore(
                root / "data",
                duckdb=DuckDBOptions(
                    memory_limit=memory_limit,
                    threads=1,
                    temp_directory=temp_dir,
                    max_temp_directory_size="2GB",
                ),
            )
            store.write_bars(bars, asset_class="crypto")
            del bars
            gc.collect()

            rss_before = 0
            process = _psutil_process()
            if process is not None:
                rss_before = process.memory_info().rss
            peaks = {"arrow": 0, "rss": rss_before, "temp": 0}

            def sample() -> None:
                while not stop.wait(0.002):
                    if process is not None:
                        peaks["rss"] = max(
                            peaks["rss"], process.memory_info().rss
                        )
                    total = 0
                    for path in temp_dir.glob("**/*"):
                        try:
                            if path.is_file():
                                total += path.stat().st_size
                        except OSError:  # pragma: no cover - rotated file
                            pass
                    peaks["temp"] = max(peaks["temp"], total)

            stop = threading.Event()
            sampler = threading.Thread(target=sample, daemon=True)
            sampler.start()
            tracemalloc.start()
            tracemalloc.reset_peak()
            service = ReplayService(
                _ArrowPeakStore(store, peaks),  # type: ignore[arg-type] - proxy
                publish_bar=_count,
                batch_size=batch_size,
                prefetch_batches=prefetch_batches,
            )
            spec = ReplaySessionSpec(
                subscriptions=(
                    BarSubscription(
                        instrument_id="binance:BTC/USDT", interval="1m"
                    ),
                ),
                start_ts_ns=start_ns,
                end_ts_ns=end_ns,
                replay_mode=mode,
                speed=speed,
            )
            started = time.perf_counter()
            session_id = service.create_session(spec)
            if mode == "step":
                # A step Session starts paused; grant enough Credits to let
                # the bounded stream drain while memory stays bounded.
                service.step_session(session_id, rows)
            finished = service.wait_session(session_id, timeout=timeout_s)
            duration = time.perf_counter() - started
            sent = service.session_bars_sent(session_id)
            service.close()
            _, python_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            stop.set()
            sampler.join(timeout=5)

        if not finished:
            typer.echo(
                f"error: {rows:,}-row session did not finish within "
                f"{timeout_s}s",
                err=True,
            )
            return 1
        rss_delta = max(0, peaks["rss"] - rss_before)
        typer.echo(
            f"{rows:>9,}  {duration:>8.3f}  {sent:>10,}  "
            f"{format_bytes(python_peak):>17}  "
            f"{format_bytes(peaks['arrow']):>19}  "
            f"{(format_bytes(rss_delta) if process else 'n/a'):>10}  "
            f"{format_bytes(peaks['temp']):>15}"
        )
    typer.echo(
        "note: tracemalloc covers Python allocations only; the Arrow native "
        "peak is the largest RecordBatch buffer handed to the Session before "
        "it is converted to Bar values; RSS is sampled process memory "
        "(psutil optional); temp disk is the sampled size of the configured "
        "DuckDB temp_directory and is never a durable copy."
    )
    return 0


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
    mode: str = typer.Option(
        "asap",
        "--mode",
        help="Replay mode for the timed and memory passes (asap|realtime|step)",
    ),
    speed: float = typer.Option(
        1_000_000.0,
        "--speed",
        help="Realtime Clock speed for --mode realtime (default: 1000000)",
    ),
    batch_size: int = typer.Option(
        4_096,
        "--batch-size",
        help="asap Replay Arrow batch size (default: 4096)",
    ),
    prefetch_batches: int = typer.Option(
        2,
        "--prefetch-batches",
        help="bounded prefetch depth in batches (default: 2)",
    ),
    memory: bool = typer.Option(
        False,
        "--memory",
        help="also run the increasing-row bounded-memory proof",
    ),
    memory_rows: str = typer.Option(
        "50000,100000,200000",
        "--memory-rows",
        help="comma-separated row counts for --memory (default: 50000,100000,200000)",
    ),
    memory_limit: str = typer.Option(
        "64MB",
        "--memory-limit",
        help="DuckDB memory limit for --memory, forcing spill (default: 64MB)",
    ),
) -> int:
    if rows <= 0:
        raise typer.BadParameter("--rows must be positive", param_hint="--rows")
    if repeat <= 0:
        raise typer.BadParameter(
            "--repeat must be positive", param_hint="--repeat"
        )
    if batch_size < 1:
        raise typer.BadParameter(
            "--batch-size must be positive", param_hint="--batch-size"
        )
    if prefetch_batches < 1:
        raise typer.BadParameter(
            "--prefetch-batches must be positive",
            param_hint="--prefetch-batches",
        )
    if mode not in {"asap", "realtime", "step"}:
        raise typer.BadParameter(
            "--mode must be 'asap', 'realtime' or 'step'", param_hint="--mode"
        )
    if speed <= 0:
        raise typer.BadParameter("--speed must be positive", param_hint="--speed")

    bars = make_bars(rows, instrument_id=instrument, interval=interval)
    start_ns, end_ns = bar_range_ns(bars)
    subscription = BarSubscription(
        instrument_id=instrument, interval=interval
    )

    durations: list[float] = []
    sent_counts: list[int] = []
    query_durations: list[float] = []
    query_counts: list[int] = []
    stream_durations: list[float] = []
    with tempfile.TemporaryDirectory(prefix="mdgw-bench-replay-") as tmp:
        store = ParquetBarStore(Path(tmp))
        store.write_bars(bars, asset_class="crypto")

        # Replay query preparation alone (Snapshot + lease acquisition,
        # DuckDB scan, close_ts filter, Revision dedupe and five-key sort),
        # before any Arrow IPC / publish cost. This fully drains the one
        # production bounded stream.
        snapshot_query = BarReadQuery(
            instrument_id=subscription.instrument_id,
            interval=subscription.interval,
            start_ns=start_ns - interval_ns(subscription.interval),
            end_ns=end_ns,
        )
        replay_query = ReplayReadQuery(
            instrument_id=subscription.instrument_id,
            interval=subscription.interval,
            open_start_ns=start_ns - interval_ns(subscription.interval),
            close_start_ns=start_ns,
            close_end_ns=end_ns,
        )
        for _ in range(repeat):
            started = time.perf_counter()
            with store.open_read(
                queries=(snapshot_query,), lease_id="bench-replay"
            ) as read:
                prepared = sum(
                    batch.num_rows
                    for batch in read.iter_replay_batches(
                        (replay_query,), batch_size=batch_size
                    )
                )
            query_durations.append(time.perf_counter() - started)
            query_counts.append(prepared)

        # The streaming asap path: Snapshot + lease + query preparation until
        # the first Arrow batch is ready (the global sort still runs first),
        # then the generator is closed instead of publishing anything.
        for _ in range(repeat):
            started = time.perf_counter()
            with store.open_read(
                queries=(snapshot_query,), lease_id="bench-replay"
            ) as read:
                batches = read.iter_replay_batches(
                    (replay_query,), batch_size=batch_size
                )
                next(batches, None)
                batches.close()
            stream_durations.append(time.perf_counter() - started)

        for index in range(repeat):
            service = ReplayService(
                store,
                publish_bar=_drop,
                max_sessions=4,
                batch_size=batch_size,
                prefetch_batches=prefetch_batches,
            )
            spec = ReplaySessionSpec(
                subscriptions=(subscription,),
                start_ts_ns=start_ns,
                end_ts_ns=end_ns,
                replay_mode=mode,
                speed=speed,
            )
            started = time.perf_counter()
            session_id = service.create_session(spec)
            if mode == "step":
                service.step_session(session_id, rows)
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
    stream_best = min(stream_durations)
    stream_median = statistics.median(stream_durations)
    typer.echo(
        f"mode: {mode} replay  rows: {sent:,}  passes: {repeat}  speed: {speed:g}"
    )
    typer.echo(
        f"query prep (streamed): best {query_best:.3f}s "
        f"({format_rate(query_rows, query_best)})  "
        f"median {query_median:.3f}s "
        f"({format_rate(query_rows, query_median)})"
    )
    typer.echo(
        f"stream first batch (duckdb): best {stream_best:.3f}s  "
        f"median {stream_median:.3f}s"
    )
    typer.echo(f"best:   {best:.3f}s  {format_rate(sent, best)}")
    typer.echo(f"median: {median:.3f}s  {format_rate(sent, median)}")
    if memory:
        return _run_memory_pass(
            _parse_row_counts(memory_rows),
            mode=mode,
            speed=speed,
            memory_limit=memory_limit,
            batch_size=batch_size,
            prefetch_batches=prefetch_batches,
            timeout_s=max(timeout_s, 300.0),
        )
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
