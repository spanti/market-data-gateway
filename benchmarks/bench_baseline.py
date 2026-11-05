#!/usr/bin/env python3
"""DuckDB/Polars migration baseline: Store, REST, pagination and Replay.

This script builds one deterministic corpus and measures the current
PyArrow/Parquet implementation on it. It complements `bench_query.py` and
`bench_replay.py` (whose generators it reuses) with the full M0 matrix:

* Store scan / count, single-file and multi-file;
* REST JSON, Arrow, CSV, Parquet/Arrow export and pagination through the real
  ASGI app (`GatewayRuntime`);
* a Revision-heavy Dataset for both raw reads and Time Cursor pagination;
* Replay query preparation, Arrow IPC encoding and end-to-end asap publishing,
  reported separately so transport/encoding cost is not mistaken for query
  cost.

For every scenario it runs a warm-up plus `--repeat` timed passes and reports
the median with its min-max spread. A separate single pass records
`tracemalloc` peak Python allocations and the Arrow allocator delta (Arrow
native buffers are not traced by `tracemalloc`).

The corpus is deterministic and is reused from `--data-dir` on later runs, so
comparisons across tickets use the same bytes. Pass `--rebuild` after changing
the generator.

Example:
    python benchmarks/bench_baseline.py --repeat 5 --data-dir /tmp/mdgw-baseline
"""

from __future__ import annotations

import argparse
import gc
import json
import platform
import shutil
import statistics
import sys
import tempfile
import time
import tracemalloc
import urllib.parse
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Callable, Optional

BENCH_DIR = Path(__file__).resolve().parent
REPO_ROOT = BENCH_DIR.parent
sys.path.insert(0, str(BENCH_DIR))
sys.path.insert(0, str(REPO_ROOT / "src"))

from _bench_common import format_bytes, format_rate, make_bars  # noqa: E402

import pyarrow as pa  # noqa: E402

from mdgw.config import load_config  # noqa: E402
from mdgw.core.bar import Bar  # noqa: E402
from mdgw.core.schema import INTERVALS, bars_to_logical_table  # noqa: E402
from mdgw.core.time import datetime_to_ns  # noqa: E402
from mdgw.ingest.fake import FakeFeedHandler  # noqa: E402
from mdgw.replay.service import ReplayService  # noqa: E402
from mdgw.replay.session import BarSubscription, ReplaySessionSpec  # noqa: E402
from mdgw.replay.source import ReplayBarSource, interval_ns  # noqa: E402
from mdgw.runtime import GatewayRuntime  # noqa: E402
from mdgw.store.parquet_dataset import (  # noqa: E402
    BarReadQuery,
    ParquetBarStore,
)
from mdgw.streaming import table_to_ipc  # noqa: E402

INSTRUMENT = "binance:BTC/USDT"
INTERVAL = "1m"
BASE = datetime(2026, 1, 1, tzinfo=UTC)
CONFIG_PATH = REPO_ROOT / "config" / "gateway.toml"
INSTRUMENTS_PATH = REPO_ROOT / "config" / "instruments"

QUERY_ROWS = 50_000
SCAN_ROWS = 100_000
MULTI_FILE_COUNT = 100
MULTI_FILE_ROWS = 500
PAGINATION_ROWS = 2_000
PAGINATION_LIMIT = 100
REVISION_IDENTITIES = 2_000
REVISIONS_PER_IDENTITY = 3
REPLAY_ROWS = 20_000


# -- corpus -----------------------------------------------------------------


@dataclass
class Corpus:
    root: Path
    rows: dict[str, int] = field(default_factory=dict)
    files: dict[str, int] = field(default_factory=dict)
    bytes: dict[str, int] = field(default_factory=dict)


def _dataset_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*.parquet"))


def _write_revision_bars(store: ParquetBarStore, count: int) -> None:
    bars: list[Bar] = []
    for identity in range(count):
        open_ts = BASE + timedelta(minutes=identity)
        for revision in range(REVISIONS_PER_IDENTITY):
            bars.append(
                Bar(
                    schema_version=1,
                    data_type="bar",
                    instrument_id=INSTRUMENT,
                    interval=INTERVAL,
                    open_ts=open_ts,
                    close_ts=open_ts + INTERVALS[INTERVAL],
                    event_ts=open_ts + INTERVALS[INTERVAL],
                    ingest_ts=open_ts + INTERVALS[INTERVAL]
                    + timedelta(milliseconds=revision),
                    feed="bench",
                    seq=None,
                    aggregation_source="external",
                    open=Decimal("100") + Decimal(revision) / Decimal("10"),
                    high=Decimal("101"),
                    low=Decimal("99"),
                    close=Decimal("100.5"),
                    volume=Decimal("1"),
                    trade_count=1,
                    revision=revision,
                )
            )
    # Multiple Data Files so pagination crosses file boundaries too.
    chunk = len(bars) // 3 + 1
    for start in range(0, len(bars), chunk):
        store.write_bars(bars[start : start + chunk], asset_class="crypto")


def build_corpus(data_dir: Path, *, rebuild: bool = False) -> Corpus:
    marker = data_dir / "_corpus.json"
    if marker.exists() and not rebuild:
        payload = json.loads(marker.read_text(encoding="utf-8"))
        return Corpus(
            root=Path(payload["root"]),
            rows=payload["rows"],
            files=payload["files"],
            bytes=payload["bytes"],
        )

    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True)
    corpus = Corpus(root=data_dir)

    def build(name: str, rows: int, writer: Callable[[ParquetBarStore], int]) -> None:
        store = ParquetBarStore(data_dir / name)
        file_count = writer(store)
        corpus.rows[name] = rows
        corpus.files[name] = file_count
        corpus.bytes[name] = _dataset_bytes(data_dir / name)

    def single_file(rows: int):
        def write(store: ParquetBarStore) -> int:
            store.write_bars(
                make_bars(rows, instrument_id=INSTRUMENT, interval=INTERVAL),
                asset_class="crypto",
            )
            return 1

        return write

    def multi_file() -> int:
        store = ParquetBarStore(data_dir / "multi")
        start = BASE
        for _ in range(MULTI_FILE_COUNT):
            store.write_bars(
                make_bars(
                    MULTI_FILE_ROWS,
                    instrument_id=INSTRUMENT,
                    interval=INTERVAL,
                    start=start,
                ),
                asset_class="crypto",
            )
            start += timedelta(minutes=MULTI_FILE_ROWS)
        return MULTI_FILE_COUNT

    build("query", QUERY_ROWS, single_file(QUERY_ROWS))
    build("scan", SCAN_ROWS, single_file(SCAN_ROWS))
    build("pagination", PAGINATION_ROWS, single_file(PAGINATION_ROWS))
    build("replay", REPLAY_ROWS, single_file(REPLAY_ROWS))
    build(
        "multi",
        MULTI_FILE_COUNT * MULTI_FILE_ROWS,
        lambda _store: multi_file(),
    )
    revision_rows = REVISION_IDENTITIES * REVISIONS_PER_IDENTITY

    def write_revisions(store: ParquetBarStore) -> int:
        _write_revision_bars(store, REVISION_IDENTITIES)
        return len(list((store.dataset_root).rglob("*.parquet")))

    build("revisions", revision_rows, write_revisions)

    marker.write_text(
        json.dumps(
            {
                "root": str(data_dir),
                "rows": corpus.rows,
                "files": corpus.files,
                "bytes": corpus.bytes,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return corpus


# -- runtime / HTTP ---------------------------------------------------------


@contextmanager
def running_runtime(data_root: Path):
    config = load_config(CONFIG_PATH)
    config = config.model_copy(
        update={
            "server": config.server.model_copy(update={"http_port": 0}),
            "instruments": config.instruments.model_copy(
                update={"path": INSTRUMENTS_PATH}
            ),
        }
    )
    feed = FakeFeedHandler(instrument_id=INSTRUMENT, bars=())
    runtime = GatewayRuntime(
        feed_handler=feed, config=config, data_root=data_root
    )
    runtime.start()
    try:
        yield runtime
    finally:
        runtime.stop()


def _url(base_url: str, path: str, **query: object) -> str:
    return f"{base_url}{path}?{urllib.parse.urlencode(query)}"


def _fetch(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=300) as response:
        return response.read()


def _fetch_json(url: str) -> dict:
    return json.loads(_fetch(url))


def _walk_pages(
    base_url: str,
    *,
    limit: int,
    max_pages: int = 10_000,
) -> tuple[list[str], int, int]:
    """Follow `next_start`; return (open_ts values, pages, duplicate count)."""
    seen: list[str] = []
    pages = 0
    cursor: Optional[str] = None
    while pages < max_pages:
        query: dict[str, object] = {
            "interval": INTERVAL,
            "instrument": INSTRUMENT,
            "limit": limit,
        }
        if cursor is not None:
            query["start"] = cursor
        payload = _fetch_json(_url(base_url, "/api/v1/bars", **query))
        pages += 1
        seen.extend(bar["payload"]["open_ts"] for bar in payload["bars"])
        page = payload["page"]
        if not page["has_more"]:
            return seen, pages, len(seen) - len(set(seen))
        cursor = page["next_start"]
    return seen, pages, len(seen) - len(set(seen))


# -- measurement ------------------------------------------------------------


@dataclass
class Measurement:
    name: str
    unit: str
    durations: list[float]
    rows: int | None = None
    notes: dict[str, object] = field(default_factory=dict)
    memory_peak_bytes: int | None = None
    arrow_native_peak_bytes: int | None = None

    @property
    def median(self) -> float:
        return statistics.median(self.durations)

    @property
    def best(self) -> float:
        return min(self.durations)

    @property
    def worst(self) -> float:
        return max(self.durations)

    @property
    def stdev(self) -> float:
        return statistics.stdev(self.durations) if len(self.durations) > 1 else 0.0

    @property
    def spread_pct(self) -> float:
        return (
            0.0
            if self.median == 0
            else (self.worst - self.best) / self.median * 100.0
        )


def _time_scenario(
    name: str,
    unit: str,
    run: Callable[[], object],
    *,
    repeat: int,
    rows: int | None = None,
) -> Measurement:
    run()  # warm-up: page cache and library init excluded
    durations: list[float] = []
    for _ in range(repeat):
        started = time.perf_counter()
        run()
        durations.append(time.perf_counter() - started)
    measurement = Measurement(
        name=name, unit=unit, durations=durations, rows=rows
    )
    # One extra pass under tracemalloc; never mixed into the timed samples.
    _memory_scenario(measurement, run)
    return measurement


def _memory_scenario(measurement: Measurement, run: Callable[[], object]) -> None:
    import threading

    gc.collect()
    arrow_before = pa.total_allocated_bytes()
    arrow_peak = [arrow_before]
    stop = threading.Event()

    def sample_arrow() -> None:
        # `total_allocated_bytes` is a point-in-time value, so sample it
        # instead of only reading it after the work (when buffers are freed).
        while not stop.wait(0.001):
            arrow_peak[0] = max(arrow_peak[0], pa.total_allocated_bytes())

    sampler = threading.Thread(target=sample_arrow, daemon=True)
    sampler.start()
    tracemalloc.start()
    tracemalloc.reset_peak()
    try:
        run()
        _, peak = tracemalloc.get_traced_memory()
    finally:
        stop.set()
        sampler.join(timeout=5)
        tracemalloc.stop()
    measurement.memory_peak_bytes = peak
    measurement.arrow_native_peak_bytes = max(0, arrow_peak[0] - arrow_before)


def _rate(measurement: Measurement) -> str:
    return (
        "n/a"
        if measurement.rows is None
        else format_rate(measurement.rows, measurement.median)
    )


# -- scenarios --------------------------------------------------------------


def run_store_scenarios(
    corpus: Corpus, measurements: list[Measurement], repeat: int
) -> None:
    scan_store = ParquetBarStore(corpus.root / "scan")
    query_store = ParquetBarStore(corpus.root / "query")
    multi_store = ParquetBarStore(corpus.root / "multi")
    revision_store = ParquetBarStore(corpus.root / "revisions")

    def scan(store: ParquetBarStore):
        return sum(
            batch.num_rows
            for batch in store.iter_batches(
                instrument_id=INSTRUMENT, interval=INTERVAL
            )
        )

    measurements.append(
        _time_scenario(
            "store.scan_100k",
            "rows",
            lambda: scan(scan_store),
            repeat=repeat,
            rows=corpus.rows["scan"],
        )
    )
    measurements.append(
        _time_scenario(
            "store.count_100k",
            "rows",
            lambda: scan_store.count_bars(
                instrument_id=INSTRUMENT, interval=INTERVAL
            ),
            repeat=repeat,
            rows=corpus.rows["scan"],
        )
    )
    measurements.append(
        _time_scenario(
            "store.read_bars_50k",
            "rows",
            lambda: query_store.read_bars(
                instrument_id=INSTRUMENT, interval=INTERVAL
            ),
            repeat=repeat,
            rows=corpus.rows["query"],
        )
    )
    measurements.append(
        _time_scenario(
            "store.scan_multi_50k",
            "rows",
            lambda: scan(multi_store),
            repeat=repeat,
            rows=corpus.rows["multi"],
        )
    )
    measurements.append(
        _time_scenario(
            "store.count_multi_50k",
            "rows",
            lambda: multi_store.count_bars(
                instrument_id=INSTRUMENT, interval=INTERVAL
            ),
            repeat=repeat,
            rows=corpus.rows["multi"],
        )
    )
    measurements.append(
        _time_scenario(
            "store.scan_revisions_6k",
            "rows",
            lambda: scan(revision_store),
            repeat=repeat,
            rows=corpus.rows["revisions"],
        )
    )
    measurements.append(
        _time_scenario(
            "store.count_revisions_6k",
            "rows",
            lambda: revision_store.count_bars(
                instrument_id=INSTRUMENT, interval=INTERVAL
            ),
            repeat=repeat,
            rows=corpus.rows["revisions"],
        )
    )


def run_rest_scenarios(
    corpus: Corpus, measurements: list[Measurement], repeat: int
) -> dict[str, object]:
    notes: dict[str, object] = {}
    with running_runtime(corpus.root / "query") as runtime:
        base = runtime.base_url
        json_url = _url(
            base, "/api/v1/bars", interval=INTERVAL, instrument=INSTRUMENT
        )
        arrow_url = _url(
            base,
            "/api/v1/bars",
            interval=INTERVAL,
            instrument=INSTRUMENT,
            format="arrow",
        )
        csv_url = _url(
            base,
            "/api/v1/bars",
            interval=INTERVAL,
            instrument=INSTRUMENT,
            format="csv",
        )
        parquet_url = _url(
            base,
            "/api/v1/bars/export",
            interval=INTERVAL,
            instrument=INSTRUMENT,
            format="parquet",
        )
        arrow_export_url = _url(
            base,
            "/api/v1/bars/export",
            interval=INTERVAL,
            instrument=INSTRUMENT,
            format="arrow",
        )
        measurements.append(
            _time_scenario(
                "rest.json_50k",
                "rows",
                lambda: _fetch(json_url),
                repeat=repeat,
                rows=corpus.rows["query"],
            )
        )
        measurements.append(
            _time_scenario(
                "rest.arrow_50k",
                "rows",
                lambda: _fetch(arrow_url),
                repeat=repeat,
                rows=corpus.rows["query"],
            )
        )
        measurements.append(
            _time_scenario(
                "rest.csv_50k",
                "rows",
                lambda: _fetch(csv_url),
                repeat=repeat,
                rows=corpus.rows["query"],
            )
        )
        export = _time_scenario(
            "rest.export_parquet_50k",
            "rows",
            lambda: _fetch(parquet_url),
            repeat=repeat,
            rows=corpus.rows["query"],
        )
        measurements.append(export)
        measurements.append(
            _time_scenario(
                "rest.export_arrow_50k",
                "rows",
                lambda: _fetch(arrow_export_url),
                repeat=repeat,
                rows=corpus.rows["query"],
            )
        )
        notes["export_parquet_bytes"] = len(_fetch(parquet_url))
        notes["export_arrow_bytes"] = len(_fetch(arrow_export_url))

    with running_runtime(corpus.root / "pagination") as runtime:
        base = runtime.base_url
        rows, pages, duplicates = _walk_pages(base, limit=PAGINATION_LIMIT)
        assert pages == (corpus.rows["pagination"] + PAGINATION_LIMIT - 1) // (
            PAGINATION_LIMIT
        )
        assert duplicates == 0
        measurement = _time_scenario(
            "rest.pagination_2k_limit100",
            "full walk",
            lambda: _walk_pages(base, limit=PAGINATION_LIMIT),
            repeat=repeat,
            rows=len(rows),
        )
        measurement.notes = {"pages": pages, "duplicate_open_ts": duplicates}
        measurements.append(measurement)

    with running_runtime(corpus.root / "revisions") as runtime:
        base = runtime.base_url
        revision_json = _url(
            base, "/api/v1/bars", interval=INTERVAL, instrument=INSTRUMENT
        )
        measurements.append(
            _time_scenario(
                "rest.revision_json_6k",
                "rows",
                lambda: _fetch(revision_json),
                repeat=repeat,
                rows=corpus.rows["revisions"],
            )
        )
        rows, pages, duplicates = _walk_pages(base, limit=PAGINATION_LIMIT)
        measurement = _time_scenario(
            "rest.revision_pagination_6k_limit100",
            "full walk",
            lambda: _walk_pages(base, limit=PAGINATION_LIMIT),
            repeat=repeat,
            rows=len(rows),
        )
        # Current Time Cursor behaviour: repeated revisions at page
        # boundaries are expected; ticket 03 owns the contract change.
        measurement.notes = {
            "pages": pages,
            "rows_returned": len(rows),
            "stored_rows": corpus.rows["revisions"],
            "extra_rows_returned": len(rows) - corpus.rows["revisions"],
            "repeated_open_ts_values": duplicates,
            "distinct_open_ts": len(set(rows)),
        }
        measurements.append(measurement)
        notes["revision_pagination"] = measurement.notes
    return notes


def run_replay_scenarios(
    corpus: Corpus, measurements: list[Measurement], repeat: int
) -> dict[str, object]:
    store = ParquetBarStore(corpus.root / "replay")
    bars = make_bars(REPLAY_ROWS, instrument_id=INSTRUMENT, interval=INTERVAL)
    start_ns = datetime_to_ns(bars[0].open_ts)
    end_ns = datetime_to_ns(bars[-1].close_ts) + 1
    subscription = BarSubscription(INSTRUMENT, INTERVAL)
    source = ReplayBarSource(store)
    encoded: list[int] = [0]

    snapshot_query = BarReadQuery(
        instrument_id=subscription.instrument_id,
        interval=subscription.interval,
        start_ns=start_ns - interval_ns(subscription.interval),
        end_ns=end_ns,
    )

    def query_prep():
        # Production path: fixed Snapshot + Lease, DuckDB scan, Revision
        # dedupe and five-key order, then the M5A full materialization.
        with store.open_read(
            queries=(snapshot_query,), lease_id="bench-baseline-replay"
        ) as read:
            result = source.read(
                [subscription],
                start_ts_ns=start_ns,
                end_ts_ns=end_ns,
                read=read,
            )
        encoded[0] = len(result)
        return result

    prepared = query_prep()
    measurements.append(
        _time_scenario(
            "replay.query_prep_20k",
            "rows",
            query_prep,
            repeat=repeat,
            rows=len(prepared),
        )
    )

    def encode_all() -> int:
        total = 0
        for bar in prepared:
            total += len(table_to_ipc(bars_to_logical_table([bar])))
        return total

    measurements.append(
        _time_scenario(
            "replay.ipc_encode_20k",
            "rows",
            encode_all,
            repeat=repeat,
            rows=len(prepared),
        )
    )

    def session_run() -> int:
        service = ReplayService(
            store,
            publish_bar=lambda _sid, bar: table_to_ipc(
                bars_to_logical_table([bar])
            ),
            max_sessions=1,
        )
        spec = ReplaySessionSpec(
            subscriptions=(subscription,),
            start_ts_ns=start_ns,
            end_ts_ns=end_ns,
            replay_mode="asap",
            speed=1.0,
        )
        try:
            session_id = service.create_session(spec)
            assert service.wait_session(session_id, timeout=300)
            return service.session_bars_sent(session_id)
        finally:
            service.close()

    sent = session_run()
    measurements.append(
        _time_scenario(
            "replay.e2e_publish_20k",
            "rows",
            session_run,
            repeat=repeat,
            rows=sent,
        )
    )
    return {
        "replay_query_rows": len(prepared),
        "replay_e2e_bars_sent": sent,
    }


# -- reporting --------------------------------------------------------------


def _environment() -> dict[str, str]:
    import fastapi
    import uvicorn
    import zmq

    return {
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "pyarrow": pa.__version__,
        "fastapi": fastapi.__version__,
        "uvicorn": uvicorn.__version__,
        "pyzmq": zmq.__version__,
        "processor": platform.processor() or "unknown",
    }


def _format_measurement(measurement: Measurement) -> str:
    memory = (
        "n/a"
        if measurement.memory_peak_bytes is None
        else format_bytes(measurement.memory_peak_bytes)
    )
    arrow_peak = (
        "n/a"
        if measurement.arrow_native_peak_bytes is None
        else format_bytes(measurement.arrow_native_peak_bytes)
    )
    return (
        f"| `{measurement.name}` | {measurement.rows or 0:,} | "
        f"{measurement.median:.4f} | {measurement.best:.4f} | "
        f"{measurement.worst:.4f} | {measurement.stdev:.4f} | "
        f"{measurement.spread_pct:.1f}% | {_rate(measurement)} | "
        f"{memory} | {arrow_peak} |"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repeat", type=int, default=5, help="timed passes per scenario"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(tempfile.gettempdir()) / "mdgw-duckdb-baseline",
        help="deterministic corpus directory (reused between runs)",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="rebuild the corpus even when _corpus.json exists",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="also write raw measurements as JSON",
    )
    args = parser.parse_args(argv)
    if args.repeat <= 0:
        parser.error("--repeat must be positive")
    # Windows consoles are not always UTF-8; keep the benchmark printable.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")

    corpus = build_corpus(args.data_dir, rebuild=args.rebuild)
    measurements: list[Measurement] = []
    run_store_scenarios(corpus, measurements, args.repeat)
    rest_notes = run_rest_scenarios(corpus, measurements, args.repeat)
    replay_notes = run_replay_scenarios(corpus, measurements, args.repeat)

    environment = _environment()
    temp_usage = shutil.disk_usage(tempfile.gettempdir())

    print("# DuckDB/Polars storage baseline (current PyArrow implementation)")
    print()
    print("## Environment")
    print()
    for key, value in environment.items():
        print(f"- {key}: {value}")
    print(f"- tempdir: `{tempfile.gettempdir()}`")
    print(
        f"- temp disk: {format_bytes(temp_usage.used)} used / "
        f"{format_bytes(temp_usage.total)} total "
        f"({format_bytes(temp_usage.free)} free)"
    )
    print()
    print("## Corpus")
    print()
    print("| Dataset | Rows | Data Files | Parquet bytes |")
    print("|---|---:|---:|---:|")
    for name in sorted(corpus.rows):
        print(
            f"| `{name}` | {corpus.rows[name]:,} | {corpus.files[name]:,} | "
            f"{format_bytes(corpus.bytes[name])} |"
        )
    print()
    print("## Measurements")
    print()
    print(
        "| Scenario | Rows | Median s | Min s | Max s | Stdev s | Spread | "
        "Throughput | tracemalloc peak | Arrow native peak |"
    )
    print(
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    )
    for measurement in measurements:
        print(_format_measurement(measurement))
    print()
    print("## Scenario notes")
    print()
    for measurement in measurements:
        for note, value in measurement.notes.items():
            print(f"- `{measurement.name}` {note}: {value}")
    print()
    print("## REST / Replay notes")
    print()
    print(f"- export parquet temp-file bytes: {rest_notes['export_parquet_bytes']:,}")
    print(f"- export arrow temp-file bytes: {rest_notes['export_arrow_bytes']:,}")
    for key, value in rest_notes.items():
        if key.startswith("revision_pagination"):
            print(f"- {key}: {value}")
    print(f"- replay query rows: {replay_notes['replay_query_rows']:,}")
    print(f"- replay bars sent: {replay_notes['replay_e2e_bars_sent']:,}")
    print()
    print(
        "`tracemalloc` covers Python allocations only. The Arrow native peak "
        "comes from sampling `pa.total_allocated_bytes()` during the pass; it "
        "is a sampled peak, not a full allocator trace. Export writes a "
        "complete temporary file before the response, so its bytes are the "
        "peak temporary-disk addition."
    )

    if args.json_out is not None:
        payload = {
            "environment": environment,
            "corpus": {
                "root": str(corpus.root),
                "rows": corpus.rows,
                "files": corpus.files,
                "bytes": corpus.bytes,
            },
            "measurements": [
                {
                    "name": item.name,
                    "unit": item.unit,
                    "rows": item.rows,
                    "durations_s": item.durations,
                    "median_s": item.median,
                    "min_s": item.best,
                    "max_s": item.worst,
                    "stdev_s": item.stdev,
                    "spread_pct": item.spread_pct,
                    "notes": item.notes,
                    "tracemalloc_peak_bytes": item.memory_peak_bytes,
                    "arrow_native_peak_bytes": item.arrow_native_peak_bytes,
                }
                for item in measurements
            ],
            "rest_notes": rest_notes,
            "replay_notes": replay_notes,
        }
        args.json_out.write_text(
            json.dumps(payload, indent=2) + "\n", encoding="utf-8"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
