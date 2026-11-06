# Benchmarks

Three small, self-contained benchmarks for the v1 Bar workload:

| Script | Measures |
|---|---|
| `bench_ingest.py` | writing Bars through `ParquetBarStore` (Arrow build, Parquet/zstd, Manifest update) |
| `bench_query.py` | direct `store.read_bars` / `iter_batches` / `count_bars`, or REST Arrow/JSON with `--base-url` |
| `bench_replay.py` | asap Replay Sessions (query preparation reported separately from read + deduplicate + order + Arrow IPC encode) |
| `bench_baseline.py` | the M0 migration matrix: Store scan/count, REST JSON/Arrow/CSV/export, pagination, multi-file, Revision-heavy and Replay query/IPC/e2e on one reusable corpus |

Each script seeds deterministic Bars into a `tempfile.TemporaryDirectory`, so
nothing is left behind and `./data` is never touched. Generation and setup are
excluded from the timed sections. Only `bench_query.py --base-url` touches the
network; it does so only when you pass that flag.

## Run

```bash
conda activate nautilus
pip install -e .

python benchmarks/bench_ingest.py --rows 50000 --repeat 3
python benchmarks/bench_ingest.py --rows 50000 --repeat 3 --aggregate

python benchmarks/bench_query.py --rows 50000 --repeat 5
python benchmarks/bench_query.py --base-url http://127.0.0.1:8080 \
    --instrument binance:BTC/USDT --interval 1m --repeat 5

python benchmarks/bench_replay.py --rows 50000 --repeat 3

python benchmarks/bench_baseline.py --repeat 5
```

All four accept `--help`. The first three stay well under a minute at the
default sizes; `bench_baseline.py` builds a deterministic corpus once
(`--data-dir`, reused between runs) and takes a few minutes because it covers
the whole M0 matrix, including memory passes.

## Interpreting results

These numbers are **not normative**. They describe one Windows developer
machine with a warm page cache and a no-op Replay publisher; absolute values
depend on CPU, disk, compression, row count and selected columns. Use them for
order-of-magnitude comparisons and for regression tracking on the same machine,
not as a service-level guarantee.

`bench_replay.py` deliberately drops every message at the publisher callback.
The measured path is the gateway's read/dedupe/order/encode work, not ZMQ or
network fan-out.

Measured results for this checkout are in [`RESULTS.md`](RESULTS.md).
The DuckDB/Polars migration baseline (same machine, reusable corpus, 5
repetitions, memory and temp-disk notes) is in
[`RESULTS-duckdb-baseline.md`](RESULTS-duckdb-baseline.md).
