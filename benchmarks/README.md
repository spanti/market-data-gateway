# Benchmarks

Three small, self-contained benchmarks for the v1 Bar workload:

| Script | Measures |
|---|---|
| `bench_ingest.py` | writing Bars through `ParquetBarStore` (Arrow build, Parquet/zstd, Manifest update) |
| `bench_query.py` | direct `store.read_bars` / `iter_batches`, or REST Arrow/JSON with `--base-url` |
| `bench_replay.py` | asap Replay Sessions (read + deduplicate + order + Arrow IPC encode) |

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
```

All three accept `--help`. Each run is intended to stay well under a minute at
the default sizes.

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
