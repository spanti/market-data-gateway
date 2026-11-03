# Benchmark results

Measured on the development machine below, **2026-11-03**. These numbers are
informational, not a service-level guarantee. See
[`README.md`](README.md) for methodology and caveats.

## Environment

| Item | Value |
|---|---|
| OS | Windows 11 Home China, 10.0.26200 |
| CPU | AMD Ryzen 7 7745HX with Radeon Graphics |
| Memory | 32 GB (33,475,293,184 bytes) |
| Python | 3.13.18 (Anaconda) |
| Environment | conda `nautilus` |
| pyarrow | 26.0.0 |
| Storage | local NTFS SSD |
| Data | deterministic in-memory Bars written to a temporary directory |

Each script was run from the repository root with `PYTHONPATH=src`, for
example:

```bash
conda run -n nautilus python benchmarks/bench_ingest.py --rows 50000 --repeat 3
conda run -n nautilus python benchmarks/bench_query.py --rows 50000 --repeat 5
conda run -n nautilus python benchmarks/bench_replay.py --rows 50000 --repeat 3
```

Generation, temporary-directory creation and Parquet seeding (for query and
replay) are outside the timed sections. Timings are wall-clock
`time.perf_counter()`, reported as best and median of the passes.

## Ingestion

`ParquetBarStore.write_bars`, `zstd`, one 1m Instrument, 50,000 Bars per pass,
3 passes.

| Path | Rows written | Best | Median | Bytes per pass | Files |
|---|---:|---:|---:|---:|---:|
| Bars only | 50,000 | 0.500 s | 0.515 s | 1.83 MiB | 2 |
| Bars + Aggregator parents | 64,200 | 0.660 s | 0.662 s | 2.36 MiB | 10 |

Rates: **~100,000 Bars/s** for Bars only; **~97,000 Bars/s** including
aggregation output. The parent rows are the extra 14,200 Bars (5m/15m/1h/1d).

## Query (direct Parquet)

`ParquetBarStore.read_bars` and `iter_batches`, 50,000 Bars, warm cache,
5 passes.

| Method | Best | Median | Throughput (best) |
|---|---:|---:|---:|
| `read_bars` | 0.007 s | 0.007 s | ~6,990,000 rows/s |
| `iter_batches` | 0.007 s | 0.007 s | ~7,230,000 rows/s |

At this size the scan is dominated by fixed per-call overhead; the result
demonstrates that a 50k-row slice is sub-10 ms, not a sustained streaming
rate. Larger ranges scale with decoded bytes and available memory.

### REST query

The same `bench_query.py --base-url` path was run against the local demo
gateway (api on loopback, `config/gateway.demo.toml`, 50,020 Bars for
BTC/USDT), 5 passes. HTTP transfer and client decoding are included.

| Format | Rows | Best | Median | Throughput (best) |
|---|---:|---:|---:|---:|
| Arrow IPC | 50,020 | 0.047 s | 0.062 s | ~1,060,000 rows/s |
| JSON | 50,020 | 1.638 s | 1.691 s | ~30,500 rows/s |

JSON is much slower because every row is rendered as a Message Envelope with
string decimals and timestamps; prefer `format=arrow` for bulk history.

## Replay

`ReplayService` + `ReplaySessionSpec`, `replay_mode="asap"`, no-op publisher
(the session still encodes one Arrow IPC stream per Bar), 50,000 Bars,
3 passes.

| Passes | Bars sent | Best | Median | Throughput (best) |
|---|---:|---:|---:|---:|
| 3 | 50,000 | 9.692 s | 10.020 s | ~5,160 Bars/s |

The asap replay path is limited by per-Bar Arrow IPC encoding and Python
object work, roughly **5,000 Bars/s**. ZMQ fan-out and network transfer are
excluded; a real publisher adds the transport cost on top.
