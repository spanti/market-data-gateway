# DuckDB/Polars storage migration — M0 performance baseline

This is the pre-migration baseline for ticket 01, measured on the current
PyArrow/Parquet implementation. It exists so later tickets can compare the
same workloads and data copy on the same machine. It is **not** an external
SLA and the absolute numbers are not portable.

## Environment and method

- Machine: Windows 11 (10.0.26200), AMD64 Family 25 Model 97 Stepping 2.
- Python 3.13.18, pyarrow 26.0.0, fastapi 0.162.0, uvicorn 0.54.0,
  pyzmq 27.2.0.
- Interpreter: `D:/Anaconda3/envs/nautilus/python.exe`.
- Command:

  ```bash
  D:/Anaconda3/envs/nautilus/python.exe benchmarks/bench_baseline.py \
      --repeat 5 --json-out benchmarks/.baseline-raw.json
  ```

- One deterministic corpus is built once and reused for every pass:
  `<tempdir>/mdgw-duckdb-baseline` (this run:
  `C:\Users\10107\AppData\Local\Temp\mdgw-duckdb-baseline`). The generator is
  `benchmarks/bench_baseline.py`; Corpus rows are fixed constants. The same
  copy was used for all 5 repetitions and can be reused by later tickets
  (pass `--data-dir` to share it; `--rebuild` regenerates it).
- Each scenario runs one warm-up pass, then 5 timed passes. Reported is the
  median with min/max, standard deviation and `(max-min)/median` spread.
- After the timed passes, one extra untimed pass records memory:
  `tracemalloc` peak covers Python allocations; the Arrow native peak is a
  1 ms sample of `pa.total_allocated_bytes()`. Arrow native memory is not
  covered by `tracemalloc`, and DuckDB native memory will need the same
  sampled-allocator treatment in later baselines.
- REST measurements use the real ASGI app through `GatewayRuntime` on an
  ephemeral port. The timed call reads the complete response body (JSON is
  not decoded; Arrow/CSV/export bytes are consumed). Memory for REST also
  includes the response bytes held by the benchmark client, so it is an upper
  bound for server-side encoding cost, not a server-only figure.
- Replay query preparation uses `ReplayBarSource.read` (physical scan +
  `close_ts` filter + Revision dedupe + five-key sort). IPC encoding uses the
  same `table_to_ipc(bars_to_logical_table([bar]))` call as the Replay
  transport adapter. End-to-end uses `ReplayService` in asap mode with that
  IPC encoder as the publisher, so the query, encode and session/thread cost
  stay separable.

## Corpus

| Dataset | Rows | Data Files | Parquet bytes | Used by |
|---|---:|---:|---:|---|
| `query` | 50,000 | 1 | 1,833 KiB | Store read, REST all formats |
| `scan` | 100,000 | 1 | 3,639 KiB | Store scan/count |
| `multi` | 50,000 | 100 | 3,808 KiB | Multi-file Store scan/count |
| `pagination` | 2,000 | 1 | 105 KiB | REST Time Cursor walk |
| `revisions` | 6,000 (2,000 identities × 3 revisions) | 3 | 108 KiB | Revision-heavy raw/paginated REST |
| `replay` | 20,000 | 1 | 754 KiB | Replay query/encode/e2e |

Total corpus size: 10,721,112 bytes (~10.2 MiB). Temporary volume:
239.19 GiB used / 300.00 GiB total, 60.81 GiB free.

## Results (5 timed passes)

| Scenario | Rows | Median s | Min s | Max s | Stdev s | Spread | Throughput | tracemalloc peak | Arrow native peak (sampled) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `store.scan_100k` | 100,000 | 0.0199 | 0.0192 | 0.0218 | 0.0010 | 13.0% | 5,020,332 rows/s | 368.40 KiB | 31.52 MiB |
| `store.count_100k` | 100,000 | 0.0193 | 0.0182 | 0.0198 | 0.0006 | 8.0% | 5,174,243 rows/s | 368.31 KiB | 17.58 MiB |
| `store.read_bars_50k` | 50,000 | 0.0098 | 0.0094 | 0.0115 | 0.0009 | 21.7% | 5,121,062 rows/s | 364.69 KiB | 17.43 MiB |
| `store.scan_multi_50k` | 50,000 | 0.1017 | 0.0997 | 0.1043 | 0.0019 | 4.5% | 491,449 rows/s | 457.66 KiB | 3.76 MiB |
| `store.count_multi_50k` | 50,000 | 0.0986 | 0.0977 | 0.0990 | 0.0005 | 1.3% | 507,246 rows/s | 457.57 KiB | 3.63 MiB |
| `store.scan_revisions_6k` | 6,000 | 0.0042 | 0.0039 | 0.0043 | 0.0002 | 10.4% | 1,434,069 rows/s | 35.28 KiB | 0.00 B |
| `store.count_revisions_6k` | 6,000 | 0.0042 | 0.0041 | 0.0048 | 0.0003 | 16.3% | 1,411,798 rows/s | 35.23 KiB | 0.00 B |
| `rest.json_50k` | 50,000 | 1.5548 | 1.4672 | 1.6672 | 0.0932 | 12.9% | 32,158 rows/s | 131.23 MiB | 14.17 MiB |
| `rest.arrow_50k` | 50,000 | 0.0825 | 0.0760 | 0.0994 | 0.0117 | 28.5% | 605,861 rows/s | 23.64 MiB | 21.69 MiB |
| `rest.csv_50k` | 50,000 | 1.2805 | 1.2419 | 1.2967 | 0.0250 | 4.3% | 39,047 rows/s | 62.71 MiB | 21.69 MiB |
| `rest.export_parquet_50k` | 50,000 | 0.0790 | 0.0716 | 0.0808 | 0.0038 | 11.7% | 633,244 rows/s | 3.13 MiB | 29.57 MiB |
| `rest.export_arrow_50k` | 50,000 | 0.0849 | 0.0595 | 0.1087 | 0.0180 | 58.0% | 589,125 rows/s | 12.10 MiB | 21.69 MiB |
| `rest.pagination_2k_limit100` (full walk, 20 pages) | 2,000 | 0.3410 | 0.2862 | 0.3865 | 0.0406 | 29.4% | 5,866 rows/s | 2.01 MiB | 990.44 KiB |
| `rest.revision_json_6k` | 6,000 | 0.2082 | 0.1886 | 0.2248 | 0.0137 | 17.4% | 28,824 rows/s | 15.66 MiB | 1.06 MiB |
| `rest.revision_pagination_6k_limit100` (full walk, 61 pages) | 6,060 | 1.2554 | 1.1657 | 1.3279 | 0.0665 | 12.9% | 4,827 rows/s | 3.57 MiB | 1.70 MiB |
| `replay.query_prep_20k` | 20,000 | 1.0565 | 0.9141 | 1.0590 | 0.0738 | 13.7% | 18,930 rows/s | 40.69 MiB | 9.90 MiB |
| `replay.ipc_encode_20k` | 20,000 | 2.7933 | 2.7025 | 2.8455 | 0.0612 | 5.1% | 7,160 rows/s | 7.32 KiB | 6.50 KiB |
| `replay.e2e_publish_20k` | 20,000 | 3.7617 | 3.7435 | 3.9838 | 0.1200 | 6.4% | 5,317 rows/s | 40.70 MiB | 9.90 MiB |

Scenario notes from the same run:

- Regular pagination: 20 pages, no duplicate `open_ts` values.
- Revision-heavy pagination: 61 pages returned 6,060 rows for the 6,000 stored
  rows — 60 extra rows from page boundaries inside a same-`open_ts` revision
  group. This is the existing Time Cursor limitation owned by ticket 03, not a
  property this migration should preserve silently. The reusable fixture is
  `tests/characterization/dataset_builder.py::revision_heavy_dataset`; the
  current behaviour is frozen in
  `tests/characterization/test_rest_characterization.py::test_revision_heavy_pagination_repeats_the_same_page_today`.
- Export writes a complete temporary file before sending it. Peak temporary
  file sizes in this run: Parquet 1,921,194 bytes, Arrow 9,204,418 bytes. The
  files are deleted when the response body finishes.
- Replay cost split: query preparation 1.06 s, IPC/Python publishing 2.79 s,
  end-to-end 3.76 s. The difference between end-to-end and query (~2.7 s) is
  the per-Bar encode/publish path, not the DataFrame/Parquet scan.

## Interpretation and caveats

- The single-file Store scan is roughly 5.0 M rows/s (~20 ms per 100k rows).
  The 100-file case is ~10× slower per row (~0.10 s per 50k rows), so file-open
  and Manifest overhead dominate before any SQL engine is involved.
- JSON and CSV are dominated by per-row Python rendering, not the Parquet
  scan: 1.55 s and 1.28 s for 50k rows versus 0.08 s for Arrow. This is the
  main CPU path the migration must not regress by more than the agreed 10%
  throughput threshold.
- `rest.json_50k` allocates ~131 MiB of traced Python memory because the full
  `bars` message list and JSON text exist at once. This is a ceiling under the
  100,000-row contract; the migration is expected to preserve the response
  shape, not necessarily this allocation profile.
- Replay materialises all 20k Bars (40.69 MiB traced) before publishing.
  Ticket M5B replaces this with bounded Arrow batches; this baseline is the
  reference for proving the memory improvement without changing event output.
- Peaks from independent passes are not additive; the Arrow sampled peak in
  particular can miss very short-lived buffers.
- The 58% spread on `rest.export_arrow_50k` and 29% spread on pagination are
  expected on this Windows developer machine (filesystem and Python GC
  variance). Medians are the comparison point; later tickets should use the
  same machine, corpus and repetition count.
