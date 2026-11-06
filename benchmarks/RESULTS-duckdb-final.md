# DuckDB/Polars storage migration — final acceptance (ticket 19)

**Status:** final acceptance for the DuckDB/Polars storage migration (M6).
**Date:** 2026-11-06.
**Branch:** `ticket/19-cross-platform-performance-acceptance` (worktree
`E:/total-project/mdgw-worktrees/duckdb-19`), based on integration tip
`aefb447`.

This report is the auditable conclusion of the migration. It records the
implemented fix for the archive Rule Version lineage defect, the complete
offline suites on Windows and Linux, the golden type/contract results, the
performance comparison against the saved M0 PyArrow baseline on the same
machine and corpus, the bounded-memory proof, failure behaviour, and the final
code audit.

The performance thresholds are migration review gates, **not an external SLA**.
The independent Revision-heavy Time Cursor contract gap remains open and is not
an acceptance condition of this migration.

## 1. Versions and platforms

| Item | Windows (measurement host) | Linux (test container) |
|---|---|---|
| OS | Windows 11 Home China 10.0.26200 | `python:3.13-slim` (Docker, Linux) |
| CPU | AMD Ryzen 7 7745HX (AMD64 Family 25 Model 97) | host CPU via Docker |
| Python | 3.13.18 (conda `nautilus`) | 3.13.18 (slim image) |
| pyarrow | 26.0.0 | 26.0.1 |
| duckdb | 2.0.0 | 2.0.0 |
| polars | 2.1.0 | 2.1.0 |
| fastapi / uvicorn | 0.162.0 / 0.54.0 | resolved by `pip install -e '.[test]'` |
| pyzmq | 27.2.0 | resolved by `pip install -e '.[test]'` |

Linux versions were probed inside the same `python:3.13-slim` image
(`benchmarks/.acceptance-logs/linux-versions.log`). pyarrow differs by one
patch release between the Windows measurement host and the Linux test image;
all contract suites pass on both.

Measurement host interpreter:
`D:/Anaconda3/envs/nautilus/python.exe`.

## 2. Test suites

### 2.1 Windows full offline suite

```bash
D:/Anaconda3/envs/nautilus/python.exe -m pytest -q
```

**Result: 1080 passed, 5 failed, 1 skipped, 1 deselected in 205.42 s.**
(1080 = the previous 1078 plus the two new archive Rule Version cases.)

The 5 failures are the pre-existing Typer/Click CLI issues known since ticket
01 and are unchanged:

- `tests/integration/test_archive_backfill.py::test_cli_backfill_requires_archive_mode`
- `tests/integration/test_archive_backfill.py::test_cli_backfill_rejects_unsupported_venue_and_market_type`
- `tests/integration/test_archive_backfill.py::test_cli_backfill_requires_start`
- `tests/integration/test_archive_backfill.py::test_cli_backfill_rejects_a_missing_archive_dir`
- `tests/unit/test_cli_flags.py::test_run_rejects_an_unknown_service_role`

The single skip is the POSIX-only
`tests/contract/test_storage_file_lifecycle.py::test_posix_unlink_of_open_file_keeps_stream_readable`;
it is executed on Linux below.

### 2.2 Linux full offline suite

```bash
MSYS_NO_PATHCONV=1 docker run --rm \
  -v "E:/total-project/mdgw-worktrees/duckdb-19:/app" -w /app \
  python:3.13-slim bash -lc \
  "pip install --quiet -e '.[test]' pandas httpx psutil && python -m pytest -q"
```

`pandas`, `httpx` and `psutil` are installed explicitly because the base
`[test]` extra does not declare these optional test imports (pyarrow
`to_pandas`, Starlette `TestClient`, and the optional RSS/thread sampler).

**Result: 1075 passed, 6 failed, 3 skipped, 1 deselected in 181.18 s.**
Log: `benchmarks/.acceptance-logs/linux-full-suite-with-test-extras.log`
(local, git-ignored).

The 6 failures are 5 known pre-existing CLI failures (same list as Windows)
plus one known timing flake. The flake was rerun once in isolation on the same
Linux image and passed:

- `tests/unit/test_ccxt_feed_handler.py::test_malformed_live_row_stops_the_source_when_quarantine_write_fails`
  → **1 passed** in isolation.

Linux skips (all expected):

- `tests/unit/test_nautilus_macd_example.py` — `nautilus_trader` not installed
  in the slim image.
- `tests/integration/test_examples_docs_smoke.py` (nautilus case) — same.
- `tests/contract/test_storage_file_lifecycle.py::test_windows_delete_after_first_batch_is_permitted_by_duckdb`
  — records Windows file-sharing behaviour; only runs on Windows.

The POSIX-only lifecycle case **executed** on Linux (it is not in the skip
list).

An earlier Linux run without the optional packages produced the same known CLI
failures plus environment-only failures (`pandas` missing in `to_pandas`,
`httpx` missing in Starlette `TestClient`); both disappeared after installing
the packages above. That run is kept as
`benchmarks/.acceptance-logs/linux-full-suite.log` for audit.

### 2.3 Linux read-lease / lifecycle / maintenance subset

Run inside the same `python:3.13-slim` container and package set as §2.2:

```bash
python -m pytest -q \
  tests/contract/test_storage_file_lifecycle.py \
  tests/contract/test_read_lease.py \
  tests/contract/test_compaction_gc.py \
  tests/contract/test_session_locks.py \
  tests/integration/test_replay_read_lease.py \
  tests/integration/test_read_lease_api.py
```

**Result: 40 passed, 1 skipped in 6.56 s** (the skip is the Windows-only
file-sharing case). The POSIX unlink-of-open-file test, Compaction/GC lease
skips, session-lock expiry and REST/Replay lease release all ran for real.
Log: `benchmarks/.acceptance-logs/linux-targeted-lifecycle-and-flake.log`.

## 3. Archive Rule Version lineage fix

Ticket 18 recorded a defect: `mdgw backfill --archive` built the Store without
`rule_versions` and called `import_archive` without `rule_version`, so a
non-default `[storage.quarantine].rule_version` never reached archive Manifest
entries or Quarantine records.

Fix (`src/mdgw/cli.py`):

- `_build_store` now passes
  `rule_versions=[config.storage.quarantine.rule_version]`.
- the archive backfill call now passes
  `rule_version=config.storage.quarantine.rule_version` to `import_archive`.

Test (`tests/integration/test_archive_backfill.py`, TDD red→green):
`test_cli_archive_backfill_uses_the_configured_rule_version` is parametrized
over the default and `custom-2`. It runs the real CLI over one valid and one
checksum-corrupt archive and asserts:

- every active Manifest entry carries `rule_versions == (expected,)`;
- the Quarantine record carries `rule_version == expected`;
- the default case still stamps `polars-1`, so existing behaviour is unchanged.

Red run: `assert {('polars-1',)} == {('custom-2',)}`. Green run:
`5 passed` (the two new cases plus the existing lineage tests).

## 4. Golden and type acceptance

Command:

```bash
D:/Anaconda3/envs/nautilus/python.exe -m pytest -q \
  tests/contract/test_duckdb_compat.py \
  tests/contract/test_polars_compat.py \
  tests/contract/test_storage_file_lifecycle.py \
  tests/contract/test_data_contract.py \
  tests/contract/test_duckdb_arrow_read.py \
  tests/integration/test_duckdb_json_count_api.py \
  tests/integration/test_duckdb_csv_export.py \
  tests/integration/test_duckdb_arrow_api.py \
  tests/integration/test_bars_api.py \
  tests/integration/test_fastapi_bars.py \
  tests/characterization/test_rest_characterization.py \
  tests/integration/test_examples_docs_smoke.py::test_benchmarks_help_exits_zero
```

**Result: 172 passed, 1 skipped in 53.30 s.** The skip is the POSIX-only
Windows gap (executed on Linux in §2.3). Log:
`benchmarks/.acceptance-logs/windows-golden-type.log`.

| Contract property | Verified by |
|---|---|
| `decimal128(38,18)` exact, no float detour, over-scale rejected | `test_decimal128_38_18_roundtrip_is_exact`, `test_decimal_values_would_change_under_a_float64_detour`, `test_over_scale_decimal_is_rejected_at_the_typed_arrow_boundary`, `test_decimal128_38_18_roundtrip_through_polars_is_exact`, `test_polars_decimal_rescaling_is_a_validation_hazard` |
| UTC nanoseconds preserved, explicit UTC tz at the Arrow boundary | `test_utc_nanoseconds_roundtrip_exactly`, `test_arrow_export_needs_explicit_utc_timezone_config`, `test_polars_receives_utc_nanoseconds_without_truncation`, `test_duckdb_arrow_read_preserves_contract_types_and_nanoseconds`, `test_iso_timestamp_roundtrip_supports_nanoseconds_and_offsets` |
| `uint32` bounds and nullable integers (`seq`, `trade_count`) | `test_uint32_bounds_and_nullable_columns_roundtrip`, `test_uint32_bounds_and_nullable_integer_columns_through_polars` |
| Field order and nullability equal `BAR_SCHEMA` / golden JSON | `test_bar_schema_has_complete_v1_contract`, `test_complete_bar_schema_order_and_nullability_survive_the_pipeline`, `test_data_contract_schema_matches_golden_file`, `test_full_bar_schema_is_restorable_after_polars_roundtrip` |
| Hive `interval` restoration for every partition | `test_duckdb_arrow_read_restores_interval_for_every_partition` |
| JSON/CSV/export golden and parity behaviour | `test_data_contract.py`, `test_duckdb_json_count_api.py`, `test_duckdb_csv_export.py`, `test_bars_api.py`, `test_fastapi_bars.py`, `test_rest_characterization.py` |
| Empty result keeps the contract schema | `test_empty_result_keeps_contract_schema`, `test_empty_result_keeps_contract_dtypes` |

## 5. Performance versus the M0 baseline

Method: one deterministic corpus, reused from the M0 run
(`C:/Users/10107/AppData/Local/Temp/mdgw-duckdb-baseline`); one warm-up plus
**5 timed passes** per scenario; medians and min/max spread reported; one
additional untimed pass per scenario records `tracemalloc` and sampled Arrow
native peak. No results were discarded or cherry-picked.

```bash
D:/Anaconda3/envs/nautilus/python.exe benchmarks/bench_baseline.py \
  --repeat 5 \
  --data-dir "C:/Users/10107/AppData/Local/Temp/mdgw-duckdb-baseline" \
  --json-out benchmarks/.acceptance-logs/final-bench-raw.json
```

Raw output: `benchmarks/.acceptance-logs/final-bench.log` and
`final-bench-raw.json` (local, git-ignored).

The saved corpus was reused, not rebuilt. Its logical rows and row counts match
the M0 corpus exactly. Its physical Parquet bytes are ~2.4% larger than the M0
table (the corpus was regenerated during the migration with the current writer;
e.g. `query` 1,922,606 bytes now), so this is the same deterministic row set
but not byte-identical files. The M0 table's `Data Files` column also reports
the generator's constant (1 for single-write datasets), whereas the current
Manifest has e.g. 2 active files for `query`; all comparisons use the same
saved files for every pass. §5.4 reruns this exact copy after the review fixes
and states the active-byte accounting precisely.

### 5.1 Latency / throughput

| Scenario | Rows | M0 median s | Final median s | Delta | Final spread | Gate |
|---|---:|---:|---:|---:|---:|---|
| `store.scan_100k` | 100,000 | 0.0199 | 0.0220 | **+10.7%** | 10.6% | deviation D1 |
| `store.count_100k` | 100,000 | 0.0193 | 0.0059 | -69.3% | 64.9% | pass |
| `store.read_bars_50k` | 50,000 | 0.0098 | 0.0180 | **+83.3%** | 10.0% | deviation D1 |
| `store.scan_multi_50k` | 50,000 | 0.1017 | 0.0377 | -62.9% | 8.3% | pass |
| `store.count_multi_50k` | 50,000 | 0.0986 | 0.0212 | -78.5% | 10.0% | pass |
| `store.scan_revisions_6k` | 6,000 | 0.0042 | 0.0077 | **+84.4%** | 21.6% | deviation D1 |
| `store.count_revisions_6k` | 6,000 | 0.0042 | 0.0067 | **+59.6%** | 96.6% | deviation D1 |
| `rest.json_50k` | 50,000 | 1.5548 | 1.5174 | -2.4% | 31.4% | pass |
| `rest.arrow_50k` | 50,000 | 0.0825 | 0.0628 | -23.9% | 46.1% | pass |
| `rest.csv_50k` | 50,000 | 1.2805 | 1.3291 | +3.8% | 3.2% | pass |
| `rest.export_parquet_50k` | 50,000 | 0.0790 | 0.0696 | -11.9% | 18.8% | pass |
| `rest.export_arrow_50k` | 50,000 | 0.0849 | 0.0648 | -23.7% | 32.6% | pass |
| `rest.pagination_2k_limit100` (20 pages) | 2,000 | 0.3410 | 0.3427 | +0.5% | 7.6% | pass |
| `rest.revision_json_6k` | 6,000 | 0.2082 | 0.1909 | -8.3% | 14.0% | pass |
| `rest.revision_pagination_6k_limit100` (61 pages) | 6,060 | 1.2554 | 1.1382 | -9.3% | 23.5% | pass |
| `replay.query_prep_20k` | 20,000 | 1.0565 | 0.5413 | -48.8% | 17.8% | pass |
| `replay.ipc_encode_20k` | 20,000 | 2.7933 | 2.7175 | -2.7% | 3.9% | pass |
| `replay.e2e_publish_20k` | 20,000 | 3.7617 | 3.5658 | -5.2% | 21.7% | pass |

The full min/max/stdev values are in `final-bench-raw.json`; the table above
uses the same medians as the M0 report. All REST formats, pagination,
Revision-heavy reads, Replay query preparation and end-to-end publish are
within the 10% gate or faster. The Revision-heavy
walk still returns 6,060 rows for 6,000 stored rows (60 page-boundary repeats,
4,060 repeated `open_ts` values), exactly the frozen existing Time Cursor
behaviour; it is not "fixed" by the migration.

### 5.2 Peak memory and temporary disk

| Scenario | M0 traced peak | Final traced peak | Delta | M0 Arrow peak | Final Arrow peak |
|---|---:|---:|---:|---:|---:|
| `store.scan_100k` | 368.40 KiB | 374.71 KiB | +1.7% | 31.52 MiB | 88.19 KiB |
| `store.count_100k` | 368.31 KiB | 27.92 KiB | -92.4% | 17.58 MiB | 0.00 B |
| `store.read_bars_50k` | 364.69 KiB | 371.08 KiB | +1.8% | 17.43 MiB | 293.12 KiB |
| `store.scan_multi_50k` | 457.66 KiB | 295.08 KiB | -35.5% | 3.76 MiB | 72.50 KiB |
| `store.count_multi_50k` | 457.57 KiB | 269.30 KiB | -41.1% | 3.63 MiB | 0.00 B |
| `store.scan_revisions_6k` | 35.28 KiB | 40.27 KiB | +14.2% | 0.00 B | 0.00 B |
| `store.count_revisions_6k` | 35.23 KiB | 27.00 KiB | -23.4% | 0.00 B | 0.00 B |
| `rest.json_50k` | 131.23 MiB | 131.24 MiB | +0.0% | 14.17 MiB | 293.12 KiB |
| `rest.arrow_50k` | 23.64 MiB | 17.75 MiB | -24.9% | 21.69 MiB | 293.12 KiB |
| `rest.csv_50k` | 62.71 MiB | 65.92 MiB | +5.1% | 21.69 MiB | 261.62 KiB |
| `rest.export_parquet_50k` | 3.13 MiB | 3.63 MiB | +15.9% | 29.57 MiB | 6.69 MiB |
| `rest.export_arrow_50k` | 12.10 MiB | 14.13 MiB | +16.8% | 21.69 MiB | 0.00 B |
| `rest.pagination_2k_limit100` | 2.01 MiB | 1.61 MiB | -19.9% | 990.44 KiB | 704.00 B |
| `rest.revision_json_6k` | 15.66 MiB | 15.67 MiB | +0.1% | 1.06 MiB | 35.38 KiB |
| `rest.revision_pagination_6k_limit100` | 3.57 MiB | 2.92 MiB | -18.1% | 1.70 MiB | 704.00 B |
| `replay.query_prep_20k` | 40.69 MiB | 38.32 MiB | -5.8% | 9.90 MiB | 0.00 B |
| `replay.ipc_encode_20k` | 7.32 KiB | 7.32 KiB | 0.0% | 6.50 KiB | 5.44 KiB |
| `replay.e2e_publish_20k` | 40.70 MiB | 25.88 MiB | -36.4% | 9.90 MiB | 6.50 KiB |

No scenario increases a peak by more than 20%; the largest increases are
+16.8% (`rest.export_arrow_50k` traced) and +15.9%
(`rest.export_parquet_50k` traced), both inside the gate. Arrow native peaks
fall in every scan path because DuckDB streams bounded RecordBatches.

Temporary disk measured by the benchmark: export writes one complete temporary
file before sending — Parquet 1,921,158 bytes, Arrow 9,204,418 bytes in this
run (M0: 1,921,194 / 9,204,418). DuckDB sort spilling during replay is
reported separately in §6.

### 5.3 Approved deviations

**D1 — fixed per-call overhead on direct one-shot `ParquetBarStore` calls
(approved).**

Four direct Store scenarios exceed the 10% latency gate:
`store.read_bars_50k` +83.3% (9.8 ms → 18.0 ms),
`store.scan_revisions_6k` +84.4% (4.2 ms → 7.7 ms),
`store.count_revisions_6k` +59.6% (4.2 ms → 6.7 ms),
`store.scan_100k` +10.7% (19.9 ms → 22.0 ms).

Approval rationale:

- The regression is a fixed **+2 to +8 ms per call**, not a throughput
  regression. It is the cost of resolving a Manifest Snapshot, taking and
  heartbeating a Dataset Read Lease, and opening/reusing a DuckDB connection
  for every read. Large and multi-file scans improved 63–78% (`scan_multi`,
  `count_multi`) and `count_100k` improved 69%.
- The user-facing REST and Replay surfaces are not regressed: they improved or
  stayed inside the gate (JSON -2.4%, Arrow -23.9%, CSV +3.8%, export
  -11.9%/-23.7%, pagination +0.5%, Revision-heavy -8.3%/-9.3%, Replay
  preparation -48.8%, end-to-end -5.2%).
- Backfill is the only remaining production direct-Store caller; ticket 17
  measured its repeat pass at +7.7%, inside the gate.
- One Manifest-aware read semantics (Snapshot + Lease + DuckDB) is the
  migration mandate. A cheap fix would mean a second read path or an engine
  switch, both explicitly forbidden by the spec; no such fallback was added.
- Absolute latency remains < 22 ms for 50k–100k rows, and peak memory on those
  same paths fell sharply (Arrow native peak -98% or more).

No peak-memory deviation above 20% was needed. No other latency/throughput
deviation above 10% was observed.

### 5.4 Same-copy rerun after the review fixes

The review fixes (Rule Version default, read-lease lifetime, Replay publish
barrier, explicit DuckDB temp directory, connection-cache shutdown) do not
change the read query, Arrow boundary, REST serialization or Replay pacing
semantics. The saved corpus was rerun unchanged (no `--rebuild`) to prove the
§5 numbers reproduce on exactly the same copy:

```bash
D:/Anaconda3/envs/nautilus/python.exe benchmarks/bench_baseline.py \
  --repeat 5 \
  --data-dir "C:/Users/10107/AppData/Local/Temp/mdgw-duckdb-baseline" \
  --json-out benchmarks/.acceptance-logs/review-fixes-same-corpus.json
```

**Methodology correction.** "Same corpus as M0" means the same directory,
deterministic row set and active Manifest bytes — not byte-identical Parquet
files. The M0 report measured files written by the pre-migration writer; the
saved directory's active files were regenerated during the migration with the
current writer and are ~2.4% larger (`query` 1,922,606 active bytes in 2 active
files versus 1,833 KiB in the M0 table). `_corpus.json` correctly records the
active byte total but its `files` column is the generator's constant (1), and
one unreferenced Parquet file not listed in the Manifest remains under
`query/`; Manifest authority excludes it. The marker existed, so it did **not** prevent
reuse. All M0 deltas below are therefore row-wise comparisons against the M0
medians already recorded in `RESULTS-duckdb-baseline.md`, on the same logical
rows but not byte-identical physical files.

| Scenario | Rows | M0 median s | Rerun median s | Delta vs M0 | Rerun spread | M0 traced peak | Rerun traced peak | M0 Arrow peak | Rerun Arrow peak |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `store.scan_100k` | 100,000 | 0.0199 | 0.0223 | +12.1% (D1) | 9.1% | 368.40 KiB | 375.42 KiB | 31.52 MiB | 88.19 KiB |
| `store.count_100k` | 100,000 | 0.0193 | 0.0069 | -64.4% | 40.1% | 368.31 KiB | 27.80 KiB | 17.58 MiB | 0 B |
| `store.read_bars_50k` | 50,000 | 0.0098 | 0.0181 | +85.1% (D1) | 5.5% | 364.69 KiB | 371.94 KiB | 17.43 MiB | 293.12 KiB |
| `store.scan_multi_50k` | 50,000 | 0.1017 | 0.0376 | -63.0% | 4.5% | 457.66 KiB | 294.82 KiB | 3.76 MiB | 76.25 KiB |
| `store.count_multi_50k` | 50,000 | 0.0986 | 0.0222 | -77.5% | 22.5% | 457.57 KiB | 270.25 KiB | 3.63 MiB | 0 B |
| `store.scan_revisions_6k` | 6,000 | 0.0042 | 0.0072 | +72.5% (D1) | 16.7% | 35.28 KiB | 41.17 KiB | 0.00 B | 0 B |
| `store.count_revisions_6k` | 6,000 | 0.0042 | 0.0064 | +53.1% (D1) | 56.6% | 35.23 KiB | 26.80 KiB | 0.00 B | 0 B |
| `rest.json_50k` | 50,000 | 1.5548 | 1.5064 | -3.1% | 4.2% | 131.23 MiB | 131.24 MiB | 14.17 MiB | 293.12 KiB |
| `rest.arrow_50k` | 50,000 | 0.0825 | 0.0634 | -23.1% | 41.1% | 23.64 MiB | 17.75 MiB | 21.69 MiB | 261.62 KiB |
| `rest.csv_50k` | 50,000 | 1.2805 | 1.3415 | +4.8% | 4.1% | 62.71 MiB | 65.92 MiB | 21.69 MiB | 261.62 KiB |
| `rest.export_parquet_50k` | 50,000 | 0.0790 | 0.0620 | -21.6% | 37.4% | 3.13 MiB | 3.13 MiB | 29.57 MiB | 6.69 MiB |
| `rest.export_arrow_50k` | 50,000 | 0.0849 | 0.0674 | -20.7% | 46.7% | 12.10 MiB | 13.13 MiB | 21.69 MiB | 293.12 KiB |
| `rest.pagination_2k_limit100` (20 pages) | 2,000 | 0.3410 | 0.3410 | 0.0% | 24.1% | 2.01 MiB | 1.61 MiB | 990.44 KiB | 704 B |
| `rest.revision_json_6k` | 6,000 | 0.2082 | 0.2100 | +0.9% | 17.1% | 15.66 MiB | 15.68 MiB | 1.06 MiB | 35.38 KiB |
| `rest.revision_pagination_6k_limit100` (61 pages) | 6,060 | 1.2554 | 1.1035 | -12.1% | 6.7% | 3.57 MiB | 2.92 MiB | 1.70 MiB | 704 B |
| `replay.query_prep_20k` | 20,000 | 1.0565 | 0.6170 | -41.6% | 17.6% | 40.69 MiB | 38.32 MiB | 9.90 MiB | 0 B |
| `replay.ipc_encode_20k` | 20,000 | 2.7933 | 2.6604 | -4.8% | 4.4% | 7.32 KiB | 7.32 KiB | 6.50 KiB | 6.50 KiB |
| `replay.e2e_publish_20k` | 20,000 | 3.7617 | 3.5906 | -4.5% | 7.7% | 40.70 MiB | 25.88 MiB | 9.90 MiB | 6.50 KiB |

The rerun reproduces §5.1 within run-to-run noise (the largest shift versus
the ticket-19 final medians is `replay.query_prep_20k` +14.0%, whose spread is
17.6%). The only >10% M0 deviations are the four already-approved D1 direct
one-shot Store calls; every REST, pagination, Revision-heavy and Replay
scenario remains inside the 10% gate and no memory peak exceeds its §5.2
value by more than noise. The bounded Replay memory proof in §6 is unchanged:
the review fixes add one publish lock and no buffering or per-row state, so
process memory remains independent of the result row count; the measured
Python/Arrow/DuckDB-temp peaks and the spill-to-disk explanation still apply.
The revision-heavy walk still returns 6,060 rows for 6,000 stored rows.

## 6. Bounded memory scaling (asap / realtime / step)

`benchmarks/bench_replay.py` was extended so `--mode step` is accepted and the
memory pass grants Step Credits (a step Session starts paused). The proof runs
one Replay Session per row count over the same query, with an explicit 64 MB
DuckDB memory limit, one thread, and a dedicated temp directory; Python
(`tracemalloc`), the largest Arrow batch, sampled process RSS and the sampled
DuckDB temp-directory size are reported.

```bash
PYTHONPATH=src D:/Anaconda3/envs/nautilus/python.exe benchmarks/bench_replay.py \
  --mode asap --rows 1000 --repeat 1 --timeout-s 600 --speed 100000000 \
  --memory --memory-rows 50000,100000,200000,400000
# repeated with --mode realtime and --mode step
```

Results (one session per row count per mode; 64 MB DuckDB limit, 1 thread):

| Mode | Rows | Replay s | Bars sent | Python peak | Arrow batch peak | RSS delta | DuckDB temp peak |
|---|---:|---:|---:|---:|---:|---:|---:|
| asap | 50,000 | 4.840 | 50,000 | 13.91 MiB | 745.00 KiB | 50.29 MiB | 0.00 B |
| asap | 100,000 | 11.040 | 100,000 | 13.91 MiB | 745.00 KiB | 66.09 MiB | 0.00 B |
| asap | 200,000 | 21.852 | 200,000 | 13.92 MiB | 745.00 KiB | 97.24 MiB | 512.00 KiB |
| asap | 400,000 | 44.257 | 400,000 | 13.95 MiB | 745.00 KiB | 93.88 MiB | **40.97 MiB** |
| realtime | 50,000 | 5.941 | 50,000 | 13.91 MiB | 745.00 KiB | 48.14 MiB | 0.00 B |
| realtime | 100,000 | 13.070 | 100,000 | 13.91 MiB | 745.00 KiB | 70.79 MiB | 0.00 B |
| realtime | 200,000 | 26.118 | 200,000 | 13.92 MiB | 745.00 KiB | 94.90 MiB | 160.00 KiB |
| realtime | 400,000 | 53.180 | 400,000 | 13.94 MiB | 745.00 KiB | 94.12 MiB | **41.56 MiB** |
| step | 50,000 | 4.856 | 50,000 | 13.91 MiB | 745.00 KiB | 50.33 MiB | 0.00 B |
| step | 100,000 | 11.056 | 100,000 | 13.91 MiB | 745.00 KiB | 68.07 MiB | 0.00 B |
| step | 200,000 | 22.026 | 200,000 | 13.92 MiB | 745.00 KiB | 95.44 MiB | 352.00 KiB |
| step | 400,000 | 44.580 | 400,000 | 13.94 MiB | 745.00 KiB | 104.30 MiB | **42.06 MiB** |

Conclusion: across an **8× increase in result rows**, the Python peak stays
within 13.91–13.95 MiB (<0.3% variation) and the largest Arrow batch stays
constant at 745 KiB in all three modes. Process RSS rises from ~50 MiB at 50k
to ~94–104 MiB at 200k–400k and then plateaus: it tracks the explicit DuckDB
sort buffer budget (64 MB) plus Arrow/Python overhead, not the row count. The
global five-key sort spills to DuckDB temporary disk at the larger sizes
(peaking at ~41–42 MiB for 400k rows); the temp files are removed with the
session and are not a durable or second result copy. This is the bounded-Replay
proof required by the migration.

Logs: `benchmarks/.acceptance-logs/memory-asap.log`, `memory-realtime.log`,
`memory-step.log` (local, git-ignored).

## 7. Failure behaviour

Command:

```bash
D:/Anaconda3/envs/nautilus/python.exe -m pytest -q \
  tests/contract/test_duckdb_compat.py::test_temp_directory_limit_raises_explicit_error_and_cleans_up \
  tests/contract/test_duckdb_compat.py::test_low_memory_sort_spills_to_configured_temp_directory_and_cleans_up \
  tests/contract/test_duckdb_arrow_read.py::test_store_startup_rejects_duckdb_1x \
  tests/integration/test_replay_read_failure_boundary.py::test_resource_exhaustion_is_an_internal_error_not_an_empty_replay \
  tests/integration/test_normalization_quarantine.py::test_normalization_quarantine_write_failure_is_fail_closed \
  tests/integration/test_normalization_quarantine.py::test_instrument_quarantine_write_failure_is_fail_closed \
  tests/integration/test_archive_backfill.py::test_archive_quarantine_write_failure_is_a_reported_failure \
  tests/integration/test_archive_backfill.py::test_cli_archive_backfill_quarantines_a_corrupt_file \
  tests/unit/test_ccxt_feed_handler.py::test_malformed_live_row_stops_the_source_when_quarantine_write_fails \
  tests/unit/test_ccxt_feed_handler.py::test_malformed_history_row_write_failure_stops_history \
  tests/unit/test_ccxt_feed_handler.py::test_queue_overflow_is_not_quarantined \
  tests/integration/test_duckdb_arrow_api.py::test_arrow_rest_aborts_midstream_on_a_scan_failure \
  tests/integration/test_duckdb_csv_export.py::test_csv_failure_after_the_response_starts_is_an_incomplete_transfer \
  tests/integration/test_duckdb_csv_export.py::test_arrow_failure_after_the_response_starts_is_an_incomplete_transfer
```

**Result: 16 passed, 1 failed in 7.06 s.** The single failure is the known
Windows `PermissionError` timing flake
`test_malformed_live_row_stops_the_source_when_quarantine_write_fails`
(log shows the expected sanitized fail-closed message and the Windows
file-sharing `PermissionError` on the Quarantine manifest). It was rerun once
in isolation and **passed** (`1 passed in 2.74 s`), consistent with the same
test passing in the full Windows suite. Logs:
`benchmarks/.acceptance-logs/windows-failure-behavior.log` and
`windows-failure-flake-isolated.log`.

Behaviour verified by this subset:

- DuckDB low-memory sort spills to the configured temp directory and cleans up;
  exceeding `max_temp_directory_size` raises an explicit error.
- Replay resource exhaustion becomes `reason="error"` /
  `internal_error`, not an empty `exhausted` replay.
- Live batch, Instrument-validation and archive Quarantine write failures are
  fail-closed; the corresponding input is not acknowledged, written, or
  advanced. Queue overflow stays a HWM/Backfill gap and is not quarantined.
- Quarantine is invisible to the Bars Store, REST and Replay.
- REST Arrow/CSV failures after the response starts abort the transfer rather
  than appending JSON or ending normally.
- A Store constructed without an importable DuckDB raises
  `DuckDBUnavailableError: DuckDB is required for Arrow history reads; install
  the declared duckdb>=2.0,<3 dependency` (manual `sys.meta_path` probe; no
  fallback path was reached). `test_store_startup_rejects_duckdb_1x` covers the
  unsupported-major-version case.

Missing-DuckDB behaviour is additionally probed by blocking the `duckdb`
import with a `sys.meta_path` hook and constructing a Store; the Store must
raise `DuckDBUnavailableError` and must not fall back to any other read path.

## 8. Final code audit

Searches:

```bash
rg -n "pyarrow.compute|pc\.|normalize_bar|read_replay_bars|_read_legacy|use_duckdb|_close_ts_ns|_install_materialized" src
rg -n "pyarrow\.dataset" src
rg -n "pyarrow\.compute|from pyarrow import compute" src
rg -n "import polars" src
```

Findings and justifications:

- No `pyarrow.compute`, no Arrow compute mask, no `read_replay_bars`, no
  `_read_legacy`, no `use_duckdb`, no `_close_ts_ns`, no
  `_install_materialized` anywhere in `src`.
- The only `normalize_bar` matches are `_normalize_bars` in
  `src/mdgw/archive/importer.py` (the archive batch Normalization adapter) and
  its call site — the single Polars rule source, not a duplicate scalar rule.
- `import polars` appears only in `src/mdgw/normalize/batch.py`; the read path
  is DuckDB → Arrow and never round-trips through Polars.
- `pyarrow.dataset` appears only in `src/mdgw/store/maintenance.py` for
  Compaction's physical merge of explicit, Manifest-eligible paths; it is not
  a production row-filtering read path.
- The remaining `pc\.` hits are `pa.ipc` stream/file readers and writers in
  `src/mdgw/streaming.py`, `src/mdgw_client/schema.py`,
  `src/mdgw_client/history.py` and `src/mdgw/api/routes/bars.py` — Arrow IPC
  transport and export, which are legitimate PyArrow boundaries.
- Replay has no full materialization: `src/mdgw/replay/source.py` converts one
  bounded Arrow batch at a time into `Bar` values behind the bounded
  prefetcher; there is no `read_all()` or whole-range Python list in the
  production Replay path.
- `src/mdgw/api`, `src/mdgw/replay` and `src/mdgw/store` contain no Quarantine
  references; the only Quarantine wiring is the Collector in
  `src/mdgw/runtime.py`. Normal Bars API/Replay cannot discover Quarantine.
- No production engine switch exists; a missing or non-2.x DuckDB fails Store
  construction.

## 9. Independent open issue

The Revision-heavy Time Cursor contract gap remains independent and open:
[`docs/revision-heavy-time-cursor.md`](docs/revision-heavy-time-cursor.md).
The final run reproduces the existing behaviour (6,060 rows returned for 6,000
stored rows; 4,060 repeated `open_ts` values across 61 pages). The migration
neither hides nor fixes it, and closing it is not a condition of this
acceptance.

## 10. Conclusion

All migration review gates are met with the explicit deviation D1 recorded
above. The Windows and Linux offline suites pass with only the pre-existing
CLI failures (and one Linux timing flake that passes in isolation); the exact
Arrow/Decimal/nanosecond/uint32/null/field-order/interval contract is
preserved; the full performance matrix is within the gates except for the
approved one-shot direct-Store fixed overhead; Replay memory is flat in
Python/Arrow while DuckDB spills to bounded temporary disk; and failure
behaviour is explicit (no silent truncation, no fallback engine, fail-closed
Quarantine).

## Review fixes

Post-acceptance two-axis review fixes on branch `ticket/review-fixes`
(worktree `E:/total-project/mdgw-worktrees/duckdb-review`), based on
integration tip `7ef2d23`.

| Commit | Fix | Regression tests |
|---|---|---|
| `b3ff488` | CCXT/composite default Rule Version now uses `NORMALIZATION_RULE_VERSION` (`polars-1`) instead of `"1"`; an explicit configured value still wins | `test_default_rule_version_matches_the_normalization_single_source`, `test_configured_rule_version_still_reaches_the_quarantine_record`, `test_build_feed_handler_defaults_to_the_single_source_rule_version`, `test_attach_quarantine_without_a_version_keeps_the_handler_default` |
| `9da68ee` | Lease lifetime: `StoreRead.close()` waits for the DuckDB scanner's real release before dropping the lease; `_reacquire()` is guarded against a close race so a replacement lease is not installed after close. Also the explicit `<store.root>/.duckdb_tmp` temp directory, `mdgw_duckdb_temp_bytes` / `mdgw_duckdb_active_connections`, and connection-cache shutdown (`ParquetBarStore.close()` idempotent; Runtime/Collector/Api close their own Store) | `test_close_during_a_snapshot_reacquire_releases_the_new_lease`, `test_close_keeps_the_lease_until_a_blocked_scan_has_released`, `test_store_uses_a_deterministic_temp_directory_under_its_root`, `test_store_keeps_an_explicitly_configured_temp_directory`, `test_duckdb_temp_bytes_sums_files_in_the_configured_temp_directory`, `test_duckdb_active_connections_tracks_one_live_scan`, `test_store_close_is_idempotent_and_refuses_new_connections`, `test_update_dataset_metrics_exposes_duckdb_temp_and_connections` |
| `ae960e6` | Replay seek/stop publish barrier: the worker re-checks cancel/generation and publishes under `_publish_lock`; seek/stop wait (bounded, 3 s) so an in-flight Bar completes first and no stale-generation Bar follows. `step()` never takes the barrier | `test_stop_waits_for_an_in_flight_publish`, `test_seek_waits_for_an_in_flight_publish_and_drops_the_old_generation`; all existing asap/realtime/step/seek/stop tests still pass |
| `d1d2ae8` | Truthful `_RowIssues.optional_integer` name; documented why `_ChunkSink.closed` stays (pyarrow requires it) | existing normalization/export suites |

**Full offline suite (Windows).**

```bash
D:/Anaconda3/envs/nautilus/python.exe -m pytest -q
```

**Result: 1094 passed, 5 failed, 1 skipped, 1 deselected in 217.32 s.** The
five failures are the same pre-existing Typer/Click CLI failures listed in
§2.1; 1094 = the previous 1080 plus the 14 review regression tests. No new
failure. `D:/Anaconda3/envs/nautilus/python.exe -m ruff check src` passes.
Log: `benchmarks/.acceptance-logs/review-fixes-full-suite.log`.

**Linux lease/lifecycle subset.**

```bash
MSYS_NO_PATHCONV=1 docker run --rm \
  -v "E:/total-project/mdgw-worktrees/duckdb-review:/app" -w /app \
  python:3.13-slim bash -lc \
  "pip install --quiet \
     --index-url https://mirrors.aliyun.com/pypi/simple \
     --trusted-host mirrors.aliyun.com -e '.[test]' \
   && python -m pytest -q \
     tests/contract/test_read_lease.py \
     tests/contract/test_storage_file_lifecycle.py \
     tests/contract/test_compaction_gc.py"
```

**Result: 26 passed, 1 skipped in 5.26 s.** The skip is the Windows-only
file-sharing case in `test_storage_file_lifecycle.py`; the POSIX
unlink-of-open-file case executed on Linux. The literal command with default
PyPI was attempted first but dependency download stalled at ~170 MB after
46 minutes without reaching the test phase, so the identical image, mount and
test selection were rerun against Aliyun's PyPI mirror; only the package
source differs. Log:
`benchmarks/.acceptance-logs/review-fixes-linux-lease.log`.

**Same-corpus rerun:** §5.4 reproduces the acceptance matrix on the saved
`C:/Users/10107/AppData/Local/Temp/mdgw-duckdb-baseline` copy without
`--rebuild` (marker reused; active bytes ~2.4% above the M0 table). Only the
approved D1 direct-Store deviations exceed 10%; all REST/pagination/Replay
scenarios pass. Replay memory scaling is unchanged (§6): the fixes add a
publish lock and no buffering.

**Merge:** `integration/duckdb-polars-storage` (tip `7ef2d23`) was merged into
`ticket/review-fixes` after the fixes; no conflicts.
