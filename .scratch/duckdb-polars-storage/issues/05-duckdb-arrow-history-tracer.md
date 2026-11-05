# 05: 打通 DuckDB → Store → REST Arrow 的 tracer bullet

**What to build:** 让一条真实 REST Arrow 历史查询通过 Manifest Snapshot、Dataset Read Lease 和 DuckDB 2.0 Relation Arrow Reader 端到端返回结果，作为后续所有 Bars 格式的垂直 tracer bullet。其他表面可以继续使用旧实现，不增加生产运行时开关。

**Blocked by:** 04 为现有读取器引入 Manifest Snapshot 与 Dataset Read Lease。

**Status:** resolved

- [x] DuckDB 只接收 Manifest Snapshot 中的显式 Data File 列表，不执行 Dataset 根目录 glob，也不读取 inactive 条目。
- [x] History 查询继续按 `open_ts` 的半开区间过滤并返回全部 raw Revisions，不添加隐式去重或全局排序。
- [x] Manifest 的 Instrument、Interval、分区与时间范围裁剪继续发生在扫描前，过滤值参数化且调用方不能注入 SQL 或任意路径。
- [x] Arrow 响应精确满足 `BAR_SCHEMA`，包括字段顺序、nullability、`decimal128(38,18)`、UTC 纳秒、uint32 和物理文件中省略的 `interval` 恢复。
- [x] 大结果通过 Relation Arrow RecordBatch Reader 逐批输出，不先物化为完整 Arrow Table、Python rows 或 Polars DataFrame。
- [x] Read Lease 和连接资源覆盖 Reader 整个生命周期，并在正常结束、异常和客户端取消后释放。
- [x] 非分页输出不承诺复刻旧物理扫描顺序；差分测试验证逐行内容与重复次数完全一致。
- [x] 覆盖空结果、单条、多文件、跨分区、时间边界和多个 Revisions 的公共 REST Arrow 测试。
- [x] 旧实现只保留给尚未迁移的表面和差分测试，新路径没有 `use_duckdb` 之类的生产配置。
- [x] 与同机旧实现基线比较吞吐、内存和临时磁盘；任何超过批准门槛的回退必须暂停并重新评审。

## Comments

### Branch and commits

- Branch: `ticket/05-duckdb-arrow-history-tracer`
- Worktree: `E:/total-project/mdgw-worktrees/duckdb-05`
- Feature commit: `f65f1a6 feat(storage): route REST Arrow history through DuckDB relation reader`
- Characterization fix: `86989f6 test(characterization): compare non-paginated Arrow rows order-insensitively`
- Parity-test fix: `deb9908 test(api): compare non-paginated format parity order-insensitively`
- Integration `integration/duckdb-polars-storage` (`8e300c1`, ticket 04) merged into the
  branch before closure; the merge was already up to date.
- No new dependencies: `duckdb>=2.0,<3` and `pyarrow` were already declared.

### DuckDB scan design (file list -> query -> Arrow reader)

New module `src/mdgw/store/duckdb_reader.py`; the Store seam is
`StoreRead.iter_arrow_batches()` in `src/mdgw/store/parquet_dataset.py`.

1. **File list.** Candidate entries come from the ticket-04 `ManifestSnapshot`
   (only active entries, selected at lease acquisition). `_select_entries()`
   re-applies Instrument/Interval/file-range pruning, then every selected path
   is checked for existence. A missing file before any row keeps the one-shot
   snapshot re-acquire; after that the read fails instead of dropping rows.
   Entries are grouped by Hive partition `interval`, because the physical
   Parquet file omits that column. DuckDB never globs: it receives exactly the
   explicit `str(dataset_root / entry["path"])` list.
2. **Query.** Static SQL:
   `SELECT <17 physical BAR_SCHEMA columns> FROM read_parquet($files,
   hive_partitioning = false)` with optional
   `WHERE instrument_id = $instrument AND open_ts >= $start::TIMESTAMPTZ_NS
   AND open_ts < $end::TIMESTAMPTZ_NS` and `LIMIT $row_limit`. The file list
   and every filter value are bound parameters; no caller text becomes SQL.
   There is no ORDER BY, DISTINCT, QUALIFY or Revision dedup.
3. **Connection.** `duckdb.connect(":memory:", config={...})` with
   `timezone=UTC`, `memory_limit`, `threads`, `temp_directory`,
   `max_temp_directory_size` and `preserve_insertion_order=false` (acceptable
   because non-paginated output has no ordering contract). A one-idle-connection
   cache (`DuckDBConnectionCache`) removes DuckDB's ~14 ms per-request setup
   cost; a checked-out connection is exclusive to one Reader, only cleanly
   exhausted scans are cached, and cancelled/failed scans close their
   connection. Startup calls `ensure_supported_duckdb()`, which raises
   `DuckDBUnavailableError` if DuckDB is missing or not major version 2.
4. **Arrow reader.** `relation.to_arrow_reader(batch_size=...)`, yielding one
   batch at a time. Each batch is rebuilt with
   `pa.RecordBatch.from_arrays(..., schema=BAR_SCHEMA)`, which restores field
   order, nullability and the Hive `interval` column. Decimals, UTC
   nanoseconds and uint32 stay typed Arrow values end to end; no Python row or
   Polars materialization.
5. **Lifecycle.** `StoreRead` keeps the Read Lease and the active scanner;
   `iter_arrow_batches()` closes the Reader between interval groups and closes
   Reader + connection in `finally`. `StoreRead.close()` interrupts an
   in-flight scan (`connection.interrupt()`), and the worker releases the
   Reader/connection when the blocking call returns. A mid-stream failure
   propagates as an aborted Arrow transfer; the route never appends JSON or
   ends the stream normally.
6. **Call-site boundary.** Only `_arrow_body` in
   `src/mdgw/api/routes/bars.py` calls `iter_arrow_batches()`; JSON, CSV and
   export keep `iter_batches()` (PyArrow). There is no runtime engine switch.
   A REST boundary test records the chosen seam per format.

Configuration: `[storage.duckdb]` in `config/gateway.toml`
(`memory_limit`, `threads`, `temp_directory`, `max_temp_directory_size`,
`arrow_batch_size`) with typed defaults in `DuckDBConfig`; all service roles,
the runtime and the CLI pass it to `ParquetBarStore`.

### Tests

Command (from the worktree):

```
D:/Anaconda3/envs/nautilus/python.exe -m pytest -q
```

Result: **854 passed, 5 failed, 1 skipped, 1 deselected** in ~142 s. The 5
failures are the known pre-existing Typer/Click baseline failures
(`tests/integration/test_archive_backfill.py` x4 and
`tests/unit/test_cli_flags.py::test_run_rejects_an_unknown_service_role`);
no new failure. Ticket-04 baseline was 826 passed; this ticket adds 28 tests:

- `tests/contract/test_duckdb_arrow_read.py` (16): one row, exact
  `BAR_SCHEMA`, decimal/nanosecond/uint32/nullability, multi-file duplicate
  counts, inactive and unlisted corrupted files never opened, half-open
  `open_ts` bounds (`start == end` included), every raw Revision, cross
  Instrument/Interval pruning, per-partition `interval` restoration, limit,
  multi-batch streaming, lease release after partial iteration and after a
  read failure, missing-file snapshot refresh, DuckDB 1.x startup rejection
  and config validation. Differential comparisons are order-insensitive
  multiset comparisons against `StoreRead.read_bars()`.
- `tests/integration/test_duckdb_arrow_api.py` (12): public REST Arrow
  empty Dataset / bounded empty result / multi-file + raw Revisions /
  half-open bounds / cross-partition / decimal + pre-epoch parity against the
  PyArrow Store read; the format call-site boundary; mid-stream scan failure
  aborts without a JSON error or normal end; client disconnect releases the
  lease and a later read succeeds; export failure (pre-response) keeps the
  stable `internal_error` envelope.

One ticket-04 test double in `tests/integration/test_read_lease_api.py`
gained an `iter_arrow_batches` delegation so the existing lease assertions
exercise the new call site; no assertion was weakened. The non-paginated
format-parity tests in `tests/integration/test_bars_api.py`,
`tests/integration/test_fastapi_bars.py` and
`tests/characterization/test_rest_characterization.py` now compare canonical
message multisets (exact content and duplicate counts) instead of physical
scan order, as this ticket explicitly drops the old non-paginated scan order
as a contract; schema, row count, Interval, Instrument, decimal and
nanosecond assertions are unchanged.

### Performance comparison (same machine and corpus)

Corpus: the M0 corpus at `C:\Users\10107\AppData\Local\Temp\mdgw-duckdb-baseline`
(reused, not rebuilt). Method: `benchmarks/bench_baseline.py` helpers,
one warm-up + 5 timed passes per session (plus a memory pass), 3 sessions.
The old-path control runs the same route with `StoreRead.iter_arrow_batches`
temporarily mapped to the PyArrow iterator, so route/ASGI/encoding cost is
identical and only the scan differs.

| Scenario | Recorded baseline | New (3 sessions) | Delta |
| --- | ---: | ---: | ---: |
| `rest.arrow_50k` median | 0.0825 s (spread 28.5%) | 0.0981 / 0.0848 / 0.0790 s; median 0.0848 s (spread 25-44%) | +2.8% vs recorded baseline |
| Same-session old-path control | 0.0941 / 0.1073 / 0.1144 s median 0.1073 s | 0.0848 s median | -21% (new faster in 2 of 3 sessions) |
| `rest.arrow_50k` tracemalloc peak | 23.64 MiB | 17.70-20.75 MB | lower |
| `rest.arrow_50k` Arrow native peak (1 ms sample) | 21.69 MiB | 0.26 MB | -99% (streamed, no materialization) |
| Process RSS peak during the request | old control 244-277 MB | 287-293 MB | +4.4% to +17.5% (under the 20% gate) |
| Store scan 100k (warm cache, 7 passes) | 0.0199 s | 0.0217 s vs old control 0.0221 s | -2.0% vs old control |
| Store multi-file 50k, 100 files | 0.1017 s | 0.0355 s vs old control 0.1054 s | -66% |
| Store revisions 6k | 0.0042 s | 0.0066 s vs old control 0.0066 s | equal |

Machine noise on this Windows host is high (spread up to 60%), so medians
are the comparison point. The one-idle-connection cache was added
specifically because per-request DuckDB setup costs ~14 ms and produced a
>10% Arrow latency regression without it; with the cache the typical Arrow
latency is at or below the recorded baseline and the old-path control. The
first Arrow request after startup still pays the one-time connection setup.
`temp_directory` is unset by default (DuckDB default); when configured,
DuckDB creates and removes per-query temp files under it. No sort/aggregate
exists on this path, so temp disk stayed empty in the benchmark.

### Merger notes

- New config section `[storage.duckdb]`; defaults are in `DuckDBConfig`.
  `temp_directory = null` (unset) means DuckDB's default temp directory.
- `ParquetBarStore.open_read(..., batch_size=None)` now resolves the PyArrow
  batch size to the previous `DEFAULT_BATCH_SIZE`; the configured
  `arrow_batch_size` applies only to `iter_arrow_batches()`.
- The connection cache retains one idle DuckDB connection per Store. It is
  an addition beyond the ticket text, recorded here: it keeps each
  connection exclusive to one Reader and closes connections on cancellation
  or failure, but a clean scan's connection is reused instead of closed.
- `DuckDBUnavailableError` now aborts Store construction on a missing or
  non-2.x DuckDB; tests that instantiate `ParquetBarStore` require the
  declared dependency.
