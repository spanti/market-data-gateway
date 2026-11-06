# 06: 迁移非分页 JSON 与 count

**What to build:** 让非分页 JSON 历史响应和 100,000 行检查通过同一 Manifest Snapshot 的 DuckDB 查询完成，保持 raw Revisions、LIMIT、精确 JSON 表示和稳定错误行为，不再经过旧手工 Arrow 过滤。

**Blocked by:** 05 打通 DuckDB → Store → REST Arrow 的 tracer bullet。

**Status:** resolved

- [x] JSON count 和实际读取共享请求级 Manifest Snapshot 与 Read Lease，不会分别看到不同 Manifest 代际。
- [x] count 和响应都保留全部 raw Revisions，100,000 行 ceiling 及错误详情中的行数不得因隐式去重改变。
- [x] LIMIT 继续只限制返回行数，不新增全局 ORDER BY，也不承诺复刻旧引擎的偶然扫描顺序。
- [x] 首版在同一 Snapshot 内分别执行精确 count 和读取，不为了合并扫描引入改变语义的窗口 SQL。
- [x] JSON Message Envelope、Decimal 字符串、九位纳秒时间、null 和响应形状与旧实现一致。
- [x] `range_too_large`、参数校验、Schema Version 不匹配和内部错误继续使用既有稳定错误码与 envelope。
- [x] 覆盖空结果、边界行数、LIMIT 与 ceiling 交互、多文件和 Revision-heavy 数据的旧新差分测试。
- [x] Reader、连接和 Lease 在响应成功、验证失败和读取异常后均正确释放。
- [x] 非分页 JSON 的典型延迟和内存相对基线没有未经批准的回退。

## Comments

- Branch: `ticket/06-duckdb-json-count`; worktree `E:/total-project/mdgw-worktrees/duckdb-06`.
- Commits: `c7c6a8b` feat(storage): route non-paginated JSON and count through
  DuckDB; this docs commit closes the ticket. Merged
  `integration/duckdb-polars-storage` (tip `ebac602`) before closing.

### Design

- `StoreRead.count_arrow()` executes `SELECT count(*)` over the explicit
  Manifest Snapshot file list with bound `instrument_id` / half-open `open_ts`
  predicates. `StoreRead.read_arrow_bars()` materializes the existing
  `iter_arrow_batches()` Relation Arrow Reader into `BAR_SCHEMA`.
- `_build_json_payload` now calls `count_arrow()` for every JSON request and
  `read_arrow_bars(limit=...)` for non-paginated JSON. Paginated JSON still
  uses the PyArrow `_read_earliest_bars` page reader; CSV/export/streaming
  code is untouched. No engine switch was added.
- Count and read are separate DuckDB queries on the same request-level
  StoreRead (Snapshot + Lease), so they cannot mix Manifest generations. The
  console has no `DISTINCT`, window function, global `ORDER BY` or dedup;
  `LIMIT` is applied only to returned rows and the ceiling keeps counting raw
  rows. The ceiling count uses Parquet footer metadata, which is exact for a
  valid Data File; a broken footer fails the request rather than miscounting.
- Resource lifecycle: the scanner closes its Arrow Reader and returns the
  connection to the one-idle cache on a clean count/read, closes it on failure,
  and `StoreRead.close()` releases the Lease on success, `range_too_large`,
  schema mismatch and read exceptions.

### Tests

Command: `D:/Anaconda3/envs/nautilus/python.exe -m pytest -q` from the
worktree. Result: **879 passed, 5 failed, 1 skipped, 1 deselected** in ~160 s.
The 5 failures are the known pre-existing Typer/Click baseline failures
(`tests/integration/test_archive_backfill.py` x4 and
`tests/unit/test_cli_flags.py::test_run_rejects_an_unknown_service_role`);
no new failure. The known Windows temp-cleanup flake
(`tests/contract/test_duckdb_compat.py::test_temp_directory_limit...`) failed
once in the full run and passed in isolation, as expected.

New coverage: `tests/contract/test_duckdb_count_read.py` (10) covers raw
Revision/duplicate counts, multi-file differential parity with the PyArrow
read, filters and half-open bounds, empty results/schema, LIMIT boundaries,
one-Snapshot count+read across a concurrent write, Revision-heavy data, and
lease/connection release after success and after count/read failures.
`tests/integration/test_duckdb_json_count_api.py` (14) covers REST-level exact
message multiplicity against the PyArrow Store read across multiple files,
Revision-heavy data, empty selections, LIMIT boundaries, patched-ceiling
boundary/limit interaction without dedup, `range_too_large`, count and read
failures mapped to the stable `internal_error` envelope without SQL/engine
leaks, schema mismatch, and lease release for every path.
`tests/integration/test_duckdb_arrow_api.py` now freezes the per-call-site
boundary: Arrow `iter_arrow_batches`; non-paginated JSON
`count_arrow`+`read_arrow_bars`; paginated JSON `count_arrow`+`iter_batches`;
CSV/export `iter_batches`. `tests/integration/test_read_lease_api.py` gained a
non-paginated one-Snapshot test and the event sequence for the DuckDB count.
Existing non-paginated parity tests in characterization/API suites now compare
exact row content and multiplicity without freezing physical scan order;
format/schema/decimal/nanosecond assertions are unchanged.

### Performance

Same machine, same M0 corpus (`C:\Users\10107\AppData\Local\Temp\mdgw-duckdb-baseline`),
one warm-up + 5 timed passes + one memory pass per measurement, two paired
sessions with old/new order alternated. The old path is the same REST route
with `_build_json_payload` temporarily restored to PyArrow count/read, so
ASGI and JSON-encoding cost is identical.

- `rest.json_50k`: new medians 1.6477 s / 1.5574 s (spread 9.8% / 12.6%);
  same-session old medians 1.6430 s / 1.5824 s. Delta -0.3% / -1.6% vs the
  old path; +6.0% / +0.2% vs the recorded 1.5548 s baseline (within the 10%
  gate). tracemalloc peak 131.24 MiB vs old 131.24-131.26 MiB and baseline
  131.23 MiB (+0.0%, within the 20% gate). Arrow native peak 293 KiB vs old
  12.96-18.89 MiB.
- `rest.revision_json_6k`: new medians 0.2193 s / 0.2206 s (spread 18.0% /
  44.5%); old 0.2270 s / 0.2355 s. Delta -3.4% / -6.3% vs old; +5.3% / +6.0%
  vs the recorded 0.2082 s baseline. tracemalloc 15.66 MiB vs baseline
  15.66 MiB.

No approved-gate regression; high spread is the documented Windows developer
host variance, so medians are the comparison point.

### Merger notes

- Non-paginated JSON intentionally no longer preserves the old PyArrow scan
  order, including which rows `limit` selects; the ticket authorizes this.
  Several order-sensitive assertions were made order-insensitive while keeping
  content/multiplicity. `tests/integration/test_duckdb_arrow_api.py` and
  `test_read_lease_api.py` now record `count_arrow`/`read_arrow_bars`.
- CSV/export/streaming production functions were not modified. Cross-format
  JSON↔CSV/export parity assertions were adjusted only for ordering.
- The exact count reads Parquet metadata, so a page-only corruption can pass
  the count and fail the subsequent read; the REST error path is covered and
  still returns the stable `internal_error` envelope.
