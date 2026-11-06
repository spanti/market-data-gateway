# 07: 迁移单 Instrument Time Cursor 分页

**What to build:** 用 DuckDB 的全局 `open_ts` 排序和 `limit+1` 查询替换当前逐批反复排序的分页实现，同时保持单 Instrument JSON 的 Time Cursor、响应形状和 raw Revision 语义。

**Blocked by:** 03 记录 Revision-heavy Time Cursor 的契约缺口；06 迁移非分页 JSON 与 count。

**Status:** resolved

- [x] 只有同时提供 Instrument 和 limit 的 JSON 查询进入分页路径，其他查询和格式的响应形状保持不变。
- [x] DuckDB 从整个请求 Snapshot 中选择全局最早的 `limit+1` 个 raw Revision 行，按 `open_ts` 升序返回前 limit 行。
- [x] `next_start` 仍是第一条未返回 Bar 的 `open_ts`，下一次作为闭区间 start 使用。
- [x] `has_more` 只考虑 end 以内的记录，范围外记录不会影响游标或分页标志。
- [x] `limit=0` 返回空 bars，同时从同一 Snapshot 探测首条记录并计算正确的分页元数据。
- [x] 多页测试覆盖跨文件、重叠 Data Files、边界时间和末页；在唯一 `open_ts` 序列上证明不重不漏。
- [x] Revision-heavy 数据只刻画当前实际行为并引用独立契约问题，不得通过隐式去重、复合游标或改变响应形状暗修。
- [x] JSON 行数 ceiling 继续基于 raw Revisions，count、探测和读取使用同一 Snapshot。
- [x] 分页全局排序的内存和临时磁盘受明确资源预算约束，超过预算时产生稳定错误而非截断页。
- [x] 与旧基线比较分页延迟和内存，不再执行每批 concat/sort 的 Python/Arrow 循环。

## Comments

- Branch: `ticket/07-duckdb-time-cursor-pagination`; worktree
  `E:/total-project/mdgw-worktrees/duckdb-07`.
- Commits: `87e79fa` feat(storage): order paginated JSON pages in DuckDB;
  `c213c46` merge: integration DuckDB CSV/export into ticket 07 (resolved the
  ticket 08 call-site conflict in `tests/integration/test_duckdb_arrow_api.py`);
  the docs commit below closes the ticket. Merged
  `integration/duckdb-polars-storage` (tip `c665437`, ticket 08) before closing.

### Sort/limit design

- `DuckDBArrowScanner.earliest_batches(files, interval, query, keep=...)` runs
  one static SQL query over every explicitly selected Data File of the request
  Snapshot:
  `SELECT <physical bars> FROM read_parquet($files, ...) WHERE <bound
  instrument/open_ts predicates> ORDER BY open_ts ASC,
  list_position($files, filename) ASC, file_row_number ASC LIMIT $row_limit`.
  All values are bound parameters; the SQL text is static and there are no
  caller-supplied identifiers.
- `StoreRead.read_earliest_arrow_bars(keep=...)` materializes the ordered rows
  into `BAR_SCHEMA`; `_build_json_payload` calls it with `keep = limit + 1`.
  The first `limit` rows are the page; the `limit+1`-th row supplies
  `next_start` (its `open_ts`) and `has_more`. The Python per-batch
  `concat_tables`/`sort_by` loop (`_read_earliest_bars`) is deleted.
- `has_more`/`next_start` only ever see rows inside `[start, end)` because the
  predicates run before the global sort and limit. `limit=0` uses `keep=1` and
  still probes the first raw row; the empty selection keeps the empty contract
  schema.
- `filename`/`file_row_number` are referenced only in `ORDER BY`; they are not
  projected, so the Arrow batches still carry exactly the physical Bar columns
  and the partition `interval` is restored unchanged. The tie-break is an
  internal reproduction of the previous stable sort (Manifest file order, then
  physical row order), not a new public ordering guarantee and not a cursor.
- Count, probe and page read all run on the one request-level `StoreRead`
  (same Manifest Snapshot + Read Lease). The exact `count_arrow` still counts
  raw Revisions; no `DISTINCT`, `QUALIFY` or window SQL was added.
- Pagination remains gated on `instrument is not None and limit is not None`.
  Non-paginated JSON, Arrow, CSV, export and streaming bodies are unchanged by
  this ticket (ticket 08 owns the CSV/export migration).

### Revision-heavy behaviour preserved

`tests/integration/test_revision_heavy_time_cursor.py` (ticket 03) and
`tests/integration/test_duckdb_pagination_api.py::test_revision_heavy_page_boundary_reproduces_the_documented_overlap`
pass unchanged. Page boundaries inside a same-`open_ts` Revision group still
repeat rows and can livelock; no dedup, composite/opaque cursor or response
shape change was introduced. The gap is owned by
`docs/revision-heavy-time-cursor.md` and remains unresolved by design.

### Tests

Command: `D:/Anaconda3/envs/nautilus/python.exe -m pytest -q` from the
worktree after merging ticket 08. Result: **913 passed, 5 failed, 1 skipped,
1 deselected** in ~157 s. The 5 failures are the known pre-existing
Typer/Click baseline failures (`tests/integration/test_archive_backfill.py` x4
and `tests/unit/test_cli_flags.py::test_run_rejects_an_unknown_service_role`);
no new failure and no temp-directory flake in this run.

New coverage:

- `tests/contract/test_duckdb_earliest_read.py` (4): global ordering across
  overlapping/append-out-of-order files; raw Revision and duplicate
  multiplicity; preserved storage tie order; half-open bounds applied before
  the limit; empty/oversized `keep`; positive-`keep` validation.
- `tests/integration/test_duckdb_pagination_api.py` (6): cross-file
  overlapping walk with no gaps/duplicates and a last page; exclusive `end`
  and rows outside the window never signalling more; `limit=0` probe from the
  same Snapshot; documented Revision-heavy overlap; paginated JSON ceiling
  counting raw Revisions; an 8 MB / 1 MB-temp global sort returning the stable
  `internal_error` envelope and no truncated page, then succeeding with the
  normal budget.
- `tests/integration/test_duckdb_arrow_api.py` and
  `tests/integration/test_read_lease_api.py` now record
  `read_earliest_arrow_bars` in the per-call-site boundary and one-Snapshot
  event sequence.

### Performance

Same machine and the M0 corpus
(`C:/Users/10107/AppData/Local/Temp/mdgw-duckdb-baseline`), warm-up + 5 timed
passes + one memory pass per measurement. New-path sessions and same-session
old-path controls (old algorithm temporarily restored, identical ASGI/JSON
cost) were alternated. Host variance on this Windows machine is high, so all
sessions are reported rather than a selected one.

- `rest.pagination_2k_limit100` (20 pages, 2,000 rows, 0 duplicate `open_ts`):
  new medians 0.3436, 0.3478, 0.3823, 0.4077, 0.3342, 0.4101 s (spreads
  15-33%); median of medians 0.3651 s vs recorded 0.3410 s (**+7.1%**, within
  the 10% gate). Same-session old controls 0.3252, 0.3922, 0.3206, 0.2906 s.
  A one-page microbenchmark shows the ordered DuckDB query itself at ~3.9 ms;
  the remaining per-page delta is DuckDB relation/scanner setup on a workload
  dominated by fixed per-request cost. tracemalloc peak 1.61 MiB vs baseline
  2.01 MiB; Arrow native peak 0 B vs 990 KiB (page-sized batches are freed).
- `rest.revision_pagination_6k_limit100` (61 pages, 6,060 rows; same 60 extra
  rows from the known Revision-heavy page-boundary overlap as the baseline):
  new medians 1.1498, 1.2648, 1.1001, 1.1097, 1.1470, 1.1825 s (spreads
  6-17%); median of medians 1.1484 s vs recorded 1.2554 s (**-8.5%**);
  same-session old controls 1.3378, 1.4051, 1.0690, 1.0360 s. tracemalloc
  2.92 MiB vs baseline 3.57 MiB; Arrow native peak 0 B vs 1.70 MiB.
- The explicit DuckDB budget (`memory_limit`, `temp_directory`,
  `max_temp_directory_size`) is applied to the global sort; the constrained
  test above proves an over-budget sort fails with `internal_error` instead of
  returning a short page. No temp-file cleanup assertion was added (the known
  Windows async-cleanup flake in `test_duckdb_compat.py` is unrelated).

### Merger notes

- `read_earliest_arrow_bars` requires a single Bar Interval; the REST
  pagination path always supplies `interval`, so no other caller can reach the
  multi-interval branch. No new config, dependency or engine switch was added.
- CSV/export/streaming functions and their new DuckDB call sites were merged
  from ticket 08 untouched; only the paginated JSON call site and the shared
  call-site test were adapted.
- Equal-`open_ts` ordering is reproduced through an internal `ORDER BY`
  tie-break; it must not be read as a new public tie-break or cursor promise.
