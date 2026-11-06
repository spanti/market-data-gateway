# 01: 建立旧实现的差分测试与性能基线

**What to build:** 在任何生产引擎替换之前，用现有公共接缝固定 Store、REST、Collector Normalization 和 Replay 的可观察行为，并提供可重复的旧实现性能基线。后续 ticket 能用同一批数据判断差异究竟来自新实现错误、非契约偶然行为，还是已批准的独立变化。

**Blocked by:** None (can start immediately).

**Status:** resolved

- [x] 建立可复用测试数据，覆盖空 Dataset、单条、单文件、多文件、跨分区、多 Instrument、多 Interval、inactive Manifest 条目和重复记录。
- [x] 固定 `open_ts` 与 `close_ts` 的半开区间边界、start 等于 end、记录恰好位于 start/end、跨文件相同时间戳和非稳定输入顺序。
- [x] 固定 REST 的 raw Revisions、JSON 行数上限、LIMIT、Time Cursor、精确 Decimal、UTC 纳秒、Arrow Schema、CSV 文本和稳定错误响应。
- [x] 固定 Replay 的完整事件序列、Revision/ingest-time tie-break、五键排序、重复订阅以及 asap/realtime/step 的控制与 End of Session 行为。
- [x] 固定 Collector 批次失败时 Aggregation、HWM 和 freshness 不推进的行为，并记录 CCXT、归档和队列溢出的现有差异。
- [x] 准备同一 `open_ts` 多个 Revisions 且跨页的样例，供独立 Time Cursor 契约 ticket 使用；不得在本 ticket 修改该行为。
- [x] 在同一机器、同一数据副本上对旧实现至少测量 5 次，报告中位数与波动，覆盖 Store 扫描/count、REST 全格式、分页、Replay 查询准备和端到端发布。
- [x] 记录峰值内存和可用的临时磁盘数据，明确区分 Replay 查询成本与 IPC/Python 发布成本。
- [x] 差分设施只用于测试与迁移，不增加生产引擎开关，也不改变现有公共行为。

## Comments

**Branch:** `ticket/01-characterization-baseline` (worktree
`E:/total-project/mdgw-worktrees/duckdb-01`).

**Commits:** `7397e75` (characterization suite), `8c68efb` (benchmark
baseline), `cf0f337` (merge of `integration/duckdb-polars-storage` at
`f4766f8`). Ticket closure is the follow-up commit.

**What was added (tests/benchmarks only; no production change, no engine
switch):**

- `tests/characterization/dataset_builder.py`: reusable deterministic Bar
  Dataset builder with named scenarios: empty, single row, single file,
  multiple files, cross-partition (BTC/ETH, 1m/5m, January/February), inactive
  Manifest entry, duplicate rows, Revision history, ingest tie-break, half-open
  boundaries and the Revision-heavy corpus.
- `tests/characterization/test_store_characterization.py`: Store
  `iter_batches`/`read_bars`/`count_bars` semantics through the public Store
  only.
- `tests/characterization/test_rest_characterization.py`: real
  `GatewayRuntime`/ASGI requests covering raw Revisions and duplicates, exact
  decimal strings, nine-digit UTC timestamps, Arrow `BAR_SCHEMA`, CSV text,
  Parquet/Arrow export, LIMIT, the 100,000-row JSON ceiling, Time Cursor pages,
  inactive entries, stable error envelopes and `X-Request-ID`.
- `tests/characterization/test_replay_characterization.py`: complete event
  sequences, five-key order, max-Revision/latest-`ingest_ts`, duplicate
  subscriptions, asap/realtime/step controls, seek credit rules and End of
  Session.
- `tests/characterization/test_collector_characterization.py`: rejected batch
  leaves Aggregation/HWM/freshness untouched (retry produces exactly one 5m
  parent); records current CCXT row-skip, archive whole-file failure and
  queue-overflow/HWM-cap behaviour.
- Revision-heavy fixture for ticket 03:
  `tests/characterization/dataset_builder.py::revision_heavy_dataset`; current
  behaviour is frozen in
  `test_rest_characterization.py::test_revision_heavy_pagination_repeats_the_same_page_today`
  (4 revisions at one `open_ts`, `limit=2` -> `next_start` repeats the same
  timestamp). Not fixed here.
- `benchmarks/bench_baseline.py` plus
  `benchmarks/RESULTS-duckdb-baseline.md`; `bench_query.py` now also times
  `count_bars` and `bench_replay.py` reports Replay query preparation
  separately from IPC/publish.

**Tests:** from the worktree,
`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q`
after merging integration `f4766f8`: **752 passed, 5 failed, 1 deselected**.
The 5 failures are the pre-existing Typer/Click exit-code failures recorded in
the shared context (`test_cli_backfill_*` x4 and
`test_run_rejects_an_unknown_service_role`), unchanged by this ticket. New
suite alone: `pytest tests/characterization -q` -> **41 passed**.

**Benchmark summary (5 timed passes, same machine/corpus, report has full
table and spreads):**

- Store: 100k single-file scan 0.0199 s (5.0M rows/s) vs 50k across 100 files
  0.1017 s (0.49M rows/s); count virtually identical to scan.
- REST 50k: JSON 1.5548 s, CSV 1.2805 s, Arrow 0.0825 s, Parquet export
  0.0790 s, Arrow export 0.0849 s. JSON tracemalloc peak ~131 MiB (full message
  list + text), CSV ~62.7 MiB.
- Pagination: 2,000 rows / 20 pages 0.3410 s. Revision-heavy 6,000 rows /
  61 pages 1.2554 s and returns 6,060 rows (60 extra boundary rows) — the
  ticket-03 problem, recorded but not fixed.
- Replay 20k: query preparation 1.0565 s, IPC encoding 2.7933 s, end-to-end
  asap publishing 3.7617 s. Query cost is therefore ~1.06 s and the
  encode/publish remainder ~2.7 s.
- Memory/disk: Traffic numbers above; corpus 10.2 MiB; export peak temp files
  1.92 MB Parquet / 9.20 MB Arrow; temp volume 60.81 GiB free at run time.
  `tracemalloc` covers Python allocations only; Arrow native memory is a
  sampled `pa.total_allocated_bytes()` peak; DuckDB native memory is not
  covered yet.

**Merger notes:** no production files changed, no new dependencies, no config
changes. Only `tests/characterization/**`, `benchmarks/**` and the benchmark
`--help` smoke-test parameter list changed. Benchmark corpus lives outside the
repo (`<tempdir>/mdgw-duckdb-baseline`), so it is not committed.
