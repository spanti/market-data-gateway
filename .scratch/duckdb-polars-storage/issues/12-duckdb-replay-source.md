# 12: 用 DuckDB 替换 Replay Source，暂时保留物化

**What to build:** 让 Replay Source 通过 DuckDB 在固定 Manifest Snapshot 上完成 close-time 过滤、Revision 选择、重复订阅消解和五键排序，同时暂时保留现有全量物化与 Session 控制循环，以完整事件序列证明查询等价。

**Blocked by:** 10 覆盖 Feed/Archive Quarantine 与 Retention/GC；11 打通 Normalization Rule Version 血缘。

**Status:** resolved

- [x] Replay 继续按 `close_ts` 的 `[start_ts,end_ts)` 过滤，并正确利用 Bar Interval 扩展物理 `open_ts` 扫描起点，不遗漏窗口开头的 Bars。
- [x] 每个 `(instrument_id, interval, open_ts)` 只保留最大 Revision，同 Revision 继续选择最新 `ingest_ts`。
- [x] 重复订阅和重叠 Data Files 不会产生重复 Replay 事件，也不会把 REST 的 raw Revision 语义带入 Replay。
- [x] 输出严格使用 `close_ts`、`instrument_id`、数值 seq 先于 null、`interval`、`open_ts` 的五键全序。
- [x] Replay Session 从创建到结束固定 Manifest Snapshot，运行期间新发布的 Data Files 不会混入。
- [x] Dataset Read Lease 继续保护整个 Session，包括初始读取、暂停和现有 seek 行为。
- [x] 暂时保留全量 Bars 和 close-time 数组以及现有 asap/realtime/step 控制循环，不在本 ticket 同时重写控制面。
- [x] 对所有 Replay Mode 比较旧新完整事件序列、控制事件、bars_sent 和 End of Session，不只比较条数。
- [x] 覆盖空结果、start/end 边界、多个 Instrument/Interval、可空 seq、重复订阅、Revision tie 和 seek 越过末尾。
- [x] 分开报告 DuckDB 查询准备与端到端发布性能，不将 IPC/Python 发布成本误算为扫描成本。

## Comments

**Branch:** `ticket/12-duckdb-replay-source` (worktree `E:/total-project/mdgw-worktrees/duckdb-12`)

**Commits:**
- `4a67780` feat(replay): query the fixed Manifest Snapshot through DuckDB

**Replay query seam:**
- `ReplayBarSource.read(..., read=StoreRead)` 现在调用 `StoreRead.read_replay_bars(queries)`；生产 Session 始终走该路径。不传 `read` 时保留旧的 PyArrow 读取，仅作为迁移差分测试和 benchmark 的私有路径，没有 `use_duckdb` 开关。
- `ReplayReadQuery` 对每个去重后的 `(instrument_id, interval)` 记录 close-time 窗口与物理扫描起点：`open_start_ns = start_ts_ns - interval`；`close_start_ns` 闭、`close_end_ns` 开。重复订阅在 `replay_read_queries()` 中先去重再建查询。
- SQL 由 `duckdb_reader._build_replay_sql()` 生成：每个不同订阅一个 `UNION ALL` 分支，分支只读 `StoreRead.snapshot.entries` 解析出的显式 Data File 列表（不 glob Dataset 根目录），过滤 `instrument_id`、`open_ts >= physical_start`、`open_ts < end`、`close_ts >= start_ts`、`close_ts < end_ts`，并把 Interval 作为绑定参数投影为 `interval` 列。分支数量是唯一随订阅变化的 SQL 结构，所有值都是 `$parameter`。
- 外层 `QUALIFY row_number() OVER (PARTITION BY instrument_id, interval, open_ts ORDER BY revision DESC, ingest_ts DESC) = 1`，随后 `ORDER BY close_ts ASC, instrument_id ASC, seq ASC NULLS LAST, interval ASC, open_ts ASC`。REST 的 raw Revision 语义不参与。
- Arrow 导出逐批累积后 `select(BAR_SCHEMA.names).cast(BAR_SCHEMA)` 恢复字段顺序与 nullability；空结果直接返回 `empty_logical_table()`。仍保留完整 `list[Bar]` 与 `close_ts` 数组，asap/realtime/step 控制循环未改。
- 查询准备（含 Snapshot/Lease 获取、DuckDB 扫描与物化）由 `bench_replay.py` 单独计时，端到端 asap 发布另计。

**Temporary read-failure boundary (ticket 16 will add `reason=error`):**
- 读取异常不再被吞成空 Bars + 正常 `exhausted` EOS。异常从 `_run_worker` 传播到 `_run`，记录在 `ReplaySession.failure`，worker 终止且不发送 End of Session；`bars_sent` 保持 0。损坏 Parquet 和 Snapshot 获取后 Data File 被外部删除都按此处理，Session 结束时 Read Lease 仍会释放。
- 缺失 Data File 在固定 Snapshot 上直接失败，不会像 REST 那样 `_reacquire` 到更新的 Manifest；这是"Session 固定 Snapshot"要求的刻意差异。
- `ReplaySession` 新增仅关键字参数 `source=`，供差分测试注入旧 PyArrow Source；`ReplayService` 不传该参数，生产无行为开关。

**Tests:**
- 新增 `tests/characterization/test_replay_duckdb_differential.py`：对 asap/realtime/step 分别运行旧 PyArrow 与新 DuckDB Session，逐条比较全部 Bar 字段、控制事件、`bars_sent` 和 EOS；覆盖空结果、半开边界与 5m 物理扩展、多 Instrument/Interval、可空 seq、重复订阅与重叠文件、Revision tie、seek 越过末尾与 seek 进入窗口。
- 新增 `tests/integration/test_replay_read_failure_boundary.py`：损坏 Parquet、Snapshot 后文件删除均不产生 EOS/exhausted，且 Lease 释放。
- 扩展 `tests/unit/test_replay_source.py`：查询级 close-window/Revision/五键排序、空结果、缺失文件显式失败、旧新完整 Bar 序列相等、重复订阅去重。
- 命令：`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q`
- 结果（合并 integration tip 后）：**1001 passed, 5 failed, 1 skipped, 1 deselected**。5 个失败是已知的 Typer/Click 既有问题，本 ticket 未修改。首次全量运行时 `tests/unit/test_ccxt_feed_handler.py::test_malformed_live_row_stops_the_source_when_quarantine_write_fails` 曾偶发失败，单跑与第二次全量运行均通过（既有临时目录清理类 flake）。

**Performance (20k Bars, 同一机器, 5 次; `benchmarks/bench_replay.py --rows 20000 --repeat 5`):**
- 基线（ticket 01）：query prep 1.0565 s，asap 端到端 3.7617 s。
- DuckDB query prep（Snapshot/Lease + 扫描 + 去重排序 + 物化）：best 0.913 s，median 0.988 s。
- 旧 PyArrow query prep（同次对比）：best 0.925 s，median 0.999 s。
- asap 端到端发布：best 3.815 s（+1.4%），median 3.880 s（+3.1%），均在 10% 门槛内；query prep median 比基线快约 6.5%。
