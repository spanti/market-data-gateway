# 13: 交付有界 asap Replay

**What to build:** 让 asap Replay Session 在固定 Manifest Snapshot 上逐批消费 DuckDB Arrow 结果并立即发布，不再为该模式保存完整 Bars 和 close-time 数组。stop、资源清理和 End of Session 与流式读取一起端到端工作。

**Blocked by:** 12 用 DuckDB 替换 Replay Source，暂时保留物化。

**Status:** resolved

- [x] asap 从 Relation Arrow Reader 按配置批次消费，不在首次发布前物化全部匹配 Bars。
- [x] asap 仍然不节流，完整事件序列、五键排序、Revision 选择和 bars_sent 与物化实现一致。
- [x] 大于单批次和需要外部排序的数据在跨批次发布时不重不漏，临时磁盘行为可观测。
- [x] stop 能取消当前查询并关闭 Reader、连接和相关执行资源，旧查询不能在 stop 后继续发布。
- [x] 正常完成、stop、读取异常和重复清理都会释放 Dataset Read Lease。
- [x] 只有查询耗尽且已预取 Bars 全部发布后才发送 `reason=exhausted`，stop 继续发送 `reason=stopped`。
- [x] 预取队列有明确上限，暂停或下游变慢不会导致无界累积。
- [x] 使用相同查询但递增总结果规模验证进程内存不随行数线性增长，并单独记录 DuckDB 临时磁盘峰值。
- [x] realtime/step 在各自流式 ticket 完成前保持现有语义，完整 Replay 回归套件继续通过。
- [x] 本 ticket 不把读取异常伪装为空回放；若完整公共失败协议尚未接入，必须保留明确错误边界供后续失败协议 ticket 完成。

## Comments

**Branch:** `ticket/13-bounded-asap-replay` (worktree `E:/total-project/mdgw-worktrees/duckdb-13`)

**Commits:**
- `eb01816` feat(replay): stream bounded asap replay

**流式读取接缝:**
- `DuckDBArrowScanner.iter_replay_batches(branches, batch_size=...)` 用 `relation.to_arrow_reader(batch_size=...)` 逐批产出同一 `_build_replay_sql()` 的结果；close-time 过滤、最大 Revision、同 Revision 最新 `ingest_ts`、重复订阅消解和五键排序全部仍在 DuckDB 内完成。生成器结束时 `_exhausted=True` 才把连接放回缓存；异常、取消和提前 close 走 `_failed` 并关闭 Reader 与连接。
- `StoreRead.iter_replay_batches(queries, batch_size=None)` 是 Store 层流式接缝：使用与 `read_replay_bars()` 相同的固定 Manifest Snapshot、Read Lease 和显式 Data File 列表（不 glob Dataset 根目录），逐批 `select(BAR_SCHEMA.names).cast(BAR_SCHEMA)` 后交给调用方。它是生成器，首次 `next()` 才开始查询；`StoreRead.close()` 会 interrupt 正在阻塞的 `read_next_batch()`。
- `ReplayBarSource.iter_bars()` 把每个 Arrow RecordBatch 在生产者线程内转成 `list[Bar]`；`read_replay_bars()` 保留给 realtime/step。
- `BoundedPrefetcher`（新文件 `src/mdgw/replay/prefetch.py`）：生产者 daemon 线程 + `queue.Queue(maxsize=prefetch_batches)`；失败按流序入队并在消费者处重新抛出，绝不变成 StopIteration/end-of-stream；`close()` 置取消、清空队列、调用 `on_close`（即 `StoreRead.close()`）中断 DuckDB，并 join 生产者。tickets 14/15 可直接复用。
- 队列上限是显式的：任何时刻在途 Bars 约为 `batch_size * (prefetch_batches + 2)`（消费者手中 1 批 + 队列 `prefetch_batches` 批 + 正在生产 1 批）。默认 `batch_size=4096`、`prefetch_batches=2`，约 16k Bars。

**Session 分流与取消/EOS 规则:**
- `ReplaySession` 只在 `replay_mode == "asap"` 走流式路径；realtime/step 仍调用 `ReplayBarSource.read()` 全量物化并保留原 cursor/clock/seek/credits 逻辑。合法操作矩阵未变（asap 仍拒绝所有控制操作）。
- 每个 Bar 发布前在 `_cond` 下检查 `_cancel`；`stop` 观测后不再发布。`stop` 通过 `_run_asap_streaming` 的 `finally` → `prefetcher.close()` → `StoreRead.close()` 取消查询、关闭 Reader/连接并释放 Lease；Session 自己的 `finally` 再次 `read.close()`，重复清理安全。
- 生产者流序为 `[batch...][END]`，只有 END 前所有已预取 Bars 都交给发布循环后 END 才会被消费，因此 `reason=exhausted` 不会越过尚未发布的 Bar。stop 竞态仍返回 `reason=stopped`。
- 读取异常（含中断以外的 DDL/损坏/缺失文件/OOM）由 prefetcher 重新抛出，`_run` 记录 `session.failure`、不发 End of Session；这是给 ticket 16 的显式内部错误边界。若异常与 stop 同时发生，按 `stopped` 处理。失败前的已发布 Bar 保留真实 `bars_sent`。
- 配置：新增 `[replay].batch_size` 与 `[replay].prefetch_batches`（`config/gateway.toml`、demo、docker 均为 4096/2），经 `ReplayConfig → ReplayService → ReplaySession` 以及 `GatewayRuntime`、`ReplayServiceRole` 透传；无引擎开关。

**测试:**
- 新增 `tests/unit/test_replay_prefetch.py`（6）：顺序、队列硬上限、失败按流序抛出、close 解除满队列阻塞、close 中断底层 read 且只调用一次 on_close。
- 新增 `tests/integration/test_replay_streaming.py`（11）：首批发布先于查询耗尽（物化实现会死锁失败）、23 Bars/批 5 跨批不重不漏、配置透传、伪 read 可观测的预取上限、stop 中断查询且观测后不再发布、Snapshot 获取期间 stop 不启动查询、mid-stream 读取异常不是 exhausted、真实 DuckDB stop 后 Lease 释放、48MB/150k 行强制外部排序的临时目录峰值 > 0、空结果、realtime/step 仍走 `read_replay_bars`。
- 扩展 `tests/unit/test_replay_source.py`（+3）：`StoreRead.iter_replay_batches` 的批次切分/BAR_SCHEMA/与物化顺序一致、惰性到消费才启动、无文件返回空。
- 扩展 `tests/characterization/test_replay_duckdb_differential.py`（+3 参数）：asap 流式 vs ticket-12 物化 DuckDB，batch_size=1/2/100 完整事件序列、全部 Bar 字段、控制事件、`bars_sent` 与 EOS 相等；asap/realtime/step 旧 PyArrow 差分继续通过。
- 更新 `tests/integration/test_replay_asap.py`、`test_replay_controls.py`、`tests/characterization/test_replay_characterization.py` 的 `_BlockingReadStore`/`_BlockingStore`：现在阻塞新的 `iter_replay_batches`（asap）或 `read_replay_bars`（realtime/step），断言未削弱。
- 命令：`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q`
- 结果（合并 integration tip `5f60122` 后）：全量运行 **1025–1026 passed, 5–6 failed, 1 skipped, 1 deselected**。5 个固定失败是 ticket 01 起已知的 Typer/Click CLI 问题，本 ticket 未修改。
- 一次全量运行额外出现 `tests/unit/test_ccxt_feed_handler.py::test_queue_overflow_is_not_quarantined`。隔离重跑时该用例在 ticket 分支 3 次中 1 次失败、在 integration tip `5f60122` 上 5 次中 2 次失败，确认是既有计时 flake，与本 ticket 无关；单跑通过。除此之外无新增失败。

**内存随结果规模有界（`benchmarks/bench_replay.py --memory`，同一查询，DuckDB `memory_limit=64MB, threads=1`，batch 4096 / prefetch 2）:**

| rows | replay s | tracemalloc peak | Arrow native peak | temp disk peak |
| --- | --- | --- | --- | --- |
| 50,000 | 4.274 | 13.92 MiB | 745.00 KiB | 0.00 B |
| 100,000 | 9.433 | 13.92 MiB | 745.00 KiB | 0.00 B |
| 200,000 | 18.951 | 13.92 MiB | 745.00 KiB | 320.00 KiB |
| 400,000 | 37.655 | 13.95 MiB | 745.00 KiB | 42.16 MiB |

- 行数增长 8 倍时 Python 峰值（tracemalloc）几乎不变（13.92→13.95 MiB），Arrow 原生峰值恒为单个批次 745 KiB；只有 DuckDB 全局排序的临时磁盘从 0 增长到 42.16 MiB，符合"临时文件不是权威副本"。
- RSS 采样（psutil，可选）受进程分配器保留影响不单调（49→70→96→121 MiB），不作为线性判据；有界性由 tracemalloc/Arrow 平坦与临时磁盘独立增长证明。
- 强制 spill 集成测试：`DuckDBOptions(memory_limit="48MB", threads=1, temp_directory=...)` + 150,000 行，发布 150,000 条严格 close_ts 有序且无重复，采样到临时目录峰值 > 0。

**性能（20k Bars，同机 `bench_replay.py --rows 20000 --repeat 5`，与 ticket 12 基线同机对比）:**
- ticket 12 基线：query prep 0.91–0.99 s，asap e2e best 3.788–3.815 s，median 3.880–3.902 s。
- 本 ticket：物化 query prep best 0.534 s / median 0.561 s（同机/缓存差异，未回退）；stream first batch best 0.030 s / median 0.045 s；asap e2e **best 3.183 s，median 3.339 s**（median 较 ticket 12 约 -14%），无吞吐/延迟回退。
- 查询准备（物化）与端到端发布仍分开报告；流式路径新增的首批延迟单独输出，不含发布成本。

**Merger notes:**
- 合并 `integration/duckdb-polars-storage` 时已是最新 tip `5f60122`，无冲突。
- 新增生产文件 `src/mdgw/replay/prefetch.py`；新增配置键 `[replay].batch_size`、`[replay].prefetch_batches`（默认 4096/2），三份 gateway TOML 已同步。
- 无新依赖；realtime/step 公共行为未改，`reason=error` 仍留给 ticket 16。
