# 15: 交付有界 step Replay 与 Step Credit 竞态保证

**What to build:** 让 step Replay 使用与其他模式相同的有界查询和 seek 代际机制，同时精确保持 Step Credit 的消费、丢弃和读取期间授予语义。客户端能逐事件控制大范围回放而不触发全量物化。

**Blocked by:** 14 交付有界 realtime Replay 与 seek 查询代际。

**Status:** resolved

- [x] step Session 启动时保持 paused，只有可用 Step Credit 才允许逐条发布，1 credit 严格对应 1 条事件。
- [x] step 不再保存完整 Bars 和 close-time 数组，跨批次消费不会重发、漏发或提前消耗 Credits。
- [x] pause 继续是 no-op，resume/set_speed 的合法性不变，stop 在有无 Credits 时都能终止并释放资源。
- [x] seek 只在新代际建立时丢弃它观察到的既有 Credits，seek 之后授予的 Credits 即使读取尚未完成也必须保留。
- [x] 旧查询代际不能消费新 Credits、发布 Bars 或修改当前 cursor；连续 seek 和 step/stop 竞态有确定性结果。
- [x] seek 后从同一 Snapshot 第一条 `close_ts >= ts` 的 Bar 继续，并保持 Revision 与五键排序语义。
- [x] Credits 为 0、恰好耗尽、超过剩余行数、读取期间授予和查询耗尽等情况都有明确 End of Session 行为。
- [x] 只有当前查询耗尽且已预取记录全部发布后才发送 exhausted，不会因暂时无 Credits 而误判结束。
- [x] 测试覆盖单条、大于单批次、空结果、seek 越过末尾、连续 seek、读取期间 step/stop 和重复控制命令。
- [x] asap/realtime 保持回归通过，三种 Replay Mode 都不再依赖完整 Bars 或 close-time 列表。

## Comments

**Branch:** `ticket/15-bounded-step-replay-credits` (worktree `E:/total-project/mdgw-worktrees/duckdb-15`)

**Commits:**
- `b99c14a` feat(replay): stream bounded step replay with step credits
- 本 ticket 的 docs closure 提交（docs(replay): close ticket 15 bounded step replay and credits）

**step 有界流式与 Credit:**
- step 与 realtime 共用 `_run_generation_streaming(read, consume)` 和 `_open_generation_prefetch()`（原 `_open_realtime_prefetch` 重命名）：每次迭代固定一个查询代际，`BoundedPrefetcher` 最多持有消费者 1 批 + 队列 `prefetch_batches` 批 + 生产者 1 批；step 不再调用 `_source.read()` / `read_replay_bars()`，也不再创建 `_bars`、`_close_ts_ns`、`_cursor`、`_pending_seek_ns`。
- `_run_step_streaming` 的消费循环 `_consume_step_stream` 对每条 Bar 调用 `_await_step_credit(generation)`：在 `_cond` 内循环检查 stop、代际和 `_credits > 0`；无可用 Credit 时 `_cond.wait()` 挂起。因此暂时缺 Credit（即使查询已全部预取）绝不会返回 `exhausted`。
- 代际检查与 `_credits -= 1` 在同一个临界区完成，是该 Bar 的提交点：旧代际已经交到消费者手里的 Bar 会在消费 Credit 之前被丢弃。发布仍在 `_cond` 外进行，`step`/`stop`/`seek` 不会被慢发布阻塞。
- `pause` 仍是 no-op（step 始终 paused）；`resume`/`set_speed` 仍非法；`stop` 在有无 Credits、读取中或发布中都会设置取消标记并 `notify_all`，worker 经 prefetcher close 取消查询、finally 关闭 `StoreRead`（Reader、连接、lease）后发出 `reason=stopped`。

**seek 与查询代际:**
- `seek()`（仅 paused 合法）执行 `_credits = 0`、`_generation += 1`、`_generation_start_ns = ts`、`notify_all`。worker 启动新代际时不重置 Credits，因此 seek 之后授予的 Credits（即使新代际读取尚未完成）原样保留；连续 seek 以最后一次目标为准，旧目标代际只返回 `superseded`。
- worker 在每批开始、每条 Bar 的 Credit 等待循环及流结束时检查代际；旧代际返回 `superseded` 后先 `prefetcher.close()`（`ReplayScanCancellation.cancel()` 关闭该代际的 `DuckDBArrowScanner`/Reader/连接，Snapshot 与 Lease 不动），再在同一 `StoreRead` 上以 `start_ts_ns = seek ts` 打开新查询。查询仍由 DuckDB 在同一固定 Snapshot 上完成 close-time 过滤、最大 Revision、同 Revision 最新 `ingest_ts`、重复订阅消解和五键排序，所以 seek 从第一条 `close_ts >= ts` 的 Bar 继续且不纳入 Snapshot 之后的 Data Files。
- 测试用脚本化 `StoreRead` 证明旧代际即使已将后续 Bar 交到 worker 手中，也不能消费 seek 后授予的 Credit 或发布；并记录 `predecessor_finished == [True, True]` 证明旧 Reader 先释放再启动新代际。

**EOS 规则:**
- `exhausted` 只在当前代际的 prefetcher 正常 END（所有已预取 Bar 均已按 Credit 发布）且代际/取消未变化时返回；输出逐批、逐条处理，`_END` 在 FIFO 中位于所有 Bar 之后，所以不会被提前看到。
- Credits 为 0 且有剩余 Bar：worker 挂起，不发 EOS；`stop` 返回 `stopped`。Credits 恰好耗尽：最后一条发布后流 END → `exhausted`。Credits 超过剩余行数：发布完剩余行后 `exhausted`，未用 Credits 保留在 `info()["credits"]`。seek 越过末尾：新代际查询为空，无需 Credit 立即 `exhausted`。
- 读取失败保持 ticket 13/14 的内部边界：不发送 End of Session，`session.failure` 记录异常，失败前 `bars_sent` 精确保留；公共 `reason=error` 属 ticket 16。旧代际的读取中断按 stop/seek 分类丢弃，不当作失败。
- `info()` 字段与合法操作矩阵不变；step 不使用 Replay Clock（`_clock_started` 始终为 False，`position_ts` 取 `_generation_start_ns`，seek 后即为 seek ts）。

**清理:**
- 删除物化 step 路径：`_install_materialized`、list-based `_run_step`、`_apply_seek_locked` 的 bisect 分支、`_bars`、`_close_ts_ns`、`_cursor`、`_pending_seek_ns` 及 `bisect_left` import。
- `ReplayBarSource.read()` / `StoreRead.read_replay_bars()` 仅保留给迁移差分测试、基准与 ticket 17 清理；生产路径的三种 Replay Mode 全部使用 `iter_bars()` / `iter_replay_batches()`。

**测试:**
- 新增 `tests/integration/test_replay_step_streaming.py`（16）：三种模式均不调用物化 seam；单批/多批流式发布；初始 paused 且 Credits=0 时查询已耗尽也不发 EOS；7 行跨 4 个 Arrow 批次逐 Credit 发布无重无漏；Credits 恰好耗尽/超过剩余行数；初始读取期间与 seek 代际读取期间授予 Credits；旧代际不消费新 Credits；连续 seek 取最新目标；seek 复用同一 Snapshot（`open_count == 1`，不纳入新 Data Files）；读取期间 stop；流中读失败不变成 exhaustion；重复 pause/seek/step/stop 幂等；5k vs 50k 行内存有界。
- 重写 `test_replay_streaming.py::test_step_streams_the_query_instead_of_materializing`（原“step 保持物化”断言）；更新 `test_replay_asap.py` 代理注释与 `test_replay_streaming.py` 模块说明。`test_replay_controls.py` / `test_replay_characterization.py` 的 gate 代理同时拦截 `iter_replay_batches`，因此 step 测试自动 gate 新的流式 seam，未削弱任何 timing/race 断言。
- 命令：`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q`
- 全量离线结果（合并 integration tip `06f957d`）：**1065 passed, 5 failed, 1 skipped, 1 deselected** in 199.11s。5 个失败均为 ticket 01 起已知的 Typer/Click CLI 问题（4 个 `test_archive_backfill.py` CLI 测试 + `test_cli_flags.py::test_run_rejects_an_unknown_service_role`），无新增失败。
- Replay 相关子集（controls/streaming/realtime/step/asap/差分/characterization/prefetch/scan-cancel/source）：**133 passed**。step + controls 连续重复 5 次：每次 **37 passed**，无抖动。

**step 不同结果规模下的内存有界证据**（`batch_size=256`、`prefetch_batches=1`，tracemalloc 峰值，独立测量脚本）：

| rows | bars sent | tracemalloc peak |
|------|-----------|------------------|
| 5,000 | 5,000 | 1,020,200 bytes (~1.02 MB) |
| 50,000 | 50,000 | 1,359,156 bytes (~1.36 MB) |

10 倍结果规模只增加约 0.33 MB（分配器噪声/固定批缓冲），不随总行数线性增长；测试断言 `large_peak <= small_peak * 2 + 4 MiB`。

**给 merger 的说明:** 无新依赖、无配置变更、无公开 REST/Replay 协议变化；仅内部 step 读取路径与对应测试变化。
