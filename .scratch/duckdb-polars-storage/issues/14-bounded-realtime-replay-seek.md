# 14: 交付有界 realtime Replay 与 seek 查询代际

**What to build:** 让 realtime Replay 在 asap 的有界读取基础设施上支持 pause、resume、set_speed、seek 和 stop。seek 在同一 Manifest Snapshot 上启动新查询代际，旧代际结果不能发布，I/O 时间仍不消耗 Replay Clock。

**Blocked by:** 13 交付有界 asap Replay。

**Status:** resolved

- [x] realtime 不再保存完整 Bars 和 close-time 数组，按有界批次等待 Replay Clock 并发布。
- [x] pause/resume/set_speed/stop 的合法状态、幂等和竞态行为保持，Speed 变化继续影响后续等待。
- [x] seek 只在既有合法暂停状态生效，从同一 Snapshot 查询第一条 `close_ts >= ts` 的 Bar，不纳入新 Data Files。
- [x] 每次初始读取和 seek 都有查询代际，旧代际即使稍后返回结果，也不能发布或修改当前 cursor、Clock 和会话状态。
- [x] seek 和 stop 使用已验证的取消机制释放旧 Reader、连接和执行资源，随后才能安全复用或替换。
- [x] Replay Clock 在首个可读批次准备好后启动；初始读取或 seek 的 I/O 时间不消耗虚拟时间。
- [x] 初始暂停或 seek 读取期间只允许有界预取，pause 不会导致整个范围提前读入内存。
- [x] 覆盖空结果、seek 到首条之前、seek 到两条之间、seek 越过末尾、连续 seek、读取期间 pause/stop 和 set_speed。
- [x] 只有当前代际耗尽且预取记录全部发布后发送 exhausted，seek 越过末尾和 stop 的 EOS 行为与契约一致。
- [x] asap 与尚未流式化的 step 不发生行为回退，realtime 的内存在不同结果规模下保持有界。

## Comments

**Branch:** `ticket/14-bounded-realtime-replay-seek` (worktree `E:/total-project/mdgw-worktrees/duckdb-14`)

**Commits:**
- `8168171` feat(replay): stream bounded realtime replay with seek generations
- `09f561e` test(replay): avoid Windows lock polling in the realtime seek snapshot test
- 本 ticket 的 docs closure 提交（docs(replay): close ticket 14 ...）

**realtime 有界流式:**
- `_run_realtime_streaming` + `_pace_stream` 消费与 asap 相同的 `BoundedPrefetcher` 批次流：消费者手中 1 批 + 队列 `prefetch_batches` 批 + 生产者至多 1 批；realtime 不再创建 `_bars` / `_close_ts_ns`（这两个字段只在 step 物化路径使用）。
- 每个 Bar 在 `_cond` 下按 Replay Clock 等待；pause/resume/set_speed 唤醒并重算等待，stop 观测后不再发布。`BoundedPrefetcher` 新增可选 `should_stop` 谓词，使 stop/seek 在阻塞查询不产出任何项、队列为空时也能在 50ms 轮询内被消费者观察到（否则消费者会永久阻塞在 `queue.get`）。
- `ReplayClock` 仍是服务端唯一节流源；`set_speed` 在暂停和运行中都会改变后续等待（运行中先结算旧速度再换挡）。

**查询代际与 seek:**
- `_generation`、`_generation_start_ns`、`_awaiting_generation` 均由 `_cond` 保护。初始读取是 generation 0；`seek()` 只在既有 paused 合法状态生效，丢弃它观察到的 credits，`_generation += 1`、记录最新 seek ts（连续 seek 最新者胜）并 `notify_all`。
- worker 在每批到达、每个 Bar 等待循环内和发布前都在 `_cond` 下检查代际；旧代际返回 `superseded`，不发布、不修改 Clock/cursor/`bars_sent`。旧代际生产者异常也按 cancel/代际分类丢弃；只有当前代际 END 且全部预取 Bar 已发布才 `exhausted`，旧代际永不发送 EOS。
- 关闭旧 prefetcher 时 `ReplayScanCancellation.cancel()`：`StoreRead.iter_replay_batches(..., cancellation=...)` 在查询尚未开始时直接返回，已开始时关闭 `DuckDBArrowScanner`（interrupt 阻塞中的 `read_next_batch()`，关闭 Arrow Reader 和连接）。Manifest Snapshot 和 Dataset Read Lease 保持不动，新代际在同一 `StoreRead` 上以 `start_ts_ns = seek ts` 重查第一条 `close_ts >= ts` 的 Bar。测试断言 `open_count == 1` 且 Snapshot 之后发布的 Data Files 不会被 seek 纳入。
- `BoundedPrefetcher.close()` join 生产者完成后 worker 才启动下一代 prefetcher；测试记录 `predecessor_finished` 顺序，证明旧 Reader 先释放再替换。stop 同样先 token 取消，最后由 worker 的 `finally` 关闭 StoreRead/Lease。

**Clock 与 I/O 时间:**
- `_awaiting_generation=True` 期间 Clock 不启动也不 resume：`resume()` 在该状态下只更新 `_paused`，由首个可读批次到达时 `_start_generation_clock_locked(start_ns)` 执行 `clock.start(start_ns)` 再按当前 `_paused` 应用 pause/resume。seek 时若 Clock 已启动只做 `clock.seek(ts) + pause` 记录位置，I/O 期间不推进。
- 测试用可控 gate 读取和 pause/resume 时序证明：读取 I/O 0.6s 后 Clock 位置仍小于 0.25s；seek I/O 0.5s 后位置仍小于 0.2s；暂停跨越数据就绪时位置精确冻结在 start/seek ts。

**EOS 与错误边界:**
- 当前代际流 END 且所有已预取 Bar 发布后才 `exhausted`；stop → `stopped`；paused 中 seek 过末尾 → 立即 `exhausted`（不等待 resume）；旧代际永不发送 EOS。
- 读取失败保持 ticket 13 的内部边界：不发 End of Session、`session.failure` 记录异常、失败前 `bars_sent` 精确保留；公共 `reason=error` 仍属 ticket 16。
- Step 继续走 `_source.read()` + bisect/credits 物化路径，行为不变（ticket 15 再迁移）；asap 仅新增 should_stop 唤醒，不节流行为不变。

**测试:**
- 新增 `tests/integration/test_replay_realtime_streaming.py`（19）：流式非物化、pause 期间有界预取（yielded <= prefetch+2）、初始读取期间 pause/stop、初始与 seek 代际读取失败、seek 到首条之前/两条之间/越过末尾/连续 seek、旧代际已到手批次不发布、反向 seek 重发与 `bars_sent`、I/O 时间排除（初始读取与 seek、pause/resume 跨越数据就绪）、空结果、运行中 set_speed、固定 Snapshot 与单次 `open_read`、5k vs 50k 行内存有界。
- 新增 `tests/unit/test_replay_scan_cancel.py`（3）：取消后的代际不启动查询且同一 StoreRead 仍可查询；取消中途后 Snapshot/Lease 可复用；重复取消幂等。
- 扩展 `tests/unit/test_replay_prefetch.py`（+1）：外部 should_stop 在空队列等待时解除消费者。
- 更新 `test_replay_streaming.py`（realtime 不再物化，step 单独断言仍物化）、`test_replay_controls.py` / `test_replay_asap.py` / `test_replay_characterization.py` 的 StoreRead 代理转发 cancellation、差分源 `iter_bars` 接受 cancellation；未削弱既有 timing/race 断言。
- 命令：`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q`
- 结果（合并 integration tip `e3c1e43`）：**1048 passed, 6 failed, 1 skipped, 1 deselected**。6 个失败中 5 个是 ticket 01 起已知的 Typer/Click CLI 问题，`test_malformed_live_row_stops_the_source_when_quarantine_write_fails` 是已知计时 flake，隔离重跑通过；新增 realtime 套件单独重跑 19 passed。
- Windows flake 处理：实时的 snapshot/lease 测试最初在活跃 lease 上轮询 `active_session_locks`，偶发 `WinError 32` 使 session 关闭时 lease unlink 失败（既有 Windows 文件锁 flake 家族）。改为断言 `open_count == 1` 证明 seek 复用同一 StoreRead/lease，未削弱竞态断言，重跑 10/10 稳定；lease 的最终释放由既有 Read Lease 测试覆盖。

**realtime 内存随结果规模有界（`benchmarks/bench_replay.py --mode realtime --memory --memory-rows 50000,100000,200000,400000 --memory-limit 64MB --speed 10000000`；worktree 运行需 `PYTHONPATH=<worktree>/src`，否则直接运行脚本会解析到主 checkout 的 editable 安装）:**

| rows | replay s | tracemalloc peak | Arrow native peak | RSS delta | temp disk peak |
| --- | --- | --- | --- | --- | --- |
| 50,000 | 6.501 | 13.91 MiB | 745.00 KiB | 26.97 MiB | 0.00 B |
| 100,000 | 13.299 | 13.91 MiB | 745.00 KiB | 61.54 MiB | 0.00 B |
| 200,000 | 26.945 | 13.92 MiB | 745.00 KiB | 80.23 MiB | 352.00 KiB |
| 400,000 | 60.544 | 13.94 MiB | 745.00 KiB | 80.65 MiB | 42.38 MiB |

- 行数增长 8 倍时 Python 峰值几乎不变（13.91→13.94 MiB），Arrow 原生峰值恒为单个批次 745 KiB；只有 DuckDB 全局排序的临时磁盘独立增长到 42.38 MiB，临时文件不是结果副本。
- 集成测试同样证明 5,000 行与 50,000 行 realtime 会话的 tracemalloc 峰值均为约 1.3 MiB（`test_realtime_process_memory_does_not_grow_with_result_size`）。

**性能（20k Bars，同机，`--rows 20000 --repeat 5`）:**
- asap e2e **best 3.273s / median 3.361s**；ticket 13 记录为 best 3.183s / median 3.339s，处于同机噪声内，无回退。
- realtime（speed 1e9，实际不节流）e2e **best 3.474s / median 3.491s**，较 asap 约 +3.9%，低于 10% 门槛；每 Bar 的 Clock 检查是主要增量。
- 查询准备与首批延迟单独报告且未回退：物化 query prep best 0.534–0.538s / median 0.557–0.561s；stream first batch best 0.029–0.032s / median 0.044–0.046s（ticket 13: 0.534/0.561 与 0.030/0.045）。

**Merger notes:**
- 无新依赖、无新配置键。内部 API 新增：`ReplayScanCancellation`、`StoreRead.iter_replay_batches(..., cancellation=...)`、`ReplayBarSource.iter_bars(..., cancellation=None)`、`BoundedPrefetcher(..., should_stop=...)`。
- step 仍物化（ticket 15）；realtime/asap 公共事件序列与 EOS 形状不变。
- benchmark 新增 `--mode asap|realtime` 与 `--speed`，默认值保持原有 asap 行为。
- `reason=error` 仍留给 ticket 16，本 ticket 未改公共失败协议。
