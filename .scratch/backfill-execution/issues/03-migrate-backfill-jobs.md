# 03: 让 Backfill Job 穿过同一 execution seam

**What to build:** 为 Backfill Job 加入第二个执行与持久化 adapter，将显式范围 Job 从 Runner 公共方法到 Dataset、HWM、health、counters、gap_ranges 和终态的完整路径迁移到同一 deep execution module。迁移后两个真实入口共享执行时序，但各自公共行为不变。

**Blocked by:** 02《让自动 HWM Backfill 穿过 deep execution module》

**Status:** resolved

- [ ] Job 执行 adapter 保留显式范围、分钟对齐、闭合 cutoff、一日 fetch chunk、500 行批和现有 merge 读取边界，不改变自动入口的全窗口策略。
- [ ] Job 继续使用 Job 级 Aggregator、Runner clock 计算 cutoff 和现有逐批 ingest_ts 取得方式；高周期 Job 仍只抓 1m 并聚合配置中的全部 Bar Intervals。
- [ ] raw 输入继续按 open_ts 去重，非 1m 和 chunk 外输入忽略，unreadable identity 计入 rejected_bars，缺失分钟继续生成 half-open gap_ranges，不引入 Sequence 连续性判断。
- [ ] Job rejection 策略继续统计 rejected_bars、让有效 Bars 计入 backfilled_bars 并继续 merge/write；自动入口仍整批失败，两条路径都不写 Quarantine。
- [ ] scanned_bars、gap_bars、backfilled_bars、written_bars、rejected_bars 的名称、计算时机与含义保持，written_bars 仍包含 parent Bars 且只在持久化成功后累计。
- [ ] Job 持久化 adapter 委托现有窄持久化能力，保留 asset class 解析、同步 Dataset 写入、Core counters、逐 Bar health、expected keys 和 sink HWM cap，不注入整个 CollectorCore。
- [ ] Dataset 写入失败时 HWM 与 written 成功计数不推进；HWM 继续单调，已完成的早期 chunk 不回滚。
- [ ] 协作取消继续只在 chunk 前、fetch 后、batch 前和 merge 后/write 前生效；阻塞 fetch/write 不新增中断承诺，已写 Revision 不回撤。
- [ ] Runner 的单 worker、queue、内存 registry、start/status/cancel、current_range、错误、七状态、同 key 冲突和 close 生命周期保持不变。
- [ ] write gate 保持在 deep module 外，锁顺序和 key 粒度不变；deep module 不读取或写入 Admin JobStore、GapStore 或 Operator Audit Event。
- [ ] succeeded、partially_succeeded、failed 的现有产出判定保持，包括全部相同而 written_bars 为零时仍可 succeeded。
- [ ] 两个入口现在都实际穿过同一 execution seam，Job 的旧重复执行 orchestration 被删除，不增加生产开关或按输入选择新旧实现的 fallback。
- [ ] 01 的 differential 在两个新 adapter 上通过，Job Runner、Admin Backfill、取消、恢复、Gap、live ingest 与 archive 聚焦测试通过。

## Comments

**Resolved (2026-11-11):** 已在 `feature/backfill-execution-03` 实现并合并（feature `7978018`，merge `a1ed9c6`）。Job `_execute` 每个对齐日 chunk 调用 `BackfillExecution.execute`，注入 Job 级 fresh Aggregator、`batch_size=500`、逐批 `utc_now`、Job `prepare`/`read_bounds`/rejection 处理与 observer；旧 chunk 内 fetch/normalize/merge/persist orchestration 已删除。五类 counters 的计算时机、四个取消检查点、`current_range`、七状态终态判定、`_JobHistoryFeed` 的稳定 fetch 错误文档，以及经 `persist_backfill_bars` 的 asset class → Dataset → Core counters/HWM cap/逐 Bar health/expected keys 顺序均保持；Runner 单 worker、queue、内存 registry、同 key 冲突与 close 生命周期不变；write gate 仍在 deep module 外。新增 3 个测试（2 个 seam、1 个 fetch error 契约）；ticket 01 differential 在两个新 adapter 下通过；聚焦 53 passed；全量 1919 passed，仅既有失败。
