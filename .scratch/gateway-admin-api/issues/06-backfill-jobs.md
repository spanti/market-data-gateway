# 06: 由 Collector 执行持久 Backfill Job

**What to build:** Admin API 持久化并调度 Backfill Job，Collector 唯一执行 Dataset 写入；实现完整状态机、部分成功、协作取消、冲突约束、1m+内部聚合、HWM 不变量、重试关联和重启恢复。

**Blocked by:** 01, 02, 03

**Status:** resolved

- [ ] queued/running/succeeded/partially_succeeded/failed/cancelling/cancelled 与计数完整。
- [ ] API 不写 Parquet/HWM，Collector 与自动 Backfill 共享写入和并发边界。
- [ ] 高周期只通过 1m 补采和内部 Bar Aggregation 完成，不调用原生高周期接口。
- [ ] HWM 单调、Data File 不可变、Revision/Data Contract 规则保持。
- [ ] 同 InstrumentId+Interval 冲突任务串行或拒绝，重试创建关联新 Job。
- [ ] 取消不撤回已写 Revision，重启恢复或明确中断，不默认成功。
- [ ] 真实临时 Dataset 集成覆盖部分成功、失败、取消、并发和恢复。

## Comments

**Resolved (2026-11-10):** Collector 执行的持久 Backfill Job 已合并 `b133160`（feature `5713e03`；发版前补 `124aa24` 迁移重编号与 `1fd899e` 取消测试稳定化）。迁移 009（从 006 重编号，避免已应用 008 后跳过）；完整状态机、计数与 current_range；API 只校验/授权/审计/持久化，Collector 唯一写 Dataset；高周期固定 1m 补采 + 内部聚合；HWM 单调、Data File 不可变、Revision 规则保持；同 instrument+interval 活跃任务 409；重试新建关联 Job 且旧 Job 不改写；取消协作式且不撤回已写 Revision；重启后不默认成功。46 个新测试，合并时 1721 passed / 5 个既有 CLI 失败。
