# 06: 由 Collector 执行持久 Backfill Job

**What to build:** Admin API 持久化并调度 Backfill Job，Collector 唯一执行 Dataset 写入；实现完整状态机、部分成功、协作取消、冲突约束、1m+内部聚合、HWM 不变量、重试关联和重启恢复。

**Blocked by:** 01, 02, 03

**Status:** ready-for-agent

- [ ] queued/running/succeeded/partially_succeeded/failed/cancelling/cancelled 与计数完整。
- [ ] API 不写 Parquet/HWM，Collector 与自动 Backfill 共享写入和并发边界。
- [ ] 高周期只通过 1m 补采和内部 Bar Aggregation 完成，不调用原生高周期接口。
- [ ] HWM 单调、Data File 不可变、Revision/Data Contract 规则保持。
- [ ] 同 InstrumentId+Interval 冲突任务串行或拒绝，重试创建关联新 Job。
- [ ] 取消不撤回已写 Revision，重启恢复或明确中断，不默认成功。
- [ ] 真实临时 Dataset 集成覆盖部分成功、失败、取消、并发和恢复。

## Comments
