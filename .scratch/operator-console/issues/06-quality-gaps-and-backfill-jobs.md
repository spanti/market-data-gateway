# 06: 实现 Unrecoverable Backfill Gap 与 Backfill Job 面板

**What to build:** Current Contract 展示 Collector 进程内 gap_unrecoverable 快照并标注重启丢失；Admin 模式展示持久 Gap、筛选、open/resolved 和关联 Backfill Job，以及完整任务状态机、进度、确认和取消。

**Blocked by:** 02

**Status:** resolved

- [ ] 当前快照不累积成伪历史，持久 Gap 与当前快照在 UI 上有明确来源区别。
- [ ] 不显示通用 Sequence 连续性结论或 seq+1 误报。
- [ ] Job 覆盖 queued/running/succeeded/partially_succeeded/failed/cancelling/cancelled。
- [ ] 触发需 InstrumentId、目标 Interval、范围、原因和确认；高周期明确显示 1m+内部聚合计划。
- [ ] 部分成功、取消已写入 Revision、重试新建关联 Job 和冲突任务均有真实表达。
- [ ] Gap 可由成功 Job 关联解决，无物理删除入口。
- [ ] Admin 功能在后端联调前只计契约测试，不标记完整交付。

## Comments

**Resolved (2026-11-09):** 合并提交 `d72bfc0`。Current Contract 显示 Collector 进程内 `gap_unrecoverable` 快照并明确“重启丢失”，不做客户端累积；Collector 不可达显示未知；无 Sequence 连续性或 `seq+1` 结论。Admin 持久 Gap 与 Backfill Job（七状态、`1m + 内部 Bar Aggregation` 计划、部分成功、协作取消、重试新建关联 Job、Gap 关联解决、无物理删除）仅契约测试。代码评审修复：Admin/能力未知模式不再请求 collector ops、版本不兼容禁用写入、统一能力门控。最终套件 458 tests。
