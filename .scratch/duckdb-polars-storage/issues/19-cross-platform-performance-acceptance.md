# 19: 完成跨平台、性能与全量验收

**What to build:** 对最终收缩后的实现执行完整跨平台、行为、资源和性能验收，给出可审查的迁移结论。只有全部硬门槛满足，DuckDB/Polars 改造才视为完成。

**Blocked by:** 18 发布三份 ADR 并同步公共文档。

**Status:** ready-for-agent

- [ ] 完整离线测试套件通过，包括 Data Contract、Manifest、Store、REST、Normalization、Quarantine、Compaction/GC、Replay 和 SDK。
- [ ] Windows 与 Linux 的读取/维护竞态、Reader 取消、文件删除和 Lease 生命周期测试均有真实执行结果，不仅依赖 mock。
- [ ] 精确 Decimal、UTC 纳秒、uint32、null、字段顺序和 `interval` 恢复的 golden/round-trip 测试全部通过。
- [ ] 使用保存的旧实现基线，在同一机器和同一数据副本上对最终实现至少测量 5 次，报告中位数、波动、峰值内存和临时磁盘。
- [ ] Store 扫描/count、REST JSON/Arrow/CSV/export、分页、多文件、Revision-heavy、Replay 查询和端到端发布均纳入最终报告。
- [ ] 典型吞吐或延迟回退超过 10%，或峰值内存增加超过 20% 时，具有明确批准的偏差记录；否则不得验收。
- [ ] 用递增总结果规模证明 asap/realtime/step 的进程内存不随结果行数线性增长，并单独报告全局排序临时磁盘峰值。
- [ ] 验证资源耗尽、依赖缺失和 Quarantine 写入失败都有明确失败行为，不会静默截断、回退旧引擎或消费未隔离输入。
- [ ] 最终代码检查确认生产 Bars API/Replay 没有手工 Parquet 行过滤、重复标量 Normalization、Replay 全量物化或生产引擎开关残留。
- [ ] 最终报告列出已验证版本、平台、测试命令、性能结果、任何已批准偏差和独立遗留的 Time Cursor 契约问题。
- [ ] 不把性能结果表述为对外 SLA，也不以关闭独立 Time Cursor 问题作为本迁移的隐式验收条件。
