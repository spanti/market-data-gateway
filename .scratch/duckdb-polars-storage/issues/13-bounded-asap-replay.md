# 13: 交付有界 asap Replay

**What to build:** 让 asap Replay Session 在固定 Manifest Snapshot 上逐批消费 DuckDB Arrow 结果并立即发布，不再为该模式保存完整 Bars 和 close-time 数组。stop、资源清理和 End of Session 与流式读取一起端到端工作。

**Blocked by:** 12 用 DuckDB 替换 Replay Source，暂时保留物化。

**Status:** ready-for-agent

- [ ] asap 从 Relation Arrow Reader 按配置批次消费，不在首次发布前物化全部匹配 Bars。
- [ ] asap 仍然不节流，完整事件序列、五键排序、Revision 选择和 bars_sent 与物化实现一致。
- [ ] 大于单批次和需要外部排序的数据在跨批次发布时不重不漏，临时磁盘行为可观测。
- [ ] stop 能取消当前查询并关闭 Reader、连接和相关执行资源，旧查询不能在 stop 后继续发布。
- [ ] 正常完成、stop、读取异常和重复清理都会释放 Dataset Read Lease。
- [ ] 只有查询耗尽且已预取 Bars 全部发布后才发送 `reason=exhausted`，stop 继续发送 `reason=stopped`。
- [ ] 预取队列有明确上限，暂停或下游变慢不会导致无界累积。
- [ ] 使用相同查询但递增总结果规模验证进程内存不随行数线性增长，并单独记录 DuckDB 临时磁盘峰值。
- [ ] realtime/step 在各自流式 ticket 完成前保持现有语义，完整 Replay 回归套件继续通过。
- [ ] 本 ticket 不把读取异常伪装为空回放；若完整公共失败协议尚未接入，必须保留明确错误边界供后续失败协议 ticket 完成。
