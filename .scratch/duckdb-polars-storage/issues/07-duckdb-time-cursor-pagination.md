# 07: 迁移单 Instrument Time Cursor 分页

**What to build:** 用 DuckDB 的全局 `open_ts` 排序和 `limit+1` 查询替换当前逐批反复排序的分页实现，同时保持单 Instrument JSON 的 Time Cursor、响应形状和 raw Revision 语义。

**Blocked by:** 03 记录 Revision-heavy Time Cursor 的契约缺口；06 迁移非分页 JSON 与 count。

**Status:** ready-for-agent

- [ ] 只有同时提供 Instrument 和 limit 的 JSON 查询进入分页路径，其他查询和格式的响应形状保持不变。
- [ ] DuckDB 从整个请求 Snapshot 中选择全局最早的 `limit+1` 个 raw Revision 行，按 `open_ts` 升序返回前 limit 行。
- [ ] `next_start` 仍是第一条未返回 Bar 的 `open_ts`，下一次作为闭区间 start 使用。
- [ ] `has_more` 只考虑 end 以内的记录，范围外记录不会影响游标或分页标志。
- [ ] `limit=0` 返回空 bars，同时从同一 Snapshot 探测首条记录并计算正确的分页元数据。
- [ ] 多页测试覆盖跨文件、重叠 Data Files、边界时间和末页；在唯一 `open_ts` 序列上证明不重不漏。
- [ ] Revision-heavy 数据只刻画当前实际行为并引用独立契约问题，不得通过隐式去重、复合游标或改变响应形状暗修。
- [ ] JSON 行数 ceiling 继续基于 raw Revisions，count、探测和读取使用同一 Snapshot。
- [ ] 分页全局排序的内存和临时磁盘受明确资源预算约束，超过预算时产生稳定错误而非截断页。
- [ ] 与旧基线比较分页延迟和内存，不再执行每批 concat/sort 的 Python/Arrow 循环。
