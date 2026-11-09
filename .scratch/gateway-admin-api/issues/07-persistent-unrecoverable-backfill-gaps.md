# 07: 持久化 Unrecoverable Backfill Gap 并关联解决 Job

**What to build:** 将当前 Collector 进程内 gap_unrecoverable 语义映射为可持久查询的 Gap 记录，保留首次/最近发现、原因、Feed、InstrumentId、Interval、范围、open/resolved 和关联 Backfill Job；不实现通用 Sequence 连续性。

**Blocked by:** 01, 02, 06

**Status:** ready-for-agent

- [ ] 进程重启后历史 Gap 仍可查询，当前内存快照与持久来源可区分。
- [ ] 重复发现更新首次/最近语义并保留追加历史，不制造无界重复记录。
- [ ] 成功 Backfill Job 可关联 resolved，失败或部分成功不误关全部 Gap。
- [ ] 查询支持时间、Feed、InstrumentId、Interval、原因和状态，无物理删除。
- [ ] 不计算或暗示 seq+1 连续性，不改变可选来源 seq 契约。
- [ ] 重启、重复、部分解决和权限测试完整。

## Comments
