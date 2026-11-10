# 07: 持久化 Unrecoverable Backfill Gap 并关联解决 Job

**What to build:** 将当前 Collector 进程内 gap_unrecoverable 语义映射为可持久查询的 Gap 记录，保留首次/最近发现、原因、Feed、InstrumentId、Interval、范围、open/resolved 和关联 Backfill Job；不实现通用 Sequence 连续性。

**Blocked by:** 01, 02, 06

**Status:** resolved

- [ ] 进程重启后历史 Gap 仍可查询，当前内存快照与持久来源可区分。
- [ ] 重复发现更新首次/最近语义并保留追加历史，不制造无界重复记录。
- [ ] 成功 Backfill Job 可关联 resolved，失败或部分成功不误关全部 Gap。
- [ ] 查询支持时间、Feed、InstrumentId、Interval、原因和状态，无物理删除。
- [ ] 不计算或暗示 seq+1 连续性，不改变可选来源 seq 契约。
- [ ] 重启、重复、部分解决和权限测试完整。

## Comments

**Resolved (2026-11-10):** 持久 Unrecoverable Backfill Gap 已合并 `da4f2c9`（feature `0dbd126`）。迁移 010；持久 gap 与 append-only occurrence 历史；Collector 进程内快照经 `gaps.snapshot` 轮询并 upsert，Collector 离线不影响历史查询；重复发现更新 first/last 与计数，不产生无界重复；只有 succeeded Job 能解决完全覆盖的 open gap，partial/failed/cancelled 只关联不误关；支持 reopen 且无物理删除；查询过滤/分页完整。33 个新测试，合并时 1756 passed / 5 个既有 CLI 失败。未引入 seq+1 连续性假设。
