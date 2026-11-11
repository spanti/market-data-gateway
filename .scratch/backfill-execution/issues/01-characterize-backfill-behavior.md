# 01: 用公共入口固定 Backfill 行为基线

**What to build:** 在不改变生产行为的前提下，通过自动 HWM Backfill 与 Backfill Job 的现有公共入口建立可重复的行为基线。让共同成功路径、Revision 修正、持久化/HWM 顺序和真实 Job Gap 证据在执行模块迁移前就有高层测试保护。

**Blocked by:** None (can start immediately)

**Status:** resolved

- [ ] 在旧实现上建立跨入口 differential，使用同一 InstrumentId、相同 history、固定 clock 和等价初始 Dataset，分别穿过自动 Backfill 与 Job Runner 公共入口。
- [ ] differential 覆盖首次写入、相同内容幂等重跑和 source correction，比较逻辑 Bars、Revision、parent Bars 与最终 High-Water Mark。
- [ ] 比较口径不要求 Data File 名称、UUID、物理文件数量或未固定 ingest_ts 相同，也不强迫 BackfillReport、Job counters、health、gap 类型或终态一致。
- [ ] 自动整批失败与 Job partial-outcome 分别建立 characterization，不把 rejection 输入做成结果必须相同的 differential。
- [ ] BackfillReport 增加完整映射断言，覆盖 history_supported、instruments、raw fetched、parent-inclusive written、merge skipped、gap_events 和 written_by_interval。
- [ ] 通过失败注入证明 Dataset 写入失败时 HWM 不推进、written 成功计数不增加，已经成功写入的早期 chunk 不回滚。
- [ ] 固定自动持久化不产生 Job 特有 Core counter/health 副作用的基线，同时固定 Job 的 Core counters、逐 Bar health、expected keys 和 sink HWM cap 行为。
- [ ] 增加真实 Runner 产生缺失输入与 terminal document，经 Admin polling 写入持久 Unrecoverable Backfill Gap 的组合测试，不注入 gap_linker，也不绕过 active→terminal 持久化跃迁。
- [ ] 真实 Gap 组合测试覆盖 observed history missing，以及 succeeded 可解决完全包含的 open Gap、partially_succeeded 只关联不解决的现有语义。
- [ ] 记录当前聚焦测试基线并确认新增 characterization 在旧实现上通过；若发现行为缺陷，只记录证据，不在本票夹带行为修复。
- [ ] 不改变生产 interface、运行时语义、公共 wire 或既有测试断言来迎合新 differential。

## Comments

**Resolved (2026-11-11):** 已在 `feature/backfill-execution-01` 实现并合并（feature `21426e1`，merge `ffd08d7`）。新增 15 个测试：`tests/characterization/backfill_support.py` 提供共享夹具；`test_backfill_differential.py` 用同一 InstrumentId、相同 history、固定 clock 与等价初始 Dataset 分别穿过自动入口和 Job Runner，覆盖首次写入、相同内容幂等重跑和 source correction，比较逻辑 Bars、Revision、parent Bars 与最终 HWM；`test_backfill_characterization.py` 覆盖 BackfillReport 全字段映射、两条路径 rejection 语义、Dataset 写失败时 HWM/written 不推进且早期成功写入不回滚、自动无 Job 特有 Core counter/health/expected-key 副作用，以及 Job counters/逐 Bar health/expected keys/sink HWM cap；`test_admin_gaps_job_real.py` 用真实 Runner + 真实 Admin polling 覆盖 observed history missing、succeeded 解决完全包含的 open Gap、partially_succeeded 只关联不解决。基线记录于 `notes/baseline.md`（6 failed / 1890 passed / 1 skipped）；本票完成后全量 1905 passed，仅既有 6 个失败。生产代码未修改。
