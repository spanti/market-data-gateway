# 04: 删除死 Gap seam 并完成 contract 验收

**What to build:** 完成 Backfill execution 的 contract 阶段：删除没有 caller 的 gap_linker 和残余旧执行代码，让真实执行与 Admin reconcile 成为唯一的 Job Gap 路径，并证明整个非 network 测试基线没有新增失败。

**Blocked by:** 03《让 Backfill Job 穿过同一 execution seam》

**Status:** resolved

- [ ] 删除 Runner 构造参数、内部类型和终态处理中的 gap_linker callback，不把它重新接入 Admin，也不保留无 caller 的兼容分支。
- [ ] Runner status 与 Admin wire 中 resolved_gap_ids 字段继续存在，resolved ids 仍只由现有 Admin polling/reconcile 流程持久化。
- [ ] Admin 继续只在实际 active→terminal 跃迁后 reconcile；succeeded 且自身无 hole 才能解决完全包含的 open Gap，其他终态只关联不解决。
- [ ] Gap reconcile/audit 失败继续只记录日志而不回滚已完成 Job，Collector 离线或空快照不删除历史。
- [ ] 01 建立的真实 Runner terminal document → Admin polling → 持久 Gap 组合测试在最终实现上通过，且没有第二套 Gap 所有权路径。
- [ ] 自动与 Job 的旧重复 orchestration 全部删除，两条公共入口都穿过同一 Backfill execution seam，不存在配置项、环境变量或运行时新旧开关。
- [ ] Job counters 文档将 rejected_bars 统一描述为 Validation rejection 与 unreadable identity，移除 Quarantine 暗示；两条 Backfill 路径仍不写 Quarantine。
- [ ] 保留 ADR-0014 对自动 HWM Backfill 整批失败与 Backfill Job partial-outcome 的明确区分，不依据旧措辞改变运行行为。
- [ ] GLOSSARY 不新增 Backfill execution 等内部架构词汇，Backfill、Backfill Job 和 Unrecoverable Backfill Gap 的领域含义保持不变。
- [ ] 自动 Backfill、Job Runner、Admin Backfill、Gap、live ingest 和 archive 聚焦测试全部通过。
- [ ] 完整非 network 测试相对 01 记录的实测基线无新增失败；若存在既有失败，逐项列出并证明不是本 feature 引入。
- [ ] 最终检查公共 Python、控制、HTTP、Dataset、HWM、health、Revision、状态、计数和 Gap 行为均未发生未授权变化。
- [ ] 保留工作区其他未提交修改，不回滚、不覆盖、不提交或推送任何未经单独授权的变更。

## Comments

**Resolved (2026-11-11):** 已在 `feature/backfill-execution-04` 实现并合并（feature `f77fdb4`，merge `19a47ac`）。彻底删除无 caller 的 `gap_linker`/`GapLinker`/`BackfillJobGapLink`、Runner 构造参数、`_finish` 终态回调与 `__all__` 条目，仓库 grep 0 命中；`resolved_gap_ids` 继续保留在 Runner status document 与 Admin wire，Admin polling/reconcile 仍是唯一 Gap 所有者。更新 `src/mdgw/admin/jobs/models.py` 的 counters 文档（`rejected_bars` = Validation rejection + unreadable identity，移除 Quarantine 暗示）、`store.py` 与 009 migration 注释；ADR-0014 的自动整批失败 vs Job partial-outcome 澄清与 GLOSSARY 语义均未改变。两条公共入口均实际穿过同一 Backfill execution seam，无旧 orchestration、无配置/环境变量/运行时新旧开关。代码复审发现由 `da17690` 修复并合并为 `5165913`（私有 adapter 形状测试改为公共 Runner/module seam 测试、docstring 时态、测试 helper 去重；`tests/` 中私有生产符号 0 命中）。最终全量非 network 测试：**1919 passed, 1 skipped, 5 failed**，失败仅为 4 个既有 archive CLI 失败 + `test_cli_flags::test_run_rejects_an_unknown_service_role`，相对 01 基线无新增失败；聚焦 123 passed + 4 个既有 archive CLI 失败。期间一次既有 Windows 时序抖动（`test_ccxt_feed_handler.py::test_queue_overflow_is_not_quarantined`）在隔离与最终全量中通过。
