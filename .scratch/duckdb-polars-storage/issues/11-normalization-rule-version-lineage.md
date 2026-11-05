# 11: 打通 Normalization Rule Version 血缘

**What to build:** 让运维人员能从 Manifest 和 Quarantine 判断一份数据由哪些 Normalization 规则产生，并在 Compaction 后保留完整来源血缘。Rule Version 只用于审计，不成为第二个公共 Schema 权威。

**Blocked by:** 09 在 Collector 批量边界落地 Polars Normalization 和 Quarantine 核心。

**Status:** ready-for-agent

- [ ] 每个新 Bars Data File 的 Manifest 条目记录产生该批次所使用的 Normalization Rule Version 集合。
- [ ] 每条 Quarantine Record 记录失败输入所适用的 Rule Version，与独立 Quarantine Schema Version 和 Data Contract Version 明确区分。
- [ ] 旧 Data Files 能表示空集合或明确的 legacy/unknown，不得伪造历史规则版本，也不得因此无法读取。
- [ ] Compaction 不重做 Normalization，输出条目的 Rule Version 集合是所有来源条目版本的并集。
- [ ] 多次 Compaction 不会重复、丢失或伪造版本信息，也不会改变 Bars 的字段值和 Revision。
- [ ] 查询层继续只根据 Data Contract Version 判断 Schema 兼容性，旧 Rule Version 不自动导致读取拒绝。
- [ ] 若规则变化会改变公共数据语义，验证与文档要求升级 Data Contract，而不能只增加 Rule Version。
- [ ] 版本元数据扩展不会破坏现有 Manifest 读取、Compaction、GC、Read Lease 和状态输出。
- [ ] 测试覆盖单版本、多版本、legacy 来源、重复 Compaction、Quarantine 血缘和不受支持的 Data Contract Version。
