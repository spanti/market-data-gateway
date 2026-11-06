# 03: 将 Compaction 与 GC 运维命令接入按需组合

**What to build:** 运维人员执行 Compaction、Bars GC 和 Quarantine GC 时，CLI 通过 Storage Composition 获得恰好需要的存储对象：前两者使用 Bars-only，后者使用 Quarantine-only。命令行参数、root 传播、dry-run、报告、Read Lease 保护和 Retention 行为保持不变，无效命令仍在任何目录写入之前失败。

**Blocked by:** 01: 用 Bars-only Storage Composition 打通只读读取路径

**Status:** ready-for-agent

- [ ] Compaction 与 Bars GC 的默认生产路径使用 Bars-only 组合，不再各自重复 Store 配置映射。
- [ ] Quarantine GC 使用 Quarantine-only 组合，不创建 ParquetBarStore、DuckDB 临时目录或 Bars Dataset 状态。
- [ ] Quarantine-only 组合每次返回新的 QuarantineStore，并在构造前根据本次实际 root 组合执行相同的 Dataset 独立性校验。
- [ ] CLI root override 优先级与配置语义保持不变；默认 Quarantine 跟随实际 root，显式 Quarantine root 继续原值优先。
- [ ] mdgw up 继续把解析后的 root 传给子进程，collector、api、replay 的启动和运维行为不因命令组合迁移而改变。
- [ ] CLI 参数校验继续发生在 Storage Composition 之前；无效时间、Retention 或其他参数错误不会创建数据 root、descriptor、Manifest 或临时目录。
- [ ] Compaction 的不可变 Data File、原子 Manifest 切换、Rule Version 集合并集、Read Lease 重查与报告语义不变。
- [ ] Bars GC 的 Retention、dry-run、Manifest 失活、删除与 Read Lease 保护语义不变；Quarantine GC 的独立 Schema、整月清理、当前月跳过和 dry-run 行为不变。
- [ ] 本次迁移不合并 DuckDBConfig 与 DuckDBOptions，不改变两个 Store 的公共构造接口，也不要求 benchmark 或测试数据构建器改用组合模块。
- [ ] 新增或调整测试穿过 Storage Composition 与 CLI 公共接口，覆盖按需对象创建、root 传播、参数错误零写入和各命令的实际运维结果，不重复断言完整构造参数列表。
- [ ] 现有 CLI flags、多进程 Service Role、Compaction、Bars GC、Quarantine Retention/GC、Manifest 与 Read Lease 测试保持通过。
- [ ] archive CLI 可在本 ticket 中暂时继续使用其现有 Quarantine 默认创建方式并保持测试通过；其完整联合组合迁移由 ticket 04 完成。
- [ ] 不新增生产组合开关、存储 adapter、全局生命周期管理者、GLOSSARY 术语或 ADR。
