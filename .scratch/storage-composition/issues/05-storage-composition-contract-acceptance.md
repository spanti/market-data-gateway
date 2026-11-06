# 05: 收缩旧组合实现并完成文档与全量验收

**What to build:** 网关的全部生产存储创建都经过已确认的按需 Storage Composition，不再存在重复的配置映射、旧组合后备或切换开关。维护者能从设计与使用文档理解实际 root 和写入前校验，测试与验收结果证明公共行为、直接 Store 使用和跨平台路径语义没有非预期变化。

**Blocked by:** 04: 将 archive backfill CLI 接入统一组合并保持直接调用兼容

**Status:** ready-for-agent

- [ ] 审计全部生产 Bars Store 与 Quarantine Store 创建点；除 Storage Composition 和 archive 直接调用的已确认兼容默认外，不再存在重复的 GatewayConfig 到 Store 参数映射。
- [ ] 若实施期间新增了等价生产组合点，也已迁移到相同规则；最终没有旧组合路径、引擎开关、组合开关或生产测试专用后备。
- [ ] Bars-only、Quarantine-only、Bars + Quarantine 三种操作均按 spec 工作，且不存在绕过实际 root 校验的生产调用。
- [ ] api、replay、collector、GatewayRuntime、Compaction、Bars GC、Quarantine GC 和 archive CLI 的默认生产路径均使用正确的按需组合。
- [ ] ParquetBarStore 与 QuarantineStore 的公共构造接口保持兼容；测试、benchmark、示例、characterization 数据构建器和 Store 子类没有被无收益地批量迁移。
- [ ] 显式注入、archive 直接调用默认 Quarantine、Feed Handler 缺失依赖填补、Rule Version 审计含义和 Dataset 独立性均与 spec 一致。
- [ ] DuckDBConfig 与 DuckDBOptions 仍未合并，配置懒加载方向未倒退，Quarantine 物理格式配置没有意外继承 Bars。
- [ ] 设计与使用文档同步说明 Storage Composition、root override、默认与显式 Quarantine root、实际 root 独立性校验和失败前零写入；文档不存在第二套规则。
- [ ] 未把内部 Storage Composition 名称加入 GLOSSARY，未新增 ADR，未实现其他架构候选或无关修复。
- [ ] Storage Composition interface 测试与现有 GatewayRuntime、Service Role、CLI、REST、Replay、archive、Normalization、Quarantine、Rule Version、Manifest 和 Data Contract 测试全部按预期执行。
- [ ] 在 Windows 与可用 POSIX 环境执行实际 root、相对路径、父目录片段和失败前零写入测试；路径比较不依赖某一操作系统的分隔符或当前目录假设。
- [ ] 实施开始时的当前完整离线基线与最终结果均已记录；最终相对基线无新增失败，命令、通过数、失败、跳过和任何波动均如实记录，未用历史结果替代实测。
- [ ] ticket 的 Comments 记录迁移审计、文档变更、测试命令与结果、跨平台执行范围以及任何未执行验证；未执行的验证不得写成通过。
- [ ] 工作区已有未提交修改得到保留，父 spec 的状态和内容不因本 ticket 收尾而被擅自修改。
