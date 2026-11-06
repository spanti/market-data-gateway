# 02: 用 Bars + Quarantine 组合打通采集写入与血缘

**What to build:** collector 与 GatewayRuntime 可以通过一次 Storage Composition 获得相互独立的 Bars Store 和 QuarantineStore，并让同一次默认生产组合中的 Bars Manifest、Quarantine Record 与默认构建的 Feed Handler 使用同一个有效 Normalization Rule Version。两个 Dataset 的物理与语义独立性、现有显式注入和采集原子性保持不变。

**Blocked by:** 01: 用 Bars-only Storage Composition 打通只读读取路径

**Status:** ready-for-agent

- [ ] Bars + Quarantine 组合返回命名明确的 ParquetBarStore 与 QuarantineStore；每次组合返回新对象，但同一次采集组合中的管线与默认 Feed Handler 共享其应有的同一个 QuarantineStore。
- [ ] 联合组合在创建任一对象前完成两种实际 root 的解析与独立性校验；校验失败时不会留下 Bars root、DuckDB 临时目录、Dataset descriptor、Manifest 或 Quarantine 状态。
- [ ] collector 与 GatewayRuntime 的所有默认生产构造路径迁移到联合组合，重复 Bars / Quarantine 参数映射被删除，不保留双路径或组合开关。
- [ ] 同一次组合创建的写入对象使用配置中的同一个有效 Normalization Rule Version；未配置时继续使用当前默认值，自定义值不依赖调用方各自补传。
- [ ] 默认与自定义 Rule Version 均能通过真实写入到达 Bars Manifest 和 Quarantine Record，并到达现有默认 Feed Handler 构建路径。
- [ ] Rule Version 继续只是审计元数据，不参与 Data Contract 兼容性判断，不新增规则实现或引擎开关。
- [ ] CollectorCore 的 Store / Quarantine 显式注入、archive 的 Quarantine 注入以及现有测试替身继续有效；组合模块只决定它创建的对象和默认构建路径。
- [ ] 已持有 Quarantine 的注入 Feed Handler 不被替换，现有只填补缺失依赖的行为保持不变；Storage Composition 不创建或拥有 Feed Handler。
- [ ] Bars 与 Quarantine 的 Schema Version、Manifest、Data File、Retention 和物理格式默认值保持独立；Quarantine 不继承 Bars 的 compression、row group size 或 format version。
- [ ] 实时批次、Instrument Validation、Quarantine fail-closed 写入、Bar Aggregation、High-Water Mark 和既有 Backfill 行为不变。
- [ ] 新增联合组合测试覆盖对象独立性、同组合 Quarantine 身份、失败前零写入、默认与自定义 Rule Version，以及真实临时目录中的持久化结果。
- [ ] 现有采集、Normalization、Quarantine、CCXT runtime、Rule Version lineage、High-Water Mark 与 GatewayRuntime 端到端测试保持通过，显式注入测试无需改写语义。
- [ ] 不引入全局 registry、生命周期管理者或存储 adapter，不修改公共 Store 构造接口、GLOSSARY 或 ADR。
