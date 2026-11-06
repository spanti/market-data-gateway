# Spec: 集中 GatewayConfig 的 Storage Composition

Status: ready-for-agent

## Problem Statement

网关维护者目前在五个生产组合点重复创建 Bars Store，在另外五个位置重复创建或默认创建 Quarantine Store。每个组合点都要重新理解 root override、DuckDB 资源、Data Contract Version、Quarantine Schema Version、Retention 和 Normalization Rule Version 的映射。新增一个存储选项或修正一次血缘默认值，往往需要同时修改多个 Service Role、GatewayRuntime 与 CLI；最近的 Rule Version 缺陷已经证明了这种分裂的维护成本。

更重要的是，配置校验只能看到静态的 storage root，看不到 Service Role 或 CLI 的实际 root override。显式 Quarantine root 不跟随 override，而默认 Quarantine root 会跟随；两者组合后，Quarantine 可能落进实际 Bars Dataset，违背 ADR-0014 的 Dataset 独立性要求。若只把构造函数搬进一个浅薄模块，而不拥有实际 root 解析、写入前校验和 Rule Version 一致性，这个问题不会消失。

运维方、嵌入式调用方和测试作者又不能让这次收敛改变既有行为：Bars Dataset、Instrument Snapshot、High-Water Mark、DuckDB 临时目录和默认 Quarantine 的位置必须保持稳定；archive 直接调用者依赖未注入 Quarantine 时使用 Store root 下的默认 Quarantine；现有显式注入 seam 与 Store 公共接口也必须继续可用。

## Solution

引入一个深的 **Storage Composition** 模块，把“已验证的 GatewayConfig 加可选 root override 如何变成正确存储组合”收进一个小 interface 后面。模块提供 Bars-only、Quarantine-only、Bars + Quarantine 三种按需组合，不引入全局 singleton、存储 adapter 或新的生命周期管理者。

组合模块统一实际 root 的优先级、DuckDB 配置转换和由它创建的对象所使用的 Normalization Rule Version。它在调用任何可能写入文件系统的 Store 构造函数之前，根据实际生效的 Bars root 重新检查 Quarantine 不位于 Bars Dataset 内；即使配置是通过程序化复制绕过原有校验构造的，也不能跳过这道检查。

全部生产组合点一次迁移，不保留双路径或开关。api 与 replay 只组合 Bars，Quarantine GC 只组合 Quarantine，collector、GatewayRuntime 和 archive CLI 按需组合两者。现有 Data Contract、REST API、Replay Protocol、CLI、Store 公共接口、archive 直接调用默认行为和显式注入语义保持不变。

测试以 Storage Composition 的 interface 为新增测试面，使用真实临时目录和真实 DuckDB / Polars 行为；现有 GatewayRuntime、Service Role、CLI、REST、Replay、archive 和 Rule Version 集成测试继续守护公共行为。

## User Stories

1. 作为网关维护者，我想要一个深的 Storage Composition 模块，这样存储组合规则只在一个地方修改。
2. 作为网关维护者，我想要 Bars Store 的配置映射集中拥有，这样新增存储选项不再需要修改五个生产组合点。
3. 作为网关维护者，我想要 Quarantine Store 的配置映射集中拥有，这样 Service Role 与 CLI 不会各自解释同一配置。
4. 作为维护者，我想要 Bars-only 组合，这样 api 与 replay 不会获得它们不需要的 Quarantine 依赖。
5. 作为运维人员，我想要 Quarantine-only 组合，这样 Quarantine GC 不需要创建 Bars Store 或 DuckDB 临时目录。
6. 作为维护者，我想要 Bars + Quarantine 组合，这样写入路径能用同一份有效配置创建两个独立 Dataset 的存储对象。
7. 作为维护者，我想要组合操作每次返回新对象，这样不同 Service Role 不会意外共享进程全局状态。
8. 作为维护者，我想要现有调用方继续拥有存储对象生命周期，这样启停与关闭责任不会转移到新的管理者。
9. 作为维护者，我想要继续使用现有具体 Store，这样只有一套生产实现时不会增加假想 adapter seam。
10. 作为配置维护者，我想要 GatewayConfig 继续负责字段语法和取值校验，这样配置模型的职责保持清楚。
11. 作为运维人员，我想要显式 root override 优先于配置中的 storage root，这样现有启动方式不改变。
12. 作为运维人员，我想要默认 Quarantine root 跟随实际生效的 storage root，这样测试与部署覆盖根目录时仍得到预期布局。
13. 作为运维人员，我想要显式 Quarantine root 继续优先且不跟随 Bars root override，这样已有部署不会被静默迁移。
14. 作为数据工程师，我想要组合前按实际 Bars Dataset 位置复核 Quarantine 独立性，这样 override 不会绕过 ADR-0014。
15. 作为数据工程师，我想要无效组合在任何文件系统写入之前失败，这样错误启动不会留下半创建的数据目录。
16. 作为维护者，我想要组合校验覆盖程序化复制得到的配置，这样绕过模型校验的调用也不能制造嵌套 Dataset。
17. 作为运维人员，我想要本次不新增反向包含禁令，这样现有合法但不同的目录布局不会被意外拒绝。
18. 作为数据工程师，我想要 Bars Dataset 继续位于实际 root 下的 bars 目录，这样已有 Data Files 与 Manifest 无需迁移。
19. 作为运维人员，我想要默认 Quarantine 继续位于实际 root 下的 quarantine 目录，这样现有审计数据仍能被发现。
20. 作为维护者，我想要 Instrument Snapshot、High-Water Mark 与 DuckDB 临时目录继续锚定同一个实际 Store root，这样派生路径不会分裂。
21. 作为数据工程师，我想要所有由同一次组合创建的写入对象使用同一个有效 Normalization Rule Version，这样血缘不会因调用点不同而分叉。
22. 作为运维人员，我想要未配置 Rule Version 时继续使用当前默认规则版本，这样升级不会改变审计值。
23. 作为运维人员，我想要自定义 Rule Version 同时到达 Bars Manifest、Quarantine Record、默认构建的 Feed Handler 和 archive CLI，这样一次运行的审计身份一致。
24. 作为契约维护者，我想要 Rule Version 继续只是审计元数据，这样它不会取代 Data Contract Version 的兼容性职责。
25. 作为嵌入式调用方，我想要显式注入的 Store、Quarantine 和 Feed Handler 依赖不被组合模块重写，这样我能保持对自定义行为的控制。
26. 作为维护者，我想要 Feed Handler 仍由现有采集组合逻辑创建，这样存储模块不会扩张成网络协议组合模块。
27. 作为 archive 直接调用方，我想要未注入 Quarantine 时继续使用 Store root 下的默认 Quarantine，这样我的现有调用无需增加 GatewayConfig。
28. 作为 archive CLI 用户，我想要运维命令继续使用配置化 Quarantine 和统一 Rule Version，这样直接调用与运维调用的既有差异保持明确。
29. 作为数据工程师，我想要 Quarantine 的物理格式默认值保持独立，这样 Bars 的压缩或行组配置不会被无意传播。
30. 作为维护者，我想要本次保留 DuckDBConfig 与 DuckDBOptions 的不同职责，这样配置校验与运行时连接行为不会因类型合并而耦合。
31. 作为维护者，我想要配置模块继续保持对 DuckDB 读取实现的懒加载依赖，这样导入依赖不会倒退。
32. 作为直接 Store 使用方，我想要两个 Store 的公共构造接口保持不变，这样测试、benchmark、子类和嵌入代码不需要批量改写。
33. 作为 REST 与 Replay 使用方，我想要线上协议与读取语义完全不变，这样存储组合重构不会影响消费者。
34. 作为 CLI 用户，我想要命令、参数、退出表现和 root 向子进程传播的方式保持不变，这样运维脚本无需修改。
35. 作为 CLI 用户，我想要参数错误在存储组合之前被发现，这样无效命令仍不会创建数据目录。
36. 作为维护者，我想要全部生产组合点一次迁移，这样代码中不会长期存在两套配置解释。
37. 作为运维人员，我想要不增加组合实现开关，这样不同部署不会运行不同的 root 或血缘规则。
38. 作为测试作者，我想要穿过 Storage Composition 的 interface 测试外部可观察结果，这样测试不会绑定私有实现。
39. 作为测试作者，我想要使用真实临时目录和真实库行为，这样路径与引擎问题不会被 mock 掩盖。
40. 作为测试作者，我想要现有公共行为集成测试尽量原样通过，这样重构有稳定的回归保护。
41. 作为测试作者，我想要现有显式注入测试继续有效，这样测试替身仍穿过原有 seam。
42. 作为测试作者，我想要重复断言构造参数的测试收敛到组合模块，这样 Service Role 测试不再复制同一映射矩阵。
43. 作为 benchmark 与测试维护者，我想要保留直接 Store 构造方式，这样本次不会产生大规模无收益改写。
44. 作为文档读者，我想要实际 root、显式 Quarantine root 和写入前校验的说明保持一致，这样文档不会出现第二套规则。
45. 作为领域模型维护者，我想要不把内部 Storage Composition 名称写进领域术语文档，这样领域模型只保留业务概念。
46. 作为实现代理，我想要不新增 ADR，这样本次只落实 ADR-0013 与 ADR-0014 的既有决策而不重新决策。
47. 作为实现代理，我想要验收以当前实测基线上的无新增失败为准，这样不会夹带修复无关问题。
48. 作为实现代理，我想要保留工作区已有未提交修改，这样本次 spec 的实施不会覆盖其他正在进行的工作。

## Implementation Decisions

### Storage Composition 模块与 interface

- 在 Store 所属包中新增 Storage Composition 模块。它是内部架构模块，不新增领域术语。
- interface 接受已验证的 GatewayConfig 和可选的实际 root override，只提供三种明确操作：组合 Bars、组合 Quarantine、组合 Bars 与 Quarantine。不使用含义模糊的通用布尔组合开关。
- Bars 操作返回 ParquetBarStore；Quarantine 操作返回 QuarantineStore；联合操作返回具有明确命名成员的两个对象，并让所有相关调用方取得同一个有效 Normalization Rule Version。
- 联合操作必须先完成两种实际 root 的解析和独立性校验，再创建任一对象，避免 Bars Store 的临时目录副作用先于 Quarantine 校验发生。
- 每种组合都在构造对象前校验本次实际 root 组合；按需指的是对象创建范围，不是跳过配置一致性检查。
- 每次组合操作返回新对象，不缓存 Store，不注册全局对象，不持有后台线程，不负责调用方的启动、停止或关闭。
- 组合模块向现有默认 Feed Handler 构建逻辑提供有效 Rule Version，但不创建、启动或拥有 Feed Handler。

### 实际 root 与 Dataset 独立性

- 实际 storage root 的优先级固定为：显式 root override，其次 GatewayConfig 中的 storage root。
- Bars Dataset root 继续是实际 storage root 下的 bars；Instrument Snapshot、High-Water Mark 和默认 DuckDB 临时目录继续使用当前基于 Store root 的派生规则。
- 未显式配置 Quarantine root 时，使用实际 storage root 下的 quarantine。
- 显式 Quarantine root 继续原值优先，不跟随 storage root override；相对显式路径继续保持相对于进程当前工作目录的含义。路径规范化只用于安全比较，不用于重新锚定或改写配置。
- 在组合时同时规范化实际 Bars Dataset root 与 Quarantine root，重新执行 ADR-0014 的单向包含检查：两者相同或 Quarantine 位于 Bars Dataset 内时失败。
- 不新增“Bars 不得位于 Quarantine 内”的反向禁令，也不把“Quarantine root 等于 storage root”擅自改成错误；这些不是本次已确认的契约变化。
- 校验必须覆盖 root override、路径中的父目录片段和绕过模型校验构造的配置。比较不得要求路径预先存在。
- 所有校验完成后才允许调用 ParquetBarStore 构造函数，因为它会创建 DuckDB 临时目录；QuarantineStore 继续保持构造时无文件系统副作用。
- GatewayConfig 保留现有字段校验和公共解析行为；组合模块补充的是实际运行时组合校验，不复制一份配置模型。
- CLI 保持先校验命令参数、后请求存储组合的顺序。

### Rule Version 与注入语义

- 同一次组合创建的所有写入对象使用配置中的同一个有效 Normalization Rule Version；未配置时仍由现有常量提供默认值。
- Bars Store 的新 Manifest 条目、Quarantine Record、默认构建的 Feed Handler 和 archive CLI 使用一致的值。
- Rule Version 只用于审计血缘，不参与 Data Contract 兼容性判断，不增加引擎或规则实现开关。
- 组合模块只决定它自己创建的对象和默认构建路径；显式注入的 Store、Quarantine 或已配置 Feed Handler 不被重写。
- 现有 CollectorCore、archive import、HTTP 应用和 Replay 内部依赖注入 seam 保持原样；不为原本没有注入参数的公共 Service Role 构造函数额外扩张接口。
- 对已经持有 Quarantine 的注入 Feed Handler，保持现有“只填补缺失依赖”的行为，不强制替换其存储对象。

### DuckDB 与物理存储配置

- DuckDBConfig 继续承担配置形状、默认值和输入校验；DuckDBOptions 继续承担运行时连接配置。两者本次不合并，不反转依赖方向。
- 生产调用方不再各自执行 DuckDB 配置转换；Storage Composition 统一完成现有转换。
- 保留配置模块对 DuckDB 读取实现的懒加载约束，不让组合模块把 DuckDB 导入重新变成配置导入的必然后果。
- DuckDB 的 memory、threads、临时目录容量、Arrow batch size、UTC 和连接缓存行为不变。
- QuarantineStore 不自动继承 Bars 的 compression、row group size 或 Parquet format version；现有 Quarantine 默认值和独立 Schema Version 保持不变。
- ParquetBarStore 与 QuarantineStore 的公共构造接口不变，Parquet + Manifest + Arrow Data Contract 不变。

### 生产调用方迁移

- GatewayRuntime 与 collector 写入路径使用 Bars + Quarantine 组合，并把同一 Quarantine 实例交给现有采集管线和默认 Feed Handler 构建逻辑。
- api 与 replay Service Role 使用 Bars-only 组合，不创建 QuarantineStore。
- Compaction 与 Bars GC 使用 Bars-only 组合；Quarantine GC 使用 Quarantine-only 组合。
- archive CLI 使用 Bars + Quarantine 组合，并把同一有效 Rule Version 传给 archive import。
- archive import 的独立调用接口保留可选 Quarantine；缺省时仍创建 Store root 下的默认 Quarantine，不要求直接调用者构造 GatewayConfig。
- 所有生产构造点迁移完成后，删除重复的参数映射，不保留旧组合路径或切换开关。
- 测试、benchmark、 characterization 数据构建器和 Store 子类仍可按照现有公共构造接口直接创建 Store，不做批量迁移。

### 兼容性与文档

- 不修改 Bars 或 Quarantine 的 Data Contract、Manifest、Data File 布局、Instrument Snapshot、High-Water Mark、REST API、Streaming Interface 或 Replay Protocol。
- 不修改 Service Role 和 CLI 的公共启动方式；mdgw up 继续把解析后的 root 传给子进程。
- 更新设计与使用文档中涉及 Storage Composition、root override、显式 Quarantine root 和写入前校验的说明。
- Storage Composition 是内部实现名称，不加入 GLOSSARY；现有 ADR-0013 与 ADR-0014 已足够，不新增 ADR。
- 实施时保留工作区已有未提交修改，不顺手处理其他架构候选或无关缺陷。

### 实施顺序

1. 新增 Storage Composition 与其 interface 测试，先证明实际 root、零写入失败、按需组合和 Rule Version 行为。
2. 迁移 GatewayRuntime 与 collector 写入路径，运行现有采集、Quarantine 和血缘集成测试。
3. 迁移 api、replay 与 CLI 运维组合点，运行 Service Role、多进程和 CLI 测试。
4. 迁移 archive CLI，保持直接调用默认行为不变，删除全部重复生产映射并同步文档。
5. 运行完整离线套件和必要的跨平台路径测试，以当前实测基线为比较，不引用历史失败数量代替本次结果。

## Testing Decisions

### 好测试的标准

- 只断言 interface 可观察结果：返回的对象类型与根路径、按需创建范围、每次返回新实例、有效 Rule Version、允许的目录布局、抛出的公开错误类型，以及失败前没有文件系统副作用。
- 不断言私有字段、内部调用次数、具体 SQL、私有辅助函数或构造函数参数列表；Service Role 与 CLI 测试不重复完整映射矩阵。
- 使用真实临时目录、真实 Parquet / Manifest 和真实 DuckDB / Polars 行为；不为本地依赖增加 mock adapter。
- 优先使用现有高 seam 守护公共行为；Storage Composition 是唯一新增的直接测试面。
- 测试需要覆盖成功与失败路径，但不能通过重新实现生产映射来计算期望值。

### Storage Composition interface 测试

- 验证 Bars-only 只创建 Bars Store，Quarantine-only 只创建 Quarantine Store，联合组合返回两个命名明确且互相独立的对象。
- 验证重复组合不会返回同一个 Store 实例，也不会引入跨组合连接或生命周期共享。
- 验证默认 root、显式 override 和显式 Quarantine root 的优先级；默认 Quarantine 跟随 override，显式 Quarantine 不跟随。
- 验证 Bars、默认 Quarantine、Instrument Snapshot、High-Water Mark 与 DuckDB 临时目录的实际根位置保持当前契约。
- 验证 override 后 Quarantine 等于或位于实际 Bars Dataset 内时失败，并覆盖父目录片段、相对路径及绕过模型校验的配置。
- 验证联合组合失败时 Bars root、DuckDB 临时目录、Dataset descriptor、Manifest 和 Quarantine 状态都没有被创建。
- 验证 ADR-0014 已允许的非嵌套布局仍可组合，不用更宽的反向包含规则制造额外失败。
- 验证默认与自定义 Rule Version 到达 Bars 和 Quarantine 写入结果；通过现有 Feed Handler 集成 seam 验证默认构建路径使用同一个值。
- 验证 DuckDB 资源配置与当前转换结果一致，未配置临时目录时仍位于实际 Store root 下。
- 验证 Quarantine 的独立 Schema Version、Retention 和物理格式默认值没有继承 Bars 配置。
- 验证每次组合得到独立对象，但同一联合组合中的采集管线共享其应有的同一个 Quarantine 实例。

### 保留的既有 seams

- **GatewayRuntime 端到端**：继续验证采集、持久化、Quarantine、Instrument Snapshot、High-Water Mark、REST 和 Replay 的公共行为。
- **Service Role 公共生命周期**：继续验证 api、replay、collector 的启动、状态、就绪、指标和关闭；不增加新的生产测试专用入口。
- **CLI 公共命令**：继续验证 root 向子进程传播、Compaction、GC、Quarantine GC、archive backfill 和参数错误零目录副作用。
- **archive import 公共接口**：现有直接调用测试继续读取 Store root 下的默认 Quarantine；注入 Quarantine 的失败行为测试保持不变。
- **Store 与 Data Contract seams**：现有 Store 子类、Schema、Manifest、Decimal、UTC 纳秒、Read Lease 和 Replay 测试不因内部组合迁移而改写语义。
- **显式注入 seams**：CollectorCore 的 Store / Quarantine 注入、archive 的 Quarantine 注入、HTTP 应用和 Replay 的 Store 注入继续由现有测试覆盖。

### 测试调整与先例

- 将重复验证同一构造参数映射的测试收敛到 Storage Composition；保留验证调用方自身行为的断言。
- 配置模型测试继续覆盖字段校验和显式 Quarantine root 的公共解析语义，不把全部配置测试机械搬到新模块。
- 新增的实际 root 竞态与零写入测试参考现有 archive 参数校验、Quarantine 独立性、Normalization Rule Version lineage 和多进程 Service Role 测试。
- archive 默认 Quarantine 的大量既有行为测试应保持原样，用它们证明兼容而不是重写预期。
- 实施开始先记录当前完整离线测试基线；最终验收是无新增失败，并单独报告平台相关跳过或既有波动，不顺手修复无关问题。

## Out of Scope

- 合并 DuckDBConfig 与 DuckDBOptions，或为两者另建共享默认值模块。
- 新增 StoragePort、Repository、存储 adapter、第二套生产存储实现或全局 Store registry。
- 改变 Bars 与 Quarantine 的独立性，把 Quarantine 纳入 Bars 查询、REST、export 或 Replay。
- 改变 Data Contract、Arrow Schema、Parquet 布局、Manifest、Rule Version 的审计含义或 Schema 兼容规则。
- 新增反向目录包含禁令，或改变显式 Quarantine root 的相对路径锚点。
- 改变 archive 直接调用的默认 Quarantine 位置，或要求直接调用者提供 GatewayConfig。
- 让 Storage Composition 创建 Feed Handler、管理网络连接、拥有 Service Role 生命周期或接管 DuckDBConnectionCache。
- 为 Quarantine 增加 Bars 物理格式配置继承，或新增配置字段。
- 批量迁移测试、benchmark、示例和直接 Store 子类到新组合模块。
- 实现架构评审中的 Snapshot / Lease、Manifest 写入、Replay scan window 或 Quarantine Record 构造候选。
- 修复与本次组合无关的 CLI、Replay、分页、文档或性能问题。

## Further Notes

- 本 spec 来自 candidate 5 的已确认设计访谈；关键决策包括按需组合、公共兼容、显式注入、唯一有效 Rule Version、完整生产迁移、写入前校验、真实依赖测试、无生命周期接管和无新增 adapter。
- 生产代码的构造点数量以当前核查为准：五个 Bars Store 组合点和五个 Quarantine Store 创建或默认创建位置；实施时若代码已变化，应迁移等价的新增生产组合点，而不是机械追求数量。
- archive 直接调用的默认 Quarantine 是有文档字符串和测试固定的兼容行为，不应被描述成缺陷。
- 当前未发现需要重新解释的 ADR；本工作是提高 locality 与 leverage，而不是重新选择存储技术。
