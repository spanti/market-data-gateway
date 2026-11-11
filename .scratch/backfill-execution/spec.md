# Spec: 深化 Backfill Execution，统一自动 Backfill 与 Backfill Job 执行时序

Status: ready-for-agent

## Problem Statement

网关维护者目前需要在自动 HWM Backfill 与手动 Backfill Job 两套执行实现中分别理解 history fetch、closed-window、Normalization、Revision merge、Dataset 持久化、High-Water Mark 和 gap 证据。两者已经共享 Normalization 与 merge helper，但调用顺序、跨步骤不变量和失败处理仍由两套 orchestration 分别拥有；修正共享执行语义时，维护者必须人工同步两条路径，并分别阅读两套测试才能判断差异是有意契约还是意外漂移。

这两条路径不能被简单粗暴地拉平。自动 Backfill 按 HWM 选窗、整批拒绝、直接写 Dataset/HWM；Backfill Job 按显式范围分 chunk、统计 rejected Bars、允许部分成功，并通过 Core/sink 的 HWM cap、health 和 counters 持久化。当前测试分别覆盖两个入口，却没有对共同输入比较最终 Dataset、Revision、parent Bars 与 HWM 的 differential 保护。

此外，Backfill Job Runner 还保留一套没有任何生产或测试 caller 的 `gap_linker` callback 设计。真正的 Gap 关联与解决已经由 Admin polling/reconcile 完成，死 seam 让维护者误以为还存在第二个 Gap 所有权路径。ADR-0014 的旧措辞也没有区分自动 HWM Backfill 与 Backfill Job 的 Normalization rejection 语义，容易诱导后续实现者做出错误的“统一”。

## Solution

引入一个深的 **Backfill execution** module，把“一个 InstrumentId + 一个实际 fetch range”内的共同执行时序收在一个小 interface 后面。自动 HWM Backfill 与 Backfill Job 成为两个执行 adapter：它们继续拥有各自的选窗、chunk、取消、状态、计数与结果映射，deep module 拥有 fetch → closed-window → Normalization → Revision merge → 持久化及执行期 gap 证据的共同顺序。

持久化同样由两个真实 adapter 承载。自动 adapter 保持直接 Dataset/HWM 写入及现有 health/counter 行为；Job adapter 委托现有窄持久化能力，保留 Core counters、逐 Bar health、expected keys 和 sink HWM cap。deep module 不依赖 AdminContext、Admin Job 状态机或 GapStore，也不接管 Collector 的后台 worker、queue、write gate 和生命周期。

现有 `run_backfill`、`BackfillReport`、`BackfillJobRunner.start_job/status/cancel`、Admin HTTP、控制 payload、Runner status document 和 Gap wire 全部保持兼容。迁移完成后删除旧双份 orchestration 与未使用的 `gap_linker`，不保留生产开关或新旧双实现。

测试继续以两个现有高层入口为主要兼容门槛，在新增的 Backfill execution seam 直接验证共同行为与两种执行策略；新增成功与 source correction 的跨入口 differential，以及真实 Runner terminal document 经 Admin polling 到持久 Gap 的组合测试。所有可观察行为保持不变。

## User Stories

1. 作为网关维护者，我想要一个深的 Backfill execution module，以便共同执行语义只在一个地方修改。
2. 作为网关维护者，我想要共享粒度明确为一个 InstrumentId 和一个实际 fetch range，以便复用共同逻辑而不吞并两个入口的生命周期。
3. 作为网关维护者，我想要 deep module 拥有完整执行时序，而不是再增加一个 shallow helper，以便跨步骤不变量获得 locality。
4. 作为嵌入式调用方，我想要现有自动 Backfill 公共入口继续可用，以便我的调用无需随内部重构修改。
5. 作为 Backfill Job 调用方，我想要 Runner 的 start、status 和 cancel 公共行为保持不变，以便控制面与现有测试继续工作。
6. 作为维护者，我想要迁移后删除旧双份 orchestration，以便不会继续维护两套执行语义。
7. 作为运维人员，我想要不增加生产实现开关，以便不同部署不会运行不同的 Backfill 语义。
8. 作为自动 Backfill 使用方，我想要继续按 HWM 或配置窗口选择补采起点，以便断线恢复行为不变。
9. 作为数据质量维护者，我想要 HWM 早于可补采窗口时继续记录 backfill limit gap，以便不可恢复部分仍然可见。
10. 作为运维人员，我想要数据源不支持 history 时继续记录 history unsupported gap 并返回原有报告，以便自动入口不会被改成 Job 式失败状态。
11. 作为维护者，我想要自动入口继续对每个 Instrument 一次物化其完整补采窗口，以便本次不悄悄改变 fetch 次数或内存边界。
12. 作为数据消费者，我想要两条路径都只接受已收盘的 1m Bar，以便未形成数据不会进入规范 Dataset。
13. 作为数据消费者，我想要高周期继续由 1m Bar 内部聚合，以便不会误调用 Venue 原生高周期历史接口。
14. 作为数据消费者，我想要 source correction 继续提升自身与 parent Bar 的 Revision，以便修正血缘保持一致。
15. 作为自动 Backfill 使用方，我想要任何 Normalization rejection 继续让当前批整批失败，以便自动 HWM Backfill 原子性不变。
16. 作为数据质量维护者，我想要自动 Backfill 继续不写 Quarantine，以便不会伪造审计记录。
17. 作为自动 Backfill 使用方，我想要继续直接持久化 Dataset 并推进 HWM，以便现有恢复顺序保持稳定。
18. 作为 Collector 维护者，我想要自动 Backfill 不新增 Core counter 或逐 Bar health 副作用，以便内部统一不会改变 status 与 freshness。
19. 作为嵌入式调用方，我想要 BackfillReport 的字段与计数定义保持不变，以便自动化消费者无需迁移。
20. 作为 Operator，我想要 Backfill Job 继续使用显式时间范围、分钟对齐和当前闭合 cutoff，以便手动补采语义不变。
21. 作为 Operator，我想要 Job 继续按现有日 chunk 执行，以便进度与 current_range 的含义保持稳定。
22. 作为维护者，我想要 Job 继续使用现有 500 行 Normalization/merge 批边界，以便本次不改变资源使用与失败粒度。
23. 作为 Operator，我想要 scanned_bars 继续表示应检查的 1m identity 数，而不是 raw 返回行数，以便进度解释保持一致。
24. 作为数据质量维护者，我想要 Job 继续按 open_ts 去重并从缺失分钟生成 gap_ranges，以便不会引入通用 Sequence 连续性断言。
25. 作为 Operator，我想要五类 Job counters 的名称和定义保持不变，以便 Operator Console 无需修改展示逻辑。
26. 作为 Operator，我想要有效 Bars 在存在 rejection 时仍可写入并得到部分成功，以便已验收的 Job partial-outcome 不被重构破坏。
27. 作为数据质量维护者，我想要 Backfill Job 同样不写 Quarantine，以便 rejected_bars 只表达 Validation rejection 和 unreadable identity。
28. 作为 Collector 维护者，我想要 Job 持久化继续经过 sink HWM cap，以便有界写队列中的丢项不会被 HWM 越过。
29. 作为 Collector 维护者，我想要 Job 持久化继续更新 Core counters、逐 Bar health 和 expected keys，以便运行状态不回归。
30. 作为数据消费者，我想要 HWM 继续单调且只在 Dataset 持久化成功后推进，以便失败不会跳过未写入历史。
31. 作为 Operator，我想要 Job 取消继续在现有 chunk 和 batch 边界协作生效，以便不会得到新的中断语义。
32. 作为 Operator，我想要取消和失败不回撤已写 Revision，以便部分结果与终态仍可被准确解释。
33. 作为 Admin API 维护者，我想要 Runner 状态、错误、current_range 和终态判定保持不变，以便持久化与恢复逻辑无需改变。
34. 作为 Collector 维护者，我想要 write gate、锁顺序、单 worker 和同 key 冲突约束保持不变，以便深化执行不会引入并发回归。
35. 作为维护者，我想要自动与 Job 各自保留 Aggregator 生命周期，以便不会意外共享本应隔离的聚合状态。
36. 作为数据消费者，我想要自动与 Job 各自保留 clock 与 ingest_ts 取得方式，以便重构不会制造无意的 Revision 差异。
37. 作为维护者，我想要 deep module 返回结构化执行结果，以便两个 adapter 分别映射现有 report、counters 和 gap 证据。
38. 作为架构维护者，我想要 Backfill execution 不依赖 Admin 状态或 GapStore，以便领域执行与管理面保持清晰 seam。
39. 作为数据质量维护者，我想要自动规划期 gap 与 Job 执行期 gap 各守现有语义，以便统一执行不会新增 gap 类型或检测范围。
40. 作为 Operator，我想要 Admin polling/reconcile 继续独占 Gap 解决与关联，以便成功、部分成功和失败的处置不变。
41. 作为维护者，我想要删除没有 caller 的 gap_linker，以便不再误以为存在第二条 Gap 所有权路径。
42. 作为 API 使用方，我想要 resolved_gap_ids wire 字段继续存在，以便删除内部 callback 不改变响应契约。
43. 作为 live ingest 与 archive 维护者，我想要共享 merge helper 的位置与语义保持不变，以便 Backfill 重构不影响其他生产消费者。
44. 作为数据安全维护者，我想要 Collector 继续是唯一 Dataset 写入方，以便 Admin API 不会获得直接写 Parquet/HWM 的能力。
45. 作为 Operator Console 使用方，我想要 Admin HTTP、控制 payload、审计、重试和恢复契约保持不变，以便本次后端深化不需要前端迁移。
46. 作为实时数据消费者，我想要 Backfill Bars 继续不进入实时发布，以便历史补采不会冒充 live ingest。
47. 作为测试作者，我想要现有自动入口与 Runner 公共入口测试尽量原样通过，以便重构具有稳定的行为基线。
48. 作为测试作者，我想要跨入口 differential 比较逻辑 Dataset、Revision、parent Bars 和 HWM，以便共同执行漂移能被直接发现。
49. 作为测试作者，我想要分别验证两种 rejection 策略与持久化/HWM 顺序，以便测试不会强迫有意差异消失。
50. 作为数据质量维护者，我想要真实 Runner terminal document 经 Admin polling 写入持久 Gap 的组合测试，以便执行与 reconciliation 的接缝得到端到端保护。
51. 作为实现代理，我想要聚焦测试和完整非 network 测试均无新增失败，以便交付结论基于实测而非推断。
52. 作为领域文档维护者，我想要 ADR-0014 明确自动 Backfill 与 Backfill Job 的 rejection 差异，以便后续实现不会误读旧措辞。
53. 作为领域模型维护者，我想要 GLOSSARY 继续区分 Backfill 与 Backfill Job 且不新增内部架构词汇，以便领域模型只保留业务概念。
54. 作为维护者，我想要本次不夹带 chunk 截断、内存 registry 或其他测试债改造，以便行为保持的重构边界清楚。
55. 作为实现代理，我想要保留工作区已有未提交修改，以便本 spec 的实施不会覆盖其他正在进行的工作。
56. 作为验收人员，我想要完成时两个入口都实际穿过同一 Backfill execution seam 且旧 orchestration 已删除，以便“统一”不是仅新增一层间接调用。

## Implementation Decisions

### Deep module 与共享粒度

- 新增内部 Backfill execution module，interface 以一个 InstrumentId 和一个实际 fetch range 为执行粒度。该名称是实现词汇，不写入 GLOSSARY。
- module 拥有该范围内的共同顺序：获取 1m history、应用 closed-window 与输入整理、执行批量 Normalization、调用现有 Revision/parent merge、通过持久化 adapter 写入，并收集执行结果与执行期 gap 证据。
- 自动入口与 Job 入口把各自规划出的实际 fetch range 交给 module。自动入口的一个范围仍是单 Instrument 的完整 HWM/配置窗口；Job 的一个范围仍是现有一日 chunk。
- Job 的 500 行批处理继续适用于其 fetch range；自动入口继续一次性处理整个范围。不得为统一代码形状改变 raw materialization、批大小或 merge 读取范围。
- module 通过显式注入使用 Feed Handler、Normalization、merge、Aggregator、clock 与持久化能力；不接受整个 CollectorCore 或 AdminContext。
- 现有 merge helper 继续独立服务 live ingest、archive import 和两条 Backfill 路径。不得把它改为 Backfill 私有实现，也不得修改 live 的 buffered sink existing-reader 语义。
- deep module 返回内部结构化执行结果，覆盖 raw 取数、接受与拒绝、merge skipped、实际写入和 gap 证据。该结果不是新的 Admin wire 或 Streaming Interface。

### 自动 HWM Backfill adapter

- 自动 adapter 继续遍历配置的 InstrumentIds，依据 HWM、`backfill_hours` 与本次 `now` 计算 inclusive 起点，并重抓 HWM 自身以允许最后一根修正。
- 无 HWM、HWM 早于窗口、history unsupported 和 backfill limit 的现有分支保持不变。规划期产生的两类 Unrecoverable Backfill Gap 仍由自动 adapter 通过 HealthTracker 记录。
- 自动 adapter 继续使用 Core 持有的 Aggregator 和同一个 `now` 作为批 ingest 时间，不新建 Job 级 Aggregator 或改用逐批墙钟。
- 自动 Normalization rejection 策略固定为整批 `BatchNormalizationError`，当前 history 批不写、不写 Quarantine；此前其他 Instrument 已完成的写入不回滚。
- 自动 adapter 保留 raw 去重、排序、closed-window 和 merge 查询边界的现有行为，不因 Job 的按日/500 行策略而改变。
- `run_backfill` 保留现有关键字 interface 和 `BackfillReport` 字段。`fetched_bars` 仍是过滤前 raw 数，`written_bars` 仍含 parent Bars，`skipped_bars` 仍来自 merge，merge revisions 不新增到 report。
- 自动入口继续只用于启动补采与显式 Python 调用，不新增 ingest loop 周期 Backfill。

### Backfill Job adapter

- Job adapter 继续拥有显式范围、分钟对齐、闭合 cutoff、日 chunk、500 行批、当前区间、五类 counters、协作取消和七种状态的生命周期。
- Job 的目标 Bar Interval 不改变执行计划：始终获取 1m，并使用配置中的全部 Bar Intervals 做内部 Bar Aggregation。
- Job 继续使用 Job 级 Aggregator；闭合 cutoff 使用 Runner clock，各批 ingest_ts 继续按现有方式取得。不得注入自动入口的共享 Aggregator 或固定 `now`。
- raw 输入继续先按 open_ts 去重；非 1m、chunk 外输入忽略，无法读取的 open_ts 计入 rejected_bars。缺失分钟继续合成 half-open gap_ranges，不做 `seq + 1` 判断。
- Job Normalization rejection 策略固定为：rejection 计入 rejected_bars，有效 base Bars 计入 backfilled_bars 并继续 merge/write；不写 Quarantine。
- `scanned_bars` 仍在 fetch 前按 chunk 内应有分钟数累计；`gap_bars` 仍是缺失分钟数；`written_bars` 仍只在持久化成功后累计且包含 parent Bars。
- 无 gap/rejection 时继续 succeeded，即使相同内容导致 written_bars 为零；存在 gap/rejection 时，按现有 backfilled/written 产出决定 partially_succeeded 或 failed。
- 取消继续只在 chunk 前、fetch 后、batch 前和 merge 后/write 前检查。阻塞 fetch 与 Dataset write 不新增可中断承诺，已写 Revision 不回滚。
- Runner 的单 worker、queue、内存 registry、状态快照、start/status/cancel、同 key 冲突与 close 生命周期保持不变。

### 两个持久化 adapter 与 HWM

- 自动持久化 adapter 保持直接 Dataset 写入，然后按 Bar Interval 推进 HWM；无写入则不推进，不调用 Job 的 Core persisted 回调。
- 自动持久化不得新增 Core `bars_written`、逐 Bar `health.record_bar` 或 expected-key 副作用；启动后的既有 expected-key 注册仍按原流程执行。
- Job 持久化 adapter 委托现有窄持久化能力，不接收整个 Core。它保留 asset class 解析、同步 Dataset 写入、Core counters、HWM、逐 Bar health 和 expected keys 的顺序。
- Job HWM 必须继续经过 sink HWM cap；不得改成直接写入候选最大 open_ts，也不得绕过有界写队列的防跳洞语义。
- 两个 adapter 都必须保证 Dataset 写入成功后才推进 HWM。持久化失败不得由 deep module 再补写 HWM 或增加成功计数。
- HWM Store 的单调性继续防止旧范围补写降低既有 HWM；本次不新增跨 chunk 或跨 Job 的存储事务。
- write gate 保持在 deep module 外，继续先取全局 slot 再按排序 key 加锁。自动入口持有现有配置 key 集，Job 持有现有单 Instrument key。

### 结果、Gap 与 Admin 所有权

- 自动 adapter 把结构化结果映射为现有 BackfillReport，并保持规划期 HealthTracker gap 行为。
- Job adapter 把结构化结果映射为现有 counters、gap_ranges、current_range、error、终态和有界 status document；snapshot 的 512 段上限保持不变。
- Backfill execution module 不读取或写入 Admin JobStore、GapStore、Operator Audit Event，也不发起 Admin polling。
- 自动 history unsupported/backfill limit 快照继续经现有 Collector gap snapshot 进入 Admin；Job 的 history missing 证据继续通过 terminal document 进入 Admin。
- Admin polling/reconcile 继续只在实际 active→terminal 持久化跃迁后执行；只有 succeeded 且自身未报告 hole 的 Job 才能解决完全包含的 open Gap，其他终态只关联不解决。
- 删除 Runner 构造参数、内部类型和终态处理中未使用的 gap_linker callback。不得改为把它重新接到 Admin，形成第二条 reconcile 路径。
- Runner status 与 Admin wire 中 resolved_gap_ids 字段继续保留；持久 Gap 返回的 resolved ids 仍由现有 Admin 流程保存。
- Gap reconcile/audit 失败继续只记录日志，不回滚已完成 Job；Collector 离线或空快照不删除历史。

### 兼容、删除与文档

- 保留自动入口、BackfillReport、Runner 公共方法、Collector 控制 action/payload、Admin HTTP、状态、错误、审计、idempotency、retry 和恢复语义。
- Admin API 继续只负责校验、授权、审计和持久化；Collector 保持唯一 Dataset 写入方，历史 Bars 不进入实时发布。
- 两个入口必须实际迁移到 deep module；旧自动 history orchestration 与 Job chunk 内重复执行 orchestration 完成后删除，不留下永久委托链或并行实现。
- 不增加配置项、环境变量或运行时开关来选择新旧 Backfill execution。
- 遵守已澄清的 ADR-0014：自动 HWM Backfill 整批失败，Backfill Job partial-outcome；两者都不写 Quarantine。
- 将 Job counters 文档中的 rejected_bars 含义统一为 Validation rejection 与 unreadable identity，移除“Quarantine”暗示。
- GLOSSARY 已区分 Backfill 与 Backfill Job，本 spec 不新增领域术语或 ADR。
- 保留工作区已有修改；实施不得覆盖、回滚或提交用户未授权的其他工作。

### 完成条件

- 两条入口共享同一 Backfill execution module，旧双份 orchestration 与 gap_linker 已删除。
- 公共 Python、控制、HTTP、Dataset、HWM、health、Revision、状态、计数和 Gap 行为与基线一致。
- 自动与 Job 的持久化、Aggregator、clock 和 rejection 差异仍由各自 adapter 明确拥有。
- 本 spec 所列新增测试与现有相关测试通过，完整非 network 测试相对实测基线无新增失败。

## Testing Decisions

### 测试 seam 与良好测试标准

- 最高优先测试 seam 仍是现有自动 Backfill 公共入口和 BackfillJobRunner.start_job/status/cancel；这些测试观察真实临时 Dataset、HWM、health、状态和错误，不直接调用私有 orchestration。
- 新增的共同行为测试只落在 Backfill execution interface；两个入口的测试不再复制共同步骤矩阵，只保留各自规划、映射、持久化和控制语义。
- Admin 测试继续使用真实 FastAPI、环回控制通道、Runner、Admin stores 和临时 Dataset 的组合；需要验证 Gap 时不以预制 terminal document 替代新增的真实 Runner 组合用例。
- 测试只断言公共可观察行为与结构化 adapter 结果，不断言私有调用次数、内部类形状或新 module 的存在本身。
- 采用 replace, don’t layer。若新 seam 已覆盖共同行为，删除只针对旧私有实现的重复测试；不得为了覆盖率为新旧实现保留两套同义测试。

### 必须保留的现有覆盖

- 自动入口继续覆盖 HWM 跨重启、幂等重抓、窗口外 gap、history unsupported、source correction、parent Revision、高周期 HWM、Normalization rejection、forming Bar 和 freshness。
- Job Runner 继续覆盖 1m 与 parent 写入、高周期计划、幂等、HWM 不降、source correction、partial-outcome、gap_ranges、history unsupported、取消、同 key 冲突和 unknown Job。
- Admin 继续覆盖真实 dispatch、进度、取消、retry、冲突、queued 恢复、interrupted、RBAC、CSRF、idempotency 和 audit fail-closed。
- Gap 继续覆盖快照幂等、持久历史、成功解决、非成功只关联、自身 hole 不解决、reopen、无物理删除和离线不删除。
- 现有 live ingest 与 archive 测试必须继续通过，因为共享 merge helper 不是 Backfill 私有依赖。

### 新增 characterization 与 differential

- 在旧实现上先建立比较口径，再迁移实现。differential 使用同一 Instrument、相同 history、固定 clock 和等价初始 Dataset，分别穿过自动与 Job 公共入口。
- 成功输入至少覆盖新建 base/parent Bars、相同内容重跑和 source correction；比较逻辑 Bars、Revision、parent Bars 与最终 HWM。
- 不比较 Data File 名称、UUID、物理文件数量或未固定的 ingest_ts；这些不是共同语义。
- 不强迫 BackfillReport、Job counters、health、gap 类型或终态一致；这些属于有意的 adapter 差异。
- 自动 fail-fast 与 Job partial-outcome 分别参数化验证，不把 rejection 输入做成“结果必须相同”的 differential。
- 增加 BackfillReport 各字段的映射断言，尤其 raw fetched、parent-inclusive written、merge skipped 和 gap_events；Job 五类 counters 继续按既有定义断言。

### 持久化、取消与 Gap 接缝

- 用失败注入证明 Dataset 写入失败时 HWM 不推进、written 成功计数不增加；已成功写入的早期 chunk 不回滚。
- 自动 adapter 验证不会产生 Job 特有的 Core counter/health 副作用；Job adapter 验证 Core counter、health 和 expected keys 保持，并覆盖 HWM cap 防止越过队列丢项。
- 取消测试继续覆盖 chunk 间与 write 前边界、已写 Revision 保留、current_range 清理和 cancelling→cancelled；不得用可中断 mock 宣称阻塞 Venue fetch 已可取消。
- 新增真实 Runner 产生缺失输入与 terminal document，经 Admin polling 写入持久 Gap 的组合测试；至少验证 observed history missing 与终态对应的解决/不解决语义。
- 该组合测试不得注入 gap_linker，也不得绕过现有 active→terminal 持久化跃迁。
- 自动真实 HealthTracker 事件到 Collector gap snapshot 的现有覆盖保留；Admin snapshot 持久化继续由现有高层测试守护。

### 范围与验收

- 本次不专门补 500 行批边界、512 段截断、Runner 内存 registry 上限等既有测试债；若实现改变了对应 adapter 规则，则该实现已越界，必须先回到 spec。
- 所有测试离线运行，不依赖真实 Venue；Feed Handler 使用受控 fake/mock，Dataset、HWM、SQLite 和控制面使用真实本地实现。
- 先运行自动 Backfill、Job Runner、Admin Backfill、Gap、live ingest 与 archive 聚焦测试，再运行完整非 network 测试。
- 实施前记录当前测试基线；验收要求无新增失败，不把历史 issue comment 中的通过数当作当前事实。

## Out of Scope

- 修改自动 Backfill 或 Backfill Job 的任何公共运行时语义、wire、状态、计数或错误契约。
- 新增 ingest loop 周期 Backfill、自动 Backfill 取消协议、跨 Job 原子事务或实时发布历史 Bars。
- 调用 Venue 原生高周期 history、引入 TradeTick/QuoteTick/Depth 或改变 Bar Data Contract。
- 将 Backfill Job 改为自动入口的整批失败，或让任一 Backfill 路径开始写 Quarantine。
- 通用 Sequence 连续性检测、`seq + 1` 假设、新的 Gap reason 或 Gap 物理删除。
- 重新设计 Admin GapStore、polling/reconcile、resolved ids 持久化或 Operator Audit Event。
- 将 gap_linker 接入生产、新增第二条 Gap 解决路径，或保留无 caller 的兼容 callback。
- 改变日 chunk、500 行批、512 段 snapshot 上限、Runner registry 淘汰策略或自动入口的全窗口 materialization。
- 改变 write gate、锁顺序、worker 数量、同 key 冲突策略、启动/停止顺序或 Runner 内存恢复语义。
- 修改共享 merge helper 的 live ingest、archive import、buffered sink 或 Revision 语义。
- Feed Subscription、Revision-safe Cursor、Admin command、Service Instance lifecycle、StoreRead 等其他架构候选。
- 为本次内部 Backfill execution 名称新增 GLOSSARY 术语，或新增另一份 ADR。
- 清理与本次 seam 无关的测试债、文档债、benchmark 或客户端代码。
- 提交、推送或合并代码；这些动作需要单独授权。

## Further Notes

- ADR-0014 已在设计过程中澄清自动 HWM Backfill 与 Backfill Job 的 rejection 差异；实施分支应保留该澄清，不得依据旧措辞反向改变已验收的 Job partial-outcome。
- 现有代码与测试核实 gap_linker 没有生产或测试 caller；删除它不影响现有 Gap spec，真正的 Gap 行为由 Admin polling/reconcile 提供。
- 自动入口与 Job 的 health/counter 差异是有意行为，不是待修复遗漏；跨入口 differential 必须避免把这些差异误判为回归。
- `persist_backfill_bars` 在本设计中只是 Job 持久化 adapter 的现有实现能力，不因此提升为新的公共 interface。
- 共享 merge helper 还有 live ingest 与 archive 消费者，是本次最重要的旁路回归约束之一。
- 本次没有产生接口原型或新的领域概念；设计决策来自已完成的逐轮确认与现有代码/spec/ADR 事实扫描。
