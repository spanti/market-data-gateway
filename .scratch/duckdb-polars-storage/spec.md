# Spec: 以 Manifest-aware DuckDB 与 Polars 收敛 Bars 读取和 Normalization

Status: ready-for-agent

## Problem Statement

Bars 的持久化读取目前分散在 PyArrow 批扫描、手工 Arrow compute mask、REST 分页物化和 Replay 的 Python 行过滤、Revision 选择与排序中。相同的数据选择语义由多个调用方分别维护，替换或修正其中一处很容易让其他读取表面产生不同结果。Replay 还会把全部匹配 Bars 和对应的 close-time 数组保存在内存中，大范围回放的成本随结果行数增长。

写入侧已有明确的 Normalization 和 Validation 规则，但它们主要以标量 Python 代码表达。无效输入的处理分散在日志、整批拒绝、单行跳过和 Backfill 缺口修复中，缺少统一、可查询的 Quarantine 审计记录。引入 Polars 不能只把同样的规则重写一遍，必须保持批次原子性、精确 Decimal、UTC 纳秒时间、Aggregation 状态和 HWM 语义。

这次改造还不能绕过现有存储契约：Manifest 是唯一活动文件清单，Data Files 不可变，Compaction/GC 会切换 Manifest 并删除旧文件。直接让 DuckDB glob Dataset 目录可能读到失效文件；依赖外部 Parquet 文件的隐式事务或打开时机也不能保证流式读取安全。

因此，用户需要的不是一次工具替换，而是一套可证明等价的读取、Normalization、快照保护、失败报告与分阶段迁移方案。

## Solution

保留 Parquet + Arrow Data Contract，以 DuckDB 2.0.x LTS 替换 Bars 生产读取路径中的手工扫描和过滤，以 Polars 实现现有批处理边界上的 Normalization/Validation 表达式，并新增独立 Quarantine Dataset。

`ParquetBarStore` 继续作为持久化门面。它在每次 HTTP 请求或 Replay Session 开始时解析 Manifest Snapshot，通过 Dataset Read Lease 保护选中的 Data Files，再把显式文件列表交给 DuckDB。禁止 DuckDB 对 Dataset 根目录做 glob 扫描，禁止引入 DuckDB 原生权威表、第二份数据 Catalog 或生产旧引擎后备开关。

REST 保留 raw Revisions、`open_ts` 的半开区间、Time Cursor、JSON 行数上限、各格式响应和稳定错误语义。Replay 保留 `close_ts` 的半开区间、自己的 Revision 规则和五键全序。两者复用 Manifest 与扫描基础设施，但不强行共享时间字段、去重或排序策略。

Replay 分两步迁移：先以 DuckDB 替换查询并保留全量物化来证明等价，再改为固定快照上的有界 Arrow 流，重新验证 seek、Step Credit、Replay Clock、取消和 End of Session。读取失败不再伪装为空回放，而是以带稳定错误的 `reason=error` 终止。

整个工作按 M0–M6 分阶段交付。真实类型与生命周期 Spike 是生产改造的 go/no-go 门槛；任何精度损失、隐式浮点转换、无法取消的旧查询或无法释放的读取资源都必须先解决。

## User Stories

1. 作为维护者，我想要 Bars 的持久化读取通过一个 Store 门面完成，这样 API、Replay 和 Backfill 不会各自实现一套 Parquet 扫描。
2. 作为维护者，我想要 History 与 Replay 使用明确的查询操作，这样我不会用一个含糊的时间字段或布尔开关组合出错误语义。
3. 作为数据工程师，我想要 DuckDB 只读取 Manifest 中的活动 Data Files，这样 Compaction 后的失效文件不会重新出现在结果里。
4. 作为数据工程师，我想要每次读取使用 Manifest Snapshot，这样同一请求不会混合不同 Manifest 代际。
5. 作为运维人员，我想要 Dataset Read Lease 保护整个 Arrow Reader 生命周期，这样 Compaction/GC 不会在流式读取中途删除文件。
6. 作为运维人员，我想要 Compaction/GC 遇到有效 Read Lease 时跳过并报告，这样维护结果可解释而不是悄悄失败。
7. 作为运维人员，我想要长期读取使用 heartbeat 并能识别过期 Lease，这样进程崩溃不会永久阻塞维护。
8. 作为 API 消费方，我想要一次 HTTP 请求中的 count、分页探测和实际读取共享快照，这样检查与结果不会来自两个数据版本。
9. 作为 API 消费方，我想要不同分页请求各自使用新快照，这样短请求不会长期持有 Dataset 锁。
10. 作为历史数据消费方，我想要 REST 继续按 `open_ts` 的 `[start,end)` 过滤，这样现有区间请求不需要改写。
11. 作为历史数据消费方，我想要 REST 继续返回全部已存储 Revisions，这样迁移不会偷偷改变行数或修正历史。
12. 作为分页消费方，我想要单 Instrument JSON 继续按 `open_ts` 升序并使用 Time Cursor，这样现有翻页客户端保持兼容。
13. 作为分页消费方，我想要 `limit=0` 仍探测首条匹配 Bar，这样存在性查询不会退化成静态空响应。
14. 作为 JSON 消费方，我想要 100,000 行限制继续基于 raw Revision 行数，这样错误边界不会因引擎去重而改变。
15. 作为 Arrow 消费方，我想要字段顺序、nullability、Decimal、纳秒时间和 unsigned integer 类型保持精确一致，这样无需修改 Schema 校验。
16. 作为 CSV 消费方，我想要 Decimal 文本和空值表示保持不变，这样现有导入工具不会得到近似数字。
17. 作为 export 消费方，我想要导出文件只包含同一 Manifest Snapshot 的活动数据，这样归档结果可解释且可复验。
18. 作为非分页消费方，我想要文档继续明确不承诺全序，这样我不会把某次 PyArrow 或 DuckDB 的扫描顺序误当业务排序。
19. 作为 API 消费方，我想要参数校验和稳定错误 envelope 不变，这样 DuckDB/Polars 异常不会成为新的公共错误格式。
20. 作为 SDK 用户，我想要流式响应中途失败被报告为不完整传输，这样我不会把截断的 Arrow/CSV 当成完整结果。
21. 作为 Replay 使用者，我想要 Replay 继续按 `close_ts` 的 `[start_ts,end_ts)` 过滤，这样回放窗口不会与 REST 历史窗口混淆。
22. 作为 Replay 使用者，我想要最大 Revision 获胜且同 Revision 取最新 `ingest_ts`，这样修正 Bars 的回放结果保持确定。
23. 作为 Replay 使用者，我想要五键全序保持不变，这样多 Instrument、多 Interval 和可空 `seq` 的顺序不会回归。
24. 作为 Replay 使用者，我想要重复订阅和重复 Data Files 不产生重复事件，这样会话输出仍然可重复。
25. 作为 Replay 使用者，我想要整个 Session 固定 Manifest Snapshot，这样运行期间新发布的数据不会混入本次回放。
26. 作为 Replay 使用者，我想要 seek 在同一 Snapshot 上重新查询，这样定位后不会看到另一代数据。
27. 作为 Replay 使用者，我想要 seek 前的 Step Credits 被丢弃而 seek 后的 Credits 保留，这样控制竞态语义不回退。
28. 作为 Replay 使用者，我想要 pause、resume、set_speed 和 stop 在读取期间仍然有效，这样流式化不会让控制命令失效。
29. 作为 Replay 使用者，我想要 I/O 时间不消耗 Replay Clock，这样慢磁盘不会无故跳过虚拟时间。
30. 作为 Replay 使用者，我想要 asap 仍然不节流，这样新流式读取不会引入额外等待。
31. 作为 Replay 使用者，我想要内存由有界批次和引擎配置控制，这样回放更大区间不会按总结果行数线性增加进程内存。
32. 作为 Replay 使用者，我想要 stop 或 seek 使旧查询停止并释放资源，这样取消的会话不会继续占用后台线程和内存。
33. 作为 Replay 使用者，我想要读取失败收到 `reason=error`，这样我能区分空 Dataset 与损坏或不可读数据。
34. 作为 Replay 使用者，我想要失败终止包含稳定错误码和实际 `bars_sent`，这样客户端能正确处理部分回放。
35. 作为 SDK 用户，我想要 SDK 把失败终止视为错误而非正常完成，这样上层策略不会误以为回放成功。
36. 作为数据工程师，我想要 Polars 在现有 Collector 批量边界执行 Normalization，这样不必为每条实时消息创建单行 DataFrame。
37. 作为数据工程师，我想要批量 Normalization 仍在 Aggregation 状态推进之前完成，这样无效批次不会污染父 Bars 或 retention window。
38. 作为数据工程师，我想要规范化继续拒绝未闭合 Bars、float-shaped 数值和过高小数精度，这样数据质量规则不会因换库而放宽。
39. 作为数据工程师，我想要 Feed Handler 继续负责 Venue 协议、原始报文和闭合状态，这样 Polars 不会被误用成网络协议状态机。
40. 作为数据工程师，我想要归档 checksum、ZIP/CSV 结构和来源时间单位规则继续保持，这样批量引擎替换不会跳过来源验证。
41. 作为运维人员，我想要无效输入进入独立 Quarantine Dataset，这样被拒绝的数据有持久审计记录而不是只剩日志。
42. 作为运维人员，我想要 Quarantine Reason 和 Source Stage 稳定且低基数，这样能按原因告警而不会被异常文本撑爆指标标签。
43. 作为运维人员，我想要 Quarantine 保存来源定位、Rule Version 和结构化详情，这样能定位并重放修复后的输入。
44. 作为运维人员，我想要 Quarantine 写入失败时对应输入不被消费或发布，这样审计系统故障不会变成静默数据丢失。
45. 作为运维人员，我想要实时 Normalization 或 Instrument 验证失败继续整批拒绝，这样 Aggregation 可以保持不变并安全重试。
46. 作为运维人员，我想要 CCXT 单行解析失败在成功隔离后继续处理来源，这样单行毒数据不必改变现有可用性语义。
47. 作为运维人员，我想要归档文件失败时整文件不写入，这样不会出现未经批准的部分归档。
48. 作为运维人员，我想要队列溢出继续通过 HWM 缺口和 Backfill 修复，而不是伪装成 Validation Quarantine。
49. 作为运维人员，我想要 Quarantine 默认保留 30 天、可配置且支持永久保留，这样审计窗口与部署需求可以匹配。
50. 作为运维人员，我想要 Quarantine GC 默认 dry-run、按完整 UTC 月份删除并跳过当前月，这样清理行为与现有维护原则一致。
51. 作为数据工程师，我想要 Data File Manifest 条目记录 Normalization 版本集合，这样能审计不同批次使用的规则。
52. 作为数据工程师，我想要 Compaction 合并来源 Rule Version 集合而不重做 Normalization，这样物理维护不会改变数据语义。
53. 作为数据工程师，我想要旧文件能表示未知或 legacy Rule Version，这样迁移不会伪造历史血缘。
54. 作为维护者，我想要 Data Contract Version 继续决定读取兼容性，这样 Normalization Rule Version 不会成为第二个公共 Schema 权威。
55. 作为维护者，我想要 DuckDB 2.0.x LTS 作为版本基线，这样 Parquet UTC 纳秒时间不会在 1.x 读取时丢失亚微秒精度。
56. 作为维护者，我想要读取结果直接以 Arrow RecordBatch 返回，这样不会为了技术栈名称多做一次 Polars 往返。
57. 作为维护者，我想要内存、线程、临时目录和批次大小可配置，这样查询资源不会完全依赖进程默认值。
58. 作为维护者，我想要生产环境没有 `use_duckdb` 后备开关，这样不同部署不会因配置不同而混合两套读取语义。
59. 作为维护者，我想要旧实现只在差分测试和迁移阶段存在，这样验收后可以真正删除手工过滤代码。
60. 作为测试作者，我想要先建立旧新实现的差分测试和性能基线，这样每个迁移阶段都有明确的等价与回退判据。
61. 作为测试作者，我想要真实验证纳秒、Decimal、uint32、null、取消和 Reader 生命周期，这样不会把库宣传当作兼容证明。
62. 作为平台维护者，我想要 Windows 和 Linux 都验证读取与维护竞态，这样不会依赖某一个操作系统的删除语义。
63. 作为性能评审者，我想要分开测量 Replay 查询和端到端发布，这样 IPC 瓶颈不会被误算为 DuckDB 扫描问题。
64. 作为文档读者，我想要 Data Contract、DESIGN、README、GLOSSARY 和 ADR 同步更新，这样实现与文档不会出现第二套说法。
65. 作为维护者，我想要 Revision-heavy Time Cursor 问题独立处理，这样引擎迁移不会通过隐式去重或新游标暗改公共契约。

## Implementation Decisions

### 1. 范围与不变量

- 迁移范围是 Bars 的 Store 扫描/count、REST Bars 全格式与 export、Replay Source 和最终的有界 Replay Session，以及现有 Collector 批量边界上的 Normalization/Validation。
- Backfill 经由 Store 的读取自动使用新实现，但其 identity、Revision merge、HWM 和 Aggregation 语义不重写。
- Instrument Reference Data 继续以 YAML 为事实来源；Instrument Snapshot 的加载和 Instruments API 的内存过滤不属于本次 DuckDB 迁移。
- Compaction/GC 的物理合并和删除算法不重写，但必须支持 Dataset Read Lease，并传播 Normalization Rule Version 集合。Quarantine 的 Retention/GC 属于本次新增能力。
- Bar Aggregation、Feed Handler 协议状态机、队列恢复、Arrow IPC/ZMQ 和通用客户端传输不因引入 Polars 而重写。SDK 只增加必要的 Replay 失败终止处理。
- Parquet Data Files 继续不可变；Parquet + Arrow Data Contract 继续是公共权威。不得引入 DuckDB 原生权威表、物化缓存或第二份 Catalog。

### 2. 依赖与精确类型

- DuckDB 首个支持基线为 2.0.x LTS，禁止 1.x。2.0 的 TIMESTAMPTZ_NS 是保留 Parquet UTC 纳秒时间的必要条件，但不是 Arrow 导出兼容的充分证明。
- Polars 版本由真实类型 Spike 选择并记录；不得以未验证的最新版本直接进入生产改造。
- 流式结果使用 DuckDB Relation API 的 Arrow RecordBatch Reader。不得使用会先复制完整结果的新版 DBAPI Arrow reader。
- 所有价格、数量等字段保持 `decimal128(38,18)`，不得经过 float64。超过 scale 的输入继续拒绝，不得静默舍入。
- 时间戳保持 UTC 纳秒，包括非微秒整倍值和 Unix epoch 前后的值。
- uint32、nullable `seq`、nullable `trade_count`、字段顺序和 nullability 必须满足现有精确 Arrow Schema。
- 物理 Parquet 中省略的 Hive `interval` 必须由 Store 恢复；分区 symbol 编码和 InstrumentId 解码规则保持不变。
- 正常读取路径为 DuckDB 到 Arrow RecordBatch。只有确有读取变换需求时才进入 Polars，并必须重新证明类型无损。

### 3. Manifest Snapshot 与 Dataset Read Lease

- Store 先读取 Manifest，选择活动且符合 Instrument、Interval、分区与时间范围的条目，再形成显式 Data File 列表。禁止根目录 glob。
- Read Lease 获取流程为：选择候选条目、获取相关分区或月份的 Lease、重新校验 Manifest 代际，变化时释放并重试，然后执行查询。
- 同一 HTTP 请求的 count、存在性探测和实际读取共享一个 Snapshot 和 Lease。不同分页请求不共享长期 Lease。
- Replay Session 的 Snapshot 和 Lease 覆盖整个 Session，包括暂停和 seek。seek 在同一 Snapshot 上建立新查询，不重新纳入新文件。
- Lease 必须持有到 Arrow Reader 完成、取消或失败并关闭之后，不能只覆盖查询启动。
- Compaction/GC 遇到有效 Lease 时跳过并报告。长期读取使用 heartbeat，过期 Lease 可按现有维护原则识别和清理。
- 读取错误不能依赖 DuckDB 未承诺的外部 Parquet 事务语义。外部删除或损坏仍按明确失败处理。
- DuckDB 连接和 Reader 的所有权限定在请求或 Session 生命周期内；Reader 未关闭前不得在同一连接上启动冲突查询。

### 4. Store 与 REST 查询

- `ParquetBarStore` 保留为领域门面，文件路径、SQL、DuckDB Relation 和连接对象不得泄漏到路由或 Replay 控制层。
- History Query 固定使用 `open_ts` 的半开区间并返回 raw Revisions；Replay Query 固定使用 `close_ts`、自己的 Revision 规则和五键排序。不得提供容易误用的通用时间字段和去重布尔组合。
- 查询输入是结构化领域参数。Manifest 文件列表通过安全的关系接口传递，过滤值参数化；调用方不能注入 SQL、任意路径或任意表达式。
- Manifest 条目的分区与时间范围继续用于打开文件前的裁剪，不能把 SQL 谓词下推当成唯一裁剪手段。
- 非分页 JSON、Arrow、CSV 和 export 不新增全局 ORDER BY，也不承诺复刻 PyArrow 的偶然物理顺序。记录及重复次数必须与旧实现一致。
- 单 Instrument 分页 JSON 使用全局 `open_ts` 升序和 `limit+1`，保持 Time Cursor、`has_more`、`next_start` 和 `limit=0` 行为。
- JSON 行数上限继续基于 raw Revision 行数。首版在同一 Snapshot 内分别执行精确 count 和读取，不为了少一次扫描引入复杂窗口 SQL。
- REST 的响应形状、Content-Type、Decimal 字符串、九位纳秒时间、请求 ID 和稳定错误码保持不变。
- 响应开始前失败使用现有 HTTP 错误 envelope。Arrow/CSV 开始输出后失败必须异常终止传输，不能追加 JSON 错误或正常结束截断流。export 继续先完整生成临时文件再发送。
- Revision-heavy Time Cursor 是独立契约问题；本次不得通过隐式 QUALIFY、DISTINCT、复合游标或改变 REST raw Revisions 来暗修。

### 5. Polars Normalization 与 Quarantine

- Polars 在 Collector 已有的 Raw Bars 批量边界执行 Normalization，替换标量规则的重复实现；不得为每条实时消息构造单行 DataFrame。
- Normalization 与 Instrument Validation 必须在 Aggregation 状态推进之前完成。失败批次保持 Aggregation 不动，并维持现有重试语义。
- Feed Handler 继续处理 Venue 报文、闭合识别、重连和来源分页；归档模块继续处理 checksum、ZIP/CSV 结构和来源时间单位。它们不是需要强搬到 Polars 的表格清洗。
- 实时 Normalization、Instrument Validation 和归档文件继续保持整批或整文件原子性。CCXT 单行解析失败只有在成功写入 Quarantine 后才能跳过并继续。
- Quarantine 是独立、版本化的 Dataset，不属于正常 Bars Dataset，不得被 REST Bars 或 Replay 注册、扫描或导出。
- Quarantine Record 包含独立 Schema Version、Source Stage、稳定 Reason Code、Normalization Rule Version、来源定位、结构化详情和可获得的原始记录表示。
- Reason Code 和 Source Stage 是稳定运维契约。异常文本只作清理后的诊断详情，不能作为程序分支或高基数指标标签。
- Quarantine 写入失败采用 fail-closed：对应输入不得被确认消费、跳过或发布；必须告警并暴露写入失败指标。
- Quarantine Retention 默认 30 天，支持配置和 0 永久保留。GC 默认 dry-run，只清理过期的完整 UTC 月份并跳过当前月，不新增自动后台维护任务。
- 新 Data File 的 Manifest 条目记录 Normalization Rule Version 集合。Compaction 取来源集合并集且不重新 Normalization；旧文件可表示 legacy/unknown。
- Data Contract Version 继续决定 Schema 兼容性。Rule Version 用于审计，不自动阻止读取旧数据；语义变化必须升级 Data Contract。

### 6. Replay 查询、流式控制与失败终止

- M5A 先以 DuckDB 完成 close-time 过滤、最大 Revision、同 Revision 最新 `ingest_ts`、重复订阅消解和五键排序，暂时保留全量物化以隔离验证查询等价。
- M5B 删除完整 Bars 列表和完整 close-time 数组，改为固定 Snapshot 上的有界 Arrow 批次消费。
- seek 只在既有合法暂停状态生效；它使当前查询代际失效，并从同一 Snapshot 查询第一条 `close_ts >= ts` 的 Bar。
- 每次读取都有代际标识。旧代际即使稍后返回批次，也不得继续发布或修改当前 cursor、Credits 和 Clock。
- seek 前的 Step Credits 被丢弃，seek 后授予的 Credits 保留。读取期间的 pause、resume、set_speed、stop 和 seek 继续满足现有竞态语义。
- 只允许有界预取。Replay Clock 在首个可读批次准备好后启动；暂停读取期间不得无界累积结果。
- stop 和 seek 必须使用 Spike 验证过的取消与关闭机制释放旧查询、Reader、连接和线程资源。
- 只有查询耗尽且预取记录全部发布后才发送 `reason=exhausted`。stop 继续产生 `reason=stopped`。
- 读取失败使用现有 End of Session 事件，新增 `reason=error` 和稳定错误对象；保留实际 `bars_sent`，不暴露 SQL、路径、堆栈或原始引擎异常。
- Schema Version 不匹配使用现有稳定错误码，其他未分类读取失败使用 `internal_error`。SDK 必须把失败终止与正常完成区分开。

### 7. 资源、依赖和运维配置

- DuckDB memory limit、线程数、临时目录和临时目录容量，以及 Replay 批次大小，都必须是显式配置。
- API 与 Replay 可使用不同资源预算。全局排序和聚合产生的临时磁盘占用必须可观测，临时文件不是权威数据副本。
- 资源耗尽必须产生明确错误，不能静默截断结果或依赖操作系统终止进程。
- DuckDB 和 Polars 是正式运行时依赖，启动时验证可用性；失败即终止，不回退旧生产过滤路径。
- 生产环境不增加引擎切换开关。旧实现只用于迁移和差分测试，发布后回退通过回退版本完成。
- 性能验收在同一机器和同一数据副本上至少重复 5 次，报告中位数与波动。典型吞吐或延迟回退超过 10%，或峰值内存增加超过 20%，必须明确批准并记录原因。
- 最终 Replay 的进程内存不得随总结果行数线性增长；全局排序的临时磁盘占用单独报告。性能门槛是迁移评审标准，不是对外 SLA。

### 8. 分阶段交付计划

- **M0：Characterization 与基线。** 建立旧新差分矩阵、真实数据类型 golden cases 和性能基线；记录 Revision-heavy Time Cursor 独立问题；固定现有 Replay 错误行为以便审查有意变化。
- **M1：阻断性 Spike。** 验证目标版本的 Decimal、UTC 纳秒、uint32、null、Arrow 流式、取消、Reader 关闭、连接释放、资源配置和 Windows/Linux 文件生命周期。失败时暂停后续生产迁移并重新评审。
- **M2：Snapshot 与 Read Lease。** 先让现有 PyArrow 读取器使用 Manifest Snapshot 和 Dataset Read Lease，验证维护竞态后再引入 DuckDB。
- **M3：DuckDB Store 与 REST。** 替换 Bars 扫描/count，切换 JSON、Arrow、CSV、export 和分页；验收后删除对应手工 Arrow 过滤路径。
- **M4：Polars 与 Quarantine。** 替换批量标量 Normalization，加入 Quarantine、Reason Code、Rule Version 和 Retention；保持批次原子性，验收后删除重复标量规则。
- **M5A：DuckDB Replay Source。** 替换 Replay 查询但暂留全量物化，以完整事件序列证明等价。
- **M5B：有界 Replay 与失败协议。** 完成流式控制、取消、Step Credit、Clock、seek、EOS 和 `reason=error`，删除完整物化状态。
- **M6：最终清理。** 删除旧生产路径和临时差分设施，完成 ADR、Data Contract、DESIGN、README、GLOSSARY 与实施记录，运行完整验收。
- 每个里程碑独立审查和回退；不得把未验收的后续阶段叠成一个大提交。

### 9. 决策记录

- 分别记录 Manifest-aware DuckDB reads、Polars batch Normalization with Quarantine、Bounded Replay with explicit failure termination 三项 ADR。
- 更新已经过时的“所有读取使用 pyarrow.dataset”实现说明。
- 更新 Data Contract 中 Manifest Rule Version 元数据、Quarantine、Read Lease 和 Replay 失败终止语义。
- 最终流式 Replay 验收后，更新 README 中全量物化的已知限制。
- GLOSSARY 使用 Normalization、Validation、Quarantine、Quarantine Reason、Manifest Snapshot 和 Dataset Read Lease，不建立泛化的 Cleaning Layer 术语。

## Testing Decisions

### 测试原则与接缝

- 测试外部可观察行为，不断言 SQL 字符串、内部调用次数、DuckDB/Polars 表达式形状或私有文件打开顺序。
- 优先复用现有接缝：REST 通过真实 API 请求与响应验证；Replay 通过 Session/Replay Protocol 的控制和发布事件验证；Normalization 通过 Collector 批处理及持久化结果验证；维护行为通过 Compaction/GC operator commands 验证。
- Store 仅作为迁移期间必要的旧新读取差分接缝，不新增生产测试专用 API。
- 契约和竞态测试使用真实 DuckDB、Polars、PyArrow、Parquet Data Files 和 Manifest，不用 mock 替代需要验证的库行为。
- 现有 Data Contract、Manifest read-safety、Store streaming、REST characterization、格式 parity、分页、Replay Source/Session 和 benchmark 测试是直接先例。

### M0 与差分测试

- 在替换前记录旧实现的完整行为和基准，不能先把新实现当成正确基准。
- 差分数据覆盖空 Dataset、单条、单文件、多文件、跨分区、多 Instrument、多 Interval、重复 Data Files、重复订阅和 inactive Manifest 条目。
- 覆盖半开时间边界、start 等于 end、记录恰好位于 start/end、跨文件相同时间戳和非稳定输入顺序。
- 覆盖 raw Revisions、最大 Revision、同 Revision 不同 `ingest_ts`、相同内容重复行和 Revision-heavy 分页。
- 非分页输出比较精确 Arrow Schema、逐行内容及重复次数，但不把旧物理扫描顺序提升为契约；分页和 Replay 按各自排序契约比较。
- Replay 比较完整事件序列、控制事件和最终状态，而不只比较条数。
- 发现的差异必须分类为新实现错误、非契约偶然行为或已批准的独立修正。

### M1 类型与生命周期验证

- Decimal 覆盖高精度边界、正负值、精确文本、null 和超额 scale 拒绝。
- 时间覆盖 1ns、999ns、1000ns、123456789ns、epoch 前后和 null，验证 Parquet 读取、Arrow 导出、Polars 输入及反向路径。
- 验证 uint32 最小/最大值、nullable `seq` 和 `trade_count`，以及完整 BAR_SCHEMA 字段顺序和 nullability。
- 验证空结果、空批次、单条、多文件和大结果的真实 Relation Arrow Reader 流式行为。
- 对长时间查询执行取消、提前关闭 Reader 和重复取消，确认旧查询停止且连接、线程和临时文件可释放。
- 设置低内存和明确临时目录执行排序/聚合，观察资源错误、临时文件和 Reader 行为。
- 在 Windows 与 Linux 上验证读取期间 Compaction/GC、文件删除和 Lease 保护。
- 任一精度或生命周期硬门槛失败，后续生产阶段不得开始。

### Manifest 与维护测试

- 验证只读取活动 Manifest 条目，inactive 文件和 Quarantine 永远不会混入正常结果。
- 验证 Manifest 在候选选择与 Lease 获取之间变化时会释放、重试并获得一致的新旧 Snapshot。
- 验证 count/read、limit=0 和 export 使用同一 Snapshot。
- 验证 Read Lease 覆盖 Reader 整个生命周期，正常完成、异常、取消和客户端断开都会释放。
- 验证 heartbeat、过期 Lease、进程异常退出、不同分页请求无长期 Lease。
- 验证 Compaction/GC 跳过受保护分区并报告，未受保护维护仍保持原有 Manifest-first 行为。
- 验证 Compaction 后 Rule Version 集合是来源并集，且数据内容未重新 Normalization。

### REST 测试

- 保留并运行现有 JSON、Arrow、CSV、export、错误 envelope、request ID、Content-Type、Decimal 和纳秒 golden tests。
- 验证 REST 返回全部 raw Revisions，JSON count 和 range ceiling 不去重。
- 验证分页全局最早 `limit+1`、`next_start`、`has_more`、end 边界、limit=0 和多页遍历。
- Revision-heavy 分页只记录当前行为并链接独立契约问题，不得用不可能满足的旧断言迫使实现暗中改契约。
- 验证非分页 LIMIT 不引入全局排序，Arrow/CSV 仍逐批输出，export 在发送前完成。
- 模拟响应开始前和开始后的读取失败；前者返回稳定 envelope，后者被客户端识别为不完整传输，且没有 JSON 错误拼接或正常截断。

### Normalization 与 Quarantine 测试

- 复用现有 closed-Bar、Decimal、UTC、Instrument、归档和 CCXT 测试，补充批量与旧标量实现的差分验证。
- 验证失败实时批次不会推进 Aggregation、HWM 或 freshness，修复参考数据后可重试。
- 验证每个 Source Stage 和 Reason Code 的记录、指标、来源定位、Rule Version 和原始表示。
- 验证 Quarantine 写入失败时对应输入不被消费、跳过或发布，并有明确告警指标。
- 验证 CCXT 单行失败在隔离后可继续，归档失败保持整文件不写入，队列溢出仍走 HWM/Backfill 而非 Quarantine。
- 验证 Quarantine 独立 Schema Version、30 天默认 Retention、永久配置、dry-run GC、整月删除和当前月保护。
- 验证正常 Bars 查询和 Replay 永远不会读取 Quarantine。

### Replay 测试

- 保留现有 close-time 区间、Revision、tie-break、五键排序、重复订阅、asap、realtime、step、seek、stop 和 EOS 测试。
- 在 M5A 比较完整物化前后的完整事件序列；在 M5B 使用大于单批次和需要外部排序的数据验证跨批次不重不漏。
- 覆盖初始读取和 seek 读取期间的 pause、resume、stop、再次 seek、set_speed 和 Step Credits。
- 明确验证 seek 前 Credits 丢弃、seek 后 Credits 保留、旧查询代际不能发布、Clock 不被 I/O 消耗。
- 验证空结果、seek 越过末尾、stop 与 exhausted 竞态，以及预取耗尽后才发 EOS。
- 验证损坏 Parquet、Schema Version 不匹配、资源耗尽和外部删除产生 `reason=error`，不会变成空回放或 exhausted。
- 验证失败后 `bars_sent` 精确、错误文本 sanitized、SDK 报告失败、Reader 和 Lease 都释放。
- 用不同总结果规模证明最终进程内存有界，并单独记录临时磁盘峰值。

### 性能与最终验收

- 基线覆盖 Store 扫描/count、REST 全格式、分页、多文件、Revision-heavy 数据、Replay 查询准备和端到端发布。
- 同机同数据至少运行 5 次，报告中位数、波动、峰值内存和临时磁盘；不得挑选有利结果。
- 超过已批准的回退门槛必须明确记录并接受，不能仅因测试通过就忽略。
- 最终运行完整离线测试套件和新增契约、竞态、平台与生命周期测试。
- 最终检查生产 Bars API/Replay 不再有手工 Parquet 行过滤、Python Replay Revision/排序过滤或重复标量 Normalization；写入、Compaction 和测试中的合法 Arrow 使用不算违规。

## Out of Scope

- 将 Instrument Reference Data 的 YAML 事实来源迁移到 DuckDB，或改造 Instrument Snapshot 与 Instruments API 的内存过滤。
- 把 Bar Aggregation、HWM、Backfill、Revision merge、Feed Handler 协议状态机或队列恢复改写为 Polars。
- 重写 Compaction/GC 的物理算法；本次只增加 Read Lease 协作、Rule Version 传播和 Quarantine 维护。
- 引入 DuckDB 原生权威表、物化视图、查询缓存、第二份 Catalog、跨时间 Dataset Version 系统或实时尾随 Replay。
- 将 PyArrow 从写入、Arrow IPC/ZMQ、Schema、export 或测试中全面移除，或移除可选示例中的 pandas。
- 重写客户端 SDK、直接 Parquet 示例、Nautilus 示例或第三方回测框架；SDK 仅适配新增的 Replay 失败终止。
- 在本次迁移中修复 Revision-heavy Time Cursor，或引入复合/不透明游标和多 Instrument 分页。
- 引入 v1 已排除的 ticks、quotes、depth、客户端拉动 Replay 或自动后台 Compaction。
- 解决现有跨进程写入者互斥问题；Read Lease 只保护读取与 Compaction/GC，不冒充通用写锁。
- 对所有部署承诺固定吞吐、QPS 或延迟 SLA。

## Further Notes

- DuckDB 官方 2.0 文档确认 TIMESTAMPTZ_NS 能保留 Parquet UTC 纳秒读取精度，并确认 Relation Arrow Reader 的流式入口；Arrow 导出映射、Polars 往返、取消和流式文件生命周期仍由 M1 实证。
- 官方文档没有明确保证显式文件列表的扫描顺序。非分页输出不得依赖该顺序；契约排序必须由对应读取表面显式实现。
- Read Lease 覆盖整个 Reader 生命周期是针对外部文件打开时机未明确这一事实的保守设计，不依赖某个操作系统的句柄删除行为。
- Revision-heavy Time Cursor 是现有契约的结构问题，不是 DuckDB 迁移引入的问题，必须保持独立可追踪。
- 三份 ADR 分别承担存储读取、数据质量和 Replay 公共行为的决策背景，避免只留下一份工具选型说明。
- 本 spec 只授权按里程碑开始实施；M1 未通过不代表可以用类型降级、DuckDB 1.x 或永久旧路径绕过。
