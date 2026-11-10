# 行情网关（Market Data Gateway）

把不可靠、写法各异的外部行情源，收敛成一套稳定、可版本化的 Data Contract，供回测框架、交易框架与自研策略消费。网关不实现任何具体框架的适配器。

> 术语以本文件为准。每个概念只保留一个规范名；括号内是它在 Arrow schema / API / 代码中的标识符。行业既有词优先，禁止自造同义词。

## Language

### 参考数据（Reference Data）

**Instrument**:
一个可交易标的的参考数据记录：交易所、资产类别、最小价位、合约乘数、时区、交易时段、上市与退市时间。
_Avoid_: Symbol、Ticker、Contract

**InstrumentId**:
`<venue>:<symbol>` 形式的复合身份，例如 `binance:BTC/USDT`、`ctp:rb2410`。网关内部所有结构只认它。
_Avoid_: 裸 symbol、交易对、代码

**Venue**:
交易场所或数据供应商，例如 `binance`、`ctp`、`akshare`。一个 Venue 由一个或多个 Feed Handler 接入。
_Avoid_: Exchange（当泛指数据供应商时）、Provider

**Instrument Reference Data**:
权威的 Instrument 元数据集合。v1 以版本控制的 `config/instruments/*.yaml` 为真相源，人工维护、可 review、可 diff。
_Avoid_: Instrument Registry、Security Master（仅股票语境）、主数据

**Instrument Snapshot**:
Instrument Reference Data 的物化只读副本（Parquet），供回放进程与外部消费者读取。
_Avoid_: Snapshot（单独使用时）、Cache

**Delisted**:
Instrument 的软删除状态。被历史数据引用过的 Instrument 永不物理删除，只标记退市时间。
_Avoid_: Deleted、Removed

### 市场数据（Market Data）

**Data Type**:
一类同构的行情数据。v1 只有 `bar`。Data Type 决定 schema、topic 与 Dataset。
_Avoid_: Channel（易与市场数据通道混淆）、Stream、Feed Type

**Bar**:
按固定周期聚合出的 OHLCV。1m Bar 由交易所原生 K 线提供（`external`），5m 及以上由 1m Bar 聚合得到（`internal`）；只有已收盘的 Bar 进入规范数据。
_Avoid_: Kline、Candle、OHLC（OHLCV 是字段集合，不是对象名）

**Bar Interval**:
Bar 的周期长度：`1m` / `5m` / `15m` / `1h` / `1d`。
_Avoid_: Resolution、Timeframe、Frequency

**Bar Aggregation**:
由若干根已收盘的 1m Bar 合成一根更高周期 Bar 的过程。它不是独立采集，也不读取交易所的原生高周期 K 线。
_Avoid_: Roll-up、Resample（pandas 的实现名）、Downsample

**Aggregation Source**:
标识 Bar 的聚合来源：`external`（交易所/供应商原生提供）或 `internal`（网关自己聚合）。对应 NautilusTrader 的 `AggregationSource`。
_Avoid_: Origin、Provider

**Revision**:
同一根 Bar 因源修正被重新发布时自增的版本号。Dataset 可保存同一 `(instrument_id, interval, open_ts)` 的多个 Revision；需要唯一 Bar 系列的读取方才保留最大 revision。
_Avoid_: Version、Correction（泛指时）

**Event Time** (`event_ts`):
事件在交易所发生的时刻，UTC 纳秒。排序、分区、回放均以它为准；Bar 的 `event_ts` 取 `close_ts`。
_Avoid_: Timestamp（裸用）、Market Time

**Ingestion Time** (`ingest_ts`):
事件进入网关的时刻。只用于诊断延迟，不参与排序。
_Avoid_: Receive Time、Local Time

**High-Water Mark** (`HWM`):
某个 `(instrument_id, interval)` 已持久化的最新 `open_ts`，用于断线后补采与缺口检测。
_Avoid_: Watermark（在 Flink/Beam 中特指事件时间完整性断言，语义不同）、Offset、Cursor

**Backfill**:
按 HWM 从数据源历史接口补采缺失 K 线的过程。
_Avoid_: Replay（Replay 是模拟行情，Backfill 是补真实缺口）

**Feed Freshness**:
某一 `(feed, instrument_id, interval)` 最近接收事件的年龄及其阈值状态，表达实时采集是否失活。
_Avoid_: Dataset Freshness、Network Latency

**Dataset Freshness**:
某一 Dataset 或 Bar Interval 最近持久化数据的时间状态，表达历史读取可见数据的更新程度；它不等同于 Feed Freshness。
_Avoid_: Feed Freshness、Realtime Freshness

### 数据质量（Data Quality）

**Normalization**:
把 Venue 或归档输入转换为规范 Bar 的过程；它不改变 Bar 的身份，也不负责生成更高周期 Bar。
_Avoid_: Cleaning（作为正式层名时）、Sanitization

**Validation**:
依据 Data Contract 与领域不变量判断输入能否成为规范数据的过程；它不猜测或修补含义不明的数据。
_Avoid_: Cleaning、Type Casting

**Quarantine**:
因明确的 Validation 失败而被拒绝的记录及其原因、来源所构成的审计集合；其中的数据不属于可查询的规范 Dataset。
_Avoid_: Dead Letter Queue、Bad Data Cache

**Quarantine Reason** (`reason_code`):
记录被隔离的稳定、低基数原因代码；它表达可分类的数据质量原因，不使用异常文本代替。
_Avoid_: Error Message、Exception

### 接口（Interfaces）

**Data Contract**:
生产方与消费方之间的正式、版本化约定：Arrow schema（字段、类型、语义）与 Parquet Dataset 布局。
_Avoid_: Contract（与金融合约冲突）、Schema（只覆盖结构，不覆盖语义）

**Streaming Interface**:
实时数据的线上约定：topic 命名、Message Envelope、序列化格式。
_Avoid_: Feed API、Pub/Sub API

**Replay Protocol**:
回放会话的命令与语义：创建、暂停、恢复、定位、调速、步进、停止。
_Avoid_: Playback API

**REST API**:
api Service Role 对外提供的历史查询 HTTP 接口，包括 `/api/v1/instruments`、`/api/v1/bars`、`/api/v1/bars/export`、`/api/v1/status`、`/api/v2/bars`，以及 `/healthz`、`/readyz`、`/metrics` 等运维端点。它只是 Data Contract 的一种读取方式，不定义第二套 schema、错误语义或排序契约。
_Avoid_: 服务层、Service Layer、Web 层、API 层（api 是 Service Role，不是分层架构中的一层）

**Time Cursor**:
v1 历史查询的读取游标，由闭区间 `start`、开区间 `end` 与分页响应给出的 `next_start` 组成。`next_start` 是第一条未返回 Bar 的 `open_ts`，下一次请求应把它作为闭区间 `start` 传回。它只表达时间位置，不携带行数或 offset。它只用于 `/api/v1/bars`；同一 `open_ts` 存在多个 raw Revision 时，页边界落在组内会产生跨页重复与不可达行，这是已记录的 v1 契约缺口（[`docs/revision-heavy-time-cursor.md`](docs/revision-heavy-time-cursor.md)），需要完整遍历 raw Revision 的客户端使用 Revision-safe Cursor。
_Avoid_: Offset、Page Token、Opaque Cursor、Watermark

**Revision-safe Cursor**:
`/api/v2/bars` 使用的不透明、带签名 keyset 游标。载荷编码上一页最后一条返回行的总序键 `(open_ts, revision, ingest_ts, row_fingerprint)` 与请求窗口的 `end`，以 HMAC-SHA256 签名、base64url 呈现；客户端只能原样回传 `next_cursor`。下一页严格从该位置之后开始，因此每条 raw Revision 恰好返回一次，且游标在 Manifest 切换、Compaction 与 GC 后仍然有效。它是位置令牌，不是授权凭证。
_Avoid_: Time Cursor、Offset、Page Number、Snapshot ID

**Message Envelope**:
每条流式消息的统一封装头：`schema_version, data_type, instrument_id, event_ts, ingest_ts, feed, seq`。
_Avoid_: Header、Metadata

**Feed Handler**:
连接一个 Venue、解析原始报文并转换为规范数据的组件。它通过 `fetch_history` 支持补采。
_Avoid_: Source、Adapter（在框架适配语境中另有所指）、Connector、Provider

**Dataset**:
一个 Data Contract 下、按 Data Type 划分的 Parquet 数据集合，例如 `data/bars/`。
_Avoid_: Table、Collection

**Data File**:
Dataset 内一个不可变、只追加的 Parquet 文件，命名 `data-<start_ns>-<end_ns>-<uuid>.parquet`。
_Avoid_: Part File、Chunk、Shard、Segment

**Manifest**:
Dataset 根目录下的 `_manifest.json`，记录当前活跃的 Data File 清单。补采与 compaction 都只通过新增文件 + 切换清单生效。
_Avoid_: Index、Catalog

**Manifest Snapshot**:
一次读取开始时从 Manifest 解析出的活动 Data File 条目集合；同一 HTTP 请求或 Replay Session 不混入之后新增的条目。
_Avoid_: Snapshot（单独使用时）、Directory Scan

**Dataset Read Lease**:
一次读取对所选 Data Files 的临时保护，阻止 Compaction 与 GC 在读取结束前删除它们；它不是权威文件清单，也不改变数据所有权。
_Avoid_: Read Lock、Snapshot Lock

**Compaction**:
把同一分区内的小 Data File 合并成更大的新文件、并用 Manifest 让旧文件失效的过程。它不修改任何已存在的文件。
_Avoid_: Merge、Rewrite、Optimize（泛指时）

**Retention**:
数据保留策略，按 Data Type 配置保留期。
_Avoid_: TTL（可接受，但非规范名）

**Garbage Collection** (`GC`):
按 Retention 删除整月分区的过程。
_Avoid_: Cleanup、Purge

### 回放（Replay）

**Replay Session**:
一次回放请求的独立生命周期，由订阅、时间范围与 Replay Mode 构成。
_Avoid_: Playback、Simulation

**Replay Mode**:
回放的推进方式：`asap`（全速）、`realtime`（按 Replay Clock 与 Speed 节流）、`step`（客户端信用驱动的逐事件步进）。
_Avoid_: Pacing、Playback Mode（可接受，但 MDG 统一用 Replay Mode）

**Speed**:
`realtime` 模式下虚拟时间相对墙钟时间的倍数，默认 1.0。
_Avoid_: Rate、Speed Factor

**Replay Clock**:
回放服务内部用于 `realtime` 与 `step` 的时间推进器。`asap` 模式下它不产生等待。
_Avoid_: Virtual Clock、Simulation Clock

**Step Credit**:
`step` 模式下客户端通过 `step(n)` 授予服务端的推送额度，1 credit = 1 条事件。
_Avoid_: Permit（可接受，但统一用 Step Credit）

**End of Session**:
一次 Replay Session 因数据耗尽、停止或读取失败而结束时，由服务端发送的一次终止通知，包含终止原因与已发送 Bar 数。
_Avoid_: EOF、Finish

### 运行时（Runtime）

**Service Role**:
一个网关进程的职责：`collector`、`api`、`replay`。每个 Service Role 可独立启动与部署。
_Avoid_: Role（单独使用时）、Worker、Process

**Gateway**:
整个行情网关系统，包含全部 Service Role 与 Data Contract。
_Avoid_: Server（只指某一进程时）

### 管理控制台（Operator Console）

**Operator Console**:
供内部人员观察 Gateway、查询规范数据并执行授权管理动作的操作界面。
_Avoid_: Admin Backend、Management Server

**Access Role**:
Operator Console 使用者的权限类别：`SuperAdmin` 拥有全部权限，`Operator` 没有生命周期与身份管理权限，`Auditor` 仅可读取与导出。
_Avoid_: Role（单独使用时）、Service Role

**Service Instance**:
某一 Service Role 的一个实际运行实例，是运行状态与生命周期管理的对象；它不等同于 Service Role 本身。
_Avoid_: Node、Master、Worker（在不存在相应集群语义时）

**Feed Subscription**:
Collector 从 Venue 持续获取指定 Instrument 数据的订阅关系，决定网关的实际采集范围。
_Avoid_: Dynamic Subscription（不限定语义时）、Topic Subscription

**WebSocket Session Subscription**:
单个 WebSocket 会话按 Topic 前缀筛选接收消息的订阅关系；它不改变 Feed Subscription。
_Avoid_: Feed Subscription、Global Subscription

**Operator Audit Event**:
一次带操作者身份的管理动作记录，表达操作者、动作、对象、变更与结果；它不属于 Quarantine 或 Rule Version 数据血缘。
_Avoid_: Audit（单独使用时）、System Log

**Backfill Job**:
一次按 InstrumentId、Bar Interval 与时间区间执行的手动 Backfill 生命周期，具有独立状态与进度；它区别于自动 HWM Backfill 和离线归档导入。
_Avoid_: Backfill Task、Replay、Archive Import

**Export Job**:
一次按 InstrumentId、Bar Interval 与时间区间异步生成原始数据文件的独立生命周期，具有结果文件与过期时间；它不改变 Dataset，也不是 Backfill Job。
_Avoid_: CSV Download（泛指异步任务时）、Backfill Job

**Unrecoverable Backfill Gap**:
自动 Backfill 因数据源不支持历史查询或补采超过上限而无法修复的数据缺口；它不是对任意来源 Sequence 连续性的通用断言。
_Avoid_: Sequence Gap（泛指当前缺口记录时）、Data Hole

## Renamed Terms（本次标准化）

| 旧名（已废弃） | 规范名 | 原因 |
|---|---|---|
| Channel | **Data Type** | 与市场数据通道/segment 混淆；NautilusTrader 用 data type |
| Roll-up | **Bar Aggregation** | 行业标准是 aggregation/resampling；roll-up 是财务合并术语 |
| Watermark | **High-Water Mark (HWM)** | Flink/Beam 的 watermark 是事件时间完整性断言，语义不同 |
| Pacing | **Replay Mode** | 行业用 replay/playback mode；pacing 不是标准词 |
| Virtual Clock | **Replay Clock** | 回放场景的标准叫法 |
| Contract（统称） | **Data Contract / Streaming Interface / Replay Protocol** | 金融领域的 contract 指衍生品合约 |
| Source | **Feed Handler** | 市场数据行业的标准组件名 |
| Part File | **Data File** | Iceberg/数据湖标准术语；`part-` 是 Spark 实现细节 |
| Snapshot（单独） | **Instrument Snapshot** | Iceberg 的 snapshot 指表状态，易混淆 |
| `recv_ts` | `ingest_ts` | 对应 Flink 的 ingestion time |
| `msg_type` | `data_type` | 与 Data Type 统一 |
| Role | **Service Role / Access Role** | 进程职责与控制台使用者权限必须区分 |
| Dynamic Subscription | **Feed Subscription / WebSocket Session Subscription** | 采集范围与单会话 Topic 过滤是不同概念 |
| Audit（泛指管理动作时） | **Operator Audit Event** | 区别于 Quarantine 与 Rule Version 数据血缘 |
| Backfill Task | **Backfill Job** | 统一表示具有独立生命周期的手动补采 |
| Sequence Gap（泛指当前缺口记录时） | **Unrecoverable Backfill Gap** | 当前记录只表示补采不可恢复，不是通用 Sequence 连续性检测 |
| Data Freshness（不限定语义时） | **Feed Freshness / Dataset Freshness** | 实时采集失活与历史数据落盘程度是不同概念 |
| Instrument Registry | **Instrument Reference Data** | 行业标准叫法 |

## Terminology Basis

- NautilusTrader：`InstrumentId`、`Bar`、`AggregationSource`（INTERNAL/EXTERNAL）、`ts_event`/`ts_init` 的时间语义。
- Apache Flink：event time / ingestion time / (event-time) watermark 的区分。
- Apache Iceberg：data file、manifest、snapshot、compaction 的层级。
- 市场数据行业：feed handler、venue、instrument、subscription。
- 数据工程：data contract = schema + semantics + quality + SLA。
