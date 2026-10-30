# Spec: Market Data Gateway v1（Bar-only）

Status: ready-for-agent

## Problem Statement

现有策略与研究代码把"连接交易所、拉取行情、补缺口、清洗时间戳、拼接 K 线、落盘"与策略逻辑混在一起。结果是：

- 每接一个数据源，策略侧就要改一次；数据源不可靠时，错误处理散落在策略里。
- 每个回测/交易框架（NautilusTrader、自研框架等）都要重新写一遍数据接入，且各自对时间、精度、K 线边界的理解不同，回测结果无法互相解释。
- 历史数据没有稳定契约：没有版本化的 schema、没有统一的分区、没有可信的缺口修复记录。
- 同一个标的在不同地方有不同写法（`BTC/USDT`、`BTCUSDT`、`binance:BTC/USDT`），导致存储、订阅与回放各写一套字符串匹配。

使用者需要的是一套稳定、可版本化、与框架无关的行情数据接口，而不是又一个绑定某个框架的数据加载器。

## Solution

提供一个独立的 Market Data Gateway，v1 只做 Bar：

- 三个 Service Role：`collector`（连接 Venue、规范化为 Bar、写 Parquet、维护 High-Water Mark、补采）、`api`（REST/WebSocket/ZMQ 分发）、`replay`（按 Replay Protocol 回放）。
- 对外的四组公开接口：Data Contract（版本化 Arrow schema + Parquet Dataset 布局）、Streaming Interface（ZMQ/WS topic 与 Message Envelope）、Replay Protocol、HTTP & WebSocket API。
- v1 数据粒度：1m Bar 来自交易所原生 K 线（Aggregation Source = `external`）；5m/15m/1h/1d 由已收盘的 1m Bar 聚合（`internal`）。只接受已收盘的 Bar。
- 统一的 InstrumentId（`<venue>:<symbol>`）、UTC 纳秒时间、`decimal128(38,18)` 价格与数量。
- Instrument Reference Data 以版本控制的 YAML 为真相源，导出 Instrument Snapshot 供所有进程读取。
- 网关不实现任何回测/交易框架适配器；框架通过 Parquet、REST、WebSocket、ZMQ 或 Replay Protocol 自行接入。
- 提供薄 Python 客户端 SDK，封装历史查询、订阅与回放会话控制。

v1 明确不做逐笔、盘口、交易执行、鉴权、Redis、分布式与自动 compaction。

## User Stories

1. 作为策略开发者，我想要通过统一的 topic 订阅 Bar，这样我不需要为每个交易所写一套接入代码。
2. 作为策略开发者，我想要所有 Bar 都带 `instrument_id`，这样我不需要用字符串猜标的是谁。
3. 作为策略开发者，我想要时间戳统一为 UTC 纳秒，这样不同数据源的数据可以直接比较与拼接。
4. 作为策略开发者，我想要价格与数量使用 `decimal128(38,18)`，这样跨源比较与累计成交量不会出现浮点偏差。
5. 作为策略开发者，我想要只收到已收盘的 Bar，这样我不会把正在形成的 K 线当成最终数据。
6. 作为策略开发者，我想要 5m/15m/1h/1d 由同一份 1m Bar 聚合，这样各周期的 open/high/low/close/volume 可加、可复算。
7. 作为策略开发者，我想要 Bar 带 `aggregation_source`，这样我能区分交易所原生 Bar 与网关聚合 Bar。
8. 作为策略开发者，我想要 Bar 修正时 `revision` 自增，这样我能识别并去重同一根 Bar 的多个版本。
9. 作为回测工程师，我想要直接读取 Parquet Dataset，这样我不必启动网关就能加载历史数据。
10. 作为回测工程师，我想要 Parquet 目录按 Data Type、Venue、Instrument、Interval、年月分区，这样 `pyarrow.dataset` 可以裁剪扫描。
11. 作为回测工程师，我想要 schema 版本写入 Data Contract，版本不匹配时直接报错，这样我不会在不知情的情况下读到不兼容的数据。
12. 作为回测工程师，我想要历史接口支持 Arrow IPC 流式输出，这样我可以拉取大区间而不把结果全量放进内存。
13. 作为回测工程师，我想要历史接口支持 CSV 流式输出，这样我可以直接导入现有工具链。
14. 作为回测工程师，我想要历史接口在 JSON 模式下对大区间返回 `range_too_large`，这样我不会用 JSON 拉一整年的 Bar。
15. 作为回测工程师，我想要 `/bars/export` 返回单个 Parquet/Arrow 文件，这样我可以一次性归档或离线分析。
16. 作为回测工程师，我想要所有历史路径对同一区间返回一致的语义，这样 REST、SDK 与直接读 Parquet 的结果可以互相验证。
17. 作为客户端开发者，我想要一个薄 Python SDK，提供订阅、历史查询与回放控制，这样我不必自己实现 ZMQ 重连与 Arrow 解码。
18. 作为客户端开发者，我想要 SDK 的 `history()` 返回 `pyarrow.Table` 并提供 `to_pandas()`，这样我可以选择 Arrow 或 pandas 工作流。
19. 作为客户端开发者，我想要 SDK 文档明确 decimal 在 pandas 中是 `Decimal` 对象，这样我不会误用 `astype(float)`。
20. 作为客户端开发者，我想要 SDK 不依赖网关内部代码，这样我可以独立升级客户端。
21. 作为回放使用者，我想要 `asap` 模式全速推送，这样回测框架可以自行驱动策略时钟。
22. 作为回放使用者，我想要 `realtime` 模式按 Speed 节流，这样我可以人工观察行情演进。
23. 作为回放使用者，我想要 `step` 模式逐事件推进，这样我可以精确调试策略在某一根 Bar 上的行为。
24. 作为回放使用者，我想要 `pause`/`resume`/`seek`/`set_speed`/`stop`，这样我可以控制回放会话的生命周期。
25. 作为回放使用者，我想要多标的按 `close_ts` 归并、同一时刻按 `instrument_id` 与 `seq` 定序，这样回放是确定性的。
26. 作为回放使用者，我想要数据耗尽时收到 End of Session，这样客户端不会无限等待。
27. 作为回放使用者，我想要多个 Replay Session 互不影响，这样我可以并行跑不同区间。
28. 作为运维人员，我想要每个 `(instrument_id, interval)` 的 High-Water Mark 持久化，这样进程重启后能从中断处继续。
29. 作为运维人员，我想要断线与重启后自动 Backfill 缺失的 Bar，这样数据不会出现无法解释的空洞。
30. 作为运维人员，我想要缺口超过 Backfill 上限时收到 `gap_unrecoverable`，这样我不会误以为数据是连续的。
31. 作为运维人员，我想要每个 `(feed, instrument, interval)` 的陈旧检测，这样静默断流会告警而不是悄悄停更。
32. 作为运维人员，我想要 `/healthz`、`/readyz`、`/status` 与 `/metrics`，这样我能判断进程是否健康、数据是否新鲜。
33. 作为运维人员，我想要 Retention 按 Data Type 配置并默认关闭，这样我不会意外删除历史数据。
34. 作为运维人员，我想要 GC 默认 dry-run，这样删除前可以先审查。
35. 作为运维人员，我想要 Compaction 通过 Manifest 切换，这样我可以在不重写已有 Data File 的前提下控制小文件数量。
36. 作为运维人员，我想要回放会话心跳锁阻止 GC 删除正在使用的 Dataset，这样回放不会中途失败。
37. 作为数据工程师，我想要 Instrument Reference Data 用 YAML 维护，这样元数据变更可以 review 与 diff。
38. 作为数据工程师，我想要 Instrument Snapshot 由网关导出，这样所有进程与外部消费者读到同一份元数据。
39. 作为数据工程师，我想要退市的 Instrument 只标记 `delisted_ts` 而不删除，这样历史数据的身份不会消失。
40. 作为数据工程师，我想要 symbol 中的 `/` 在路径里百分号编码，这样目录不会因标的命名而断裂。
41. 作为数据工程师，我想要 Data File 一经写入不可变，这样补采与修正是追加而不是破坏。
42. 作为数据工程师，我想要读路径先读 Manifest 再打开 Data File，这样 compaction 与读取可以安全并发。
43. 作为数据工程师，我想要 schema、Arrow IPC、Manifest 与路径编码有 Data Contract 测试，这样契约变更会在 CI 中暴露。
44. 作为框架集成者，我想要网关内核不 import 任何回测框架，这样网关不会被某个框架的版本绑定。
45. 作为框架集成者，我想要网关只暴露 Parquet、REST、WebSocket、ZMQ 与 Replay Protocol，这样任何语言都能接入。
46. 作为开发者，我想要一个实现 FeedHandler Protocol 的假数据源，这样端到端测试不依赖网络。
47. 作为开发者，我想要 `GatewayRuntime` 作为唯一的行为测试缝，这样测试针对外部行为而不是内部实现。
48. 作为开发者，我想要真实 Binance 测试默认跳过、仅在 `network` 标记下运行，这样 CI 稳定。
49. 作为开发者，我想要配置分为运行配置 TOML 与 Instrument Reference Data YAML，这样配置与数据各归其位。
50. 作为开发者，我想要 `mdgw run --service`、`mdgw up`、`mdgw gc`、`mdgw compact`、`mdgw backfill --archive` 等命令，这样每个 Service Role 都能独立运行与运维。
51. 作为开发者，我想要 Windows 上默认使用 `tcp://127.0.0.1` 而不是 `ipc://`，这样本地开发不会因 pyzmq 的 Windows wheel 限制失败。
52. 作为开发者，我想要网关默认只绑定 `127.0.0.1` 且 v1 不做鉴权，这样默认配置不会把行情服务暴露到公网。

## Implementation Decisions

### 架构与运行时

- 三个 Service Role：`collector`、`api`、`replay`，各自可独立启动；`mdgw up` 仅作为开发期 supervisor，按 `collector → ready → api` 顺序拉起。
- 进程间通信只用 ZMQ：行情 PUB/SUB，控制 REQ/REP；同一抽象支持 `inproc://`（测试）、`tcp://`（默认）、`ipc://`（仅 Linux）。v1 不引入 Redis。
- High-Water Mark 与 Replay Session 状态存放在各进程自己的 SQLite。
- HTTP/WebSocket 传输层使用 `aiohttp`，但路由、参数校验、错误码、CSV 流式输出与 WS 订阅管理全部自研；不使用 FastAPI/Flask/Starlette。
- 网关内核禁止 import 任何回测/交易框架；CI 用 AST 扫描或 import-linter 强制。

### Data Contract

- Bar 与 Instrument 的字段、类型、语义按 DESIGN.md §3.1 执行。
- 时间：`timestamp[ns, tz=UTC]`；`event_ts` 取 Bar 的 `close_ts`；`ingest_ts` 为网关接收/生成时刻。
- 价格/数量：`decimal128(38,18)`；JSON 中为字符串；CSV 中为十进制原文；任何路径不得经过 float。
- Bar 只包含已收盘数据；字段包含 `interval`、`open_ts`、`close_ts`、`ingest_ts`、`feed`、`aggregation_source`、OHLC、`volume`、`trade_count`（可空）、`revision`。
- Message Envelope：`schema_version`、`data_type`、`instrument_id`、`event_ts`、`ingest_ts`、`feed`、`seq`。
- Data Contract 版本为 v1；读取方遇到不匹配的 `schema_version` 直接报 `schema_version_mismatch`。

### Parquet Dataset

- Dataset 根为 Data Type（v1 为 `bars`）；分区为 Hive `key=value`：`asset_class`、`venue`、`symbol`（百分号编码）、`interval`、`year`、`month`。
- 每个 Dataset 根有 `_dataset.json`（schema、分区规范、命名规范）与 `_manifest.json`（活跃 Data File 清单）。
- Data File 命名 `data-<start_ns>-<end_ns>-<uuid>.parquet`；一经写入内容不变。
- 写入参数：Parquet `version="2.6"`、`use_deprecated_int96_timestamps=False`、zstd、`row_group_size=1_000_000`、单文件目标 64–256MB。
- 读取一律走 `pyarrow.dataset`，先读 Manifest，再按分区与时间范围下推过滤。
- Compaction 写入新 Data File，更新 Manifest 使旧文件 inactive，再删除旧文件；不修改任何已存在的文件。

### Ingestion、HWM 与 Backfill

- ccxt Feed Handler 使用 `watch_ohlcv` / `watch_ohlcv_for_symbols`（1m）与 `fetch_ohlcv`；能力判断用 `exchange.has[...]`；`enable_rate_limit=True` 并叠加按 Venue 的令牌桶。
- 只接受已收盘 Bar：WS 用 `x` 标志；REST 用 `closeTime < 服务器时间` 过滤。
- 每个 `(instrument_id, interval)` 的 HWM 为最后已持久化的 `open_ts`；启动/重连从 HWM 补采，无 HWM 则从 `now - backfill_hours`（默认 24h）开始。
- 5m/15m/1h/1d 不单独采集，统一由 1m Bar 聚合。
- 深历史走 `data.binance.vision` K 线归档；归档时间戳 2025 年起为微秒、之前为毫秒，规范化层显式判定。
- 幂等键 `(instrument_id, interval, open_ts)`；内容一致跳过，源修正则 `revision += 1` 并新增 Data File。
- 陈旧检测按 `(feed, instrument, interval)` 维护 `last_event_ts`；超过 `stale_after_s` 标记 stale 并告警。
- 采集 → 聚合 → 写盘之间使用有界队列（默认 10k），满时丢最旧并记 `lag`；写盘独立 writer。

### Bar Aggregation

- 1m 为唯一原始粒度；更高周期按 `open=首根`、`high/low=极值`、`close=末根`、`volume/trade_count=求和` 聚合。
- 只有整段 1m 都已到达且已收盘，父周期才发布；不发布部分周期。
- 1m 为 `external`，聚合出的高周期为 `internal`。
- 1m 修正时父周期同步重发并自增 `revision`。
- 只持久化已收盘 Bar；历史接口与回放只读取它们。

### Replay Protocol

- `create_session(subscriptions, start_ts, end_ts, replay_mode, speed?)` 返回 `session_id`；控制命令为 `pause`、`resume`、`seek`、`set_speed`、`step`、`stop`。
- Replay Mode：`asap`、`realtime`、`step`；`step` 使用 Step Credit（1 credit = 1 条）。
- Replay Clock 归服务端；`asap` 不节流；`realtime` 按 Speed 节流。
- 多标的按 `close_ts` 归并；同刻按 `instrument_id`、再按 `seq` 定序。
- 数据耗尽发送 End of Session；`seek` 仅允许在 `pause` 状态；默认最多 4 个并发会话。

### HTTP 与 WebSocket API

- REST：`/api/v1/instruments`、`/api/v1/instruments/{instrument_id}`、`/api/v1/bars`、`/api/v1/bars/export`、`/api/v1/status`、`/healthz`、`/readyz`、`/metrics`。
- 历史分页只用时间游标；`interval` 必填；`format=json|arrow|csv`；JSON 超过 100,000 行返回 `range_too_large`；Arrow/CSV 流式无上限。
- `/api/v1/status` 暴露每个 Dataset 的 `last_flushed_ts`；历史接口最多滞后一个写缓冲周期（默认 1 小时）。
- WebSocket 支持 `subscribe`/`unsubscribe`/`ping`；慢消费者发送 `lag_notice` 而不断开。
- 统一错误体 `{"error":{"code","message","details","request_id"}}`；错误码稳定。
- 默认绑定 `127.0.0.1`，v1 不做鉴权。

### Reference Data 与配置

- Instrument Reference Data 真相源为 `config/instruments/*.yaml`；启动时校验并导出 Instrument Snapshot；被历史数据引用过的 Instrument 只标记 `delisted_ts`。
- 运行配置为 TOML；Feed Handler 配置为 `[[feeds]]` 数组；Data Type 配置为 `data_types=["bar"]`。
- 写入缓冲：64MB 或 1 小时（先到者）。
- Retention 默认关闭；只删除整月分区，跳过当前月。

### 客户端 SDK

- `mdgw_client` 是独立包，只依赖 `pyarrow`、`pyzmq` 与 schema 定义。
- 提供历史查询（返回 Arrow Table / pandas）、实时订阅、Replay Session 控制。
- 明确 decimal 在 pandas 中为 `Decimal` 对象；不强制 `coerce_temporal_nanoseconds`。

### 交付与运维

- 交付 `environment.yml` 与 `pyproject.toml`，以 conda `nautilus` 环境为准，补装 pytest。
- 交付 `docker-compose.yml`；生产可再套 systemd。
- 提供 `mdgw run --service`、`mdgw up`、`mdgw gc`、`mdgw compact`、`mdgw backfill --archive`。

## Testing Decisions

- 好测试只断言外部行为：给定的 Feed Handler 输入、Parquet/HTTP/ZMQ 输出、Replay Session 事件序列；不断言内部类结构或私有方法。
- 主 seam：`GatewayRuntime`。它是一个组合根，接收配置、假 `FeedHandler` 与临时数据目录，在单进程内用 `inproc://` ZMQ 跑起 collector → Parquet → api → replay。所有行为测试从这里进入。
- 第二 seam：Data Contract 字节。用黄金文件断言 Arrow schema、Arrow IPC 往返、`_dataset.json`/`_manifest.json`、topic 与路径编码。
- 网络 seam：真实 Binance 仅以 `@pytest.mark.network` 运行，默认跳过。
- 测试模块：
  - Ingestion / HWM / Backfill：假 Feed Handler 模拟收盘、断线、缺口、修正。
  - Bar Aggregation：1m → 5m/15m/1h/1d 的 OHLCV 与只发布完整周期。
  - Store：Data File 写入、Manifest 切换、Compaction、Retention GC、路径编码。
  - Replay：asap/realtime/step、pause/resume/seek、排序、End of Session、确定性。
  - API：REST 格式与错误码、WS 订阅与 `lag_notice`、`/status` 的 `last_flushed_ts`。
  - Client SDK：history/订阅/回放控制；decimal 与 pandas 行为。
- 关键用例：
  1. 假 Feed Handler 发出已收盘 1m Bar → flush → REST 返回同样的 Bar；Parquet schema 与黄金文件一致。
  2. 未收盘 Bar 被丢弃，不进入规范流。
  3. 断线产生缺口 → 假 Feed Handler 的历史接口被调用 → HWM 前进、缺口补齐。
  4. 修正一根 1m Bar → `revision` 自增，父周期同步重发。
  5. 同一 Replay Session 跑两次，事件序列逐字节相同。
  6. 超过 JSON 行数上限返回 `range_too_large`。
  7. 路径编码包含 `/` 的 symbol 后，Dataset 仍可读写。
  8. GC 在活跃 Replay Session 存在时跳过对应 Dataset。
  9. CI 中网关包不出现 `nautilus_trader` import。
- 仓库为 greenfield，无既有测试先例；Data Contract 黄金文件是首批先例。

## Out of Scope

- 逐笔成交（TradeTick）、盘口（Depth）、BBO（QuoteTick）、交易状态流。
- 下单、账户、持仓、风控。
- 鉴权、多租户、公网暴露、TLS 终止。
- Redis、Kafka、分布式部署、多机分片。
- 任何具体回测/交易框架的适配器（NautilusTrader、vnpy 等）。
- 自动 Compaction、可视化界面。
- 未收盘 Bar、forming bar 推送、Bar 内价格路径重建。
- 多 Venue 的符号映射魔法；跨源对齐由使用者显式声明。
- 归档数据的全量导入之外的历史修复工具。
- 期货/股票以外的资产类别在 v1 的实现（配置可预留，但只验证 Binance 现货）。

## Further Notes

- 术语与决策以 `GLOSSARY.md` 与 `docs/adr/` 为准；实现中不得引入术语表 `_Avoid_` 列出的同义词。
- 原始需求文档中"逐 Tick 步进""最新 Tick/Orderbook"与 v1 的 Bar-only 决策冲突，需在需求文档中同步删除或标记为 v2。
- v1 的核心取舍：用 Bar-only 换取与日增 <10GB 规模匹配的存储与实现复杂度；接受无法做逐 Tick 回放与精确滑点建模的代价（ADR-0010）。
- 已知风险：交易所 K 线可能与逐笔聚合不一致、最后一根 K 线未完成、decimal 在 pandas 中为 object、Windows 下 ZMQ `ipc://` 不可用。
- 里程碑：M1 Data Contract 与存储（含 asap 回放骨架）→ M2 实时采集与补采 → M3 服务与 API → M4 Replay Protocol 与客户端 SDK → M5 归档补采、Compaction 与交付。
- 本地 tracker 约定：本文件为 `.scratch/market-data-gateway-v1/spec.md`；拆票时写到同目录 `issues/NN-<slug>.md`，每张票顶部写 `Status:`。
