# Market Data Gateway — 设计共识

> 状态：**待确认**（确认后落为实现规格）
> 术语以 [GLOSSARY.md](GLOSSARY.md) 为准；关键决策见 [docs/adr/](docs/adr/)。
> 原始需求：[需求整理.md](需求整理.md)
>
> **v1 数据粒度：仅 Bar。** 不采集、不存储、不对外提供逐笔成交与盘口；1m Bar 来自交易所原生 K 线（Aggregation Source = `external`），5m 及以上由 1m Bar 聚合得到（`internal`）。需求文档中"逐 Tick 步进""最新 Tick/Orderbook"两处需同步删除。

## 1. 目标与非目标

### 目标

1. 把多个外部行情源收敛为**一套稳定、版本化的 Data Contract**，使策略/回测代码不感知数据源差异。
2. 解除三类耦合：策略 ↔ 数据源、策略 ↔ 存储格式、网关 ↔ 回测框架。
3. 支持实时（WebSocket / ZMQ）、历史（REST / Parquet / 客户端 SDK）与回放（Replay Protocol）三种消费方式。
4. 数据源与框架可扩展：新增 Feed Handler 不改核心；新增框架不改网关。

### 非目标（v1 明确不做）

- 逐笔成交（TradeTick）、盘口（Depth）、BBO（QuoteTick）、交易状态流。
- 下单/交易执行、账户与持仓。
- 鉴权、多租户、公网暴露。
- Redis、消息队列、分布式部署。
- 任何具体回测/交易框架的适配代码（NautilusTrader 等）。
- **自动** compaction 与图形界面（v1 提供手动 `mdgw compact`，见 §5.5）。

## 2. 系统形态

### 2.1 Service Role

| Service Role | 职责 | 进程 |
|---|---|---|
| `collector` | 连接 Venue、规范化、接收 1m Bar 并聚合、写 Parquet、维护 HWM、补采 | 独立进程 |
| `api` | REST / WebSocket / ZMQ 行情分发、状态与指标 | 独立进程 |
| `replay` | 读取 Parquet、按 Replay Protocol 回放 | 按需独立进程 |

- 每个 Service Role 可单独启动：`mdgw run --service collector|api|replay`。
- 开发期可用 `mdgw up` 作为 supervisor，按 `collector → 等待 ready → api` 顺序拉起并负责重启。
- 交付可选 `docker-compose.yml`；生产环境可再套 systemd。

### 2.2 进程间通信

- **ZMQ 单一总线**：行情 PUB/SUB，控制 REQ/REP。
- 同一套抽象支持 `inproc://`（测试）、`tcp://`（默认）、`ipc://`（仅 Linux 可选）。
- **v1 不引入 Redis**；HWM 与会话状态放在各进程自己的 SQLite。
- PUB/SUB 慢加入问题由启动顺序 + 就绪握手解决（api 就绪后 collector 才开始 publish）。

### 2.3 数据流

```
Feed Handler(1m klines) ──▶ Normalize (Polars batch) ──▶ Bar Aggregation ──▶ Parquet Data Files + Manifest
                                      │                         │
                                      │ rejected rows           ├──▶ ZMQ PUB (market.bar.*) ──▶ api ──▶ WS / ZMQ 消费者
                                      ▼                         └──▶ HWM (SQLite)
                             Quarantine Dataset

Manifest Snapshot + Read Lease ──▶ Store (DuckDB explicit file list) ──▶ replay (Replay Clock) ──▶ ZMQ PUB (replay.<session>.*)
                                                                     └──▶ api REST history ──▶ JSON / Arrow IPC / CSV
```

## 3. 公开接口

网关只交付以下四组公开接口；接入方自行编写适配代码。

### 3.1 Data Contract：版本化 Arrow Schema（v1）

所有时间字段为 `timestamp[ns, tz=UTC]`（底层 int64 纳秒）。
所有价格、数量、金额字段为 `decimal128(38, 18)`（20 位整数 + 18 位小数），**不使用 float64**。
JSON 中 decimal 字段为字符串（如 `"65000.1"`），CSV 输出十进制原文，绝不经过浮点；接入方用 `decimal.Decimal` 解析。
时间字段在 JSON 中为 ISO-8601 纳秒字符串（如 `2026-10-30T12:34:56.123456789Z`）——JSON 数字无法无损表达纳秒。

**Instrument**

| 字段 | 类型 | 说明 |
|---|---|---|
| `instrument_id` | string | `<venue>:<symbol>`，主键 |
| `venue` | string | `binance` / `ctp` / ... |
| `symbol` | string | 交易所原生写法 |
| `asset_class` | string | `crypto` / `futures` / `equity` |
| `instrument_type` | string | `spot` / `perp` / `future` / `stock` |
| `base` / `quote` | string | 基础/计价资产 |
| `tick_size` | decimal128(38,18) | 最小价位 |
| `lot_size` | decimal128(38,18) | 最小数量单位 |
| `multiplier` | decimal128(38,18) | 合约乘数（现货为 1） |
| `timezone` | string | IANA 时区（分区与日线边界用 UTC，此字段仅描述源） |
| `listing_ts` | timestamp | 上市时间 |
| `delisted_ts` | timestamp? | 退市时间；非空即已退市 |

**Bar**（只包含已收盘的 Bar）

| 字段 | 类型 | 说明 |
|---|---|---|
| `instrument_id` | string | |
| `interval` | string | `1m` / `5m` / `15m` / `1h` / `1d` |
| `open_ts` / `close_ts` | timestamp | 周期边界，UTC；`close_ts = open_ts + interval` |
| `ingest_ts` | timestamp | 网关接收/生成该版本的时刻 |
| `feed` | string | Feed Handler 名称，例如 `binance_spot` |
| `aggregation_source` | string | `external`（交易所原生）或 `internal`（网关聚合） |
| `open/high/low/close` | decimal128(38,18) | |
| `volume` | decimal128(38,18) | |
| `trade_count` | uint32? | 交易所 K 线给出的成交笔数；没有则为空 |
| `revision` | uint32 | 默认 0；源修正导致同一 `(instrument_id, interval, open_ts)` 被重新发布时自增 |

**Message Envelope 列**（在每条 RecordBatch 中与 payload 同列）

| 字段 | 类型 | 说明 |
|---|---|---|
| `schema_version` | uint16 | 当前为 `1` |
| `data_type` | string | 恒为 `bar` |
| `instrument_id` | string | |
| `event_ts` | timestamp | 取 `close_ts` |
| `ingest_ts` | timestamp | |
| `feed` | string | |
| `seq` | int64? | 源内序号，无则为空 |

JSON 线格式为：

```json
{
  "schema_version": 1,
  "data_type": "bar",
  "instrument_id": "binance:BTC/USDT",
  "event_ts": "2026-10-30T12:35:00.000000000Z",
  "ingest_ts": "2026-10-30T12:35:00.130000000Z",
  "feed": "binance_spot",
  "seq": 123456,
  "payload": {
    "interval": "1m",
    "open_ts": "2026-10-30T12:34:00.000000000Z",
    "close_ts": "2026-10-30T12:35:00.000000000Z",
    "aggregation_source": "external",
    "open": "65000.1",
    "high": "65010.0",
    "low": "64990.0",
    "close": "65005.5",
    "volume": "12.34",
    "trade_count": 128,
    "revision": 0
  }
}
```

### 3.2 Data Contract：Parquet Dataset 布局

```
data/
  bars/asset_class=<...>/venue=<...>/symbol=<file_symbol>/interval=<interval>/year=YYYY/month=MM/
  instruments/snapshot-<config_hash>.parquet
```

- **所有层级统一为 Hive 风格 `key=value`**；Manifest 条目记录同一组分区字段，读取前据此裁剪到 Instrument、Interval 与文件时间范围，物理 Data File 保持自包含。`bars` 是 Dataset 根。
- `file_symbol` = URL 百分号编码后的 `symbol`，例如 `BTC%2FUSDT`；`instrument_id` 始终保存在行内。
- 每个 Dataset 根目录放 `_dataset.json`（`schema_version`、Arrow schema 的 JSON 表示、分区规范、文件命名规范）与 `_manifest.json`（当前活跃的 Data File 清单）。
- Data File：`data-<start_ns>-<end_ns>-<uuid>.parquet`，**一经写入内容不变**；compaction 通过写入新文件并把旧文件标记为 inactive 来实现（见 §5.5）。
- 写入参数：`version="2.6"`（只有 2.6 原生支持 ns 时间戳；1.0/2.4 会截断为 µs）、`use_deprecated_int96_timestamps=False`、`compression="zstd"`、`row_group_size=1_000_000`、单文件目标 **64–256MB**（Bar 体量小，通常由 compaction 达成）。
- **读取路径只有一个门面**：`ParquetBarStore`。每次 HTTP 请求或 Replay Session 开始时解析一个固定的 Manifest Snapshot，为所选分区/月份获取 Dataset Read Lease，再把该 Snapshot 的显式 Data File 列表交给 DuckDB 2.x Relation Arrow Reader。禁止对 Dataset 根目录 glob，禁止 DuckDB 原生权威表、第二份 Catalog 或生产引擎开关。
- 同一 HTTP 请求的 count、存在性探测和实际读取共享一个 Snapshot 与 Lease；不同分页请求各自使用新 Snapshot。Replay Session 固定一个 Snapshot 与 Lease 直到结束，包括暂停与 seek。
- DuckDB 只返回 Arrow RecordBatch；Store 在 Arrow 边界恢复 `BAR_SCHEMA` 的字段顺序、nullability、Decimal、UTC 纳秒与 Hive `interval`。除确有读取变换需求外不进入 Polars。PyArrow 仍用于 Data File 写入、Arrow IPC/ZMQ、export、Compaction 与测试。
- Lease 覆盖 Reader 的完整生命周期；Compaction/GC 遇到有效 Lease 时跳过并报告，过期心跳在超时后失效。读取方若 `schema_version` 不匹配则**直接报错**（`schema_version_mismatch`），不猜测、不降级。
- 分区按 `open_ts` 的 UTC 年月。
- 读取失败是明确失败，不能转换成空结果或短结果；非分页 REST 输出不承诺全序（见 §4.1 与 ADR-0013/0015）。

### 3.3 Streaming Interface：Topic 与 Message Envelope

- 实时：`market.bar.<interval>.<instrument_id>`（如 `market.bar.1m.binance:BTC/USDT`）。
- 回放：`replay.<session_id>.<原始 topic>`。
- 控制：REQ/REP 到 `<bus_prefix>.control`，不走 topic。
- ZMQ 线格式：**多帧消息** `[topic][Arrow IPC stream]`，第二条消息帧是一个完整的 IPC stream（schema + 1..N 个 RecordBatch）。
- collector 按 `batch_window_ms=100` 或 `batch_max_events=500`（先到者）聚合成批再发布；Bar 流本身频率低，批量主要用于多个标的合并。
- WebSocket 线格式：JSON envelope（见 3.1）。
- 前缀订阅天然支持 `market.bar.1m.binance:` 这类聚合订阅。

### 3.4 Replay Protocol

**会话生命周期**

```
create_session(subscriptions, start_ts, end_ts, replay_mode, speed?)
  -> { session_id }
pause(session_id) / resume(session_id)
seek(session_id, ts)
set_speed(session_id, speed)
step(session_id, credits)        # replay_mode=step 时使用
stop(session_id)
  -> { ok } | { error: { code, message } }
```

- `subscriptions`：`[{ data_type: "bar", instrument_id, interval }]`。
- `replay_mode`：
  - `asap`：不做任何等待，尽可能快地推送（Replay Clock 不节流）。
  - `realtime`：按 `speed`（默认 1.0）以 Replay Clock 节流。
  - `step`：客户端每次 `step(n)` 授予 n 个 Step Credit，服务端最多推 n 条。
- **过滤与去重**：`start_ts` 闭区间、`end_ts` 开区间，均作用于 `close_ts`；物理 `open_ts` 扫描下界向前放宽一个 Bar Interval。同一 `(instrument_id, interval, open_ts)` 只保留最大 `revision`，同 Revision 再取最新 `ingest_ts`；重复订阅在查询前折叠。
- **排序**：所有订阅按 `close_ts`（即 envelope 的 `event_ts`）、`instrument_id`、`seq`（NULL 最后）、`interval`、`open_ts` 五键全序归并。
- **有界读取**：三种 Replay Mode 都消费 DuckDB 的有界 Arrow 批次和容量为 `[replay].prefetch_batches` 的有界预取，不在发布首条消息前物化整个范围。`asap` 只是不做 Clock 节流。
- **固定快照与 seek**：Session 的 Manifest Snapshot 与 Dataset Read Lease 覆盖整个生命周期，包括暂停和 seek。`seek` 只允许在 `pause` 状态下执行，递增查询代际并在同一 Snapshot 上重开查询；旧代际的批次、错误和结束信号被丢弃。seek 前的 Step Credits 丢弃，seek 后的 Credits 保留；Clock 在新代际首个批次就绪时才重新锚定，读取 I/O 不消耗虚拟时间。
- **取消与资源**：`stop`、seek 关闭旧查询前会 interrupt DuckDB 读取并释放 Reader/连接；Session 结束时 `StoreRead.close()` 释放 Lease。读取期间的 `pause`、`resume`、`set_speed`、`stop`、`step` 继续有效。
- **End of Session**：数据耗尽推送一次 `reason="exhausted"`，主动停止推送 `reason="stopped"`；读取失败推送 `reason="error"` 与 `error: {code, message}`，保留实际 `bars_sent`。失败码为 `schema_version_mismatch` 或 `internal_error`，消息已 sanitized；SDK 抛出 `ReplayFailedError`。失败不会被伪装成空回放或正常耗尽。详见 [ADR-0015](docs/adr/0015-bounded-replay-with-explicit-failure-termination.md)。
- 同一进程支持多个会话（默认 `[replay].max_sessions = 4`），会话之间互不影响。

## 4. HTTP 与 WebSocket API

HTTP 面（REST、WebSocket 与运维端点）统一由 FastAPI（Starlette/ASGI）承担，旧的手写 aiohttp 分发与 `HttpServer` 已退役。生命周期由 `AsgiServer` 持有：它在 daemon 线程上运行私有事件循环与纯 `uvicorn`（`log_config=None`、`access_log=False`），保留 `start`、`stop`、`call_soon_threadsafe`、`bound_port`、`base_url` 五个公共成员，以及幂等启停、端口 0 解析、通配地址回显 `127.0.0.1`、启动失败完整回滚后重抛原异常的语义。路由按资源拆分在 `routes/bars.py`、`routes/instruments.py`、`routes/ops.py`、`routes/websocket.py`；中间件分层为请求上下文（`X-Request-ID`、请求指标、完成日志）与限流，二者都不缓冲 Arrow/CSV 流式响应体。`redirect_slashes=False`，末尾斜杠不会触发隐式重定向；`/api/v1/instruments/`（空 instrument id）返回 `not_found` 404；不注册 HEAD 路由，HEAD 行为不在契约内。决策记录见 [ADR-0011](docs/adr/0011-fastapi-asgi-http-surface.md) 与 [ADR-0012](docs/adr/0012-single-instrument-pagination.md)。

### 4.1 REST

```
GET /api/v1/instruments?venue=&asset_class=&include_delisted=false
GET /api/v1/instruments/{instrument_id}
GET /api/v1/bars?instrument=&interval=&start=&end=&format=json|arrow|csv&limit=
GET /api/v1/bars/export?instrument=&interval=&start=&end=&format=parquet|arrow
GET /api/v1/status
GET /healthz
GET /readyz
GET /metrics
```

- `interval` 必填。
- 分页只用时间游标（Time Cursor），不支持 offset 或不透明 cursor。分页元数据只对同时带 `instrument` 与 `limit` 的 JSON 查询生效：响应为 `{"bars": [...], "page": {"has_more": ..., "next_start": ...}}`；`next_start` 是第一条未返回 Bar 的 `open_ts`（ISO-8601 纳秒字符串），下一次作为闭区间 `start` 使用。`limit=0` 返回空 `bars` 但仍报告 `has_more` / `next_start`；分页 JSON 按 `open_ts` 升序，非分页 JSON、Arrow、CSV 与 export **不承诺全序，也不承诺物理存储顺序**。多 Instrument 与 `/api/v1/instruments` 不提供分页（[ADR-0012](docs/adr/0012-single-instrument-pagination.md)）。
- REST 历史查询（JSON、Arrow、CSV、export 与分页 JSON）返回全部已存储 raw Revisions，不按 revision 去重；JSON 的 100,000 行上限按 raw Revision 行数计算。Replay 才按最大 Revision / 最新 `ingest_ts` 选择并应用五键排序。Revision-heavy 数据下的 Time Cursor 重复/不可达问题是独立契约缺口（[`docs/revision-heavy-time-cursor.md`](docs/revision-heavy-time-cursor.md)），本次存储迁移不修复。
- `format=arrow`：流式 Arrow IPC（decimal128 原样）；`format=csv`：流式 CSV（decimal 输出十进制原文，禁止经过 float）；`format=json`：仅允许小范围，超过 100,000 行返回 `range_too_large`，decimal 以字符串输出。
- `format=arrow|csv` 无行数上限，边读边发，不把结果全量放进内存。
- `export` 把同一读取器的结果落成一个 Parquet/Arrow 文件返回（一次性大文件下载）。
- `/openapi.json` 与 `/docs` 始终开启且没有配置开关，`/redoc` 关闭。OpenAPI 只是由代码生成的发现文档，规范性真相仍是 `docs/data-contract.md` 与 Streaming Interface。
- 限流由 `[rate_limit]` 配置控制，默认关闭；开启后按 `(源 IP, 路由模板)` 维护进程内令牌桶（`requests_per_second`、`burst`），并用 `max_ws_sessions_per_ip` 限制每个 IP 的并发 WebSocket 会话。`/healthz`、`/readyz`、`/metrics`、`/openapi.json`、`/docs` 固定豁免且不可配置；未匹配任何路由的请求使用固定 `<unmatched>` 模板作为桶键与指标标签，避免随机 404 造成基数膨胀；超限返回 429、`Retry-After` 与稳定错误码 `rate_limited`；WebSocket 在 `accept()` 之前拒绝。状态只在单 worker 进程内，不引入跨进程配额。
- `/metrics` 输出 Prometheus 文本格式；除既有计数器与 gauge 外，新增 `mdgw_api_http_request_duration_seconds` histogram（标签 `method`、`path` 路由模板、`status`；桶为 0.005–5 秒与 `+Inf`）和 `mdgw_api_rate_limited_total{path}`。可选的 `docker-compose.observability.yml` 在默认 `docker compose up` 之外启动 Prometheus（9090）与 Grafana（3000），并抓取 collector、api、replay 三个 Service Role 的 `/metrics`。
- 历史查询只读已落盘的 Data File；collector 的写缓冲（默认 1 小时）意味着**最近一段数据可能查不到**。`/api/v1/status` 必须暴露每个 Dataset 的 `last_flushed_ts`，客户端据此判断；需要实时性的消费者应使用 WebSocket / ZMQ，而不是历史接口。
- 默认只绑定 `127.0.0.1`；v1 不做鉴权。

### 4.2 WebSocket

```
/ws
客户端 -> {"op":"subscribe","topics":["market.bar.1m.binance:BTC/USDT", ...]}
       -> {"op":"unsubscribe","topics":[...]}
       -> {"op":"ping"}
服务端 -> {"op":"pong"}
       -> {"op":"event","topic":..., "data": {...envelope...}}
       -> {"op":"lag_notice","dropped":N,"since":"..."}
```

慢消费者：丢弃并发送 `lag_notice`，**不断开连接**。

### 4.3 错误模型

```json
{"error": {"code": "invalid_instrument", "message": "...", "details": {}, "request_id": "..."}}
```

稳定错误码：`invalid_instrument`、`instrument_not_found`、`invalid_interval`、`invalid_parameter`、`invalid_format`、`invalid_range`、`range_too_large`、`schema_version_mismatch`、`session_not_found`、`not_found`、`method_not_allowed`、`rate_limited`、`internal_error`。`dataset_not_found` 与 `feed_unavailable` 仅为未来候选，当前未实现。HTTP 状态码只做粗分类（400/404/405/409/429/500）。REST API 的所有失败——包括 404、405、429 与参数校验——都使用上述 envelope，并回写 `X-Request-ID`；FastAPI 默认的 422 与 `{"detail": ...}` 从不暴露。collector / replay 的 `ServiceHealthServer` 运维端点（`/healthz`、`/readyz`、`/metrics`、`/status`）保留既有响应形状，不套用该 envelope。

## 5. 运行规则

### 5.1 Instrument Reference Data

- 真相源：`config/instruments/*.yaml`（人工维护、可 review、可 diff）。
- 启动时校验并导出 Instrument Snapshot：`data/instruments/snapshot-<config_hash>.parquet`。
- 所有进程（含 replay）只读 Instrument Snapshot；REST `/instruments` 也读它。
- Bar 的 `instrument_id` 不在 Instrument Snapshot 中时一律拒绝（`instrument_not_found`），不回退到任何默认 asset_class，也不写入任何 Bar。
- 被历史数据引用过的 Instrument 永不物理删除，只设 `delisted_ts`。

### 5.2 采集、HWM 与补采

- 采集对象是**交易所原生 K 线**：`watch_ohlcv` / `watch_ohlcv_for_symbols`（1m）。
- **只接受已收盘的 Bar**：WS 用 K 线事件的 `x`（is-closed）标志；REST 历史用 `closeTime < 服务器时间` 过滤，绝不把正在形成的最后一根当作规范 Bar。
- 每个 `(instrument_id, interval)` 在 `state.db` 保存 High-Water Mark（最后已持久化的 `open_ts`）。
- 启动/重连：从 HWM 补采；无 HWM 则从 `now - backfill_hours` 开始（默认 24h）。
- 补采用 `fetch_ohlcv(symbol, '1m', since, limit=1000)`：24 小时 = 1440 根，约 2 次请求，成本可忽略。
- 5m/15m/1h/1d **不单独采集**，一律由 1m Bar 聚合，保证所有周期出自同一份数据。
- 深历史（数月/数年）走 `data.binance.vision` 的 K 线归档（日/月 ZIP，带 SHA-256 校验），由 `mdgw backfill --archive` 实现。
- 归档的时间戳单位有坑：Binance 现货 2025-01-01 起为**微秒**，之前为**毫秒**；规范化层必须按量级/日期显式判定，不能一律乘 1000。
- 补采回溯上限 ≤ 对应 Data Type 的 Retention；超限记 `gap_unrecoverable` 并继续，不假装连续。
- 去重：以 `(instrument_id, interval, open_ts)` 为幂等键；已落盘且内容一致则跳过，内容修正则 `revision += 1` 并新增 Data File。
- **陈旧检测**：每个 `(feed, instrument, interval)` 维护 `last_event_ts`；超过 `stale_after_s`（默认 max(2×interval, 120s)）即标记 stale、记指标并告警——Binance 会主动断开 24h 长连接，静默断流比报错更危险。

### 5.3 Bar 收口与 Bar Aggregation

- 1m Bar 是唯一原始粒度；5m/15m/1h/1d 由已收盘的 1m Bar 聚合。
- UTC 日界，`1d` 即 UTC 00:00–24:00。
- 聚合规则：`open` 取首根、`close` 取末根、`high/low` 取极值、`volume` 与 `trade_count` 求和。
- 只有整段 1m 都已到达且已收盘，父周期才发布；不发布"部分周期"的 Bar。
- 1m Bar 的 `aggregation_source=external`，聚合出的高周期 Bar 为 `internal`。
- 若某根 1m Bar 因源修正而 `revision += 1`，其所有父周期的 Bar 同步重发并自增 `revision`。
- Bar 落盘；历史接口与回放只读取已收盘的 Bar。

### 5.4 Retention 与 Garbage Collection

- 默认关闭；配置开启后按 Data Type 保留（默认 `bars_days=0`，即永久）。
- 只删除**整月分区**，跳过当前月；绝不重写 Data File。
- `mdgw gc --dry-run`（默认）与 `mdgw gc --apply`。
- 每次选中 Data Files 的读取与每个 Replay Session 在 `_sessions/` 写心跳锁（Dataset Read Lease）；GC 跳过心跳在超时窗口内的分区/月份并在报告中列出。进程崩溃后心跳停止，过期锁自动失效。
- GC 不影响 Instrument Reference Data 与 HWM。

### 5.5 写入批次与 Compaction

- 写盘按 `flush_max_bytes=64MB` **或** `flush_max_age_s=3600`（先到者）落一个 Data File。Bar 体量小，1 小时 flush 是为了控制历史接口的新鲜度；实时消费者走 WS/ZMQ。
- `mdgw compact --older-than 24h` 把同分区内的小 Data File 合并为 64–256MB 的新文件，更新 `_manifest.json` 将旧文件标记 inactive，再删除旧文件。Compaction 只做"新文件 + 清单切换"，不修改任何已存在的文件；受有效 Dataset Read Lease 保护的分区跳过并报告。
- Compaction 不重新 Normalization；新 Manifest 条目的 `rule_versions` 是来源条目集合的并集，Bar 字段值与 Revisions 原样保留。
- 监控指标：每个 Dataset 的 Data File 数量、中位文件大小、`last_flushed_ts`。中位文件大小持续 <10MB 是必须触发 compaction 的信号。

### 5.6 ccxt Feed Handler（v1）

- 使用 `ccxt.pro`（异步）：`watch_ohlcv(symbol, '1m')`；若交易所提供 `watch_ohlcv_for_symbols(symbols, '1m')` 则优先用它。
- 能力判断读 `exchange.has['watchOHLCV']` / `['fetchOHLCV']`，不靠交易所名字硬编码。
- 历史补采用 `fetch_ohlcv`；`enable_rate_limit=True`，并叠加按 Venue 的令牌桶。
- ccxt 自身会重连并重订阅，网关仍需自己的 HWM + 补采，因为重连期间的缺口不会自动补。
- 关闭时 `await exchange.close()`。
- **不使用** `watch_trades` / `watch_bids_asks` / `watch_ticker`（v1 无逐笔与盘口）。

### 5.7 背压

- 采集 → 聚合 → 写盘之间使用有界队列（默认 10k 条）；满时丢弃最旧并累加 `lag` 指标。
- 写盘独立 writer，按 §5.5 的 `flush_max_bytes` / `flush_max_age_s` 批量 flush。
- ZMQ PUB 设置显式 HWM（ZeroMQ 的 high-water mark，与 §5.2 的 High-Water Mark 同名不同义，代码中必须用 `zmq_sndhwm` 之类的前缀区分）；api 的 WS 队列满时发 `lag_notice`。

> 注意：ZeroMQ 的 HWM 指 socket 发送/接收队列上限；§5.2 的 High-Water Mark 指数据水位。两者同名纯属巧合，代码中前者写 `zmq_hwm`，后者写 `data_hwm`。

### 5.8 Normalization、Validation 与 Quarantine

- Collector 每次 `ingest()` 排空 Feed Handler 后得到一个 RawBar 批次；`normalize_batch`（Polars）为整批计算 Instrument、Interval 与 closed-Bar 规则，不为每条消息构造单行 DataFrame。Decimal 在进入 Arrow/Polars 前用 Python `Decimal` 精确校验并拒绝 float 与超 scale 值；时间、`uint32` 与 nullable 字段按 `BAR_SCHEMA` 检查。
- 该批量规则是唯一规则来源：Live ingest、Backfill 与 archive import 共用它。Feed Handler 继续负责 Venue 协议与闭合状态，archive 继续负责 checksum/ZIP/CSV 与来源时间单位。
- 实时批次任一 Normalization 或 Instrument Validation 失败时整批拒绝：Aggregation、HWM 与 freshness 不推进，可修复参考数据后重试。Archive 保持整文件原子性；CCXT 单行解析失败只有先成功写入 Quarantine 才能跳过并继续。
- Quarantine 是独立、版本化的 Dataset（默认跟随有效 storage root：`<有效 root>/quarantine`，禁止放进 Bars Dataset）；Bars Store、REST、export、Compaction 与 Replay 永不读取它。Reason Code、Source Stage、Rule Version、来源定位与结构化详情见 [ADR-0014](docs/adr/0014-polars-batch-normalization-with-quarantine.md) 与 `docs/data-contract.md`。
- Quarantine 写入 fail-closed：写入失败时对应输入不得确认、跳过或发布，Collector 暴露写入失败与按 stage/reason 的记录指标。默认保留 30 天，`0` 永久；`mdgw quarantine-gc` 默认 dry-run，只清理过期完整 UTC 月份并跳过当前月，`--apply` 才删除。没有后台自动 GC。
- 新 Data File 的 Manifest 条目记录 `rule_versions`；它是审计元数据，Data Contract Version 仍是读取兼容性的唯一权威。

### 5.9 Storage Composition 与有效 Storage Root

- 全部生产 Service Role（`collector`、`api`、`replay`、`GatewayRuntime`）与运维命令（`compact`、`gc`、`quarantine-gc`、`backfill --archive`）不再各自把 `GatewayConfig` 映射为 Store；内部 Storage Composition 模块是唯一生产组合点，按需提供 Bars-only、Quarantine-only 与 Bars + Quarantine 三种操作。每次组合返回新对象，不缓存、不注册全局对象、不拥有启停或关闭生命周期，也不构建 Feed Handler。
- 有效 storage root 的优先级固定为：显式 `--root` override，其次 `[storage].root`。Bars Dataset、Instrument Snapshot、High-Water Mark 数据库与默认 DuckDB 临时目录都锚定该有效 root（`bars/`、`instruments/`、state db、`.duckdb_tmp`）。
- 未配置 `[storage.quarantine].root` 时，Quarantine root 跟随有效 root（`<有效 root>/quarantine`）。显式配置的 Quarantine root 按原值使用：相对路径保持相对进程当前工作目录，不跟随 root override，也不被重新锚定或改写。
- 组合时会把实际生效的 Bars Dataset root 与 Quarantine root 规范化，并复核 ADR-0014 的单向包含关系（Quarantine 不得等于或位于 Bars Dataset 内；不增加反向禁令）。Bars Store 构造会立即创建 DuckDB 临时目录，因此校验先于任何 Store 构造；失败时不会创建 root、descriptor、Manifest、临时目录或 Quarantine 状态。
- 同一次组合创建的写入对象使用同一个有效 Normalization Rule Version（默认 `polars-1`，可由 `[storage.quarantine].rule_version` 覆盖）。默认与自定义值同时到达 Bars Manifest 条目、Quarantine Record 和默认构建的 Feed Handler；它始终只是审计元数据，不参与 Data Contract 兼容性判断。
- 组合模块只决定它创建的对象与默认构建路径；显式注入的 Store、Quarantine 与 Feed Handler 不被重写。archive 直接调用 `import_archive` 时若省略 Quarantine，仍使用 `<store.root>/quarantine`，不要求调用方构造 `GatewayConfig`。Quarantine 的 Schema Version、Retention 与物理格式默认值保持独立，不继承 Bars 的 compression、row group size 或 format version。

## 6. 配置

`config/gateway.toml`（运行配置）示例：

```toml
[server]
host = "127.0.0.1"
http_port = 8080
ws_path = "/ws"

[bus]
transport = "tcp"              # 仅单进程 GatewayRuntime 使用；Service Role 总是走显式 tcp://
zmq_hwm = 10000
stream_bind_host = "127.0.0.1"
stream_pub_port = 0            # 0 = 随机端口
collector_pub_bind = "tcp://127.0.0.1:5555"
collector_pub_connect = "tcp://127.0.0.1:5555"
api_pub_bind = "tcp://127.0.0.1:5556"
api_pub_connect = "tcp://127.0.0.1:5556"

[service]
health_host = "127.0.0.1"
collector_health_port = 8081
replay_health_port = 8082
log_level = "INFO"
collector_required = false
wait_for_api_ready = false

[storage]
root = "./data"
flush_max_bytes = 67108864     # 64MB，先到者触发
flush_max_age_s = 3600         # 1 小时
row_group_size = 1000000
compression = "zstd"
format_version = "2.6"         # 只有 2.6 原生支持 ns 时间戳
schema_version = 1

[storage.duckdb]
memory_limit = "1GB"
threads = 4
# temp_directory 未设置时为 <有效 storage root>/.duckdb_tmp，而不是 DuckDB 自己的默认临时目录
max_temp_directory_size = "10GB"
arrow_batch_size = 65536

[storage.quarantine]
# root 未设置时为 <有效 storage root>/quarantine；显式值按原值使用（相对路径相对进程 CWD），且不得位于 Bars Dataset 内
schema_version = 1             # 独立于 Bar Data Contract
rule_version = "polars-1"      # 审计元数据，不是 Schema 权威
retention_days = 30            # 0 = 永久保留

[retention]
enabled = false
bars_days = 0            # 0 = 永久

[collector]
backfill_hours = 24
stale_after_s = 120
state_db_name = "state.db"
write_queue_size = 10000

[rate_limit]
enabled = false          # 默认关闭；开启后按 (源 IP, 路由模板) 限流
requests_per_second = 50
burst = 100
max_ws_sessions_per_ip = 16   # 0 = 不限

[[feeds]]
name = "binance_spot"
kind = "ccxt"
venue = "binance"
market_type = "spot"
enable_rate_limit = true
data_types = ["bar"]
symbols = ["BTC/USDT", "ETH/USDT"]
backfill_hours = 24
stale_after_s = 120
batch_window_ms = 100
batch_max_events = 500

[aggregation]
base_interval = "1m"
intervals = ["1m", "5m", "15m", "1h", "1d"]

[replay]
control_bind_host = "127.0.0.1"
control_port = 0               # 0 = 随机端口
publish_bind_host = "127.0.0.1"
publish_port = 0
max_sessions = 4
heartbeat_s = 1.0
batch_size = 4096              # DuckDB Arrow 批次
prefetch_batches = 2           # 有界预取深度
```

`config/instruments/*.yaml` 示例：

```yaml
- instrument_id: "binance:BTC/USDT"
  venue: binance
  symbol: "BTC/USDT"
  asset_class: crypto
  instrument_type: spot
  base: BTC
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.00001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
```

## 7. 仓库结构

```
pyproject.toml
environment.yml
docker-compose.yml
config/
  gateway.toml
  instruments/*.yaml
src/
  mdgw/
    core/          # InstrumentId、时间、schema、envelope、错误码
    ingest/        # FeedHandler Protocol、注册表、ccxt Feed Handler
    normalize/
    aggregate/     # 1m 接收与 Bar Aggregation
    store/         # Parquet 写入、Manifest Snapshot/Read Lease、DuckDB 读取、GC、compaction、Storage Composition
    quarantine.py  # 独立、版本化的 Quarantine Dataset 与 Retention/GC
    streaming.py   # Streaming Interface：Bar topic 语法、Arrow IPC、PUB/SUB、READY、status/control
    replay/        # 有界 Replay 查询、Clock、Step Credit、失败终止
    api/           # REST、WebSocket、metrics
    cli/           # run --service / up / gc / quarantine-gc / export / compact / backfill --archive
  mdgw_client/     # 薄客户端 SDK（不依赖 mdgw 内部实现）
tests/
  contract/        # schema 黄金文件、编解码往返、目录布局
  unit/            # Bar Aggregation、收口、HWM、编码
  integration/     # 假 Feed Handler 端到端、回放确定性
  network/         # 真实交易所，@pytest.mark.network，默认跳过
docs/adr/
GLOSSARY.md
DESIGN.md
```

## 8. 依赖与环境

- 唯一环境：conda `nautilus`（Python 3.13.18），提交 `environment.yml` + `pyproject.toml`，补装 `pytest`。
- 运行时依赖：`pyarrow`、`duckdb>=2.0,<3`、`polars==2.1.0`、`pyzmq`、`ccxt`、`fastapi`、`uvicorn`（纯包，不使用 `[standard]` extra）、`websockets`、`msgspec`/`orjson`、`PyYAML`。DuckDB 2.x 是正式读取基线，启动时验证；**禁止 1.x**（无法保证 Parquet UTC 纳秒精度），也没有 `use_duckdb` 或旧引擎后备开关。HTTP/WebSocket 面不再直接依赖 `aiohttp`；它可能仍作为 `ccxt` 的传递依赖出现在环境中。
- 三个技术栈的职责边界：DuckDB 负责 Bars 读取（Manifest Snapshot 下的显式文件列表与 Arrow 批次）；Polars 只负责批量 Normalization（Collector、Backfill、archive），不在读取路径上；PyArrow 负责 Data File 写入、Arrow Schema/IPC/ZMQ、export、Compaction 与测试。
- **禁止**网关包 import `nautilus_trader`（CI 用 AST 扫描或 import-linter 强制）。
- `mdgw_client` 只依赖 `pyarrow`、`pyzmq` 与 schema 定义。
- decimal128 的 pandas 表示：`to_pandas()` 返回 `object` dtype 的 `decimal.Decimal`（pandas 无原生 decimal dtype）；SDK 文档必须写明，避免使用者误用 `astype(float)`。需要高性能数值计算的使用者应留在 Arrow 层或显式量化。

## 9. 测试与验收

### 9.1 测试缝（Seams）

仓库从零开始，无既有 seam。按"尽可能高、尽可能少"的原则，确定 **四个**：

1. **`GatewayRuntime` 端到端（主 seam）**：组合根接收配置、一个实现 `FeedHandler` Protocol 的假数据源、一个临时数据目录，在单进程内用 `inproc://` ZMQ 跑起 collector → Parquet → api → replay。所有用户可见的 REST / WebSocket / 运维行为从这里进入，包括错误 envelope、分页、限流与豁免、`/openapi.json`、`/docs`、`/metrics`、`/healthz`、`/readyz` 与 `/status`。
2. **线上字节（Data Contract seam）**：Arrow schema 黄金文件、Arrow IPC 往返、`_dataset.json`/`_manifest.json` 校验、topic 与路径编码。它不是行为 seam，而是 Data Contract 夹具；迁移前捕获的 characterization 黄金样本（JSON envelope、Arrow 表语义、CSV 文本、decimal 字符串、纳秒时间、错误体）在删除 aiohttp 实现后必须原样通过。
3. **`AsgiServer` 与 `ApiService` 的公共接口（HTTP 生命周期 seam）**：刻意新增的第三个 seam。用真实 socket 与端口 0 直接验证启动、幂等、停止、失败回滚、`base_url`/`bound_port` 与跨线程派发，不经过完整的 feed → collector → api → replay 链路；全链路行为仍从 `GatewayRuntime` 进入。
4. **`StreamingInterface` 的公共接口（Streaming Interface seam）**：用真实 ZMQ 在 `inproc://` 与端口 0 上验证 topic 语法、Arrow IPC 往返、READY、计数器与转发，不经过 HTTP 服务器；全链路行为仍从 `GatewayRuntime` 进入。

令牌桶（注入假时钟）、histogram 文本渲染与配置校验是纯单元测试，不构成新的功能 seam。

真实 Binance 只出现在 `@pytest.mark.network` 的测试里，默认跳过。

### 9.2 测试内容

- Data Contract 测试：Arrow schema 黄金文件；Arrow IPC 与 JSON 编解码往返；Parquet 目录布局；`_dataset.json`/`_manifest.json` 校验。
- 单元测试：1m 收口、Bar Aggregation、revision、HWM 推进、topic/路径编码、错误码映射、decimal 不经浮点；令牌桶注入假时钟；`MetricsRegistry.render()` 的 histogram `_bucket`/`_sum`/`_count` 与 `le` 标签；`[rate_limit]` 配置校验。
- 集成测试：假 Feed Handler → collector → Parquet → api → replay 全链路，不触网；错误 envelope（404/405/429/校验失败/500）、分页、限流豁免、`/openapi.json`、`/docs` 与 `/redoc` 404。
- HTTP 生命周期测试：`AsgiServer` 与 `ApiService` 在端口 0 上用真实 socket 验证启动、幂等、停止、失败回滚、`base_url`/`bound_port` 与跨线程派发；不构造完整链路。
- 回放确定性：同一 Replay Session 跑两次，输出事件序列逐字节相同。
- 验收标准：
  1. 新数据源只实现 `FeedHandler` Protocol + 配置即可接入，不改核心代码。
  2. 任意框架不 import 网关代码也能消费 Parquet / ZMQ / REST。
  3. 断线后补采能把缺口补齐，HWM 可解释。
  4. 同一份历史数据经 REST/SDK/直接读 Parquet 得到的语义一致。
  5. 未收盘的 K 线永远不进入规范流。

## 10. 里程碑

| 里程碑 | 内容 | 完成标志 |
|---|---|---|
| M1 Data Contract 与存储 | schema v1、envelope、Dataset 布局、Instrument Reference Data、假 Feed Handler、Parquet 读写、Data Contract 测试、**asap 回放骨架** | 假数据源 → 落盘 → asap 回放闭环 |
| M2 实时采集 | ccxt 1m K 线、收口判断、Bar Aggregation、HWM、断线补采（REST 24h） | Binance 实时 Bar 持续落盘，重启可续 |
| M3 服务 | ZMQ 多进程、Service Role CLI、REST/WS、status/metrics | 三进程可独立启动，外部可订阅 |
| M4 回放与 SDK | 完整 Replay Protocol（asap/realtime/step）、`mdgw-client` | 客户端 SDK 可完成查询 + 回放 |
| M5 交付 | K 线归档补采（data.binance.vision）、compaction、docker-compose、示例消费者、基准、文档 | 一条命令起全套；示例可运行；深历史可导入 |

## 11. 风险与待观察

- **bars-only 的能力边界**：无法还原周期内价格路径、点差、深度与排队位置；依赖精确止损/止盈、紧贴盘口成交的策略不能用它做可信回测。Nautilus 的 bar execution 只是把 OHLC 拆成 O→H→L→C 四个合成事件，属于启发式重建。这是 v1 明确接受的代价。
- **交易所 K 线的不一致性**：即使原始 K 线也可能与逐笔聚合结果存在笔数/成交量差异；REST 的最后一根是未完成状态，必须用 `closeTime < server time` 或 WS 的 `x` 标志过滤。
- **decimal128 的代价**：pandas 无原生 decimal dtype，`to_pandas()` 得到 `object`/`Decimal`；聚合与统计比 float64 慢，且 JSON 必须以字符串传输。这是为精度付出的成本，接受它。
- **ccxt 交易所差异**：`watch_ohlcv_for_symbols` 并非所有交易所都支持；Feed Handler 必须用 `exchange.has[...]` 做能力声明 + 回退，不能假设统一。
- **Data File 小文件**：Bar 体量小，按小时 flush 也会产生大量小文件；已改为 64MB/1 小时 flush + 手动 compaction，仍需用文件数指标持续观察。
- **pandas 3 的时间分辨率**：`to_pandas()` 可能把 `timestamp[ns]` 转成 `datetime64[us]`；客户端 SDK 不强制 `coerce_temporal_nanoseconds=True`（该选项在 pandas 3 上会越界或静默损坏 1677 年前的数据），值本身仍精确。
- **Windows 的 ZMQ ipc**：PyPI 的 Windows wheel 默认禁用 `ipc://`；默认改用 `tcp://127.0.0.1`，`ipc://` 仅 Linux 启用。
- **PUB/SUB 慢加入**：依赖启动顺序与就绪握手；未来若拆机部署，需引入重连与快照补偿。
- **Bar Revision 的下游幂等**：接入方必须按最大 revision 去重，文档需显著提示；REST 返回全部 raw Revisions，Replay 才做最大 Revision 选择。
- **DuckDB 资源与临时磁盘**：分页与 Replay 的全局排序可能落临时磁盘；`[storage.duckdb]` 的 `memory_limit`、`threads`、`temp_directory`、`max_temp_directory_size` 必须按部署显式配置，资源错误必须是明确失败而不是截断。临时文件不是数据副本。
- **Read Lease 与维护竞态**：Compaction/GC 必须尊重心跳在超时窗口内的 Lease 并在报告中列出跳过；进程崩溃后依赖心跳过期回收，不能靠操作系统删除语义。
- **Quarantine 容量与可用性**：默认 30 天保留需要运维执行 `mdgw quarantine-gc`；写入 fail-closed 意味着审计存储故障会停止对应输入的消费，这是有意的完整性优先取舍。
- **Retention 与补采的边界**：REST 回溯上限必须 ≤ Retention，配置加载时校验。
- **历史接口的新鲜度**：写缓冲意味着最近 1 小时可能查不到；`/status.last_flushed_ts` 必须被客户端尊重。
- **uvicorn 在后台线程中的启动**：端口 0 必须从自有 socket 解析实际绑定端口；启动失败（例如端口占用或 lifespan startup 异常）要先清理线程、事件循环与 socket，再重抛原始异常，避免孤儿线程。`AsgiServer` 的端口 0 与失败回滚测试覆盖这一风险。
- **ASGI lifespan 与 `on_shutdown` 顺序**：`on_shutdown` 在 lifespan shutdown 阶段、应用自身 shutdown handler 之后运行；其异常被记录并吞掉，不能阻止后续拆除。hub 关闭顺序由 `GatewayRuntime` 端到端测试覆盖。
- **ASGI 流式响应**：请求中间件只能观察 response start，绝不能缓冲或重读 body；Arrow / CSV / Parquet 仍逐批发送，限流也只限制请求建立。
- **uvicorn 日志覆盖**：必须保持 `log_config=None`、`access_log=False`，否则 uvicorn 会在后台线程重配日志并压掉网关自己的结构化日志。

## 12. 已拒绝的方案（备查）

- 在网关内实现 NautilusTrader catalog/DataClient 适配。
- ~~Flask + gevent 或 FastAPI 等 Web 框架。~~ **已被 [ADR-0011](docs/adr/0011-fastapi-asgi-http-surface.md) 推翻**：HTTP 面现在同时包含 REST、WebSocket、运维指标与机器可读文档，手写 aiohttp 分发会让参数校验、错误映射、限流与可观测性在多个 handler 中重复；因此 HTTP 面统一迁移到 FastAPI + ASGI。原条目保留在此以示该决定是显式取代，而非静默删除。
- 多 Instrument 的时间游标分页：单个 `next_start` 无法同时表达每个 Instrument 的读取进度，取最小或最大 `open_ts` 都会导致重复或跳过。要让多 Instrument 分页正确，必须引入 `(open_ts, instrument_id)` 复合游标或每 Instrument 一份游标，并额外定义同一时间戳的稳定次序与客户端状态管理，复杂度不划算（[ADR-0012](docs/adr/0012-single-instrument-pagination.md)）。消费方应逐 Instrument 查询，或使用 Arrow/CSV 一次性下载后自行排序。
- Redis 作为总线或实时缓存。
- ZMQ 上使用 JSON / msgpack。
- 逐笔成交 / 盘口进入 v1（存储与复杂度超出日增 <10GB 的目标）。
- 客户端定时拉取（pull）式回放。
- 用 symbol 原始字符串直接做路径与主键。
- 按秒 flush 写盘（会造成小文件灾难）。
- 直接 `pyarrow.dataset` 扫描或对 Dataset 根目录 glob 作为生产读取路径：绕过 Manifest 权威，可能读到 Compaction 已失效的文件，也无法提供统一的 Snapshot/Read Lease 与 Arrow 流式 Reader（[ADR-0013](docs/adr/0013-manifest-aware-duckdb-reads.md)）。
- DuckDB 原生权威表、物化视图或第二份 Catalog：与不可变 Data File + Manifest 形成第二个真相源。
- 生产 `use_duckdb` 或旧读取引擎后备开关：会让不同部署产生两套可观察语义，回退应通过回退软件版本完成。
- Replay 在发布首条消息前全量物化，或把读取失败当作空回放/正常 exhausted（[ADR-0015](docs/adr/0015-bounded-replay-with-explicit-failure-termination.md)）。

## 13. 联网审查记录（2026-10-30）

审查后对本文档的修订：

1. **ccxt 方法名**：确认 `watch_trades_for_symbols` 与 `watch_bids_asks` 存在、不存在 `watch_bbo`；v1 只使用 `watch_ohlcv`。（来源：CCXT Pro Manual、CCXT 官方博客）
2. **历史补采上限**：`fetch_trades` 单次 1000 条，深历史需走归档；v1 改为 Bar 后，`fetch_ohlcv` 每次 1000 根，24h 仅约 2 次请求，成本问题消失。（来源：CCXT Binance Fetch All Trades、Binance Public Data）
3. **归档时间戳单位**：Binance 现货自 2025-01-01 起为微秒，此前为毫秒；规范化层需显式判定。（来源：binance/binance-public-data README）
4. **Parquet 文件大小**：100MB 属于下限，生产建议 128MB–1GB；已改为 64–256MB 目标、64MB/1 小时 flush、手动 compaction，并写入 `version="2.6"`（只有 2.6 原生支持 ns）。（来源：Apache Parquet dev list、Microsoft Fabric 指南、PyArrow `write_table` 签名）
5. **pandas 3 时间分辨率**：`timestamp[ns]` 经 `to_pandas()` 可能变为 `datetime64[us]`；`coerce_temporal_nanoseconds=True` 在 pandas 3 上可能导致越界或静默损坏，SDK 不启用它。（来源：pandas 3.0 release notes、PyArrow pandas 文档、SPARK-60499）
6. **Windows ZMQ ipc**：PyPI Windows wheel 默认禁用 `ipc://`，确认默认使用 `tcp://127.0.0.1`。（来源：pyzmq changelog / issue #1959）
7. **Arrow IPC 用法**：确认 `pa.ipc.new_stream` / `RecordBatchStreamWriter` 的流式写法；每条 ZMQ 消息封装为完整 IPC stream，并按 100ms/500 条批量发布以摊薄 schema 开销。（来源：Apache Arrow Python IPC 文档）
8. **分区布局**：所有层级改为 Hive `key=value` 形式；当时用于 `pyarrow.dataset` 裁剪，现在同一组分区字段由 Manifest Snapshot 在读取前使用。（来源：PyArrow dataset 分区约定）
9. **价格类型**：确认采用 `decimal128(38, 18)`，JSON 中 decimal 以字符串传输，CSV 输出十进制原文；pandas 侧为 `Decimal` 对象。（决策来源：用户确认，2026-10-30）
10. **数据粒度**：确认 v1 只做 Bar，不采集逐笔与盘口；1m 用交易所原生 K 线，高周期由 1m 聚合。接受无法做逐 Tick 回放与精确滑点建模的代价。（决策来源：用户确认，2026-10-30；依据：Nautilus 数据粒度文档、Binance K 线一致性讨论、NxCore tick vs minute 对比）
11. **术语标准化**：废弃 `Channel`、`Roll-up`、`Watermark`、`Pacing`、`Virtual Clock`、`Part File`、`Source`、`Contract` 等自造或冲突词，改用 Data Type、Bar Aggregation、High-Water Mark、Replay Mode、Replay Clock、Data File、Feed Handler、Data Contract。（依据：NautilusTrader、Apache Flink、Apache Iceberg、市场数据行业用法；映射见 GLOSSARY.md）
