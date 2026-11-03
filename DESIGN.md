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
Feed Handler(1m klines) ──▶ Normalize ──▶ Bar Aggregation ──▶ Parquet (data files)
                                                  │
                                                  ├──▶ ZMQ PUB (market.bar.*) ──▶ api ──▶ WS / ZMQ 消费者
                                                  └──▶ HWM (SQLite)

Parquet ──▶ replay (Replay Clock) ──▶ ZMQ PUB (replay.<session>.*)
Parquet ──▶ api REST history ──▶ JSON / Arrow IPC / CSV
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

- **所有层级统一为 Hive 风格 `key=value`**，使 `pyarrow.dataset` 能按分区裁剪；`bars` 是 Dataset 根。
- `file_symbol` = URL 百分号编码后的 `symbol`，例如 `BTC%2FUSDT`；`instrument_id` 始终保存在行内。
- 每个 Dataset 根目录放 `_dataset.json`（`schema_version`、Arrow schema 的 JSON 表示、分区规范、文件命名规范）与 `_manifest.json`（当前活跃的 Data File 清单）。
- Data File：`data-<start_ns>-<end_ns>-<uuid>.parquet`，**一经写入内容不变**；compaction 通过写入新文件并把旧文件标记为 inactive 来实现（见 §5.5）。
- 写入参数：`version="2.6"`（只有 2.6 原生支持 ns 时间戳；1.0/2.4 会截断为 µs）、`use_deprecated_int96_timestamps=False`、`compression="zstd"`、`row_group_size=1_000_000`、单文件目标 **64–256MB**（Bar 体量小，通常由 compaction 达成）。
- 读取一律走 `pyarrow.dataset`，按 `instrument_id`/时间范围下推过滤。
- 读取方若 `schema_version` 不匹配则**直接报错**（`schema_version_mismatch`），不猜测、不降级。
- 分区按 `open_ts` 的 UTC 年月。

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
- **排序**：所有订阅按 `close_ts`（即 envelope 的 `event_ts`）归并；同一时刻以 `instrument_id`、再以 `seq` 定序。
- 数据耗尽推送一次 End of Session（控制消息或专用 topic `replay.<session_id>.control`）。
- `seek` 只允许在 `pause` 状态下执行。
- 同一进程支持多个会话（默认上限 4），会话之间互不影响。

## 4. HTTP 与 WebSocket API

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
- 分页只用时间游标，不支持 offset。
- `format=arrow`：流式 Arrow IPC（decimal128 原样）；`format=csv`：流式 CSV（decimal 输出十进制原文，禁止经过 float）；`format=json`：仅允许小范围，超过 100,000 行返回 `range_too_large`，decimal 以字符串输出。
- `format=arrow|csv` 无行数上限，边读边发，不把结果全量放进内存。
- `export` 把同一读取器的结果落成一个 Parquet/Arrow 文件返回（一次性大文件下载）。
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

稳定错误码：`invalid_instrument`、`invalid_interval`、`invalid_parameter`、`invalid_range`、`range_too_large`、`schema_version_mismatch`、`dataset_not_found`、`session_not_found`、`feed_unavailable`、`internal_error`。
HTTP 状态码只做粗分类（400/404/409/429/500）。

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
- 回放会话在 `_sessions/` 写心跳锁；GC 跳过有活跃锁的 Dataset。
- GC 不影响 Instrument Reference Data 与 HWM。

### 5.5 写入批次与 Compaction

- 写盘按 `flush_max_bytes=64MB` **或** `flush_max_age_s=3600`（先到者）落一个 Data File。Bar 体量小，1 小时 flush 是为了控制历史接口的新鲜度；实时消费者走 WS/ZMQ。
- `mdgw compact --older-than 24h` 把同分区内的小 Data File 合并为 64–256MB 的新文件，更新 `_manifest.json` 将旧文件标记 inactive，再删除旧文件。Compaction 只做"新文件 + 清单切换"，不修改任何已存在的文件。
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

## 6. 配置

`config/gateway.toml`（运行配置）示例：

```toml
[server]
host = "127.0.0.1"
http_port = 8080
ws_path = "/ws"

[bus]
transport = "tcp"        # tcp | ipc（ipc 仅 Linux 默认启用）
bind_host = "127.0.0.1"
base_port = 5555
zmq_hwm = 10000

[storage]
root = "./data"
flush_max_bytes = 67108864     # 64MB，先到者触发
flush_max_age_s = 3600         # 1 小时
row_group_size = 1000000
compression = "zstd"
format_version = "2.6"         # 只有 2.6 原生支持 ns 时间戳
schema_version = 1

[retention]
enabled = false
bars_days = 0            # 0 = 永久

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
max_sessions = 4
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
    store/         # Parquet 读写、_dataset.json/_manifest.json、HWM、GC、compaction
    bus/           # ZMQ PUB/SUB + REQ/REP、Arrow/JSON 编解码
    replay/
    api/           # REST、WebSocket、metrics
    cli/           # run --service / up / gc / export / compact / backfill --archive
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
- 运行时依赖：`pyarrow`、`pyzmq`、`ccxt`、`aiohttp`（仅作 HTTP/WS 传输层，不用其框架特性）、`msgspec`/`orjson`、`PyYAML`。
- **禁止**网关包 import `nautilus_trader`（CI 用 AST 扫描或 import-linter 强制）。
- `mdgw_client` 只依赖 `pyarrow`、`pyzmq` 与 schema 定义。
- decimal128 的 pandas 表示：`to_pandas()` 返回 `object` dtype 的 `decimal.Decimal`（pandas 无原生 decimal dtype）；SDK 文档必须写明，避免使用者误用 `astype(float)`。需要高性能数值计算的使用者应留在 Arrow 层或显式量化。

## 9. 测试与验收

### 9.1 测试缝（Seams）

仓库从零开始，无既有 seam。按"尽可能高、尽可能少"的原则，确定 **三个**：

1. **`GatewayRuntime` 端到端（主 seam）**：组合根接收配置、一个实现 `FeedHandler` Protocol 的假数据源、一个临时数据目录，在单进程内用 `inproc://` ZMQ 跑起 collector → Parquet → api → replay。所有行为测试从这里进入。
2. **线上字节（Data Contract seam）**：Arrow schema 黄金文件、Arrow IPC 往返、`_dataset.json`/`_manifest.json` 校验、topic 与路径编码。它不是行为 seam，而是 Data Contract 夹具。
3. **`HttpServer` 与 `ApiService` 的公共接口（HTTP 生命周期 seam）**：刻意新增的第三个 seam。用真实 socket 与端口 0 直接验证启停、失败回滚与跨线程派发，不经过完整的 feed → collector → api → replay 链路；全链路行为仍从 `GatewayRuntime` 进入。

真实 Binance 只出现在 `@pytest.mark.network` 的测试里，默认跳过。

### 9.2 测试内容

- Data Contract 测试：Arrow schema 黄金文件；Arrow IPC 与 JSON 编解码往返；Parquet 目录布局；`_dataset.json`/`_manifest.json` 校验。
- 单元测试：1m 收口、Bar Aggregation、revision、HWM 推进、topic/路径编码、错误码映射、decimal 不经浮点。
- 集成测试：假 Feed Handler → collector → Parquet → api → replay 全链路，不触网。
- HTTP 生命周期测试：`HttpServer` 与 `ApiService` 在端口 0 上用真实 socket 验证启动、幂等、停止、失败回滚与跨线程派发；不构造完整链路。
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
- **Bar Revision 的下游幂等**：接入方必须按最大 revision 去重，文档需显著提示。
- **Retention 与补采的边界**：REST 回溯上限必须 ≤ Retention，配置加载时校验。
- **历史接口的新鲜度**：写缓冲意味着最近 1 小时可能查不到；`/status.last_flushed_ts` 必须被客户端尊重。

## 12. 已拒绝的方案（备查）

- 在网关内实现 NautilusTrader catalog/DataClient 适配。
- Flask + gevent 或 FastAPI 等 Web 框架。
- Redis 作为总线或实时缓存。
- ZMQ 上使用 JSON / msgpack。
- 逐笔成交 / 盘口进入 v1（存储与复杂度超出日增 <10GB 的目标）。
- 客户端定时拉取（pull）式回放。
- 用 symbol 原始字符串直接做路径与主键。
- 按秒 flush 写盘（会造成小文件灾难）。

## 13. 联网审查记录（2026-10-30）

审查后对本文档的修订：

1. **ccxt 方法名**：确认 `watch_trades_for_symbols` 与 `watch_bids_asks` 存在、不存在 `watch_bbo`；v1 只使用 `watch_ohlcv`。（来源：CCXT Pro Manual、CCXT 官方博客）
2. **历史补采上限**：`fetch_trades` 单次 1000 条，深历史需走归档；v1 改为 Bar 后，`fetch_ohlcv` 每次 1000 根，24h 仅约 2 次请求，成本问题消失。（来源：CCXT Binance Fetch All Trades、Binance Public Data）
3. **归档时间戳单位**：Binance 现货自 2025-01-01 起为微秒，此前为毫秒；规范化层需显式判定。（来源：binance/binance-public-data README）
4. **Parquet 文件大小**：100MB 属于下限，生产建议 128MB–1GB；已改为 64–256MB 目标、64MB/1 小时 flush、手动 compaction，并写入 `version="2.6"`（只有 2.6 原生支持 ns）。（来源：Apache Parquet dev list、Microsoft Fabric 指南、PyArrow `write_table` 签名）
5. **pandas 3 时间分辨率**：`timestamp[ns]` 经 `to_pandas()` 可能变为 `datetime64[us]`；`coerce_temporal_nanoseconds=True` 在 pandas 3 上可能导致越界或静默损坏，SDK 不启用它。（来源：pandas 3.0 release notes、PyArrow pandas 文档、SPARK-60499）
6. **Windows ZMQ ipc**：PyPI Windows wheel 默认禁用 `ipc://`，确认默认使用 `tcp://127.0.0.1`。（来源：pyzmq changelog / issue #1959）
7. **Arrow IPC 用法**：确认 `pa.ipc.new_stream` / `RecordBatchStreamWriter` 的流式写法；每条 ZMQ 消息封装为完整 IPC stream，并按 100ms/500 条批量发布以摊薄 schema 开销。（来源：Apache Arrow Python IPC 文档）
8. **分区布局**：为让 `pyarrow.dataset` 正确裁剪，所有层级改为 Hive `key=value` 形式。（来源：PyArrow dataset 分区约定）
9. **价格类型**：确认采用 `decimal128(38, 18)`，JSON 中 decimal 以字符串传输，CSV 输出十进制原文；pandas 侧为 `Decimal` 对象。（决策来源：用户确认，2026-10-30）
10. **数据粒度**：确认 v1 只做 Bar，不采集逐笔与盘口；1m 用交易所原生 K 线，高周期由 1m 聚合。接受无法做逐 Tick 回放与精确滑点建模的代价。（决策来源：用户确认，2026-10-30；依据：Nautilus 数据粒度文档、Binance K 线一致性讨论、NxCore tick vs minute 对比）
11. **术语标准化**：废弃 `Channel`、`Roll-up`、`Watermark`、`Pacing`、`Virtual Clock`、`Part File`、`Source`、`Contract` 等自造或冲突词，改用 Data Type、Bar Aggregation、High-Water Mark、Replay Mode、Replay Clock、Data File、Feed Handler、Data Contract。（依据：NautilusTrader、Apache Flink、Apache Iceberg、市场数据行业用法；映射见 GLOSSARY.md）
