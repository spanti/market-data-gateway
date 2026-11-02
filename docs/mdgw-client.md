# mdgw-client SDK

`mdgw_client` 是 Market Data Gateway 的薄客户端：查历史、订阅实时、控制 Replay
Session。它只依赖 `pyarrow`、`pyzmq` 与标准库；WebSocket 是可选依赖。SDK 不
import 任何 `mdgw` 内部模块，因此可以在独立进程、独立仓库或没有网关源码的机器
上使用。

## 安装

```bash
pip install -e .
# WebSocket 传输需要可选依赖（本仓库 extra 名为 client-ws）
pip install -e ".[client-ws]"
```

REST 历史查询使用标准库 `urllib.request`，不需要 `requests`/`aiohttp`。ZMQ 需要
`pyzmq`。WebSocket 需要 `websockets>=13`。若 SDK 以 `mdgw-client` 名称分发，对应
的 extra 写法是 `pip install "mdgw-client[ws]"`。

## 创建客户端

```python
from mdgw_client import GatewayClient

client = GatewayClient(
    "http://127.0.0.1:8080",
    stream_endpoint="tcp://127.0.0.1:5555",          # 实时 ZMQ PUB
    replay_publish_endpoint="tcp://127.0.0.1:5556",  # Replay PUB
    replay_control_endpoint="tcp://127.0.0.1:5557",  # Replay REQ/REP
)
# 或 with GatewayClient(...) as client:
```

`ws_url` 缺省从 `base_url` 推导：`http://host:port` → `ws://host:port/ws`，
`https://` → `wss://`。也可以显式传入。

`close()` 关闭所有订阅与 Replay 连接；推荐使用 context manager。

## 历史查询

```python
table = client.history(
    "1m",
    instrument="binance:BTC/USDT",
    start="2026-10-30T00:00:00Z",
    end="2026-10-30T01:00:00Z",
    limit=10_000,
)
```

- 返回 `pyarrow.Table`，schema 为 Data Contract v1 的 `BAR_SCHEMA`。
- `start`/`end` 接受 `datetime`、UTC 纳秒 `int` 或 ISO-8601 字符串；naive
  `datetime` 按 UTC 处理。
- 空结果返回带完整 schema 的空 Table。
- 请求固定使用 `GET /api/v1/bars?format=arrow`，不经过 JSON，decimal 与时间
  精度不丢失。
- HTTP 错误体会解析为 `ClientError`，保留服务端的 `code`、`message`、
  `details`、`request_id` 与 HTTP `status`。

### DataFrame 与 decimal

```python
frame = table.to_pandas()
assert frame["close"].dtype == object
```

pandas 没有原生 decimal dtype，因此所有价格/数量列（`open`、`high`、`low`、
`close`、`volume`）是 `object` dtype，元素为 `decimal.Decimal`。**不要**
`astype(float)`：这会丢失 `decimal128(38,18)` 的精度。需要高性能数值计算时，留在
Arrow 层，或按明确的量化规则转换。

### pandas 3 与时间分辨率

Arrow 中的时间字段是 `timestamp[ns, tz=UTC]`。`to_pandas()` 在 pandas 3 上可能
把纳秒列呈现为微秒（`datetime64[us, UTC]`）。SDK 不启用
`coerce_temporal_nanoseconds`：该选项在 pandas 3 上可能越界或损坏 1677 年以前的
数据。值本身仍是精确的 UTC 时间；对纳秒有硬性要求时直接使用 Arrow 列，例如
`table["event_ts"].cast(pa.int64())`。

## 实时订阅

```python
with client.subscribe(
    ["market.bar.1m.binance:BTC/USDT"],
    transport="auto",   # 有 stream_endpoint 用 zmq，否则用 ws
    queue_size=1000,
) as live:
    for event in live:
        if event.kind == "bar":
            print(event.topic, event.table.num_rows)
        elif event.kind == "lag_notice":
            print("dropped", event.lag_notice["dropped"])
```

- `transport="zmq"`：`[topic][Arrow IPC stream]` 两帧消息，解码为
  `StreamEvent.table`。`_internal.ready` 会被跳过。
- `transport="ws"`：发送 `{"op":"subscribe","topics":[...]}`，再发送
  `{"op":"ping"}` 并等待 `pong`，确认订阅已被服务端处理后才返回。`event`
  消息用 SDK 自带的 schema 解码；`lag_notice` 产出
  `StreamEvent(kind="lag_notice")`。
- `StreamEvent` 字段：`topic`、`table`、`kind`（`"bar"` / `"lag_notice"`）、
  `data`（原始 envelope 或 lag 消息）、`lag_notice`。
- `next(timeout=...)` 在超时时抛 `ClientError(code="timeout")`；
  `close()` 后迭代结束。

### topic 前缀语义

ZMQ 与 WebSocket 都按**前缀**匹配 topic：

- `market.bar.1m.binance:BTC/USDT`：精确 topic。
- `market.bar.1m.binance:`：该 interval 下所有 `binance:` 标的。
- `market.bar.`：所有 Bar。

### 重连

- ZMQ 连接本身会自动重连；SDK 在 `ZMQError` 时重建 socket、重新订阅，并可通过
  `reconnect()` 手动触发、`reconnect_count` 观察次数。
- WebSocket 在连接关闭或异常时按 `reconnect_backoff_s` 退避（默认
  `(0.1, 0.2, 0.5, 1.0, 2.0, 5.0)`），重连后重新发送 subscribe + ping。
- 订阅使用有界 `queue.Queue`：消费慢时对接收线程形成背压，不静默丢数据。

## Replay Session

```python
session = client.replay.create_session(
    subscriptions=[
        {"data_type": "bar", "instrument_id": "binance:BTC/USDT", "interval": "1m"}
    ],
    start_ts="2026-10-30T00:00:00Z",
    end_ts="2026-10-30T01:00:00Z",
    replay_mode="realtime",   # asap | realtime | step
    speed=1.0,
)
print(session.session_id, session.publish_endpoint)

session.pause()             # realtime / step
session.seek("2026-10-30T00:30:00Z")  # 仅暂停时
session.set_speed(2.0)      # 仅 realtime
session.step(2)             # 仅 step：授予 2 个 Step Credit
session.resume()            # 仅 realtime

for event in session:       # 直到 End of Session
    bar = event.table
print(session.end_of_session)
# {"op": "end_of_session", "reason": "exhausted"|"stopped", "bars_sent": N, ...}
```

`create_session` 会先建立 Replay SUB 并等待 `_internal.ready`（Replay Server 每
50 ms 发送一次），确认订阅已转发后才发送 create 请求，避免 asap 模式漏掉开头的
事件。每个 Replay Session 有独立队列，`next_event(timeout=...)` 只返回本会话的
事件；多个会话可以独立消费，互不吞事件。

三种 Replay Mode：

- `asap`：全速推送，不节流。
- `realtime`：按 `speed` 节流；支持 `pause` / `resume` / `seek` / `set_speed`。
- `step`：创建即暂停；每次 `step(n)` 最多推送 n 条。最后一条推送后自动发送
  End of Session。

`seek(ts)` 只允许在暂停状态，且必须在 `[start_ts, end_ts]` 内；越界返回
`ClientError(code="invalid_range")`。**向后 seek 会重新发送已经发送过的 Bar**，
消费方需要自行按 `revision` / `open_ts` 去重。

`session.close()` 会停止未完成的会话并释放该会话的队列；共享的 Replay SUB 在
最后一个会话释放后才关闭。context manager 退出时执行 `stop()` + `close()`。

## 错误模型

```python
from mdgw_client import ClientError

try:
    client.history("1m")
except ClientError as exc:
    print(exc.code, exc.message, exc.status, exc.details, exc.request_id)
```

- REST/Replay 控制返回的服务端错误保留原 `code`。
- 网络失败：`code="connection_error"`；超时：`code="timeout"`。
- 未安装 `websockets` 却使用 `transport="ws"`：`code="ws_unavailable"`。
  安装：`pip install 'market-data-gateway[client-ws]'`（extra 名为 `client-ws`）。
- 缺少 Replay endpoint 时访问 `client.replay`：`code="replay_unavailable"`。

## 已知限制

- **ZMQ 静默断线不可检测**：对端消失时 SUB socket 可能不报错、也不重连；SDK
  只能处理 `ZMQError` 与显式 `reconnect()`。需要强可靠的部署应配合心跳或
  WebSocket。
- **Replay 预订阅要等 ready**：`create_session` 会等 `_internal.ready`；若 Replay
  Server 不可达，会在 `timeout_s` 后报错。
- **每会话队列有界**：每个 Replay Session 的队列默认 `queue_size=1000`；消费过慢
  时会丢弃最旧的 Bar 并计入 `session.dropped`。End of Session 永不丢弃。
- **WebSocket 依赖可选**：基础安装只保证 ZMQ/REST/Replay。
- **pandas 时间分辨率**：pandas 3 可能把 ns 呈现为 µs；Arrow 值仍精确。
- **向后 seek 会重发**：同一 Bar 可能被消费多次。
- 历史接口只读已落盘的数据；写缓冲内的最新 Bar 可能还查不到。
