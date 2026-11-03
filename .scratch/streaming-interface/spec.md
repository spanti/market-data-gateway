# Spec: 一个深模块拥有 Streaming Interface

Status: ready-for-agent

## Problem Statement

网关的 Streaming Interface 目前没有单一的拥有者。topic 语法、Arrow IPC 编解码、READY 握手与发布/订阅循环散落在四个模块：

- `GatewayRuntime` 与 `CollectorService` 各自实现 `_publish_bars`：按 `(interval, instrument_id)` 分组、拼 topic、编码 Arrow、发布；
- `ApiService`、`GatewayRuntime`、`CollectorService` 各自实现 READY 认识与发布循环；`ReplayServer` 又实现第四套；
- `MARKET_BAR_TOPIC_PREFIX` 在 `runtime.py`、`service/api.py`、`service/collector.py` 各声明一次，Replay 在 `session.py` 里内联拼 `replay.<session_id>.market.bar.…`；
- `_internal.status` 在 collector 与 api 各拼一次；
- Arrow 编解码函数放在 `bus/zmq_bus.py`，于是每个生产者为了编码都要依赖传输模块；
- `ReplayServer` 不复用 `ZMQBus`，自己创建 `zmq.Context`、PUB、READY 线程，并重复 `SNDHWM = 10_000`。

结果是：改变任何线上约定（topic、READY 语义、帧格式、错误策略）都要在多个模块间对照修改；READY 握手同时以“返回的 Event”和“订阅 READY topic 的 handler”两种可观察形式存在；topic 语法与就绪行为没有直接测试，只能通过 `GatewayRuntime` 的端到端测试间接覆盖；`ZMQBus` 被当作 Streaming Interface 使用，但它并不拥有 topic 语法或 Bar 语义。

## Solution

引入一个深模块 `mdgw.streaming`，公开 `StreamingInterface`，把 Streaming Interface 的全部实现收进一个小接口后面：

- 拥有 Bar topic 语法、Arrow IPC 帧编解码、Bar 发布/订阅、READY 握手、`_internal.status` 诊断通道，以及 Replay 的 `{namespace}.control` 事件通道；
- 拥有一个 `zmq.Context`、一个 PUB、一个 SUB 与订阅线程；不感知 aiohttp、`WebSocketHub` 或任何 Service Role；
- 调用方只学习 `start`、`stop`、`publish_bars`、`subscribe_bars`、`publish_status`、`subscribe_status`、`mark_ready`、`wait_until_peer_ready`、`peer_ready` 与计数器；
- `ReplayServer` 用 `namespace="replay.<session_id>"` 复用同一模块发布 Bar 与 End of Session，REQ/REP 控制协议仍留在它自己的控制循环里；
- 迁移完成后删除 `mdgw/bus/zmq_bus.py`，`table_to_ipc`、`ipc_to_table` 与 `READY_TOPIC` 由 `mdgw.streaming` 导出。

线上行为完全不变：topic 字符串、Arrow schema、JSON Message Envelope、配置字段、Replay Mode 语义与 REST/WS 接口均不改动。

## User Stories

1. 作为网关维护者，我想要一个模块拥有 Streaming Interface 的线上约定，这样修改 topic、帧格式或 READY 语义时只改一处。
2. 作为网关维护者，我想要 Bar topic 只由一个函数生成，这样 collector、api、runtime、replay 不会再各自拼出略有差异的字符串。
3. 作为网关维护者，我想要 READY 握手只有一种实现与一种可观察形式，这样 `peer_ready` 与等待逻辑不会互相矛盾。
4. 作为网关维护者，我想要 Arrow IPC 编解码属于 Streaming Interface 而不是 ZMQ 传输模块，这样生产者不必为了编码而 import 总线。
5. 作为网关维护者，我想要 `_internal.status` 是一个类型化通道，这样 collector 与 api 不再各自拼 topic 与处理 JSON 解析失败。
6. 作为网关维护者，我想要 Replay 用同一套 publisher 与 topic builder 发布 Bar 和 End of Session，这样 `ReplayServer` 不再重复 PUB/READY 实现。
7. 作为网关维护者，我想要删除 `ZMQBus`，这样 `StreamingInterface` 不会退化成旧总线的转发壳。
8. 作为网关维护者，我想要 Streaming 模块不 import HTTP/WebSocket 代码，这样它可以用真实 ZMQ 在无 aiohttp 的测试里验证。
9. 作为测试作者，我想要直接测试 topic 语法与 namespace，这样不需要启动完整网关。
10. 作为测试作者，我想要用 `inproc://` 测试 READY 握手、超时与 `await_ready`，这样这些分支不再只能通过端到端测试覆盖。
11. 作为测试作者，我想要直接测试 malformed Bar 帧、malformed status JSON、handler 异常与计数器，这样错误策略有明确的行为断言。
12. 作为测试作者，我想要测试 `forward=True` 逐字节转发，这样 api 的中继路径不依赖解码再编码。
13. 作为 api Service Role 维护者，我想要 `subscribe_bars(handler, forward=True)` 把收到的原帧转发到本实例 PUB，这样外部消费者不依赖 collector 地址，也不增加一次重编码。
14. 作为 api Service Role 维护者，我想要 `peer_ready` 非阻塞属性，这样 `/readyz` 与 `/status` 可以在每次请求时判断 collector 是否就绪。
15. 作为 collector Service Role 维护者，我想要 `wait_until_peer_ready(timeout)` 返回 bool，这样“超时只告警、继续 ingest”的策略仍留在角色里。
16. 作为 runtime 维护者，我想要 `subscribe_bars(await_ready=True)` 等自己的 inproc 订阅生效，这样没有 peer 的环境也能安全启动。
17. 作为 replay 维护者，我想要 Session 只调用 `publish_bar(bar)` 与 `publish_event(mapping)`，这样 topic 与 IPC 构造不再出现在 `session.py`。
18. 作为运维人员，我想要 `market.bar.*`、`_internal.ready`、`_internal.status` 与 `replay.<id>.control` 的字符串与语义保持不变，这样现有消费者无需改动。
19. 作为 SDK 维护者，我想要 `mdgw_client` 保持独立，并有一个契约测试锁定它与服务端相同的 topic 字符串，这样客户端不必 import 服务端内部模块。
20. 作为 AFK 实现代理，我想要迁移拆成可独立验证的提交，这样每个提交都能单独 review 与回退。
21. 作为后续架构工作的维护者，我想要本次只处理 Streaming Interface，不顺手修改 ApiSurface、Service Role 生命周期、持久化/HWM 或其他候选，这样评审范围保持单一。

## Implementation Decisions

### 新模块与导出

- 新建模块 `src/mdgw/streaming.py`，公开类 `StreamingInterface`。
- 公开导出仅限：`StreamingInterface`、`bar_topic`、`bar_topic_prefix`、`table_to_ipc`、`ipc_to_table`、`READY_TOPIC`。
- 模块内部拥有 topic 常量、socket、订阅记录、线程与计数器；不导出 `ZMQBus`。
- 不 import `mdgw.api`、`mdgw.service` 或 `mdgw.replay`；只依赖 `mdgw.core`。

### 接口

```python
MARKET_BAR_TOPIC_PREFIX = "market.bar."
STATUS_TOPIC = "_internal.status"
READY_TOPIC = "_internal.ready"

def bar_topic(bar: Bar, *, namespace: str = "") -> str
def bar_topic_prefix(*, interval: str | None = None,
                     instrument_id: str | None = None,
                     namespace: str = "") -> str
def table_to_ipc(table: pa.Table) -> bytes
def ipc_to_table(payload: bytes) -> pa.Table

class StreamingInterface:
    def __init__(self, *, bind: str | Iterable[str],
                 connect: str | None = None,
                 namespace: str = "",
                 zmq_hwm: int = 10_000) -> None

    def start(self) -> None
    def stop(self) -> None

    @property
    def endpoints(self) -> tuple[str, ...]
    @property
    def peer_ready(self) -> bool
    @property
    def published_frames(self) -> int
    @property
    def received_frames(self) -> int
    @property
    def decode_errors(self) -> int
    @property
    def handler_errors(self) -> int

    def publish_bars(self, bars: Sequence[Bar]) -> None
    def publish_status(self, status: Mapping[str, Any]) -> None
    def publish_event(self, event: Mapping[str, Any]) -> None
    def mark_ready(self) -> None
    def wait_until_peer_ready(self, timeout: float | None = None) -> bool

    def subscribe_bars(self, handler: Callable[[str, pa.Table], None], *,
                       await_ready: bool = False,
                       forward: bool = False) -> None
    def unsubscribe_bars(self) -> bool
    def subscribe_status(self, handler: Callable[[dict[str, Any]], None]) -> None
```

### Topic 语法与 namespace

- Bar topic 保持 `market.bar.<interval>.<instrument_id>`。
- `namespace` 以“段”为单位前置：`namespace="replay.rpl-abc"` 得到 `replay.rpl-abc.market.bar.1m.binance:BTC/USDT`；模块负责补分隔符，调用方不传尾随点。
- `_internal.status` 与 `_internal.ready` 永远是全局 topic，不受 namespace 影响。
- `publish_event` 发布到 `{namespace}.control`；namespace 为空时抛 `ValueError`。
- `bar_topic` 与 `bar_topic_prefix` 接受与构造器相同的 `namespace` 关键字，测试与客户端契约测试可以锁定两种形式。

### 生命周期与线程

- `start()` 幂等：创建 context、绑定 PUB 到全部 `bind` endpoint、在 `connect` 存在时连接 SUB、启动订阅线程；返回后 `endpoints` 为解析后的绑定 endpoint（按绑定顺序）。
- `stop()` 幂等：停止 READY 心跳、结束订阅线程、关闭 socket、`term()` context；`stop()` 之后 `publish_*` 为 no-op。
- 启动失败时完整清理 context、socket 与线程，再重新抛出原始异常。
- 实例是单次使用的：`start()` 在 `stop()` 之后抛 `RuntimeError`。
- `publish_*`、`subscribe_*`、`mark_ready`、`wait_until_peer_ready` 在 `start()` 之前抛 `RuntimeError`；`peer_ready` 在启动前与停止后为 `False`。
- 发布使用模块内锁；多个 Replay Session 线程可以并发调用 `publish_bars`。

### 发布

- `publish_bars(Sequence[Bar]) -> None`：按 `(interval, instrument_id)` 分组、构造逻辑表、编码 IPC、逐 topic 发送；返回 None。
- `publish_status(Mapping) -> None`：以 `json.dumps(..., ensure_ascii=False, default=str)` 编码到 `_internal.status`。
- `publish_event(Mapping) -> None`：同编码规则，发布到 `{namespace}.control`；仅 Replay 使用。
- 发布在 `start()` 之前抛 `RuntimeError`；在 `stop()` 之后静默返回，保证关停竞态不产生异常。

### 订阅与转发

- 每个实例最多一个活动 Bar 订阅与一个活动 status 订阅；重复 `subscribe_*` 抛 `RuntimeError`。
- `subscribe_bars` 的 handler 在订阅线程上执行，签名 `(topic, pa.Table)`；订阅覆盖全部 Bar topic，不提供 prefix 过滤。
- `subscribe_bars(..., forward=True)` 在调用 handler 之前，把收到的原帧逐字节转发到本实例 PUB；转发失败（关停竞态）被抑制，handler 仍执行。
- `unsubscribe_bars() -> bool`：有活动订阅时移除并返回 `True`，否则返回 `False`；SUB 连接保持、READY 状态不受影响、允许再次 `subscribe_bars`。
- 不提供 `unsubscribe_status`；status 订阅持续到 `stop()`。
- handler 抛异常时记录日志、`handler_errors` 加一，订阅线程继续运行。

### 就绪

- `mark_ready()` 幂等：启动 50 ms READY 心跳，发布到本实例 PUB；心跳持续到 `stop()`，与订阅状态无关。
- `peer_ready` 在 `connect` 上首次收到 READY 后为 `True`，运行期间保持；无 `connect` 时始终为 `False`。
- `wait_until_peer_ready(timeout)`：已就绪或超时内就绪返回 `True`；超时或无 `connect` 返回 `False`；`start()` 之前抛 `RuntimeError`。
- `subscribe_bars(await_ready=True)` 使用内部 READY ping（与旧 `wait_for_subscriber` 相同）等待自己的 SUB 生效；固定 2.0 秒超时，超时抛 `TimeoutError`；不要求先调用 `mark_ready()`。

### 状态、事件与失败语义

- `subscribe_status` 的 handler 收到解码后的 `dict`；JSON 解析失败被记录、`decode_errors` 加一并丢弃。
- `subscribe_bars` 的 Arrow 解码失败被记录、`decode_errors` 加一并丢弃。
- READY 帧由模块内部消费，不进入 handler，也不计入 `published_frames` / `received_frames`。
- 计数器从实例创建起单调递增、线程安全：`published_frames`（发出的 Bar 帧，含转发）、`received_frames`（收到的 Bar 帧）、`decode_errors`、`handler_errors`。status、READY、event 帧不计入前两个。

### Replay 发布接缝

- `ReplaySession` 与 `ReplayService` 的发布回调改为类型化：
  - `publish_bar(bar: Bar) -> None`
  - `publish_event(event: Mapping[str, Any]) -> None`
- `ReplaySession` 不再构造 topic 或 IPC 帧；`ReplayServer` 把两个回调接到 `StreamingInterface`。
- `GatewayRuntime.create_replay_session(publish=...)` 的测试接缝同步改为这两个回调；测试仍可观察同一份帧。
- `ReplayServer` 保留自己的 REP 控制 socket 与控制循环；它可以使用自己的 `zmq.Context`，不与数据 PUB 共享。

### 调用方迁移

| Service Role / 接缝 | `bind` | `connect` | `namespace` | 行为 |
|---|---|---|---|---|
| collector | `collector_pub_bind` | `api_pub_connect` | — | 发布 Bar + status；`mark_ready()`；按配置 `wait_until_peer_ready(api_ready_timeout_s)` |
| api | `api_pub_bind` | `collector_pub_connect` | — | `subscribe_bars(handler, forward=True)`；`subscribe_status`；收到首帧后 `mark_ready()`；`_collector_is_ready()` = `peer_ready or received_frames > 0` |
| runtime | `[inproc, tcp stream endpoint]` | `inproc` | — | `subscribe_bars(..., await_ready=True)`；发布 Bar；`stream_endpoint = endpoints[-1]` |
| replay | replay publish endpoint | — | `replay.<session_id>` | 发布 Bar + event；`mark_ready()`；REP 控制仍在 `ReplayServer` |

- `GatewayRuntime`、`CollectorService`、`ApiService` 不再声明 `MARKET_BAR_TOPIC_PREFIX` 或 `READY_INTERVAL_S`。
- `ApiService` 的 `publish_endpoint` 与 `GatewayRuntime.stream_endpoint` 委托给 `endpoints`。
- REST/WS 层保留 `call_soon_threadsafe(hub.publish)` 的线程交接；Streaming 模块不感知 hub。
- `CollectorService` 删除 `_api_ready`、`_api_ready_signal`、`_run_ready_loop`；`ApiService` 删除 `_ready_event`、`_run_ready_loop`。

### 契约与文档

- 不修改 `GLOSSARY.md`：`Streaming Interface` 已是规范术语，本次只是让一个模块拥有它。
- 不新增 ADR。ADR-0003（ZMQ 唯一总线）与 ADR-0004（ZMQ 上 Arrow、WS 上 JSON）不受影响：线上格式不变，只是拥有者改变。
- 不新增配置字段；`zmq_hwm`、endpoint、namespace 都由现有配置或调用方传入。

### 迁移顺序

1. 新增 `mdgw.streaming` 与其单元/契约测试；`bus/zmq_bus.py` 暂时保留并 re-export 编解码与 `READY_TOPIC`，避免两份实现；不迁移调用方。
2. 迁移 collector；多进程与 collector 单元测试保持通过。
3. 迁移 api；streaming / bars / status 集成测试保持通过。
4. 迁移 `GatewayRuntime`；streaming / bars / status 集成测试保持通过。
5. Replay 发布接缝改为类型化回调（prefactor）；Replay 行为与测试不变。
6. Replay 组合 `StreamingInterface`（namespace、Bar、End of Session、READY），删除 `mdgw/bus/`，更新剩余 import；完整离线套件无新增失败。

## Testing Decisions

### 好测试的标准

- 只断言公共接口行为：topic 字符串、收到的帧、`peer_ready`、计数器、抛出的错误类型、转发字节。
- 不断言私有字段（`_context`、`_publisher`、`_subscribers`、线程名之外的内部结构）。
- 使用真实 `inproc://` ZMQ 与端口 0；不引入传输假实现（单实现不构成接缝）。
- 模块测试不启动 aiohttp；HTTP/WS 行为继续由既有 `GatewayRuntime` 端到端测试守护。

### 接缝

- **新增接缝**：`StreamingInterface` 的公共接口。至少覆盖：
  1. `bar_topic` / `bar_topic_prefix` 的基础形式与 namespace 形式；
  2. PUB 绑定端口 0 后 `endpoints` 返回解析后的 endpoint；
  3. inproc 上 `publish_bars` → `subscribe_bars` 的 Arrow 往返；
  4. `publish_status` → `subscribe_status` 的 dict 往返；
  5. `subscribe_bars(await_ready=True)` 等待自己的 SUB 生效；
  6. `mark_ready()` 后 `peer_ready` 在另一实例上变真，`wait_until_peer_ready` 返回 `True`；
  7. 无 peer 时 `wait_until_peer_ready(0.01)` 返回 `False`；
  8. 重复 `subscribe_bars` / `subscribe_status` 抛 `RuntimeError`；
  9. `unsubscribe_bars()` 的 `True` / `False` 与再次订阅；
  10. `forward=True` 在本实例 PUB 上逐字节转发原帧；
  11. malformed Bar 帧与 malformed status JSON 被丢弃并计入 `decode_errors`；
  12. handler 抛异常后订阅线程继续、`handler_errors` 增加；
  13. `publish_*` 在 `start()` 前抛 `RuntimeError`、`stop()` 后为 no-op；
  14. `publish_event` 在空 namespace 上抛 `ValueError`；
  15. `stop()` 幂等且释放端口，`start()` 在停止后抛 `RuntimeError`。
- **既有接缝**：`tests/integration/test_streaming_interface.py` 继续断言外部 ZMQ/WS 行为，仅更新 `ipc_to_table` 的 import。
- **既有接缝**：`tests/integration/test_multiprocess_services.py` 继续断言 collector/api/replay 的跨进程 topic 与 READY 行为；只更新 `READY_TOPIC` import。
- **既有多进程/回放测试**：`test_replay_asap.py`、`test_replay_controls.py`、`test_client_live.py` 继续守护外部行为。
- **客户端契约**：新增一个 parity 测试，断言 `mdgw_client` 的 topic 字符串解析与 `READY_TOPIC` 与服务端 `bar_topic` / `READY_TOPIC` 一致；客户端仍不 import `mdgw.streaming`。

### 测试改动

- 新增 `StreamingInterface` 的单元/契约测试模块；用 `inproc://` 与端口 0，不依赖 `GatewayRuntime`。
- 将 `tests/contract/test_zmq_arrow_ipc.py` 从 `ZMQBus` 改写为 `StreamingInterface`，保留 Arrow 往返断言。
- 更新 `tests/contract/test_data_contract.py` 的编解码 import。
- 更新所有引用 `READY_TOPIC` / `ipc_to_table` / `ZMQBus` 的测试 import；不改变其断言语义。
- 新增客户端 parity 契约测试。

### 基线说明

提交前先运行当前离线测试套件记录基线（`pytest -m 'not network'`）。本次以“无新增失败”为验收标准；既有的 Typer/Click 退出码差异不在本次范围内。

### 既有测试先例

- `tests/contract/test_zmq_arrow_ipc.py`：inproc Arrow 往返与 READY 先例。
- `tests/integration/test_streaming_interface.py`：外部 ZMQ/WS 行为与慢加入重试。
- `tests/integration/test_multiprocess_services.py`：真实 Service Role 进程、topic 与 READY 探测。
- `tests/integration/test_replay_asap.py` / `test_replay_controls.py`：Replay 数据帧、End of Session 与控制命令。
- `tests/integration/test_client_live.py`：客户端 ZMQ/WS 订阅与 READY 先例。
- `tests/unit/test_replay_clock.py`：通过依赖注入测试时间行为，说明可注入接缝的价值。

## Out of Scope

- `ApiSurface` / REST/WS 组合、`create_app` 参数形状、`WebSocketHub` 的队列与 lag notice。
- Service Role 生命周期与可观测性聚合、`/healthz` / `/readyz` / `/metrics` / `/status` 的统一实现。
- Bar 查询对象、Store 写接缝、持久化与 HWM、compaction/GC、Replay Session 的时钟/数据源注入、Replay Mode 合法性表。
- `GatewayRuntime` 的测试 loopback 与 `received_bar_count` 同步垫片。
- Arrow schema、JSON Message Envelope、topic 字符串、配置字段、Replay Mode / Replay Protocol 语义。
- `mdgw_client` 的内部实现、客户端重连策略、SDK 发布流程。
- 新配置字段、新 ADR、新 `GLOSSARY.md` 术语。

## Further Notes

- 深度来自“小接口、大实现”：调用方只学习发布、订阅、就绪与计数器；context、socket、线程、编解码、topic 语法、错误策略全部在实现内部。
- `forward=True` 存在的唯一原因是 api Service Role 的职责：把 collector 的帧转发到自己的 PUB，使外部消费者不依赖 collector 地址，同时避免解码再编码。
- `publish_event` 存在的唯一原因是 Replay 的 End of Session：它走 PUB，但不是 Bar 或 status；REQ/REP 控制协议仍在 `ReplayServer`。
- 若迁移中发现其他走 PUB 的非 Bar 流量，应记录为后续 ticket，而不是扩大 `StreamingInterface` 的通道集合。
- 本次不改变任何 Data Contract、Streaming Interface 线上行为或 Replay Protocol；全部迁移提交结束时，现有外部消费者无需改动。
