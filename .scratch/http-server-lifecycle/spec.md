# Spec: 一个模块拥有 HTTP 服务器生命周期

Status: ready-for-agent

## Problem Statement

网关的三个 Service Role 都需要在一个后台线程上运行 aiohttp `AppRunner`，但这段生命周期目前被写了三遍：

- `GatewayRuntime` 与 `ApiService` 各自创建线程、事件循环、`AppRunner`、`TCPSite`，并各自解析实际绑定端口与拆除顺序；
- `ServiceHealthServer`（collector / replay 的 `/healthz`、`/readyz`、`/metrics`）又重复了同一套 start/stop；
- `ApiService` 没有任何直接测试，只被多进程集成测试通过启动子进程间接覆盖。

结果是同一段最容易出现关停竞态的代码有两套实现、三处副本。改变启动/停止行为时，维护者必须在多个模块间对照修改；启动失败（端口占用、`AppRunner.setup()` 抛错）时，线程与事件循环的清理责任散落在各调用方；测试只能通过真实端口或子进程观察，难以直接验证回滚与跨线程派发。

对使用者而言，问题不是“多了一个类”，而是：关停行为不可解释、`ApiService` 的启动路径不可测试、以及未来任何 HTTP 生命周期调整都要支付三倍的验证成本。

## Solution

引入一个深模块 `HttpServer`（模块名 `mdgw.api.http_server`），把“在后台线程上运行一个 aiohttp 应用”的全部实现收进一个接口后面：

- 线程、事件循环、`AppRunner`、`TCPSite`、端口解析、有序拆除，全部由 `HttpServer` 拥有；
- `GatewayRuntime`、`ApiService`、`ServiceHealthServer` 都组合它，不再各自维护生命周期字段；
- 对外的公共属性（`base_url`、`http_port`、`bound_port`）名称、类型、语义与可见行为保持不变，内部改为委托给 `HttpServer`；
- 不新增配置字段，不改变任何 HTTP 端点、WebSocket 行为、ZMQ topic、Replay Protocol 或 ingest 语义。

迁移分四步、每步保持测试全绿：先落地 `HttpServer` 与其测试，再迁移 `GatewayRuntime`，再迁移 `ApiService` 并补上首个直接测试，最后迁移 `ServiceHealthServer`。

## User Stories

1. 作为网关维护者，我想要一个模块拥有 HTTP 服务器生命周期，这样启动/停止逻辑只在一个地方修改。
2. 作为网关维护者，我想要启动失败时保证清理线程、事件循环与已绑定 socket，这样端口占用不会留下孤儿线程。
3. 作为网关维护者，我想要启动失败时重新抛出原始异常，这样调用方看到的原因不被包装层掩盖。
4. 作为网关维护者，我想要 `stop()` 可重复调用且不报错，这样错误处理路径可以无条件调用它。
5. 作为网关维护者，我想要 `stop()` 在关闭钩子抛错时仍然完成后续拆除，这样一个 WebSocket 会话的异常不会阻断进程退出。
6. 作为网关维护者，我想要关闭钩子在 `AppRunner.cleanup()` 之前、在事件循环上执行，这样 WebSocket hub 与 HTTP 路由按确定的顺序关闭。
7. 作为网关维护者，我想要一个线程安全的派发入口，这样 collector 的 ZMQ 订阅线程可以把消息送到 WebSocket hub，而不需要直接持有事件循环。
8. 作为网关维护者，我想要 `call_soon_threadsafe` 在服务器已停止时抛出 `RuntimeError`，这样关停竞态不会被静默吞掉。
9. 作为网关维护者，我想要从服务器自身线程调用 `stop()` 时抛出明确错误而不是死锁，这样接口的线程约束可被发现。
10. 作为网关维护者，我想要 `start()` 在已启动时返回既有端口而不重复创建线程，这样调用方不必各自维护“是否已启动”的守卫。
11. 作为网关维护者，我想要 `bound_port` 与 `base_url` 在未启动时抛出错误，这样调用方能在启动完成前得到明确失败。
12. 作为网关维护者，我想要通配地址（`0.0.0.0` / `::`）在 `base_url` 中改写为 `127.0.0.1`，这样本机脚本拿到的 URL 始终可用。
13. 作为运维人员，我想要 collector 与 replay 的 `/healthz`、`/readyz`、`/metrics`、`/status` 行为完全不变，这样 Docker health check 与 `mdgw up` 的就绪探测无需调整。
14. 作为运维人员，我想要每个 Service Role 的 HTTP 线程名保持可区分，这样线程转储与日志仍然能定位到具体角色。
15. 作为网关维护者，我想要 `GatewayRuntime.base_url`、`ApiService.base_url`、`ApiService.http_port`、`ServiceHealthServer.base_url`、`ServiceHealthServer.bound_port` 的名称、类型与语义不变，这样现有调用方与测试不需要改写。
16. 作为测试作者，我想要用一个普通 `web.Application` 直接测试 `HttpServer` 的 start/stop/回滚/派发，这样不需要构造完整网关或子进程。
17. 作为测试作者，我想要 `ApiService` 能在同一进程内以端口 0 启动、访问 `/healthz`、`/readyz`、`/api/v1/status` 并干净停止，这样 API Service Role 不再只能靠子进程覆盖。
18. 作为测试作者，我想要 `HttpServer` 的测试只断言公共接口行为（端口、响应、线程存活、错误类型），不断言私有字段，这样重构实现时测试不必跟着改。
19. 作为网关维护者，我想要既有的 `GatewayRuntime` 端到端测试继续守护 HTTP 行为，这样迁移期间外部行为有回归保护。
20. 作为网关维护者，我想要原先的启动失败回滚测试改为命中新接缝，这样它验证的是生产代码而不是测试专用垫片。
21. 作为网关维护者，我想要 `HttpServer` 不拥有 `WebSocketHub`、不感知 ZMQ 与 Service Role，这样它可以被一个哑应用测试，也不会把传输细节拉进接口。
22. 作为网关维护者，我想要不新增配置字段、不新增 ADR、不新增领域术语，这样这次改动只收缩实现，不扩张契约。
23. 作为 AFK 实现代理，我想要迁移拆成四个可独立验证的提交，这样每个提交都能单独 review 与回退。
24. 作为后续架构工作的维护者，我想要这次改动只处理 HTTP 生命周期，不顺手修改 Bar 语义、存储接缝或 topic 语法，这样评审范围保持单一。

## Implementation Decisions

### 新模块

- 新建模块 `mdgw.api.http_server`，公开类 `HttpServer`。
- `HttpServer` 只处理“在后台线程上服务一个已构建的 aiohttp 应用”。它不拥有 `WebSocketHub`，不 import `mdgw.bus`、`mdgw.replay` 或任何 Service Role 模块。
- 应用由调用方同步构建后传入；`WebSocketHub` 与 `create_app` 都不需要运行中的事件循环，因此不存在 `app_factory` 这一层假设接缝。

### 接口

- `start(app, *, host, port, thread_name, on_shutdown=None) -> int`：启动后台线程与事件循环，绑定 `TCPSite`，返回解析后的实际端口。`port=0` 时由操作系统分配端口。
- `stop() -> None`：在事件循环上执行关闭钩子、`AppRunner.cleanup()`，然后停止循环、join 线程、关闭循环。
- `call_soon_threadsafe(fn, *args) -> None`：线程安全地把可调用对象派发到 HTTP 事件循环；服务器未启动时抛 `RuntimeError`。
- `bound_port: int`：未启动时抛 `RuntimeError`。
- `base_url: str`：未启动时抛 `RuntimeError`；`host` 为 `0.0.0.0` 或 `::` 时报告 `127.0.0.1`。

### 行为保证

- `start()` 幂等：已启动时返回既有端口，不创建新线程。
- `stop()` 幂等：未启动时为 no-op。
- `start()` 失败（应用启动钩子、`AppRunner.setup()`、`TCPSite.start()` 抛错）时，先完整清理线程、事件循环与 socket，再重新抛出原始异常。
- `on_shutdown` 只在 `stop()` 时执行；在 `runner.cleanup()` 之前、在事件循环上执行。它的异常被记录并吞掉，后续拆除继续。
- `stop()` 从服务器自身线程调用时抛 `RuntimeError`，而不是在 `future.result()` 上死锁。
- 超时保持模块常量：启动 10 秒、关闭 future 10 秒、线程 join 5 秒。不新增配置项。
- 后台线程保持 `daemon=True`；线程名由调用方通过 `thread_name` 传入。

### 调用方迁移

- `GatewayRuntime`：同步创建 `WebSocketHub` 与 `create_app`，保留 `_hub`，用 `_http.call_soon_threadsafe` 替换对事件循环的直接持有；`base_url` 委托给 `HttpServer`。移除 `_loop`、`_loop_thread`、`_runner`、`_port`。
- `ApiService`：同样同步创建 hub 与应用，传入 metrics / status / ready provider；`base_url` 与 `http_port` 委托给 `HttpServer`。移除生命周期私有字段。
- `ServiceHealthServer`：保留自己的端点处理函数、`service` / `host` / `port` 字段以及公开的 `start`、`stop`、`bound_port`、`base_url`；内部持有 `_http: HttpServer`，在 `start()` 中构建应用并委托。`bound_port` 与 `base_url` 通过属性转发到内部实例。

### 契约与文档

- 不修改 `GLOSSARY.md`：`HttpServer` 是基础设施模块，不是领域概念。
- 不新增 ADR。ADR-0002（Replay Clock）与 ADR-0003（ZMQ 总线）不受影响；本改动不触及 Replay Protocol、Streaming Interface 或 Data Contract。
- 不新增配置字段。

### 迁移顺序

1. 新增 `HttpServer` 与其测试；不迁移任何调用方。
2. 迁移 `GatewayRuntime`；既有集成测试保持通过。
3. 迁移 `ApiService`，并新增首个直接测试。
4. 迁移 `ServiceHealthServer`；collector / replay 多进程测试保持通过。

## Testing Decisions

### 好测试的标准

- 只断言外部行为：解析后的端口、HTTP 响应、`base_url`、抛出错误类型、线程是否存活、端口是否可复用。
- 不断言私有字段（`_loop`、`_thread`、`_runner`、`_port`），不使用测试专用生产垫片。
- 使用端口 0 与真实 HTTP 请求，避免固定端口冲突；测试结束必须停止服务器并验证线程已退出。

### 接缝

- **唯一新增接缝**：`HttpServer` 的公共接口。十个用例：
  1. 端口 0 启动后 `bound_port` 与 `base_url` 反映实际端口；
  2. `0.0.0.0` / `::` 在 `base_url` 中改写为 `127.0.0.1`；
  3. 真实请求命中哑应用并返回预期响应；
  4. 重复 `start()` 返回同一端口且只有一个线程；
  5. 重复 `stop()` 为 no-op；
  6. `stop()` 后端口可立即复用；
  7. 绑定失败后无线程/循环残留，并重新抛出原始错误；
  8. `on_shutdown` 在 `runner.cleanup()` 之前执行；其异常不阻断拆除；
  9. `call_soon_threadsafe` 可从另一线程派发；`stop()` 后抛 `RuntimeError`；
  10. 从服务器线程调用 `stop()` 抛 `RuntimeError` 而不是死锁。
- **既有接缝**：`GatewayRuntime` 端到端（DESIGN §9.1）继续守护 HTTP 行为；现有 `tests/integration` 中的 Bars / status / streaming 测试不改语义。
- **既有接缝**：`ApiService` 的公共 `start` / `stop` / `base_url`。新增一个进程内集成测试：端口 0、无 collector、断言 `/healthz` 200、`/readyz` 200（`collector_required` 默认 false）、`/api/v1/status` 报告 `service: "api"`，随后干净停止且 HTTP 线程退出。
- **既有接缝**：`ServiceHealthServer` 的公共接口。不新增专用测试；其转发行为由 `HttpServer` 测试与既有 collector / replay 多进程测试共同覆盖。

### 测试改动

- 新增 `HttpServer` 的集成测试模块（十个用例）。
- 新增 `ApiService` 的进程内集成测试模块。
- 将 `GatewayRuntime` 的启动失败测试从 monkeypatch `_start_http` 改为 monkeypatch `runtime._http.start`，保留“失败后 feed / state / bus 已关闭、`_started` 为 false”的断言。

### 既有测试先例

- `tests/integration/test_bars_api.py`：端口 0 + `runtime.base_url` 的 HTTP 往返。
- `tests/integration/test_status_api.py`：端口 0 + status 端点断言。
- `tests/integration/test_multiprocess_services.py`：真实 Service Role 进程与 `/healthz` 探测。
- `tests/unit/test_supervisor.py`：可注入的子进程与就绪探测，说明基础设施行为可以通过公共接口测试。
- `tests/unit/test_buffered_bar_sink.py`：线程与关闭顺序的单元测试写法。

## Out of Scope

- HTTP 端点行为、`create_app` 的参数形状、路由、错误模型、CSV/Arrow/Parquet 导出。
- `WebSocketHub` 的职责、队列、lag notice 与订阅匹配。
- ZMQ 总线、topic 语法、Streaming Interface、Replay Protocol。
- collector ingest、Bar Aggregation、High-Water Mark、Backfill、archive import。
- 新配置字段、TLS / 鉴权 / 公网绑定、进程模型或 supervisor 变化。
- 为 `ServiceHealthServer` 新增专用测试接缝。
- 架构审查报告中的其他候选（Bar 语义、store 写接缝、closed-Bar 规则、store 构造工厂、topic 语法、compaction 私有锁、Service Role 可观测性、客户端契约副本）。

## Further Notes

- 该改动是对 `GatewayRuntime` / `ApiService` / `ServiceHealthServer` 内部实现的收缩，不改变任何 Data Contract、Streaming Interface 或 Replay Protocol。
- `HttpServer` 的深度来自“小接口、大实现”：调用方只学习 `start`、`stop`、`call_soon_threadsafe` 与两个属性，线程、循环、绑定、超时与拆除顺序全部在实现内部。
- 迁移期间若发现 `ApiService` 在无 collector 时存在额外的启动依赖，应记录为后续 ticket，而不是扩大本次范围。
