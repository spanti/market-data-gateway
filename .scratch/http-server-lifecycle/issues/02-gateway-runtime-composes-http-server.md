# 02: 让 GatewayRuntime 组合 HttpServer

**What to build:** `GatewayRuntime` 不再自己创建线程、事件循环、`AppRunner` 与 `TCPSite`。它同步构建 `WebSocketHub` 与 `create_app`，把应用交给 `HttpServer`，保留 hub 所有权，并通过 `HttpServer.call_soon_threadsafe` 把 ZMQ 线程收到的 Bar 派发到 WebSocket 会话。公共 `base_url` 的名称、类型、语义与可见行为保持不变，内部委托给 `HttpServer`。

**Blocked by:** 01

**Status:** ready-for-agent

- [ ] `GatewayRuntime` 不再持有 `_loop`、`_loop_thread`、`_runner`、`_port`，也不直接调用 `asyncio.run_coroutine_threadsafe` 或 `AppRunner`。
- [ ] 启动时先同步构建 hub 与应用，再调用 `HttpServer.start`；关闭钩子关闭 hub，顺序为 hub → runner cleanup。
- [ ] WebSocket fan-out 通过 `HttpServer.call_soon_threadsafe` 完成；关停竞态表现为可捕获的 `RuntimeError`。
- [ ] `base_url` 保持原有名称、返回类型与语义，内部委托；`start()` / `stop()` 的可见行为不变。
- [ ] 既有 `GatewayRuntime` 端到端测试全部通过：Bars、status、Streaming Interface、replay 使用场景。
- [ ] 启动失败回滚测试改为命中 `runtime._http.start`，并保持“失败后 feed / state / bus 已关闭、`_started` 为 false”的断言。
- [ ] 离线测试套件全绿；不修改 HTTP 端点、ZMQ topic、Replay Protocol 或配置。
