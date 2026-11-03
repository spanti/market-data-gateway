# 04: 迁移 GatewayRuntime

**What to build:** `GatewayRuntime` 组合 `StreamingInterface`，绑定 inproc 与外部 TCP endpoint、连接 inproc 订阅、发布 Bar，并让 `stream_endpoint` 委托给解析后的绑定 endpoint。测试 loopback 与 WebSocket 线程交接保持现状。

**Blocked by:** 01

**Status:** ready-for-agent

- [ ] runtime 用 `StreamingInterface` 替代 `ZMQBus` 与本地 `_bind_streaming_bus` / `_publish_bars` / `_on_bar_message` 的编解码逻辑。
- [ ] 绑定顺序保持 `[inproc, tcp]`，`stream_endpoint` 返回解析后的 TCP endpoint；`base_url` 行为不变。
- [ ] 内部订阅使用 `subscribe_bars(..., await_ready=True)`，保证首条 Bar 在 ingest 返回前已被订阅端接收。
- [ ] 外部 ZMQ 消费者与 `/ws` 的行为不变；handler 仍把 JSON Message Envelope 交给 `WebSocketHub`。
- [ ] 既有 `_wait_for_received` / `received_bar_count` 测试垫片保留（属于 Out of Scope），但其数据来源改为新订阅 handler。
- [ ] 启动失败路径仍关闭 bus/stream、Feed Handler 与 state，并重新抛出原始异常；失败后 `_started` 为 false。
- [ ] streaming、bars、status、client live/history 集成测试通过；离线套件无新增失败。
- [ ] 不修改配置字段、topic 字符串、HTTP/WS 行为或 runtime 的测试专用同步语义。

## Comments
