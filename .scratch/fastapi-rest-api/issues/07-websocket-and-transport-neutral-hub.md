# 07: WebSocket 路由与传输中性 hub

**What to build:** 在新 FastAPI 应用上提供 `/ws`，并把 `WebSocketHub` 从 aiohttp 类型解耦为只依赖 `send_json` 与 `close` 的传输中性 Protocol。订阅、取消订阅、`ping`/`pong`、Message Envelope、`lag_notice`、慢消费者不断连的语义必须与迁移前一致。现有 `websockets` 客户端测试应能在新应用上通过。

**Blocked by:** 05 (FastAPI 应用骨架、运维与 Instrument 端点对等)

**Status:** ready-for-agent

- [ ] 新应用提供 `/ws`，Starlette `WebSocket` 可直接交给 hub
- [ ] `WebSocketHub` 不再 import 或注解 aiohttp 类型
- [ ] 订阅、取消订阅与 `ping`/`pong` 行为与迁移前一致
- [ ] Message Envelope 与 `lag_notice` 行为与迁移前一致
- [ ] 慢消费者被丢弃消息但连接不断开
- [ ] 现有 `websockets` 客户端测试在新应用上通过
- [ ] 生产仍走旧 aiohttp 应用；既有测试全绿
