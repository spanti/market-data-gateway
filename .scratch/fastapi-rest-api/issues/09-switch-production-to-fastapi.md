# 09: 切换生产到 FastAPI 应用 + AsgiServer

**What to build:** 把 `ApiService` 与 `GatewayRuntime` 切换到新 FastAPI 应用与 `AsgiServer`，让 05–08 的所有端点成为真实生产行为。切换后所有既有集成测试与 characterization 必须全绿；旧 aiohttp 应用与 `HttpServer` 暂时保留在代码中但不再被使用，方便单独回退。

**Blocked by:** 02 (characterization 测试), 04 (健康端点迁移), 05 (FastAPI 应用骨架), 06 (Bars REST 对等), 07 (WebSocket 路由与 hub), 08 (OpenAPI 与文档端点)

**Status:** ready-for-agent

- [ ] `ApiService` 使用新 FastAPI 应用与 `AsgiServer`
- [ ] `GatewayRuntime` 使用新 FastAPI 应用与 `AsgiServer`，`base_url` 语义不变
- [ ] `ApiService.base_url`、`http_port` 与 `GatewayRuntime.base_url` 的名称与语义不变
- [ ] 所有既有集成测试通过
- [ ] characterization 测试在新实现上通过
- [ ] WebSocket、ZMQ 转发与 `lag_notice` 行为不变
- [ ] 旧 aiohttp 应用与 `HttpServer` 仍存在但无生产调用方
