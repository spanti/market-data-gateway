# 11: 限流

**What to build:** 为 REST API 增加默认关闭的每 IP / 每路由限流。新增 `[rate_limit]` 配置段：`enabled`、`requests_per_second`、`burst`、`max_ws_sessions_per_ip`（0 表示不限制）。键为源 IP + 路由模板，不信任 `X-Forwarded-For`。`/healthz`、`/readyz`、`/metrics`、`/openapi.json`、`/docs` 固定豁免。超限返回 429、`Retry-After` 与 `rate_limited` 错误码；WebSocket 在 `accept()` 之前按每 IP 会话上限拒绝。限流只作用于请求建立，不限制 Arrow/CSV 流式响应体。新增 `mdgw_api_rate_limited_total{path}` 计数器。单 worker、进程内状态。

**Blocked by:** 01 (ADR 与 glossary), 09 (切换生产到 FastAPI 应用 + AsgiServer)

**Status:** ready-for-agent

- [ ] 新增 `[rate_limit]` 配置段并校验 `requests_per_second > 0`、`burst >= 0`、`max_ws_sessions_per_ip >= 0`
- [ ] 默认 `enabled=false`，升级不改变现有行为
- [ ] 键为源 IP + 路由模板，不读取 `X-Forwarded-For`
- [ ] 固定豁免五个路径，不可配置
- [ ] 超限返回 429 + `Retry-After` + `rate_limited`，`details` 含 `retry_after_s`
- [ ] WebSocket 在 `accept()` 前按每 IP 会话上限拒绝
- [ ] 流式响应体不被限速
- [ ] `mdgw_api_rate_limited_total{path}` 出现在 `/metrics`
- [ ] 令牌桶有注入假时钟的单元测试；集成测试通过 `GatewayRuntime` 验证默认关闭、`burst=0`、豁免与 WebSocket 上限
