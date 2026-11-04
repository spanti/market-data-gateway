# 11: 限流

**What to build:** 为 REST API 增加默认关闭的每 IP / 每路由限流。新增 `[rate_limit]` 配置段：`enabled`、`requests_per_second`、`burst`、`max_ws_sessions_per_ip`（0 表示不限制）。键为源 IP + 路由模板，不信任 `X-Forwarded-For`。`/healthz`、`/readyz`、`/metrics`、`/openapi.json`、`/docs` 固定豁免。超限返回 429、`Retry-After` 与 `rate_limited` 错误码；WebSocket 在 `accept()` 之前按每 IP 会话上限拒绝。限流只作用于请求建立，不限制 Arrow/CSV 流式响应体。新增 `mdgw_api_rate_limited_total{path}` 计数器。单 worker、进程内状态。

**Blocked by:** 01 (ADR 与 glossary), 09 (切换生产到 FastAPI 应用 + AsgiServer)

**Status:** resolved

- [x] 新增 `[rate_limit]` 配置段并校验 `requests_per_second > 0`、`burst >= 0`、`max_ws_sessions_per_ip >= 0`
- [x] 默认 `enabled=false`，升级不改变现有行为
- [x] 键为源 IP + 路由模板，不读取 `X-Forwarded-For`
- [x] 固定豁免五个路径，不可配置
- [x] 超限返回 429 + `Retry-After` + `rate_limited`，`details` 含 `retry_after_s`
- [x] WebSocket 在 `accept()` 前按每 IP 会话上限拒绝
- [x] 流式响应体不被限速
- [x] `mdgw_api_rate_limited_total{path}` 出现在 `/metrics`
- [x] 令牌桶有注入假时钟的单元测试；集成测试通过 `GatewayRuntime` 验证默认关闭、`burst=0`、豁免与 WebSocket 上限

## Comments

- Resolved on `integration/fastapi-rest-api` (merge `ef44c52`; code-review fixes `c067310`). 新增默认关闭的 `[rate_limit]` 配置、注入时钟的令牌桶、IP+路由模板限流中间件、固定豁免、429 + Retry-After + `rate_limited`、每 IP WebSocket 会话上限与 `mdgw_api_rate_limited_total`。`create_app` 仅追加可选关键字参数 `rate_limit`。 Full offline suite: 698 passed, 5 known Typer/Click baseline failures, no new failures.
