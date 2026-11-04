# 05: FastAPI 应用骨架、运维与 Instrument 端点对等

**What to build:** 建立新的 FastAPI 应用工厂，保留 `create_app(...)` 的参数形状，并让以下端点在 `AsgiServer` 上工作且与 characterization 一致：`/healthz`、`/readyz`、`/status`、`/metrics`、`/api/v1/status`、`/api/v1/instruments`、`/api/v1/instruments/{id}`。同时加入 `X-Request-ID`、请求指标中间件与统一错误映射：参数校验错误映射到既有稳定码，未知路径返回 `not_found`，未知方法返回 `method_not_allowed`，未处理异常返回 `internal_error`；不向客户端暴露 FastAPI 的 422 或 `{"detail": ...}`。生产仍由旧 aiohttp 应用服务。

**Blocked by:** 01 (ADR 与 glossary), 02 (characterization 测试), 03 (AsgiServer 生命周期模块)

**Status:** resolved

- [x] 新 FastAPI 应用工厂保留 `create_app(...)` 的名称与参数形状
- [x] 上述运维与 Instrument 端点的状态码、响应体与 Content-Type 与 characterization 一致
- [x] `X-Request-ID` 生成/透传与响应头行为一致
- [x] 参数校验错误返回既有稳定码，不出现 422 或 `{"detail": ...}`
- [x] 未知路径返回 `not_found`，未知方法返回 `method_not_allowed`
- [x] 未处理异常返回 `internal_error` 并记录结构化日志
- [x] 应用通过 `AsgiServer` 在端口 0 启动并通过真实 HTTP 验证
- [x] 生产仍走旧 aiohttp 应用；既有测试全绿

## Comments

- Resolved on `integration/fastapi-rest-api` (merge `ab0583e`; code-review fixes `c067310`). 新增 `mdgw.api.app.create_app`、`errors.py`、`middleware.py` 与拆分后的路由包；运维/Instrument 端点、X-Request-ID、404/405/校验/500 统一 envelope 由 19 个真实 HTTP 测试覆盖。 Full offline suite: 698 passed, 5 known Typer/Click baseline failures, no new failures.
