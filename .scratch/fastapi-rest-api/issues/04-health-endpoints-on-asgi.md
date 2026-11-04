# 04: collector / replay 健康端点迁移到 ASGI

**What to build:** 让 collector 与 replay 的 `ServiceHealthServer` 用新的 `AsgiServer` 服务一个小型 ASGI 应用，同时保持 `/healthz`、`/readyz`、`/metrics`、`/status` 的路径、payload、状态码与 Content-Type 不变。这是第一个可演示的迁移切片：`mdgw run --service collector` 与 `--service replay` 仍能被 Docker health check 与 `mdgw up` 的就绪探测正常识别。

**Blocked by:** 03 (AsgiServer 生命周期模块)

**Status:** resolved

- [x] `ServiceHealthServer` 使用 `AsgiServer`，不再自己创建 aiohttp 生命周期
- [x] `/healthz`、`/readyz`、`/metrics`、`/status` 的行为与迁移前一致
- [x] collector 与 replay 的多进程测试全绿
- [x] 线程名仍可区分具体 Service Role
- [x] api Service Role 仍走旧实现，行为不变

## Comments

- Resolved on `integration/fastapi-rest-api` (merge `0c3b7b7`; code-review fixes `c067310`). `ServiceHealthServer` 改用 `AsgiServer` + Starlette Router；`/healthz`、`/readyz`、`/metrics`、`/status`、线程名与 `base_url`/`bound_port` 语义不变；多进程测试通过。 Full offline suite: 698 passed, 5 known Typer/Click baseline failures, no new failures.
