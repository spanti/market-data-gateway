# 03: 让 ApiService 组合 HttpServer，并补首个直接测试

**What to build:** `ApiService` 不再自己创建线程、事件循环、`AppRunner` 与 `TCPSite`，改为把同步构建的应用交给 `HttpServer`，并通过 `HttpServer.call_soon_threadsafe` 派发 collector 的 Bar 消息。公共 `base_url` 与 `http_port` 保持原语义并委托。`ApiService` 获得第一个进程内直接测试，不再只依赖多进程子进程覆盖。

**Blocked by:** 01

**Status:** resolved

- [x] `ApiService` 不再持有 `_loop`、`_loop_thread`、`_runner`、`_port`，也不直接调用 `AppRunner` / `TCPSite`。
- [x] 应用仍携带 metrics、status、ready provider 与 `service_role="api"`；WebSocket hub 由 `ApiService` 构建并交给关闭钩子。
- [x] `base_url` 与 `http_port` 保持原有名称、类型、语义，内部委托给 `HttpServer`。
- [x] 新增进程内集成测试：`http_port=0`、`api_pub_bind=tcp://127.0.0.1:0`、无 collector 进程。
- [x] 测试断言 `/healthz` 返回 200、`/readyz` 返回 200（`collector_required` 默认 false）、`/status` 与 `/healthz` 报告 `service: "api"`、`/api/v1/status` 报告 `dataset: "bars"`。
- [x] 测试结束调用 `stop()`，断言 HTTP 线程已退出，测试进程可继续复用端口。
- [x] 既有 collector → api 多进程测试与 Streaming Interface 测试保持通过。
- [x] 离线测试套件无新增失败（基线已知 5 个 Typer/Click 退出码失败，与本次无关）；不修改端点行为或配置默认值。

## Comments

- Resolved on `integration/http-server-lifecycle` (merge `bbf28f0`, fix pass `f281c7c`). Full offline suite: 528 passed, 5 known Typer/Click baseline failures, no new failures. The `/api/v1/status` assertion was corrected to `dataset: "bars"`; `service: "api"` is asserted on `/status` and `/healthz`.
