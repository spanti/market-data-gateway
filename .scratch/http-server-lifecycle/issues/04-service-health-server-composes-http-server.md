# 04: 让 ServiceHealthServer 组合 HttpServer

**What to build:** collector 与 replay 的 `ServiceHealthServer` 保留自己的端点处理函数、`service` / `host` / `port` 字段与公开的 `start`、`stop`、`bound_port`、`base_url`；内部改为持有一个 `HttpServer`，在 `start()` 中构建应用并委托生命周期，`bound_port` 与 `base_url` 通过属性转发。

**Blocked by:** 01

**Status:** ready-for-agent

- [ ] `ServiceHealthServer` 不再持有 `_loop`、`_thread`、`_runner`、`_bound_port`，改为持有一个 `HttpServer`。
- [ ] 公开 `start` / `stop` / `bound_port` / `base_url` 的名称、类型、语义与可见行为不变；`bound_port` 与 `base_url` 转发到内部实例。
- [ ] `/healthz`、`/readyz`、`/metrics` 与可选 `/status` 的响应保持原样。
- [ ] collector 与 replay 的 `ServiceHealthServer` 启动、停止、健康探测行为不变。
- [ ] 既有多进程 Service Role 测试与 `mdgw up` 就绪探测测试保持通过。
- [ ] 不新增 `ServiceHealthServer` 专用测试接缝；其转发行为由 `HttpServer` 测试与多进程测试共同覆盖。
- [ ] 离线测试套件全绿；不修改 Docker health check、端口默认值或 supervisor 行为。
