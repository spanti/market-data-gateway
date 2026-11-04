# 13: HTTP 延迟 histogram

**What to build:** 为自研 `MetricsRegistry` 增加 histogram 能力，输出 Prometheus 文本格式的 `_bucket`、`_sum`、`_count` 与 `le` 标签。新增 `mdgw_api_http_request_duration_seconds{method,path,status}`，桶为 `0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, +Inf` 秒。现有计数器、gauge 与 Dataset 指标保持不变。不引入 `prometheus_client`。

**Blocked by:** 09 (切换生产到 FastAPI 应用 + AsgiServer)

**Status:** ready-for-agent

- [ ] `MetricsRegistry` 支持 histogram，渲染 `_bucket`、`_sum`、`_count` 与 `le`
- [ ] `/metrics` 暴露 `mdgw_api_http_request_duration_seconds`
- [ ] 标签为 `method`、`path`（路由模板）、`status`
- [ ] 桶边界与约定一致，包含 `+Inf`
- [ ] 现有 `mdgw_up`、`mdgw_ready`、WS 指标与 Dataset 指标不变
- [ ] 渲染文本有单元测试；集成测试验证 `/metrics` 内容
- [ ] 不引入 `prometheus_client` 或 tracing
