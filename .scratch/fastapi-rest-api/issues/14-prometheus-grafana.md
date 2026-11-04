# 14: Prometheus + Grafana 部署

**What to build:** 提供一套可选的 Prometheus + Grafana 部署，不改变默认 compose 行为。新增 observability 部署目录，包含 Prometheus 抓取配置、Grafana datasource 与一个 dashboard；新增 `docker-compose.observability.yml` 覆盖文件，启动 Prometheus（9090）与 Grafana（3000）。Prometheus 同时抓取 collector、api、replay 三个 Service Role 的 `/metrics`，抓取间隔默认 15 秒。默认 `docker compose up` 不启动这两个服务。

**Blocked by:** 04 (健康端点迁移), 09 (切换生产到 FastAPI 应用 + AsgiServer), 13 (HTTP 延迟 histogram)

**Status:** ready-for-agent

- [ ] observability 部署目录包含 Prometheus 配置、Grafana datasource 与一个 dashboard
- [ ] `docker-compose.observability.yml` 启动 Prometheus 9090 与 Grafana 3000
- [ ] Prometheus 抓取 collector、api、replay 的 `/metrics`
- [ ] 抓取间隔默认 15 秒
- [ ] 默认 `docker compose up` 不启动 Prometheus/Grafana
- [ ] 配置可解析；dashboard 至少展示请求速率、延迟与限流命中
