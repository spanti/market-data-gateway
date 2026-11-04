# 14: Prometheus + Grafana 部署

**What to build:** 提供一套可选的 Prometheus + Grafana 部署，不改变默认 compose 行为。新增 observability 部署目录，包含 Prometheus 抓取配置、Grafana datasource 与一个 dashboard；新增 `docker-compose.observability.yml` 覆盖文件，启动 Prometheus（9090）与 Grafana（3000）。Prometheus 同时抓取 collector、api、replay 三个 Service Role 的 `/metrics`，抓取间隔默认 15 秒。默认 `docker compose up` 不启动这两个服务。

**Blocked by:** 04 (健康端点迁移), 09 (切换生产到 FastAPI 应用 + AsgiServer), 13 (HTTP 延迟 histogram)

**Status:** resolved

- [x] observability 部署目录包含 Prometheus 配置、Grafana datasource 与一个 dashboard
- [x] `docker-compose.observability.yml` 启动 Prometheus 9090 与 Grafana 3000
- [x] Prometheus 抓取 collector、api、replay 的 `/metrics`
- [x] 抓取间隔默认 15 秒
- [x] 默认 `docker compose up` 不启动 Prometheus/Grafana
- [x] 配置可解析；dashboard 至少展示请求速率、延迟与限流命中

## Comments

- Resolved on `integration/fastapi-rest-api` (merge `21ff82b`; code-review fixes `c067310`). 新增 `observability/`（Prometheus 15s 抓取 collector/api/replay、Grafana datasource + dashboard）与 `docker-compose.observability.yml`（9090/3000）；默认 `docker compose up` 不启动它们。 Full offline suite: 698 passed, 5 known Typer/Click baseline failures, no new failures.
