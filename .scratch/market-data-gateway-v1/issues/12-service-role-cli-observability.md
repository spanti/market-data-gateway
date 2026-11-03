# 12: Service Role CLI、可观测性、docker-compose

**What to build:** 运维人员可以用一个命令分别启动 collector/api/replay，或用 `mdgw up` 按顺序拉起；有健康检查、状态、指标与结构化日志；docker-compose 可一键起全套。

**Blocked by:** 07, 08, 10

**Status:** resolved

- [x] `mdgw run --service collector|api|replay`。
- [x] `mdgw up` 按 collector → ready → api 顺序拉起并负责重启。
- [x] `/healthz`、`/readyz`、`/status`、`/metrics`。
- [x] 结构化 JSON 日志，带 request/session/instrument。
- [x] docker-compose；Windows 默认 `tcp://127.0.0.1`。
- [x] 多进程集成测试通过。

## Comments

- Resolved in `integration/market-data-gateway-v1` after the v1 delivery code review and follow-up fixes.
