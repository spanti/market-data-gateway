# 15: 最终文档与一致性收尾

**What to build:** 在所有实现票完成后，把 `DESIGN.md`、`README.md` 与 `docs/data-contract.md` 更新到与实现一致：FastAPI/ASGI 与 `AsgiServer`、移除 aiohttp 直接依赖、统一错误码、`/openapi.json` 与 `/docs`、限流配置与 429、单 Instrument 分页 JSON 形状、histogram 与 Prometheus/Grafana 部署、测试接缝与风险。确认 ADR-0011/0012 与 glossary 术语和实现一致。无代码行为变化。

**Blocked by:** 08 (OpenAPI 与文档端点), 10 (删除旧 aiohttp 实现), 11 (限流), 12 (单 Instrument 分页), 13 (HTTP 延迟 histogram), 14 (Prometheus + Grafana 部署)

**Status:** resolved

- [x] `DESIGN.md` 的 HTTP API、依赖、测试接缝、风险与已拒绝方案已更新
- [x] `README.md` 的入口、验证命令与已知限制已更新
- [x] `docs/data-contract.md` 包含分页 JSON 形状、新稳定错误码与 Time Cursor 语义
- [x] ADR-0011/0012 与 `GLOSSARY.md` 的术语和实现一致
- [x] 无代码行为变化；全部测试通过且无新增失败

## Comments

- Resolved on `integration/fastapi-rest-api` (merge `c5b4289`; code-review fixes `c067310`). `DESIGN.md`、`README.md`、`docs/data-contract.md`、`environment.yml` 与实现一致；ADR/GLOSSARY 术语核对通过；无代码行为变化。 Full offline suite: 698 passed, 5 known Typer/Click baseline failures, no new failures.
