# 08: OpenAPI 与文档端点

**What to build:** 让新应用始终暴露 `/openapi.json` 与 `/docs`，关闭 `/redoc`。OpenAPI schema 必须描述统一错误 envelope、各 `format` 的 Content-Type、decimal 为字符串、时间为 ISO-8601 纳秒字符串；HTTP 状态码覆盖 400/404/405/429/500。OpenAPI 只是生成的文档，规范真相仍是 Data Contract。文档与 schema 端点加入后续限流的固定豁免列表。

**Blocked by:** 05 (FastAPI 应用骨架、运维与 Instrument 端点对等), 06 (Bars REST 对等), 07 (WebSocket 路由与传输中性 hub)

**Status:** resolved

- [x] `/openapi.json` 返回可解析的 schema，覆盖全部 REST 端点
- [x] `/docs` 返回 200；`/redoc` 返回 404
- [x] schema 描述统一错误 envelope 与 400/404/405/429/500
- [x] schema 描述 Arrow/CSV/Parquet 的 Content-Type
- [x] schema 标注 decimal 为字符串、时间为 ISO-8601 纳秒字符串
- [x] 文档内容不宣称自己是规范真相源
- [x] 生产仍走旧 aiohttp 应用；既有测试全绿

## Comments

- Resolved on `integration/fastapi-rest-api` (merge `f528b3c`; code-review fixes `c067310`). 始终开启 `/openapi.json` 与 `/docs`，关闭 `/redoc`；schema 描述统一错误 envelope、各 format Content-Type、decimal 字符串与纳秒时间，并移除 422。 Full offline suite: 698 passed, 5 known Typer/Click baseline failures, no new failures.
