# 12: 单 Instrument JSON 分页元数据

**What to build:** 为同时带 `instrument` 与 `limit` 的 JSON Bars 查询返回分页元数据：`{"bars": [...], "page": {"has_more": bool, "next_start": ISO-8601-ns|null}}`。处理端读取 `limit + 1` 行，按 `open_ts` 升序物化排序后返回前 `limit` 行；`next_start` 是第一条未返回 Bar 的 `open_ts`，下一次作为闭区间 `start` 使用；`has_more` 只统计 `end` 以内的行。`limit=0` 返回空 `bars`，同时给出存在性与首行 `open_ts`。其余查询保持 `{"bars": [...]}`；Arrow/CSV/export 与 `/api/v1/instruments` 完全不变。该票同时落定“分页 JSON 按 `open_ts` 升序”的排序契约。

**Blocked by:** 01 (ADR 与 glossary), 09 (切换生产到 FastAPI 应用 + AsgiServer)

**Status:** resolved

- [x] 仅同时有 `instrument` 与 `limit` 时出现 `page` 对象
- [x] `next_start` 为第一条未返回 Bar 的 `open_ts`，ISO-8601 纳秒字符串
- [x] `has_more` 只统计 `end` 以内的行；无更多行时 `next_start` 为 `null`
- [x] `limit=0` 返回空 `bars`，并给出存在性与首行 `open_ts`
- [x] 缺少 `instrument` 或 `limit` 时响应保持 `{"bars": [...]}`
- [x] 时间缺口、`end` 边界与单 Instrument 多页场景被测试覆盖
- [x] Arrow/CSV/export 与 `/api/v1/instruments` 行为不变
- [x] 排序契约写入 Data Contract 与 ADR-0012

## Comments

- Resolved on `integration/fastapi-rest-api` (merge `38208b2`; code-review fixes `c067310`). 仅对同时带 `instrument` 与 `limit` 的 JSON 查询返回 `page.{has_more,next_start}`；按 `open_ts` 升序（全局 top-k，避免存储序造成跳页/重复），`limit=0`、`end` 边界与多页无重无漏均有测试；OpenAPI 与 Data Contract 同步。 Full offline suite: 698 passed, 5 known Typer/Click baseline failures, no new failures.
