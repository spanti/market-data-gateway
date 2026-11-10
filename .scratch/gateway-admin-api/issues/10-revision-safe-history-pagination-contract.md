# 10: 决定 Revision-heavy Time Cursor 的公开契约修复

**What to build:** 由维护者选择并记录独立 Data Contract/ADR，解决同一 open_ts 多 Revision 跨页时 next_start 重复或不前进的问题。候选包括保持 v1 并新增修订安全版本，或明确改变单 Instrument 页边界语义；决策前不得修改公开 `/api/v1/bars`。

**Blocked by:** None（需要维护者契约决策）

**Status:** resolved

- [ ] 明确是否版本化公开 API，以及 ADR-0012 是否需要 supersede。
- [ ] 保证 Revision 组不会跨页造成重复、跳行或死循环。
- [ ] 保持单 Instrument、open_ts 升序、start 闭/end 开和 100,000 原始行保护，或显式记录契约变化。
- [ ] 构造单 open_ts 多 Revision 且跨页的回归测试，证明 next_start/新游标严格可推进。
- [ ] 同步 GLOSSARY、data-contract、OpenAPI、SDK 与前端适配，不把生成文档当真相源。
- [ ] 前端在修复上线前继续检测不前进并失败，不无限循环。

## Comments

ADR-0016 只授权 Admin API 架构归属，不授权改变公开历史 Data Contract；本 issue 需要自己的契约决策。

**Comment (2026-11-10):** Tickets 01-09 are resolved and the Admin API is merged on `integration/gateway-admin-api`. This decision remains open for the maintainer: the branch did not change public `/api/v1/bars` pagination or Revision-heavy Time Cursor semantics, and the frontend non-advance guard remains in place. ADR-0017 covers only the Admin control channel and does not authorize a public history contract change.

**Decision (2026-11-10):** 维护者选择候选 A。`/api/v1/bars` 与 ADR-0012 保持原样；新增 revision-safe 的 `GET /api/v2/bars`（JSON、单 Instrument、返回全部 raw Revisions、不透明行级 keyset 游标）。游标键与排序均为 `(open_ts, revision, ingest_ts, 行指纹)`；首页使用 `start`/`end`，后续页使用 `cursor`；`limit` 为 1..100000；响应为 `{"bars":[...],"page":{"has_more":...,"next_cursor":...}}`。新增 ADR-0018；同步 data-contract、GLOSSARY、OpenAPI、SDK 与前端。Status 切到 ready-for-agent 以实施。

**Resolved (2026-11-10):** 维护者选择候选 A 并已实现，合并 `7d5b4dc`（feature `154575a`）。`/api/v1/bars` 与 ADR-0012 保持原样；新增 `GET /api/v2/bars`（JSON、单 Instrument、全部 raw Revisions、`limit` 1..100000）。总序与 keyset 均为 `(open_ts, revision, ingest_ts, 行指纹)`，游标为 HMAC-SHA256 签名的不可解释 token，首页 `start`/`end`、后续页 `cursor`，响应 `{"bars":[...],"page":{"has_more","next_cursor"}}`。新增 accepted `ADR-0018`；同步 `docs/data-contract.md`、`GLOSSARY.md`、OpenAPI、SDK（`build_bars_v2_url`/`fetch_bars_v2_page`/`iter_bars_v2_pages`）与前端（`next_cursor` 守卫，禁止不前进循环）。回归测试覆盖 6 个 revision-heavy 场景、游标篡改/版本/错配、compaction、v1 不变；Python 1875 passed（仅 5 个既有 CLI 失败，另有 1 个 Windows flake 隔离通过），Web typecheck/lint/462 tests/build 全通过；独立契约复审未发现违规。
