# 08: Security & Audit 完整迁移

**What to build:** Auditor 与其他授权使用者可以在 D 风格的 Security 页面查看当前身份和 Access Role、筛选 Operator Audit Event、检查 Operation 时间线与脱敏 Diff，并始终看到 Request ID。原始审计证据保持准确，现有只读边界不变。

**Blocked by:** 01 监控全页建立 D 视觉与共享基础

**Status:** resolved

- [ ] 当前身份、Access Role、审计查询、事件列表、详情与 Operation 时间线采用共享卡片、状态药丸和简洁标题，Access Role 不与 Service Role 混用。
- [ ] 时间、操作者、Access Role、动作、对象、结果与 Request ID 筛选保持现有行为，筛选标签、校验和空状态清晰。
- [ ] requested 与 succeeded/failed 等关联事件继续以追加写时间线表达，不伪装成原地更新。
- [ ] 脱敏 Diff 与相关原始证据保持逐字准确，包含 `·` 或 `：` 的样例不改写，周围界面生成文案不包含这两个符号。
- [ ] 不提供用户或角色 CRUD、审计编辑、删除或新增审计导出，权限与能力未知状态诚实呈现。
- [ ] Loading、Empty、Error、Unknown 使用真实请求状态，错误与 Request ID 就地显示，补充概念进入 `?`，不隐藏必要审计信息。
- [ ] 状态不只依赖颜色，时间线、详情和筛表单支持键盘，触屏帮助可打开、关闭并用 Escape 退出。
- [ ] 1440、1280、1024 下查询、时间线和 Diff 无整页横向溢出，宽内容仅在局部滚动。
- [ ] typecheck、lint、相关 Vitest、production build 与身份、Access Role、审计筛选、原始 Diff 和布局浏览器验收通过。

## Comments

**Resolved (2026-11-16):** 已在 `2960fd5` 实现，并在合并最新 integration、保留 Bars 与 Security 双方布局检查后以分支 tip `72cfcd0` 全绿合并为 `aca1ddf`。当前身份、Access Role、审计查询、事件详情与 Operation 时间线采用共享卡片、状态药丸与简洁标题，Access Role 不与 Service Role 混用；时间、操作者、Access Role、动作、对象、结果与 Request ID 筛选保持既有行为，requested 与 succeeded/failed 继续以追加写时间线表达。脱敏 Diff、redaction path、reason 与后端错误以 `data-raw-evidence` 逐字展示，含 `·` 或 `：` 的样例不改写，周围界面生成文案保持干净；不提供用户/角色 CRUD、审计编辑、删除或新增审计导出，能力未知状态诚实呈现。分支 tip `61` 个测试文件 / `503` 个 Vitest 测试通过（本次复核，合并提交 `72cfcd0` 亦记录同一结果），typecheck、lint、production build 与 `16` 项定向 Playwright 通过（`8` REAL、`8` CONTRACT-ONLY）；Admin 审计查询与身份流程仅有 CONTRACT-ONLY 证据，不宣称真实 OIDC/Admin 端到端通过。
