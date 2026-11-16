# 08: Security & Audit 完整迁移

**What to build:** Auditor 与其他授权使用者可以在 D 风格的 Security 页面查看当前身份和 Access Role、筛选 Operator Audit Event、检查 Operation 时间线与脱敏 Diff，并始终看到 Request ID。原始审计证据保持准确，现有只读边界不变。

**Blocked by:** 01 监控全页建立 D 视觉与共享基础

**Status:** ready-for-agent

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
