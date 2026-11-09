# 07: 实现真实 OIDC 会话、Access Role 与审计查询界面

**What to build:** 在 Admin API 模式下接入 OIDC 登录/注销、会话到期、CSRF、当前身份和统一 401/403；按三角色执行路由与操作控制，实现 Operator Audit Event 筛选、脱敏 Diff 和关联操作时间线。Current Contract 不模拟身份。

**Blocked by:** 02

**Status:** resolved

- [ ] OIDC Token、密码和会话不进入 URL/localStorage，前端无角色选择器。
- [ ] 30 分钟空闲与 8 小时绝对会话提示、401 停止写操作和注销流程可用。
- [ ] Auditor 的只读导出/WS 会话订阅与禁止管理写准确实现。
- [ ] 无权限控件禁用并说明原因，同时处理直接路由和 API 403。
- [ ] 审计筛选包含时间、操作者、角色、动作、对象、结果和 Request ID。
- [ ] requested/succeeded/failed 通过 Operation ID 组成时间线，不伪装原地更新。
- [ ] Diff 支持 before/after null 和稳定 REDACTED，无编辑、删除、导出按钮。
- [ ] 未与真实 OIDC/Admin API 联调时明确报告验证缺口。

## Comments

**Resolved (2026-11-09):** 合并提交 `680a4c8`。OIDC 会话生命周期（跳转登录、会话探测、内存 CSRF、注销、30m idle / 8h absolute 5 分钟预警、401 停止写入、403 不登出、无 token/密码持久化）、角色矩阵、Operator Audit Event 筛选/分页/Operation ID 时间线/脱敏 before/after 仅契约测试（Admin API/OIDC 不存在）。Current Contract 无假登录、无角色切换器。代码评审修复：能力/版本不兼容门控、429 门控、术语与类型统一。最终套件 458 tests。
