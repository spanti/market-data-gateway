# 07: 实现真实 OIDC 会话、Access Role 与审计查询界面

**What to build:** 在 Admin API 模式下接入 OIDC 登录/注销、会话到期、CSRF、当前身份和统一 401/403；按三角色执行路由与操作控制，实现 Operator Audit Event 筛选、脱敏 Diff 和关联操作时间线。Current Contract 不模拟身份。

**Blocked by:** 02

**Status:** ready-for-agent

- [ ] OIDC Token、密码和会话不进入 URL/localStorage，前端无角色选择器。
- [ ] 30 分钟空闲与 8 小时绝对会话提示、401 停止写操作和注销流程可用。
- [ ] Auditor 的只读导出/WS 会话订阅与禁止管理写准确实现。
- [ ] 无权限控件禁用并说明原因，同时处理直接路由和 API 403。
- [ ] 审计筛选包含时间、操作者、角色、动作、对象、结果和 Request ID。
- [ ] requested/succeeded/failed 通过 Operation ID 组成时间线，不伪装原地更新。
- [ ] Diff 支持 before/after null 和稳定 REDACTED，无编辑、删除、导出按钮。
- [ ] 未与真实 OIDC/Admin API 联调时明确报告验证缺口。

## Comments
