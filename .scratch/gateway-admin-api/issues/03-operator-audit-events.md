# 03: 实现追加写 Operator Audit Event 与高危 fail-closed

**What to build:** 建立带哈希链的审计事件、查询时间线、敏感值脱敏、异步 requested/result 关联和审计不可用时高危动作零执行。普通查询/CSV/WS 报文不逐条审计，首版无自动清理和审计导出。

**Blocked by:** 01, 02

**Status:** ready-for-agent

- [ ] 事件包含完整身份、动作、对象、原因、Diff、结果和关联 ID。
- [ ] requested 在任何高危执行前持久化，最终事件追加而不更新旧行。
- [ ] 审计写入失败阻止高危动作，并返回稳定错误。
- [ ] Secret/Token/Cookie/密码/OIDC 敏感声明统一 REDACTED。
- [ ] 哈希链篡改检测测试通过，文档不宣称 WORM 或绝对不可篡改。
- [ ] 查询过滤、分页、Operation 时间线和无编辑/删除 API 完整。
- [ ] 认证/授权失败与所有管理写均按范围记录。

## Comments
