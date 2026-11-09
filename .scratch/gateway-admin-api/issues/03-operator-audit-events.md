# 03: 实现追加写 Operator Audit Event 与高危 fail-closed

**What to build:** 建立带哈希链的审计事件、查询时间线、敏感值脱敏、异步 requested/result 关联和审计不可用时高危动作零执行。普通查询/CSV/WS 报文不逐条审计，首版无自动清理和审计导出。

**Blocked by:** 01, 02

**Status:** resolved

- [ ] 事件包含完整身份、动作、对象、原因、Diff、结果和关联 ID。
- [ ] requested 在任何高危执行前持久化，最终事件追加而不更新旧行。
- [ ] 审计写入失败阻止高危动作，并返回稳定错误。
- [ ] Secret/Token/Cookie/密码/OIDC 敏感声明统一 REDACTED。
- [ ] 哈希链篡改检测测试通过，文档不宣称 WORM 或绝对不可篡改。
- [ ] 查询过滤、分页、Operation 时间线和无编辑/删除 API 完整。
- [ ] 认证/授权失败与所有管理写均按范围记录。

## Comments

**Resolved (2026-11-10):** 追加写 Operator Audit Event 已合并 `8c8c4f9`（feature `2070a17`）。迁移 003 建立 append-only 事件；SHA-256 哈希链提供篡改证据（文档明确非 WORM/不可篡改存储）；敏感值统一 `[REDACTED]` 并记录 JSON Pointer；`context.audit.begin()` 在任何管理写执行前持久化 requested，审计不可用返回 503 且零执行；认证/授权失败经 sink 进入审计；查询、详情与 Operation 时间线完整，无编辑/删除/导出路由。95 个测试，合并时 1517 passed / 5 个既有 CLI 失败。
