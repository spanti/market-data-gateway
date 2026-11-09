# 01: 建立 Admin API 基线、管理存储与能力发现

**What to build:** 在现有 FastAPI api Service Role 中增加 `/api/v1/admin` 路由边界、统一错误、Request ID、独立 SQLite migrations、受控配置和 capabilities。保持现有公开 REST/WS/Dataset/Replay 不变，不引入 Node 或新 Service Role。

**Blocked by:** None (can start immediately)

**Status:** resolved

- [ ] capabilities 区分不支持、已启用和适配器不可用，包含管理面版本与认证要求。
- [ ] Admin 错误使用统一 envelope 和稳定码，不泄露默认 422/detail。
- [ ] 管理 SQLite 独立于 Bars/Quarantine，包含 migrations、事务和单 API 实例约束。
- [ ] 同源/TLS/可信代理配置明确，不增加宽松 CORS。
- [ ] 控制通道提供允许列表、schema、超时、相关 ID 和最小权限基础。
- [ ] 现有公开 API、WS、Replay、Manifest 与 Dataset 测试保持通过。

## Comments

**Resolved (2026-11-10):** Admin API 基线与能力发现已实现并合并 `7c3fe65`（feature `e194f03`）。`src/mdgw/admin/` 提供 `AdminContext`、能力文档（版本/认证/适配器/能力/limits）、统一错误码与 envelope、独立管理 SQLite（WAL、migrations 001、事务、单实例租约）、可信代理解析和不接受任意命令的控制 envelope。`[admin] enabled=false` 默认保持 Current Contract 404；公开 REST/WS/Replay/Dataset 契约未变。新增 124 项测试，合并时 1262 passed / 5 个既有 CLI 失败。代码评审后补上租约心跳（S4）、Collector/Supervisor 控制端口拆分（S3）与 ADR-0017。
