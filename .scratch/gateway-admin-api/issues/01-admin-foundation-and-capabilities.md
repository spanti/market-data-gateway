# 01: 建立 Admin API 基线、管理存储与能力发现

**What to build:** 在现有 FastAPI api Service Role 中增加 `/api/v1/admin` 路由边界、统一错误、Request ID、独立 SQLite migrations、受控配置和 capabilities。保持现有公开 REST/WS/Dataset/Replay 不变，不引入 Node 或新 Service Role。

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] capabilities 区分不支持、已启用和适配器不可用，包含管理面版本与认证要求。
- [ ] Admin 错误使用统一 envelope 和稳定码，不泄露默认 422/detail。
- [ ] 管理 SQLite 独立于 Bars/Quarantine，包含 migrations、事务和单 API 实例约束。
- [ ] 同源/TLS/可信代理配置明确，不增加宽松 CORS。
- [ ] 控制通道提供允许列表、schema、超时、相关 ID 和最小权限基础。
- [ ] 现有公开 API、WS、Replay、Manifest 与 Dataset 测试保持通过。

## Comments
