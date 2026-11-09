# 02: 实现 OIDC、服务端会话和三角色服务端授权

**What to build:** 实现 Authorization Code + PKCE、不透明 HttpOnly 会话、CSRF、注销、30 分钟空闲/8 小时绝对超时、确定性 Access Role 映射和每个 Admin 路由/资源所有权校验。

**Blocked by:** 01

**Status:** ready-for-agent

- [ ] 未映射/冲突映射默认拒绝并产生安全事件，客户端不能选择角色。
- [ ] Cookie、PKCE、会话固定防护、CSRF、同源和注销符合 spec。
- [ ] Token/Secret 不落审计或浏览器可读存储。
- [ ] SuperAdmin/Operator/Auditor 路由方法矩阵逐项参数化验证。
- [ ] Auditor 只读 WS 会话订阅与禁止管理写不冲突。
- [ ] Export 所有权和 SuperAdmin 全量可见在服务端执行。
- [ ] 离线 OIDC 测试替身与真实 FastAPI/SQLite 集成覆盖会话到期和 401/403。

## Comments
