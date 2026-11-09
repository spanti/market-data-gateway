# 02: 建立生产前端基线、真实契约适配与运行模式

**What to build:** 按 issue 01 选定的设计系统建立 `web/` 生产工程，配置 Vite、React、TypeScript、npm、shadcn/ui、Tailwind CSS 4.0、路由、TanStack Query、Axios、表单、测试和同源代理。实现现有 REST/WS/ops 类型与错误适配、能力发现和 Current Contract/Admin API/能力未知状态；不包含 issue 01 的 throwaway 页面。

**Blocked by:** 01

**Status:** ready-for-agent

- [ ] 生产工程、lockfile、环境示例和 npm scripts 完整，静态构建不依赖 Node 运行时。
- [ ] 采用选定设计 tokens 重写生产组件，不直接提升原型代码。
- [ ] REST 错误信封、Request ID、ops/Prometheus 非统一响应分别有适配器。
- [ ] 能力发现失败不伪装只读；Current Contract 匿名只读且无角色切换器。
- [ ] 路由、五个一级模块、URL 查询状态、非敏感偏好和权限守卫符合 spec。
- [ ] 代理不修改 Python/Compose，开发环境可连接真实 API、WS 和分角色运维端点。
- [ ] typecheck、lint、unit test、production build 基线通过。

## Comments
