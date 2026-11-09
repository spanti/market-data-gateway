# 02: 建立生产前端基线、真实契约适配与运行模式

**What to build:** 按 issue 01 选定的设计系统建立 `web/` 生产工程，配置 Vite、React、TypeScript、npm、shadcn/ui、Tailwind CSS 4.0、路由、TanStack Query、Axios、表单、测试和同源代理。实现现有 REST/WS/ops 类型与错误适配、能力发现和 Current Contract/Admin API/能力未知状态；不包含 issue 01 的 throwaway 页面。

**Blocked by:** 01

**Status:** resolved

- [ ] 生产工程、lockfile、环境示例和 npm scripts 完整，静态构建不依赖 Node 运行时。
- [ ] 采用选定设计 tokens 重写生产组件，不直接提升原型代码。
- [ ] REST 错误信封、Request ID、ops/Prometheus 非统一响应分别有适配器。
- [ ] 能力发现失败不伪装只读；Current Contract 匿名只读且无角色切换器。
- [ ] 路由、五个一级模块、URL 查询状态、非敏感偏好和权限守卫符合 spec。
- [ ] 代理不修改 Python/Compose，开发环境可连接真实 API、WS 和分角色运维端点。
- [ ] typecheck、lint、unit test、production build 基线通过。

## Comments

**Resolved (2026-11-09):** 生产前端基线与运行模式已实现。合并提交 `7326744`（基础工程）与 `3a6cd70`（共享 WebSocket transport）。`web/` 包含 Vite 6 + React 19 + TypeScript 5.7 + Tailwind 4.0.17、shadcn 风格原语、五模块路由与 AppShell、能力发现状态机（Current Contract / Admin API / 能力未知）、REST/ops/Prometheus 适配器、Access Role 矩阵与禁用原因、URL 时间范围语义、CSV 文件名解析、非敏感偏好。基线关闭时 92 个单元测试通过；typecheck、lint、production build 通过。未修改 Python/Compose；生产包不含原型代码。后续 ticket 04/05 复用该 transport。
