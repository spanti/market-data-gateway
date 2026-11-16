# 04: Console Shell 与 Feed Subscription 完成迁移

**What to build:** Operator 可以在统一的 Console Shell 中进入 Feed Subscription，通过 D 卡片和状态药丸区分 Desired、Observed、Drift 与 Last Error，完成现有采集配置和流控操作。Feed Subscription 与 WebSocket Session Subscription 保持不同概念，权限和冲突处理不变。

**Blocked by:** 01 监控全页建立 D 视觉与共享基础

**Status:** resolved

- [ ] Console 三个页签的 Shell、标题、状态与操作区域采用共享 D 语言，为后续 Service Instance 和 WebSocket Debug 保留现有路由与页签行为。
- [ ] Feed Subscription 的 Desired、Observed、Drift、Last Error 分组清晰，不用一个合并状态替代，也不与 WebSocket Session Subscription 混用。
- [ ] API Rate Limit 与 WebSocket Slow Consumer 保持独立设置，不扩展为 Venue、Collector 写队列或存储背压控制。
- [ ] Current Contract 的不可用写操作显示真实依赖与禁用原因，Admin API 按服务端 Access Role 开放能力，不增加假登录或角色切换。
- [ ] 表单标签、单位、校验、提交状态和操作结果就地可见，补充字段定义进入 `?`，不以减少文案为由隐藏风险。
- [ ] 409 冲突要求刷新后人工合并，不自动覆盖他人修改；401、403、429 与 Request ID 继续遵循现有行为。
- [ ] Loading、Empty、Error、Unknown 使用真实请求与能力状态，界面生成文案无 `·` 或 `：`，后端原始错误保持原样。
- [ ] 1440、1280、1024 下三个页签和 Feed 表单无整页横向溢出，键盘与触屏帮助可用。
- [ ] typecheck、lint、相关 Vitest、production build 与 Feed Subscription、Access Role、冲突及布局浏览器验收通过。

## Comments

**Resolved (2026-11-16):** 已在 `f6fbbf7` 实现 Console Shell 与 Feed Subscription，并在合并 Quality 集成后以分支 tip `5458831` 全绿合并为 `f3f019e`。三个 Console 页签保持现有路由与页签行为，Shell、标题、状态与操作区采用共享 D 语言；Feed Subscription 分别呈现 Desired、Observed、Drift 与 Last Error，不用合并状态替代，也不与 WebSocket Session Subscription 混用。API Rate Limit 与 WebSocket Slow Consumer 保持独立设置，未扩展为 Venue、Collector 写队列或存储背压控制；Current Contract 不可用写操作显示真实依赖与禁用原因，Admin API 按服务端 Access Role 开放能力，未增加假登录或角色切换；409 冲突要求刷新后人工合并，401/403/429 与 Request ID 保持既有行为。表单标签、单位、校验、提交状态与操作结果就地可见，补充字段定义进入 `?`。分支 tip `61` 个测试文件 / `515` 个 Vitest 测试通过（本次复核），typecheck、lint、production build 与 `28` 项定向 Playwright 通过；审查修复阶段进一步把两个 flow editor 的共享生命周期与标记拆入 `flow-control-section.tsx`。该票行为随最终集成树通过全量验收（见 09 与 `web/docs/acceptance-report-v2.md`）。
