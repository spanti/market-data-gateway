# 06: WebSocket Debug 与原始证据完成迁移

**What to build:** 排障人员可以在 D 风格的 WebSocket Debug 中建立现有 WebSocket Session Subscription、筛选有界报文并检查原始 JSON，准确识别断线、界面丢弃和服务端 lag_notice。界面文案遵守符号禁令，原始证据保持逐字准确。

**Blocked by:** 04 Console Shell 与 Feed Subscription 完成迁移

**Status:** resolved

- [ ] WebSocket Session Subscription 与 Feed Subscription 在布局和帮助中明确区分，只筛选当前会话 Topic，不暗示会改变实际采集范围。
- [ ] Topic 提示、subscribe/unsubscribe、ping/pong、连接与接收时间、筛选、清空和暂停自动滚动采用共享卡片与状态模式。
- [ ] 订阅没有 ACK，ping/pong 后只显示已同步，不显示订阅已确认；Auditor 仅使用受控 Topic 表单，自由 JSON 仍按现有权限开放。
- [ ] 内存最多保留 10,000 条并继续使用虚拟列表，本地界面丢弃与服务端 lag_notice 分开显示，报文不持久化。
- [ ] 断线明确创建不连续接收段，不宣称无缝恢复；Loading、Empty、Error、Unknown 反映真实会话状态。
- [ ] 原始 JSON 详情作为文本安全渲染，包含 `·` 或 `：` 的样例仍原样展示，周围界面生成文案不包含这两个符号。
- [ ] 必要状态、权限、校验、断线和操作结果就地可见，补充概念进入支持 hover、focus、touch 和 Escape 的 `?`。
- [ ] 1440、1280、1024 下虚拟列表、原始详情和操作控件无整页横向溢出，键盘与触屏帮助可用。
- [ ] typecheck、lint、相关 Vitest、production build 与真实 WS、断线、有界列表、权限、原始证据及布局验收通过。

## Comments

**Resolved (2026-11-16):** 已在 `eab9b58` 实现，并在合并最新 integration 后以分支 tip `80bd301` 全绿合并为 `8db73b0`。WebSocket Debug 与 Feed Subscription 在布局和帮助中明确区分，只筛选当前会话 Topic，不暗示改变实际采集范围；subscribe/unsubscribe、ping/pong、连接与接收时间、筛选、清空和暂停自动滚动采用共享卡片与状态模式。订阅没有 ACK，ping/pong 后只显示已同步，不显示订阅已确认；Auditor 仅使用受控 Topic 表单，自由 JSON 按现有权限开放。内存最多保留 10,000 条并继续使用虚拟列表，本地界面丢弃与服务端 `lag_notice` 分开显示，报文不持久化；断线明确创建不连续接收段，不宣称无缝恢复。原始 JSON 详情作为文本安全渲染，含 `·` 或 `：` 的样例以 `data-raw-evidence` 原样展示，周围界面生成文案保持干净。分支 tip `61` 个测试文件 / `531` 个 Vitest 测试通过（本次复核），typecheck、lint、production build 与 `18/18` 定向 Playwright 通过，包含真实 WebSocket 会话、断线、有界列表、权限、原始证据及布局验收。
