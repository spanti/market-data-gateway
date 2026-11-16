# 02: Bar 查询、图表、Revision 与直接 CSV 完成迁移

**What to build:** 数据分析人员可以在 D 风格的查询与图表卡片中按 InstrumentId、Bar Interval 和显式时间范围完成查询，对照最大 Revision 图和全部原始 Revision，并通过现有小范围 CSV 路径下载未被前端改写的数据。共享查询 URL、时间、分页和实时跟随行为保持不变。

**Blocked by:** 01 监控全页建立 D 视觉与共享基础

**Status:** resolved

- [ ] 查询表单、Bar 图、Revision 明细和直接 CSV 区域采用共享卡片、状态药丸、简洁标题与 `?` 帮助，不保留数字标题、整页装饰分割线或冗余说明。
- [ ] 五种 Bar Interval、默认最近 24 小时、单次图表最多 31 个 UTC 天、start 闭区间与 end 开区间保持现有行为，无数据时不自动扩大范围。
- [ ] 图、原始 Revision 表、CSV 与 URL 查询参数共享同一上下文，浏览器前进后退与可分享链接不回退。
- [ ] 最大 Revision 仅用于图，原始 Revision 表与 CSV 不去重，服务端列顺序和精确十进制文本保持不变。
- [ ] Revision-safe Cursor 保持不透明并按原契约回传，raw Revision 分页不重不漏；不重新引入 v1 Time Cursor 修复或 offset 分页。
- [ ] UTC 与浏览器本地时间切换、ECharts 键盘读数、数据表替代和可视缩放保持可用，视觉样式变化不改变数据语义。
- [ ] 实时跟随仍需用户明确开启且仅用于当前范围，断线标记新接收段不连续，不宣称无缝恢复。
- [ ] 小范围 CSV 保留安全文件名、未知长度进度、取消、Blob 错误信封和 Request ID 行为，不以前端重新序列化替代下载。
- [ ] 权限、能力未知、校验、加载、空、错误和操作结果就地显示，字段定义进入 `?`，界面生成文案无 `·` 或 `：`，后端原始错误保持原样。
- [ ] 1440、1280、1024 下查询和图表可操作且无整页横向溢出，宽表仅在局部滚动。
- [ ] typecheck、lint、相关 Vitest、production build 与 Bar 查询、图表、分页、实时跟随及真实下载浏览器验收通过。

## Comments

**Resolved (2026-11-16):** 已在 `4c598cc` 完成并合并为 `b3c68f1`。Bars 查询、图表、原始 Revision 表、直接 CSV 与实时跟随迁移到共享 Card/ChartPanel、HelpTrigger 与 StatusBadge；五种 Bar Interval、默认最近 24 小时、图表最多 31 个 UTC 天、start 闭区间与 end 开区间、URL 与浏览器前进后退共享上下文保持不变。最大 Revision 仅用于图，原始 Revision 表与 CSV 不去重，服务端列顺序与精确十进制文本保持原样；Revision-safe Cursor 继续以不透明 `next_cursor` 原样回传。修正了既有 `bars-chart-cursor.spec.ts` 中针对 `next_start`/Time Cursor 缺陷的过时断言，改为当前 opaque cursor 行为，并以真实栈证明 5001 个 Revision 恰在两个 opaque-cursor 页内完成、唯一 cursor 原样回传、无重复无缺口且无旧 Time Cursor 回退；原始 JSON 与后端错误以 `data-raw-evidence` 原样展示。CSV 保留安全文件名、未知长度进度、取消、Blob 错误信封与 Request ID 行为。分支 tip `61` 个测试文件 / `495` 个 Vitest 测试通过（本次复核），typecheck、lint、production build 与 `10/10` 定向 Playwright 通过，真实 Bar 查询、图表、分页、实时跟随及下载均纳入验收。
