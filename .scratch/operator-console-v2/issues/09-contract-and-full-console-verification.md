# 09: 收缩兼容层并完成全控制台验收

**What to build:** 五个模块全部迁移后，删除已无调用方的旧共享接口和过渡形态，以完整用户任务验证 D 视觉、界面文案、真实状态、帮助与可访问性的一致性。交付报告明确区分真实联调、CONTRACT-ONLY 和未执行项目，不隐藏失败或把原型证据当成生产验收。

**Blocked by:** 01 监控全页建立 D 视觉与共享基础；03 Export Job 完整生命周期完成迁移；05 Service Instance 生命周期操作完成迁移；06 WebSocket Debug 与原始证据完成迁移；07 Quality Gap 与 Backfill Job 完成迁移；08 Security & Audit 完整迁移

**Status:** resolved

- [ ] 旧共享卡片、标题、描述、状态和帮助兼容形式只在确认无调用方后删除，收缩过程不改变五模块外部行为。
- [ ] 所有界面生成的标题、标签、状态、说明、占位符、校验、错误前缀与动态拼接文本在可见 DOM 中无 `·` 或 `：`。
- [ ] 符号检查覆盖五模块默认页、Console 三个页签、展开的 `?`、表单校验、危险确认、Loading、Empty、Error、Unknown 和操作结果。
- [ ] WebSocket 原始 JSON、Operator Audit Event 脱敏 Diff 与后端原始错误使用含禁用符号样例验证原样展示，不被全控制台清理误伤。
- [ ] 五模块不存在整页装饰分割线或数字下划线标题，卡片、状态药丸和简洁标题一致，必要风险、权限、错误、结果和 Request ID 未藏入帮助。
- [ ] 共享帮助在五模块均支持 hover、focus、触屏重复 tap、Escape、可访问名称、碰撞处理和不遮挡目标，图表数值 Tooltip 与帮助 Tooltip 不混淆。
- [ ] 1440 与 1280 完整可用，1024 无整页横向溢出；Console 页签、宽表、确认框和图表读数均纳入真实浏览器检查。
- [ ] 浅色 D 截图审查通过，现有暗色主题保持基本可读，状态不只依赖颜色，键盘与触屏关键任务可完成。
- [ ] 真实状态不会把旧 KPI 或旧健康药丸继续标为当前健康，保留缓存时具有最后成功时间和过期语义。
- [ ] typecheck、lint、完整 Vitest、production build 和完整 Playwright 套件通过，打包结果经过检查，无未评估的新 bundle 退化。
- [ ] Current Contract 使用真实本地 FastAPI、测试 Dataset、Prometheus 和 WS 验收；Admin API 仅有契约证据时明确标注 CONTRACT-ONLY，不宣称真实 OIDC 全链路通过。
- [ ] 生产树不包含 throwaway 原型路由、Variant、切换器或模拟状态，原型与截图继续只作为 throwaway 分支设计证据。
- [ ] 记录实际执行命令、通过数量、截图、CONTRACT-ONLY、未执行项与失败项，旧功能 ticket 和历史验收记录不被篡改。

## Comments

**Resolved (2026-11-16):** 已在 `c77da37` 完成并合并为 `ca1109e`；随后审查修复 `02ada44`/`e0b77c9` 以 `5f37ce5` 合并，且 `5f37ce5` 与完全通过测试的 review-fix tip `e0b77c9` 树完全一致。按零调用方证据删除 `KpiCell`、`ModulePlaceholder`、`FilterRow`/`Toolbar`、`EventTimeline`、`Panel`/`PanelHeader`/`PanelTitle`/`PanelDescription` 与未使用的 badge/button variants，并移除 `PageHeader.meta`、`ChartPanel.description`、`TrendChart.description`、`HelpTrigger.className`/`contentClassName` 等兼容接口；五模块外部行为不变。全控制台符号检查覆盖五模块默认页、三个 Console 页签、展开的 `?`、表单校验、危险确认、Loading/Empty/Error/Unknown 与操作结果，并扫描可见文本、placeholder、aria 属性与 `sr-only` 文本；WebSocket 原始 JSON、Audit 脱敏 Diff 与后端原始错误经含禁用符号样例验证原样展示。09 分支 tip `61` 个测试文件 / `544` 个 Vitest 测试通过（本次复核），完整真实栈 Playwright `56/56`；审查修复后最终门禁为 typecheck PASS、lint PASS、`65` 个 Vitest 文件 / `560` 个测试 PASS、production build PASS、完整 Playwright `56/56`（`37` REAL、`19` CONTRACT-ONLY）。截图位于 `web/docs/acceptance-v2/`，完整命令、计数、CONTRACT-ONLY、BACKEND-BLOCKED、NOT RUN、偏差与 bundle 评估记录在 `web/docs/acceptance-report-v2.md`。主 chunk 为 `564.32 kB` / `168.33 kB` gzip，对比 v2 前基线 `541.80 kB` / `162.22 kB`；保留经评估的 `manualChunks` vendor split，并保留仅链接 worktree 生效的开发期 `fs.allow`，两者均在报告中说明。审查修复共解决 16 项 Standards/Spec findings，并将 Monitoring Current Contract 的按 Role 行文案由 Service Instance 更正为 Service Role，Admin 行仍标 Service Instance。
