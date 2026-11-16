# 09: 收缩兼容层并完成全控制台验收

**What to build:** 五个模块全部迁移后，删除已无调用方的旧共享接口和过渡形态，以完整用户任务验证 D 视觉、界面文案、真实状态、帮助与可访问性的一致性。交付报告明确区分真实联调、CONTRACT-ONLY 和未执行项目，不隐藏失败或把原型证据当成生产验收。

**Blocked by:** 01 监控全页建立 D 视觉与共享基础；03 Export Job 完整生命周期完成迁移；05 Service Instance 生命周期操作完成迁移；06 WebSocket Debug 与原始证据完成迁移；07 Quality Gap 与 Backfill Job 完成迁移；08 Security & Audit 完整迁移

**Status:** ready-for-agent

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
