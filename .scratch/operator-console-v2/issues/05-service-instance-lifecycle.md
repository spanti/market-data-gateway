# 05: Service Instance 生命周期操作完成迁移

**What to build:** SuperAdmin 可以在 D 风格的 Service Instance 页面中安全执行 Start、Graceful Stop 和 Reload，清楚看到目标实例、操作原因、确认要求与真实执行状态。其他 Access Role 看到准确权限边界，现有高危操作与审计行为不变。

**Blocked by:** 04 Console Shell 与 Feed Subscription 完成迁移

**Status:** ready-for-agent

- [ ] Service Role 与 Service Instance 在标题、表格、状态和帮助中严格区分，不出现 Master、Worker、Leader 或分片语义。
- [ ] Start、Graceful Stop、Reload 使用共享卡片和状态药丸，目标、原因、危险后果、确认与操作结果就地可见。
- [ ] Graceful Stop 和 Reload 继续要求输入目标 Service Instance 名，所有三类操作要求原因和确认并防止重复提交。
- [ ] SuperAdmin 才能执行生命周期操作，Operator 与 Auditor 的禁用原因和只读能力准确，前端权限展示不替代服务端授权。
- [ ] 待提交、提交中、accepted/running、succeeded、failed 与状态未知保持区分，写操作默认不自动重放。
- [ ] Reload 或 Stop 导致断连时可进入状态未知，不直接判为失败；PID 存在不等于 Ready，Reload 不宣传热加载或零停机。
- [ ] 表单校验、401/403/409/429、网络错误与 Request ID 就地显示，补充概念进入 `?`，界面生成文案无 `·` 或 `：`，后端原始错误保持原样。
- [ ] 1440、1280、1024 下实例列表与确认对话框无整页横向溢出，键盘可完成或取消操作。
- [ ] typecheck、lint、相关 Vitest、production build 与生命周期、权限、高危确认及状态未知浏览器验收通过，CONTRACT-ONLY 明确标注。

## Comments
