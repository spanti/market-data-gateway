# 01: 监控全页建立 D 视觉与共享基础

**What to build:** 内部运维人员可以通过五模块顶部导航进入完整 D 监控页，用统一白色卡片、状态药丸和按需 `?` 帮助查看 KPI、趋势、异常与数据明细。共享视觉与交互能力随这个真实页面交付，其他模块可在后续票复用，页面继续消费现有真实监控数据与权限能力。

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] 五模块顶部导航保持现有路由与权限行为，浅色 D 使用中性画布、白底微圆角卡片和极浅阴影，不使用整页装饰分割线或数字下划线标题。
- [ ] 共享 Metric Card、Chart Card、Status Badge、Help Trigger 和状态区域形成可复用接口，同时保留其他尚未迁移模块可继续使用的兼容形式。
- [ ] `?` 帮助支持鼠标 hover、键盘 focus、触屏重复点击、Escape 关闭和碰撞处理，文本可读且不遮挡触发目标；必要风险与操作反馈不进入悬浮层。
- [ ] Realtime QPS、Peak QPS、HTTP P99 与独立 Dataset Freshness 使用 KPI 卡，QPS 与 HTTP P50/P95/P99 使用独立趋势卡并保留键盘读数和数据表。
- [ ] Feed Freshness、Service Instance、页面内告警、上游状态与数据明细完成 D 布局，Feed Freshness 和 Dataset Freshness 不合并，Service Role 和 Service Instance 不混用。
- [ ] 上游连接、心跳、重连与网络能力未暴露时明确显示未知，不从 Freshness 推断连接健康。
- [ ] 内部 series identity 与显示名称分离，显示名称移除禁用分隔符后仍读取正确指标，并有 identity 与 label 可独立变化的回归测试。
- [ ] Loading、Empty、Error、Unknown 由真实请求与能力状态驱动，不把旧快照继续标为当前健康；保留缓存时显示最后成功时间与过期语义。
- [ ] 已迁移区域的界面生成文案无 `·` 或 `：`，必要标签、单位、状态、错误和 Request ID 保持就地可见，后端原始错误不被改写。
- [ ] 1440 与 1280 完整可用，1024 无整页横向溢出；状态不只依赖颜色，现有暗色主题保持基本可读。
- [ ] 不直接复制 throwaway 原型，不引入生产 Mock、Variant 切换器、模拟状态 URL、新组件库或全局状态库。
- [ ] typecheck、lint、相关 Vitest、production build 与监控真实浏览器验收通过，并记录实际结果。

## Comments
