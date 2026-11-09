# 05: 实现 Console 控制状态与有界 WebSocket 调试台

**What to build:** 建立 Feed Subscription、Service Instance、WebSocket Debug 三个 Console 视图。Current Contract 真实实现 WS 会话订阅和服务状态，管理写功能按 capabilities 禁用；Admin 模式实现 Desired/Observed、流控、生命周期确认和异步操作状态。

**Blocked by:** 02

**Status:** ready-for-agent

- [ ] Feed 与 WS Session Subscription 不混淆，409 和 Drift 可见，删除不影响历史数据的语义清楚。
- [ ] API Rate Limit 与 WebSocket Slow Consumer 分开，无通用模糊阈值。
- [ ] Start/Graceful Stop/Reload 符合角色、原因、实例名确认、防重复和不自动重试。
- [ ] 已接受不等于成功，Reload/Stop 断连进入真实查询或状态未知。
- [ ] WS 支持受控 Topic 表单、白名单 JSON、ping/pong 屏障、lag_notice 和安全 JSON 详情。
- [ ] 10,000 条虚拟列表、本地/服务端丢弃分离、筛选/清空/暂停自动滚动可用。
- [ ] Auditor 可建立只读会话订阅但不能使用自由 JSON；所有角色矩阵经组件与契约测试。

## Comments
