# 03: 实现真实监控仪表盘与能力缺口呈现

**What to build:** 使用真实 Service `/status`、Prometheus 和 Feed/Dataset Freshness 构建监控页，呈现 QPS、Peak QPS、HTTP 分位数、Service Instance、Freshness 和上游状态；指标不存在时明确显示缺口，不生成演示趋势。

**Blocked by:** 02

**Status:** ready-for-agent

- [ ] 统一时间范围控制作用于所有趋势，显示统计窗口、来源和最后成功更新时间。
- [ ] `/status` 5 秒与 Prometheus 15 秒默认刷新可配置，旧数据不继续显示健康。
- [ ] QPS/Peak/HTTP P50/P95/P99 查询与单位准确，HTTP 延迟注明包含流式响应。
- [ ] Feed Freshness 与 Dataset Freshness 分开，倒计时使用服务端 age/threshold/stale。
- [ ] Venue 从真实数据派生，连接/心跳/重连/网络/IO 缺失不做推断。
- [ ] 图表具有图例、表格读数、键盘/hover、非颜色状态标识，并符合选定视觉系统。
- [ ] 使用本地真实服务与 Prometheus 做集成验收，记录任何指标缺口。

## Comments
