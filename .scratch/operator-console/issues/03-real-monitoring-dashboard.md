# 03: 实现真实监控仪表盘与能力缺口呈现

**What to build:** 使用真实 Service `/status`、Prometheus 和 Feed/Dataset Freshness 构建监控页，呈现 QPS、Peak QPS、HTTP 分位数、Service Instance、Freshness 和上游状态；指标不存在时明确显示缺口，不生成演示趋势。

**Blocked by:** 02

**Status:** resolved

- [ ] 统一时间范围控制作用于所有趋势，显示统计窗口、来源和最后成功更新时间。
- [ ] `/status` 5 秒与 Prometheus 15 秒默认刷新可配置，旧数据不继续显示健康。
- [ ] QPS/Peak/HTTP P50/P95/P99 查询与单位准确，HTTP 延迟注明包含流式响应。
- [ ] Feed Freshness 与 Dataset Freshness 分开，倒计时使用服务端 age/threshold/stale。
- [ ] Venue 从真实数据派生，连接/心跳/重连/网络/IO 缺失不做推断。
- [ ] 图表具有图例、表格读数、键盘/hover、非颜色状态标识，并符合选定视觉系统。
- [ ] 使用本地真实服务与 Prometheus 做集成验收，记录任何指标缺口。

## Comments

**Resolved (2026-11-09):** 合并提交 `1401b7b`。真实联调通过：本机 `mdgw up` + collector/api/replay 与 `prom/prometheus:v2.55.1`，经同源代理取得 QPS 0.392–0.426、Peak QPS 2.572、P50 3.2 ms、P95 21.5 ms、P99 24.3 ms；Feed Freshness 使用服务端 `age_s/threshold_s/stale` 与阈值剩余倒计时，Dataset Freshness 独立；Venue 由真实 instruments/feed 派生；连接/心跳/重连/网络/I/O 明确显示未暴露；Prometheus 停止后趋势显示未知且不保留健康状态。Admin monitoring proxy 仅契约测试（后端不存在）。代码评审修复：Admin 模式不再直连 Prometheus、版本不兼容时禁用、统一能力门控。最终套件 458 tests；typecheck/lint/build 通过。
