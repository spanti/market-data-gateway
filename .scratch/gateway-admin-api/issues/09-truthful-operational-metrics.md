# 09: 补齐真实上游连接、I/O 与受控监控查询

**What to build:** 为 Feed Handler 暴露真实连接状态、心跳/事件、重连和错误；为可实际测量的 API 响应字节与 Collector 存储写入字节增加准确指标。Admin 模式提供经 OIDC 授权的有限 Prometheus 查询模板代理，不开放任意 PromQL。

**Blocked by:** 01, 02

**Status:** ready-for-agent

- [ ] 上游状态来自真实生命周期，Venue 不硬编码，Freshness 不作为连接健康替代。
- [ ] API/存储 I/O 分别命名和计量，无法获得的上游字节继续不可用。
- [ ] ingest_ts-event_ts 只命名为 Ingestion Delay 并说明源时钟偏差。
- [ ] 只有真实协议 RTT 才能命名为 Network Latency，否则保持缺口。
- [ ] Feed Freshness 与 Dataset Freshness 语义不被新指标混淆。
- [ ] 监控代理认证、授权、模板参数、范围上限和错误处理经过测试。
- [ ] 现有 HTTP 分位数注明流式响应耗时，指标标签和重置行为有测试。

## Comments
