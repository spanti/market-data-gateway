# 09: 在 Collector 批量边界落地 Polars Normalization 和 Quarantine 核心

**What to build:** 让现有 Collector 的一批 Raw Bars 通过 Polars 完成与旧实现等价的 Normalization/Validation，然后才进入 Instrument 校验、Revision merge 和 Bar Aggregation。失败批次不发布且留下独立 Quarantine 审计记录，Quarantine 写入失败时整批 fail-closed。

**Blocked by:** 07 迁移单 Instrument Time Cursor 分页；08 迁移 CSV、export 与流式失败行为。

**Status:** ready-for-agent

- [ ] Polars 在现有 Collector 批量边界运行，不为每条实时消息创建单行 DataFrame，也不改变 Feed Handler 的协议状态机。
- [ ] 批量结果与旧标量 Normalization 逐字段等价，包括 InstrumentId、Bar Interval、closed-Bar 检查、UTC、close/event/ingest 时间、Decimal scale 和固定 envelope 字段。
- [ ] float-shaped 数值、未闭合 Bars、非法 Interval 和超额小数精度继续被拒绝，不发生隐式 float 转换或舍入。
- [ ] Normalization 和 Instrument Validation 在 Aggregation 状态推进之前完成；任何失败都保持 Aggregation、HWM、freshness 和持久化结果不动。
- [ ] 建立独立、版本化 Quarantine Dataset，记录 Source Stage、稳定 Reason Code、来源定位、结构化详情和可获得的原始记录表示。
- [ ] Quarantine 使用独立 Schema，不会被正常 Bars Store、REST 或 Replay 注册、扫描或导出。
- [ ] Quarantine 写入失败时对应批次不得被确认消费、跳过或发布，必须暴露明确错误和失败指标。
- [ ] Reason Code 与 Source Stage 为稳定低基数词汇，异常文本仅作为 sanitized 诊断详情，不能用于程序分支或指标标签。
- [ ] 保留旧标量实现仅用于本阶段差分测试，不增加生产 Normalization 引擎开关；最终删除由收缩 ticket 完成。
- [ ] 覆盖空批、单行、多行、混合 Instrument、失败重试和 Quarantine 故障的端到端 Collector 测试。
