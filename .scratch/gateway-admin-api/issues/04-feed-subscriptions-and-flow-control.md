# 04: 实现版本化 Feed Subscription 与两类动态流控

**What to build:** 以管理 SQLite 保存运行时 Feed Desired 状态，通过受控 Collector 通道应用并回报 Observed/Drift；支持乐观版本、冲突、审计和重启恢复。分别实现 API Rate Limit 与 WebSocket Slow Consumer 动态设置，不改 TOML/YAML。

**Blocked by:** 01, 02, 03

**Status:** ready-for-agent

- [ ] InstrumentId/Venue/bar/Interval 和重复订阅经过校验，Instrument Reference Data 保持只读。
- [ ] Desired/Observed/Drift/Last Error 和版本冲突真实可见，Collector 确认才算生效。
- [ ] 下发失败不伪造自动回滚，重启恢复持久期望状态。
- [ ] 删除订阅不删除历史 Bar、Instrument 或审计。
- [ ] 两类流控独立 schema、版本和审计，可信代理解析真实来源 IP。
- [ ] 不支持热应用的配置在 capabilities 中明确，Venue/写队列/背压不纳入。
- [ ] 并发、Collector 拒绝、重启和冲突集成测试完整。

## Comments
