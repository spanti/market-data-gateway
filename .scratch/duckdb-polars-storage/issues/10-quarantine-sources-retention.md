# 10: 覆盖 Feed/Archive Quarantine 与 Retention/GC

**What to build:** 将 Quarantine 从 Collector 核心扩展到 CCXT 单行解析和归档验证，并提供可运维的指标、默认 30 天 Retention 与 dry-run 整月 GC。不同来源保持各自已批准的失败原子性，不把所有数据问题都当成同一种清洗失败。

**Blocked by:** 09 在 Collector 批量边界落地 Polars Normalization 和 Quarantine 核心。

**Status:** ready-for-agent

- [ ] CCXT 单行无法解析时，只有成功写入 Quarantine 才能跳过并继续该来源；隔离写入失败时不得越过该记录。
- [ ] CCXT 的闭合识别、修正 Bars、重连、历史分页和队列行为保持不变，Venue 协议逻辑不迁移到 Polars。
- [ ] 归档 checksum、ZIP/CSV 结构、关键字段和来源时间单位失败时，整个归档文件不写入规范 Dataset，并留下可定位来源的 Quarantine 记录。
- [ ] 队列溢出继续通过 HWM 缺口与 Backfill 修复，不生成虚假的 Validation Quarantine Reason。
- [ ] Quarantine 指标按稳定 Source Stage 和 Reason Code 聚合，不使用异常文本、文件路径、request ID 等高基数标签。
- [ ] Quarantine Retention 默认 30 天，可配置，0 表示永久保留；容量与写入失败状态可观测并触发告警。
- [ ] GC 默认 dry-run，只删除超过 Retention 的完整 UTC 月份，跳过当前月，不引入自动后台维护任务。
- [ ] GC 不影响 Bars、Instrument Reference Data、HWM 或正常 Dataset Manifest，也不会让 Quarantine 进入正常查询。
- [ ] 测试覆盖隔离成功后来源继续、隔离失败时停止消费、归档整文件原子性、永久保留、月份边界和当前月保护。
