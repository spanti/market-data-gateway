# 16: 完成 Replay 失败终止协议与 SDK 行为

**What to build:** 让所有 Replay Mode 在 Parquet、Manifest、Schema 或查询资源失败时向客户端发送明确的失败终止，而不是把异常转换成空 Bars 后正常 exhausted。客户端和 SDK 能区分正常完成、主动停止与读取失败，并获得准确的已发送数量。

**Blocked by:** 15 交付有界 step Replay 与 Step Credit 竞态保证。

**Status:** ready-for-agent

- [ ] 扩展现有 End of Session 事件，支持 `reason=error`，并在失败时附带稳定错误对象和实际 `bars_sent`。
- [ ] `exhausted` 与 `stopped` 的现有形状和含义保持不变，失败错误对象只在 `reason=error` 时出现。
- [ ] Schema Version 不匹配映射为现有 `schema_version_mismatch`，其他未分类读取失败映射为 `internal_error`。
- [ ] 公共错误消息经过 sanitization，不包含 SQL、Data File 路径、堆栈或原始 DuckDB/Polars 异常。
- [ ] 损坏 Parquet、Manifest/外部文件异常、Schema 不兼容、资源耗尽和查询取消都不能被转换成空回放或正常 exhausted。
- [ ] 失败前已经发布的 Bars 不回滚、不重发，`bars_sent` 与实际发布数量一致。
- [ ] 失败终止后当前查询代际失效，Reader、连接、执行线程和 Dataset Read Lease 全部释放。
- [ ] SDK 将 `reason=error` 作为失败向调用方报告，不再视为正常回放完成；正常 exhausted/stopped 行为保持。
- [ ] 测试覆盖 asap/realtime/step 的初始读取失败、部分发布后的失败、seek 读取失败、资源失败和控制命令竞态。
- [ ] 公共契约测试证明空 Dataset 与读取失败具有可区分的外部行为，REST 错误协议不受此变化影响。
