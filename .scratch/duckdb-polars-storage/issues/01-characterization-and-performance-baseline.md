# 01: 建立旧实现的差分测试与性能基线

**What to build:** 在任何生产引擎替换之前，用现有公共接缝固定 Store、REST、Collector Normalization 和 Replay 的可观察行为，并提供可重复的旧实现性能基线。后续 ticket 能用同一批数据判断差异究竟来自新实现错误、非契约偶然行为，还是已批准的独立变化。

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] 建立可复用测试数据，覆盖空 Dataset、单条、单文件、多文件、跨分区、多 Instrument、多 Interval、inactive Manifest 条目和重复记录。
- [ ] 固定 `open_ts` 与 `close_ts` 的半开区间边界、start 等于 end、记录恰好位于 start/end、跨文件相同时间戳和非稳定输入顺序。
- [ ] 固定 REST 的 raw Revisions、JSON 行数上限、LIMIT、Time Cursor、精确 Decimal、UTC 纳秒、Arrow Schema、CSV 文本和稳定错误响应。
- [ ] 固定 Replay 的完整事件序列、Revision/ingest-time tie-break、五键排序、重复订阅以及 asap/realtime/step 的控制与 End of Session 行为。
- [ ] 固定 Collector 批次失败时 Aggregation、HWM 和 freshness 不推进的行为，并记录 CCXT、归档和队列溢出的现有差异。
- [ ] 准备同一 `open_ts` 多个 Revisions 且跨页的样例，供独立 Time Cursor 契约 ticket 使用；不得在本 ticket 修改该行为。
- [ ] 在同一机器、同一数据副本上对旧实现至少测量 5 次，报告中位数与波动，覆盖 Store 扫描/count、REST 全格式、分页、Replay 查询准备和端到端发布。
- [ ] 记录峰值内存和可用的临时磁盘数据，明确区分 Replay 查询成本与 IPC/Python 发布成本。
- [ ] 差分设施只用于测试与迁移，不增加生产引擎开关，也不改变现有公共行为。
