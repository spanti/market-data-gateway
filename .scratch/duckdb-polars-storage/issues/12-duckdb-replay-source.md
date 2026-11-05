# 12: 用 DuckDB 替换 Replay Source，暂时保留物化

**What to build:** 让 Replay Source 通过 DuckDB 在固定 Manifest Snapshot 上完成 close-time 过滤、Revision 选择、重复订阅消解和五键排序，同时暂时保留现有全量物化与 Session 控制循环，以完整事件序列证明查询等价。

**Blocked by:** 10 覆盖 Feed/Archive Quarantine 与 Retention/GC；11 打通 Normalization Rule Version 血缘。

**Status:** ready-for-agent

- [ ] Replay 继续按 `close_ts` 的 `[start_ts,end_ts)` 过滤，并正确利用 Bar Interval 扩展物理 `open_ts` 扫描起点，不遗漏窗口开头的 Bars。
- [ ] 每个 `(instrument_id, interval, open_ts)` 只保留最大 Revision，同 Revision 继续选择最新 `ingest_ts`。
- [ ] 重复订阅和重叠 Data Files 不会产生重复 Replay 事件，也不会把 REST 的 raw Revision 语义带入 Replay。
- [ ] 输出严格使用 `close_ts`、`instrument_id`、数值 seq 先于 null、`interval`、`open_ts` 的五键全序。
- [ ] Replay Session 从创建到结束固定 Manifest Snapshot，运行期间新发布的 Data Files 不会混入。
- [ ] Dataset Read Lease 继续保护整个 Session，包括初始读取、暂停和现有 seek 行为。
- [ ] 暂时保留全量 Bars 和 close-time 数组以及现有 asap/realtime/step 控制循环，不在本 ticket 同时重写控制面。
- [ ] 对所有 Replay Mode 比较旧新完整事件序列、控制事件、bars_sent 和 End of Session，不只比较条数。
- [ ] 覆盖空结果、start/end 边界、多个 Instrument/Interval、可空 seq、重复订阅、Revision tie 和 seek 越过末尾。
- [ ] 分开报告 DuckDB 查询准备与端到端发布性能，不将 IPC/Python 发布成本误算为扫描成本。
