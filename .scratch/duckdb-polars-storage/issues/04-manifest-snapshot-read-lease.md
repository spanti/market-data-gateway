# 04: 为现有读取器引入 Manifest Snapshot 与 Dataset Read Lease

**What to build:** 在继续使用 PyArrow 的情况下，让一次 HTTP 请求或 Replay Session 读取固定 Manifest Snapshot，并通过 Dataset Read Lease 阻止 Compaction/GC 删除仍在使用的 Data Files。先证明快照与维护协议，再让 DuckDB 接入同一协议。

**Blocked by:** 01 建立旧实现的差分测试与性能基线；02 完成 DuckDB 2.0/Polars 类型与生命周期 Spike。

**Status:** ready-for-agent

- [ ] Store 只从 Manifest 选择活动 Data Files，并继续使用 Instrument、Interval、分区与时间范围进行打开前裁剪；不得引入根目录 glob。
- [ ] Lease 流程覆盖候选选择、Lease 获取、Manifest 代际复核和变化时释放重试，读取不会混合不同 Manifest 代际。
- [ ] 同一 HTTP 请求的 count、存在性探测和实际读取共享 Snapshot 与 Lease；不同分页请求不持有跨请求 Lease。
- [ ] Replay Session 在整个生命周期使用固定 Snapshot，包括暂停与 seek，并兼容现有分区保护行为。
- [ ] Lease 持有到 Reader 完成、取消或失败并关闭之后；客户端断开、异常和重复关闭也能释放。
- [ ] 长时间读取使用 heartbeat，能够识别和处理进程异常退出后的过期 Lease。
- [ ] Compaction/GC 遇到有效 Lease 时跳过并报告，未受保护数据仍按 Manifest-first 原则维护。
- [ ] 使用并发测试证明维护切换发生在候选选择、Lease 获取和读取中的不同阶段时，结果始终是一致的新快照或旧快照。
- [ ] 保留现有 Schema Version 校验、稳定错误映射和 raw Revision 行为，不引入 DuckDB 生产查询。
- [ ] 现有 Store、REST、Replay 和维护测试保持通过，并新增成功、异常、取消与过期 Lease 的生命周期测试。
