# 17: 迁移剩余 Store 调用方并删除旧生产路径

**What to build:** 将 Backfill 等剩余 Store 调用方统一切换到新的 Manifest-aware 读取接口，然后收缩旧实现：删除手工 Parquet 行过滤、重复标量 Normalization 和 Replay 全量物化状态。最终生产环境只有一套 Bars 读取与 Normalization 语义。

**Blocked by:** 16 完成 Replay 失败终止协议与 SDK 行为。

**Status:** ready-for-agent

- [ ] Backfill 和其他 Store 调用方都通过新 History/Store 查询接口读取，不直接拼接 SQL、文件路径或私有 DuckDB 对象。
- [ ] Backfill 的 identity、Revision merge、HWM、缺口检测和 Aggregation 行为保持不变，并复用现有 Backfill 集成测试证明。
- [ ] 删除生产 Bars API/Replay 中旧的 PyArrow compute 行过滤、Python close-time/Revision/排序过滤和不再使用的全量读取路径。
- [ ] 删除重复的生产标量 Normalization 规则，保留 Polars 批量实现作为唯一规则来源。
- [ ] 删除 Replay 完整 Bars 和完整 close-time 列表及其不再使用的控制分支，三种 Replay Mode 都使用有界实现。
- [ ] 删除迁移专用生产分支和任何引擎开关；发布后回退通过回退软件版本完成。
- [ ] 迁移用旧实现和差分设施可以从生产路径移除；必要的测试工具不得成为运行时后备。
- [ ] 保留 PyArrow 在 Schema、Data File 写入、Arrow IPC/ZMQ、export、Compaction 和合法测试中的使用，不做技术栈纯净式删除。
- [ ] 检查正常 API/Replay 不会发现或过滤 Quarantine，Instrument Snapshot 与示例仍保持明确的本项目范围边界。
- [ ] 完整离线套件、Data Contract、Manifest 维护、REST、Normalization 和 Replay 测试全部通过。
