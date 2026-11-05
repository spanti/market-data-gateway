# 05: 打通 DuckDB → Store → REST Arrow 的 tracer bullet

**What to build:** 让一条真实 REST Arrow 历史查询通过 Manifest Snapshot、Dataset Read Lease 和 DuckDB 2.0 Relation Arrow Reader 端到端返回结果，作为后续所有 Bars 格式的垂直 tracer bullet。其他表面可以继续使用旧实现，不增加生产运行时开关。

**Blocked by:** 04 为现有读取器引入 Manifest Snapshot 与 Dataset Read Lease。

**Status:** ready-for-agent

- [ ] DuckDB 只接收 Manifest Snapshot 中的显式 Data File 列表，不执行 Dataset 根目录 glob，也不读取 inactive 条目。
- [ ] History 查询继续按 `open_ts` 的半开区间过滤并返回全部 raw Revisions，不添加隐式去重或全局排序。
- [ ] Manifest 的 Instrument、Interval、分区与时间范围裁剪继续发生在扫描前，过滤值参数化且调用方不能注入 SQL 或任意路径。
- [ ] Arrow 响应精确满足 `BAR_SCHEMA`，包括字段顺序、nullability、`decimal128(38,18)`、UTC 纳秒、uint32 和物理文件中省略的 `interval` 恢复。
- [ ] 大结果通过 Relation Arrow RecordBatch Reader 逐批输出，不先物化为完整 Arrow Table、Python rows 或 Polars DataFrame。
- [ ] Read Lease 和连接资源覆盖 Reader 整个生命周期，并在正常结束、异常和客户端取消后释放。
- [ ] 非分页输出不承诺复刻旧物理扫描顺序；差分测试验证逐行内容与重复次数完全一致。
- [ ] 覆盖空结果、单条、多文件、跨分区、时间边界和多个 Revisions 的公共 REST Arrow 测试。
- [ ] 旧实现只保留给尚未迁移的表面和差分测试，新路径没有 `use_duckdb` 之类的生产配置。
- [ ] 与同机旧实现基线比较吞吐、内存和临时磁盘；任何超过批准门槛的回退必须暂停并重新评审。
