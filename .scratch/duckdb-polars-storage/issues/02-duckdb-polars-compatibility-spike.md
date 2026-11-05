# 02: 完成 DuckDB 2.0/Polars 类型与生命周期 Spike

**What to build:** 用不接入生产代码的真实验证程序确定 DuckDB 2.0.x LTS 和候选 Polars 版本能否无损承载项目 Data Contract，并证明流式读取、取消和资源生命周期可满足后续设计。输出明确的 go/no-go 结论，而不是根据库文档推断兼容性。

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] 在项目使用的 Python 3.13 环境验证 DuckDB 2.0.x LTS 和候选 Polars 版本，记录精确版本与平台；DuckDB 1.x 不得作为后备。
- [ ] 验证 `decimal128(38,18)` 的正负高精度值、边界值、精确文本、null 和超额 scale 拒绝，任何路径都不得经过 float64 或静默舍入。
- [ ] 验证 UTC 纳秒时间包含 1ns、999ns、1000ns、123456789ns、epoch 前后和 null，覆盖 Parquet → DuckDB → Arrow → Polars 及反向路径。
- [ ] 验证 uint32 最小/最大值、nullable `seq`、nullable `trade_count`、空结果和完整 `BAR_SCHEMA` 的字段顺序与 nullability。
- [ ] 证明 Relation API 的 Arrow RecordBatch Reader 真正逐批输出，并确认未使用会先复制完整结果的 DBAPI reader。
- [ ] 对长时间查询验证取消、Reader 提前关闭、重复取消和连接复用，确认旧查询停止且连接、线程与临时文件能够释放。
- [ ] 在低 memory limit、明确 temp directory、不同线程数下执行排序/聚合，记录流式行为、临时文件、资源错误和清理结果。
- [ ] 验证文件在首次读取前和部分批次输出后被删除/重命名的行为；不得把结果当作事务保证，Read Lease 仍须覆盖整个 Reader 生命周期。
- [ ] 在 Windows 与 Linux 上执行关键文件生命周期实验，或明确记录无法覆盖的平台并给出阻塞后续实施的验证安排。
- [ ] 形成书面 go/no-go 结论；任何精度损失、隐式浮点转换、不可取消旧查询或资源泄漏都视为失败，不得通过降低契约绕过。
- [ ] 不修改生产读取器、Normalization 或公共 API。
