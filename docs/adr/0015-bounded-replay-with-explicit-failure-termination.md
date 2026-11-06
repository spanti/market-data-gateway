# 有界 Replay：批次、查询代际与显式失败终止

Replay 不再在发布第一条 Bar 之前把整个时间范围的 Bars 与其 close-time 数组读入内存。每个 Replay Session 在创建时获取一个固定的 Manifest Snapshot 与 Dataset Read Lease，然后消费 DuckDB 查询产出的有界 Arrow RecordBatch 流；`asap`、`realtime` 和 `step` 共用这一条读取路径，分别由“不节流”“Replay Clock”和“Step Credit”控制发布节奏。读取失败不再被转换成空回放或正常 `exhausted`，而是以 `reason="error"` 和稳定的 sanitized 错误对象终止 Session。这项决定同时解决大范围回放的进程内存随结果行数线性增长，以及“损坏数据看起来像没有数据”的公共行为歧义。

## 有界批次与有界预取

DuckDB 在一个查询中完成 `close_ts` 过滤、最大 Revision/最新 `ingest_ts` 选择、重复订阅消解与五键排序，并以 `relation.to_arrow_reader(batch_size=...)` 逐批输出。生产者线程把每个 RecordBatch 转成一批 `Bar`，放入容量为 `[replay].prefetch_batches` 的有界 FIFO；消费者按 Replay Mode 发布。队列和正在转换/发布的批次共同限制在途数据，约为 `batch_size × (prefetch_batches + 2)` 条 Bar，而不是总结果行数。默认 `batch_size = 4096`、`prefetch_batches = 2`，`[replay].max_sessions = 4` 限制单进程并发 Session 数。

`asap` 只是不启动 Replay Clock 节流，仍然逐批读取、发布，不会先把全部结果物化。`realtime` 和 `step` 使用同一流，只是在每批内部按 Clock 或 Credit 等待。只有查询真正耗尽且预取中的 Bar 全部发布后才发送 `reason="exhausted"`；生产者的异常按流顺序传给消费者并重新抛出，不会被当作流结束。取消、暂停或 seek 导致的提前结束不会报告 `exhausted`，因此 `stopped` 与 `exhausted` 不会互相冒充。

## seek 在同一 Snapshot 上的查询代际

一个 Replay Session 只获取一次 Snapshot 与 Lease，它们覆盖整个 Session，包括暂停和之后的 seek。每次 seek 在既有暂停状态下递增查询代际，并记录新的起始 `close_ts`；当前代际的 `ReplayScanCancellation` 关闭该代际的 DuckDB scanner，中断阻塞中的 `read_next_batch()`，释放该查询的 Reader 与连接。随后 worker 在同一个 `StoreRead`、同一个 Manifest Snapshot 上打开新代际的查询。旧代际此后返回的批次、错误和结束信号都被丢弃，不会发布 Bar、消耗 Credit 或改写 Clock。

seek 前授予的 Step Credit 被丢弃，seek 后授予的 Credit 保留，即使新代际的读取尚未完成。`realtime` 的 Clock 在新代际首个可读批次准备好时才重新锚定到 seek 位置，因此磁盘 I/O 不消耗虚拟时间；暂停期间 Clock 保持冻结。读取期间的 `pause`、`resume`、`set_speed`、`stop` 与 `step` 继续有效；`seek` 只允许在暂停状态执行，`asap` 不支持 seek。重复订阅在查询前折叠为不同的 `(instrument_id, interval)` 分支，查询内用 `UNION ALL` 组合，因此重复订阅不会产生重复事件。

## 取消、Reader、连接与 Lease 的生命周期

一个 DuckDB 连接在任一时刻只服务一个 Reader；scanner 关闭会从另一个线程 `interrupt()` 当前查询，并由拥有阻塞调用的工作线程回收 Reader 和连接。Reader 在耗尽、异常、取消时都会关闭；正常耗尽的连接最多回到单个空闲缓存，失败或取消的连接直接关闭。Replay Session 的 `StoreRead.close()` 在 worker 的 `finally` 中无条件执行，负责关闭活跃 scanner、取消预取并删除 Dataset Read Lease；心跳线程在此之前保持 Lease 新鲜，因此 Compaction/GC 不会在 Session 中途删除文件，暂停再久也一样。

Session 的公共终止事件恰好一次。`stop` 与读取失败竞态时以 `stopped` 终止且不带错误对象；被 seek 取代的旧代际失败被静默丢弃；End of Session 发送失败只记录日志，不能产生第二个终止事件。失败后 Session 进入终态，控制命令被拒绝，`stop` 保持幂等；Reader、连接、执行线程和 Lease 均已释放，Store 可立即复用。

## 显式失败终止

读取失败复用现有 End of Session 事件：

```json
{
  "op": "end_of_session",
  "session_id": "rpl-...",
  "reason": "error",
  "bars_sent": 42,
  "error": {"code": "internal_error", "message": "replay read failed"}
}
```

`error` 对象只在 `reason="error"` 时出现。Dataset `schema_version` 不匹配映射为既有的 `schema_version_mismatch`，消息固定为 `Dataset schema version is not supported`；损坏 Parquet、Manifest/Data File 缺失、Arrow 读取失败、资源耗尽与未分类查询失败统一映射为 `internal_error`，消息固定为 `replay read failed`。公共消息不包含 SQL、Data File 路径、堆栈或原始 DuckDB/Polars 异常文本，真实异常保留在服务端日志与 Session 诊断字段中。`reason` 的取值为 `exhausted`、`stopped` 或 `error`。

`bars_sent` 在终止时读取，等于失败前实际发布给客户端的 Bar 数；已经发布的 Bars 不回滚、不重发，因此客户端可以把部分回放与完整回放精确区分。失败后 Session 不能恢复或继续 seek。SDK 对 `reason="error"` 抛出 `ReplayFailedError`（携带 server `code`、`message` 与 `bars_sent`），后续读取继续抛出同一失败；正常 `exhausted` / `stopped` 仍以 `StopIteration` 结束。REST 的错误 envelope 不受这条 Replay 协议变化影响。

## 兼容性影响

这是有意的公共行为变化，但只增加明确性：此前无法区分的“空数据集”和“读取失败”现在分别是 `exhausted` 与 `reason="error"`；`exhausted` 与 `stopped` 的事件形状保持原样。解析 End of Session 的客户端必须处理新的 `reason` 值；忽略未知 reason 的客户端可能把失败当普通结束，因此 SDK 的默认行为改为抛错。`bars_sent` 在失败路径上成为精确计数，客户端应把它当作已消费的部分结果而不是总数。错误码沿用既有稳定错误码，不新增协议错误码；消息文本 sanitized，不能用于程序分支。回退到旧的全量物化行为只能通过回退软件版本完成，生产环境没有引擎或 Replay 策略开关。

## 被拒绝的方案

**保留全量物化。** 在发布第一条消息前构建完整 Bars 列表和 close-time 数组，进程内存与首条消息延迟都随结果行数线性增长；M5A 曾用它与新查询做差分验证，但验收后必须删除。它也无法在长时间读取中及时响应 stop 和 seek。

**把读取失败当作空回放。** 把损坏 Parquet、Manifest 缺失或资源耗尽转换成 0 条 Bar 的 `exhausted`，会让客户端把数据故障误认为没有数据，并可能用空结果覆盖下游状态。只有真正耗尽查询、且预取全部发布后才能报告 `exhausted`。

**轮询整个结果集。** 通过反复查询或轮询 DBAPI/结果对象来“流式”消费，仍需要持有完整结果或重复扫描，无法提供有界内存、及时取消和精确的 `bars_sent`；用轮询替代 Arrow RecordBatch 流还会把读延迟与发布节奏绑在一起。Replay 的流式语义必须来自 DuckDB 的 Relation Arrow Reader 和任务有界预取。

## 代价与后果

实现比“读完再发”复杂：需要查询代际、可取消的 scanner、Step Credit 与 Clock 的竞态处理，以及 stop/seek/失败三者的终止优先级。全局五键排序可能使用 DuckDB 临时磁盘，`memory_limit`、`threads`、`temp_directory`、`max_temp_directory_size` 和 Replay 批次大小必须显式配置；资源耗尽会产生 `reason="error"`，而不是静默截断。有界预取意味着发布速度受下游消费者和配置的批次深度限制，内存不再随总行数增长，但单批大小仍决定最小内存占用。客户端必须适配新的失败终止语义，这是把“数据故障不可见”变成可处理错误的必要成本。
