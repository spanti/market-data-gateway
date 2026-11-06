# Manifest-aware DuckDB 读取：快照、租约与显式文件列表

Bars 的读取不再由各调用方自行扫描 Parquet。`ParquetBarStore` 仍是唯一的持久化门面：每次 HTTP 请求或 Replay Session 开始时，它先解析一个固定的 Manifest Snapshot，为所选 Data Files 获取 Dataset Read Lease，再把该 Snapshot 的显式文件列表交给 DuckDB 的 Relation Arrow Reader。DuckDB 只读取这些文件，从不对 Dataset 根目录做 glob；Manifest 仍是活动 Data File 的唯一权威。这个决定同时消除了手工 Arrow compute mask、REST 与 Replay 各自的 Python 行过滤，以及“读到哪些文件取决于目录扫描时机”的隐含行为。

## Manifest Snapshot

读取的第一步是确定一个不会混合两代 Manifest 的条目集合。Store 计算 `_manifest.json` 内容哈希作为 generation，按 active 条目、Instrument、Interval 以及条目记录的 `start_ns`/`end_ns` 做文件级裁剪；Instrument 由分区路径中的百分号编码 symbol 还原，不依赖打开 Parquet。随后在所选分区与月份上获取 Lease，并重新读取 Manifest generation；如果期间有写入者切换了 Manifest，Lease 被释放，整个“选择候选、获取 Lease、复核 generation”的流程最多重试三次，仍不一致则以 `ManifestSnapshotRaceError` 失败，绝不返回混合代际的结果。

同一个 HTTP 请求的 count、存在性探测和实际读取共享一个 Snapshot 与 Lease；不同分页请求各自获取新 Snapshot，短请求不会长期持有 Dataset 保护。Replay Session 则在创建时获取一次 Snapshot 与 Lease，覆盖整个 Session，包括暂停期间和之后的 seek；seek 只在该 Snapshot 上建立新查询，不会把运行期间新增的 Data File 纳入本次回放。Manifest 条目中的 `rule_versions` 只随 Snapshot 作为审计元数据保留，不参与 Schema 兼容判断。

REST 的读取在本次 StoreRead 尚未产生任何行、且还没有执行过其他扫描之前，如果发现所选文件已被删除，会刷新一次 Snapshot 后重试；一旦已经执行过读取或已经开始输出，缺失文件就是明确失败，不会被静默当成空结果或短结果。Replay 的 Snapshot 在 Session 内固定，文件缺失直接走失败终止。Manifest 条目缺少操作字段时由文件名和文件系统元数据补齐，这是读兼容规则，不是错误。

## Dataset Read Lease

Lease 是 Dataset 根下 `_sessions/<id>.lock` 的心跳锁，与 Replay Session 使用同一文件格式。短读取的 Lease 记录它实际选中的精确分区与月份；Replay Session 继续记录订阅，以便旧维护逻辑继续按 Instrument/Interval 保护。Lease 创建一个后台心跳线程，心跳间隔由 Store 或 Session 决定（Replay 默认 1 秒，短读取默认 5 秒）；维护操作只把心跳在超时窗口（默认 60 秒）内的锁视为有效，进程崩溃后停止刷新，过期锁自然失效，不会永久阻塞维护。

Lease 必须覆盖 Reader 的完整生命周期：正常耗尽、异常、取消和客户端断开都由 `StoreRead.close()` 释放；不能只保护查询启动。DuckDB 对外部 Parquet 文件没有事务保证，Spike 已观察到已打开文件被删除时可能继续读出，而尚未打开的文件会在流中途失败；因此 Compaction 与 GC 在扫描、Manifest 切换前和 unlink 前多次检查 active Lease，遇到受保护的分区/月份就跳过并在报告中列出，而不是删除后让读者失败。Lease 是读取保护，不是通用写锁，也不改变 Data File 的所有权。

## 显式文件列表与 Arrow 边界

DuckDB 收到的是 `read_parquet($files)` 的绑定参数，即当前 Snapshot 的显式路径列表；SQL 文本是静态的，Instrument、Interval、时间边界和行数上限全部是绑定值。Manifest 的分区与时间裁剪发生在连接打开之前，因此查询不会为无关分区付出文件打开成本；物理文件中的 Hive `interval` 被省略时，Store 按分区组在 Arrow 边界恢复该列。

Arrow 边界就是 Data Contract。DuckDB 2.x 以 `relation.to_arrow_reader(batch_size=...)` 逐批导出；连接显式设置 `timezone=UTC`，保证 `timestamp[ns, tz=UTC]`。DuckDB 导出会把所有字段标记为 nullable，并且看不到 Hive `interval`，所以每个 RecordBatch 都按 `BAR_SCHEMA` 的字段顺序和 nullability 重建，`interval` 由所在分区显式填入。Decimal 与时间戳始终是带类型的 Arrow/Parquet 值，不经过 SQL 文本 cast、Python 行转换或 Polars 往返。一个连接在任一时刻只服务一个 Reader，Reader 关闭前不会启动冲突查询；正常耗尽的连接最多保留一个空闲实例复用，取消或失败的连接直接关闭。

## 资源预算与连接缓存

每个 Reader 独占一条连接，并各自套用配置中的 `memory_limit`、`threads`、`max_temp_directory_size` 与 Arrow batch size；并发 Reader 不会共享同一份执行内存或临时目录预算，也不会在同一连接上启动冲突查询。`DuckDBConnectionCache` 只是至多保留一个空闲连接的复用槽：正常耗尽的连接放回缓存，取消或失败的连接直接关闭。它是有界连接池，不是共享 Catalog、Manifest 权威或第二份数据来源；缓存的连接不携带任何跨请求的查询状态。

未显式配置时，临时目录确定为 `<store.root>/.duckdb_tmp` 并在 Store 启动时创建；显式配置的路径优先。全局排序与聚合的 spill 文件不是权威数据副本，其占用通过 `mdgw_duckdb_temp_bytes` 暴露，签出连接数通过 `mdgw_duckdb_active_connections` 暴露；两者都不改变 `/api/v1/status` 的 JSON 形状。

## REST 与 Replay 的语义差异

两者共享 Manifest 与扫描基础设施，但不共享查询语义。REST History 固定按 `open_ts` 的半开区间 `[start, end)` 过滤，返回全部 raw Revisions；JSON 行数上限由同一 Snapshot 上的精确 raw 行 count 决定，不做隐式去重。非分页 JSON、Arrow、CSV 和 export 没有全局 `ORDER BY`，因此不承诺任何全序，也不承诺物理存储顺序。单 Instrument 分页 JSON 是唯一的显式排序面：一个查询按 `open_ts` 升序取全局最早的 `limit + 1` 行，返回前 `limit` 行并据此给出 `has_more` / `next_start`；同 `open_ts` 的文件位置与文件内行号 tie-break 只为复现既有可观察行为，不是公共排序承诺。Revision-heavy 数据下游标无法表达同时间戳内的行进度，这是既有契约缺口，保持独立（`docs/revision-heavy-time-cursor.md`）。

Replay 固定按 `close_ts` 的半开区间 `[start_ts, end_ts)` 过滤，物理 `open_ts` 下界向前放宽一个 Bar Interval；同一 `(instrument_id, interval, open_ts)` 只保留最大 `revision`，同 Revision 再取最新 `ingest_ts`；重复订阅在进入查询前折叠为不同的 `(instrument_id, interval)` 分支；最终排序是 `close_ts`、`instrument_id`、`seq`（NULL 最后）、`interval`、`open_ts` 五键全序。Store 不提供“通用时间字段 + 去重布尔开关”的模糊 API：History 与 Replay 是两个结构化查询入口，调用方不能注入 SQL、任意路径或表达式。

## 被拒绝的方案

**直接使用 `pyarrow.dataset` 扫描。** 旧读取路径把 Manifest 解析、PyArrow 数据集扫描和每批 compute mask 分散在多个调用方；同一查询语义需要重复维护，目录布局或扫描顺序的变化会悄悄改变结果。即使只把显式文件列表交给 `pyarrow.dataset`，也没有统一的 Snapshot/Lease 生命周期、DuckDB 的 Arrow 流式 Reader 与精确的 `TIMESTAMPTZ_NS` 读取。PyArrow 仍用于 Data File 写入、Arrow IPC/ZMQ、export、Compaction 和测试，但不再是 Bars 读取门面。

**使用 DuckDB 原生权威表或第二份 Catalog。** 把 Parquet 导入 DuckDB 表、建立物化视图或维护第二份文件目录，会让“权威数据在哪里”出现两个答案：Manifest 切换、不可变 Data File 和 Compaction/GC 都要在第二套状态上再实现一次，崩溃恢复和审计也会分叉。Data Contract 的权威继续是 Parquet + Manifest + Arrow Schema。

**对 Dataset 根目录 glob。** glob 会读到 Compaction 已标记 inactive 但尚未删除的旧文件，也无法在打开前按 Manifest 分区裁剪；DuckDB 惰性打开文件且不提供外部文件事务语义，维护竞态只能变成随机失败。显式文件列表加 Lease 把“读哪些文件”和“读的时候它们不会被删”变成可验证的前置条件。

**保留生产 `use_duckdb` 引擎开关。** 两套读取实现会产生两套可观察语义，任何部署差异、回退演练和故障排查都要同时覆盖；回退应通过回退软件版本完成，而不是让配置决定走哪套过滤、排序和失败行为。DuckDB 是正式运行时依赖，启动时验证 2.x，不存在 1.x 或旧过滤路径的后备。

## 代价与后果

DuckDB 2.x（`duckdb>=2.0,<3`）成为正式依赖，1.x 被明确拒绝：2.0 的 `TIMESTAMPTZ_NS` 是保留 Parquet UTC 纳秒的前提。每次读取多一次 Manifest 解析、Lease 文件与 generation 复核；DuckDB 连接有固定建立成本，短查询因此比纯 PyArrow 扫描多几毫秒。Lease 与心跳文件需要维护，进程异常退出后要等超时窗口才能让维护回收文件。分页与 Replay 的全局排序可能使用临时磁盘，资源上限必须显式配置，临时文件不是数据副本。非分页输出失去任何顺序暗示，客户端不能把扫描顺序当作业务排序。最后，回退只能通过回退软件版本完成，不能在同一版本内切换回旧读取路径。
