# 17: 迁移剩余 Store 调用方并删除旧生产路径

**What to build:** 将 Backfill 等剩余 Store 调用方统一切换到新的 Manifest-aware 读取接口，然后收缩旧实现：删除手工 Parquet 行过滤、重复标量 Normalization 和 Replay 全量物化状态。最终生产环境只有一套 Bars 读取与 Normalization 语义。

**Blocked by:** 16 完成 Replay 失败终止协议与 SDK 行为。

**Status:** resolved

- [x] Backfill 和其他 Store 调用方都通过新 History/Store 查询接口读取，不直接拼接 SQL、文件路径或私有 DuckDB 对象。
- [x] Backfill 的 identity、Revision merge、HWM、缺口检测和 Aggregation 行为保持不变，并复用现有 Backfill 集成测试证明。
- [x] 删除生产 Bars API/Replay 中旧的 PyArrow compute 行过滤、Python close-time/Revision/排序过滤和不再使用的全量读取路径。
- [x] 删除重复的生产标量 Normalization 规则，保留 Polars 批量实现作为唯一规则来源。
- [x] 删除 Replay 完整 Bars 和完整 close-time 列表及其不再使用的控制分支，三种 Replay Mode 都使用有界实现。
- [x] 删除迁移专用生产分支和任何引擎开关；发布后回退通过回退软件版本完成。
- [x] 迁移用旧实现和差分设施可以从生产路径移除；必要的测试工具不得成为运行时后备。
- [x] 保留 PyArrow 在 Schema、Data File 写入、Arrow IPC/ZMQ、export、Compaction 和合法测试中的使用，不做技术栈纯净式删除。
- [x] 检查正常 API/Replay 不会发现或过滤 Quarantine，Instrument Snapshot 与示例仍保持明确的本项目范围边界。
- [x] 完整离线套件、Data Contract、Manifest 维护、REST、Normalization 和 Replay 测试全部通过。

## Comments

**Branch:** `ticket/17-contract-legacy-read-paths`（worktree `E:/total-project/mdgw-worktrees/duckdb-17`）

**Commits:**
- `730b4ac` refactor(normalize): make normalize_batch the only rule source
- `303244d` refactor(storage): one DuckDB Store read and one bounded Replay path
- `6341de6` test(store): assert DuckDB read content without an old-path baseline
- `eafe595` docs(normalize): describe the narrow typed batch columns
- `ecd84a6` test(storage): keep read assertions order-insensitive
- 本 ticket 的 docs closure 提交

**统一读取与删除的旧路径:**
- `ParquetBarStore.iter_batches/count_bars/read_bars` 保留公共 API，但每个调用打开一个 `StoreRead` 并委托其 `iter_arrow_batches/count_arrow/read_arrow_bars`；`StoreRead.iter_batches/count/read_bars` 同样委托 DuckDB 方法。Backfill 的 `read_existing_bars` 继续通过该 Store seam 读取，不拼接 SQL、文件路径或私有 DuckDB 对象。
- 删除：`PHYSICAL_READ_COLUMNS`、`DEFAULT_BATCH_SIZE`、`StoreRead._open_files` / `_open_parquet_files` / `_close_parquet_files` / `_with_partition_interval` / `_filter_batch`（PyArrow 行过滤）、`open_read(batch_size=...)`、`StoreRead.read_replay_bars`、`DuckDBArrowScanner.replay_batches`、`ReplayBarSource.read` / `_read_legacy` / `bar_order_key` / `_revision_key`、`src/mdgw/normalize/bar.py`、`SCALAR_NORMALIZATION_RULE_*`、`tests/characterization/test_replay_duckdb_differential.py`。
- Backfill 与 archive import 改为 `normalize_batch`。Backfill 的 interval/窗口过滤、`open_ts` 去重与排序逻辑不变，rejection 继续抛异常（`BatchNormalizationError`，`ValueError` 子类）且整批不写；archive rejection 映射为 `ArchiveFieldError`（消息只含稳定 Reason Code，不含被拒值），保持整文件失败与 ticket 10 Quarantine 路径。两者 Manifest/Quarantine lineage 统一为 `polars-1`。
- Replay 三种 Mode 只消费 `iter_replay_batches` 有界流；删除 materialized 分支和测试替身里的 `read_replay_bars`。Replay 测试改为直接断言 streaming seam 的固定期望序列，保留 Revision / `ingest_ts` tie-break / 五键排序 / 重复订阅覆盖。
- `normalize_batch` 只向 Polars 传规则实际读取的列（instrument_id、interval、open_ns），并避免逐行 `to_dicts()` 回转，修复了批量路径相对已删除标量实现的性能回退；拒绝分类与 Bar 输出完全不变。

**Grep 审计**（`rg -n "pyarrow.dataset|pys\.dataset|normalize_bar|read_replay_bars|_read_legacy|iter_batches\(" src`）:
- `src/mdgw/store/maintenance.py` 的 `pyarrow.dataset`：Compaction 读取源 Data Files 做物理合并，合法保留。
- `src/mdgw/store/parquet_dataset.py` 的 `iter_batches`：公共 Store/StoreRead API，现已委托 DuckDB，内部无 PyArrow 行过滤。
- `src/mdgw/backfill_common.py` 的 `store.iter_batches(...)`：Backfill 通过公共 Store seam 读取。
- `src/mdgw/archive/importer.py` 的 `_normalize_bars`：新 archive helper 名称，不是已删除的 `normalize_bar`。
- 生产代码无 `read_replay_bars` / `_read_legacy` / `normalize_bar` 命中。
- `Store` / `Replay` / `api` 不 import、扫描或过滤 Quarantine；Instrument Snapshot 与 examples 未改动。

**测试:**
- 命令：`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q`
- 全量离线结果：**1078 passed, 5 failed, 1 skipped, 1 deselected**。5 个失败均为 ticket 01 起已知的 Typer/Click CLI 问题（4 个 `test_archive_backfill.py` CLI 测试 + `test_cli_flags.py::test_run_rejects_an_unknown_service_role`），无新增失败。最终一次运行命中已知的 `test_duckdb_compat.py::test_temp_directory_limit...` flake，单独重跑通过。
- 因 `read_bars` 改为 DuckDB（不保证物理扫描顺序），把 4 处依赖旧 PyArrow 扫描顺序的断言改为排序比较或 row multiset；相关测试连续 5 次重跑稳定通过。
- 相比 ticket 16 的 1088 passed：删除 M5A 旧/新差分测试 12 个 case，新增 2 个后端归一化拒绝测试（Backfill rejection、archive sanitized rejection），原 order-key 单测改为真实 streaming 顺序测试；行为覆盖未削弱。
- 关键子集：Store contract 60 passed；Replay 相关 137 + 93 passed；Normalization/Backfill/archive 相关 82 passed。

**性能（同机、同数据、5 次 median）:**
- Backfill repeat pass（20k rows 全量重读/重归一化/merge）：1.809s → 1.948s（**+7.7%**，在 10% 门槛内）；其中 `_normalize_history` 经上述优化后 0.542s → 0.265s，`read_existing_bars` 0.785s → 0.812s。
- Live ingest（50k rows）：0.491s → 0.489s（基本不变）。
- Replay query prep（20k rows）：materialized 0.547s → streamed drain 0.025s；asap 端到端 3.343s → 3.281s。
- 直接 `ParquetBarStore` 单文件 50k：`read_bars` 0.014s → 0.017s、`iter_batches` 0.013s → 0.016s（DuckDB 查询/连接固定开销约 +3ms，超过 10%）；`count_bars` 0.013s → 0.005s（约 2.5× 更快）。说明：REST 非分页 JSON/Arrow 自 M3 起已经使用 DuckDB，本次切换只影响直接 Store 调用方；Backfill 是唯一剩余生产调用方且总耗时在门槛内。该一次性小扫描回退是删除重复读取语义的既定代价（ticket 要求），记录供 ticket 19 性能验收。

**给 merger 的说明:** 无新依赖、无配置变更；公开 REST/Replay 行为不变。文档/ADR 更新属于 ticket 18；本 ticket 只做代码路径收缩与测试迁移。
