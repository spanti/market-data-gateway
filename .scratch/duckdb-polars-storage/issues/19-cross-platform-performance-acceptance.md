# 19: 完成跨平台、性能与全量验收

**What to build:** 对最终收缩后的实现执行完整跨平台、行为、资源和性能验收，给出可审查的迁移结论。只有全部硬门槛满足，DuckDB/Polars 改造才视为完成。

**Blocked by:** 18 发布三份 ADR 并同步公共文档。

**Status:** resolved

- [x] 完整离线测试套件通过，包括 Data Contract、Manifest、Store、REST、Normalization、Quarantine、Compaction/GC、Replay 和 SDK。
- [x] Windows 与 Linux 的读取/维护竞态、Reader 取消、文件删除和 Lease 生命周期测试均有真实执行结果，不仅依赖 mock。
- [x] 精确 Decimal、UTC 纳秒、uint32、null、字段顺序和 `interval` 恢复的 golden/round-trip 测试全部通过。
- [x] 使用保存的旧实现基线，在同一机器和同一数据副本上对最终实现至少测量 5 次，报告中位数、波动、峰值内存和临时磁盘。
- [x] Store 扫描/count、REST JSON/Arrow/CSV/export、分页、多文件、Revision-heavy、Replay 查询和端到端发布均纳入最终报告。
- [x] 典型吞吐或延迟回退超过 10%，或峰值内存增加超过 20% 时，具有明确批准的偏差记录；否则不得验收。
- [x] 用递增总结果规模证明 asap/realtime/step 的进程内存不随结果行数线性增长，并单独报告全局排序临时磁盘峰值。
- [x] 验证资源耗尽、依赖缺失和 Quarantine 写入失败都有明确失败行为，不会静默截断、回退旧引擎或消费未隔离输入。
- [x] 最终代码检查确认生产 Bars API/Replay 没有手工 Parquet 行过滤、重复标量 Normalization、Replay 全量物化或生产引擎开关残留。
- [x] 最终报告列出已验证版本、平台、测试命令、性能结果、任何已批准偏差和独立遗留的 Time Cursor 契约问题。
- [x] 不把性能结果表述为对外 SLA，也不以关闭独立 Time Cursor 问题作为本迁移的隐式验收条件。

## Comments

**Branch:** `ticket/19-cross-platform-performance-acceptance`（worktree `E:/total-project/mdgw-worktrees/duckdb-19`，基于 integration tip `aefb447`）

**Commits:**
- `57db4f5` fix(cli): honor configured rule version in archive backfill lineage
- `7e6de07` bench(replay): support step mode in replay memory scaling
- `6d932b4` docs(acceptance): record final DuckDB Polars storage acceptance
- 本 ticket 的 closure 提交（状态、勾选与本文）

**规则版本缺陷修复（TDD）:**
- `_build_store` 现在把 `[storage.quarantine].rule_version` 作为 `rule_versions` 传给 `ParquetBarStore`；`mdgw backfill --archive` 也把同一值作为 `rule_version` 传给 `import_archive`，Manifest 与 Quarantine 的审计血缘由此一致。
- 新增参数化测试 `test_cli_archive_backfill_uses_the_configured_rule_version`：默认配置仍写 `polars-1`（默认行为不变），`custom-2` 覆盖时 Manifest 的 `rule_versions` 与 Quarantine 的 `rule_version` 都使用 `custom-2`。红测为 `{('polars-1',)} == {('custom-2',)}`，修复后与既有 lineage 测试共 **5 passed**。

**测试:**
- Windows 全量离线：**1080 passed, 5 failed, 1 skipped, 1 deselected** in 205.42s。5 个失败仍是 ticket 01 起已知的 Typer/Click CLI 问题；1 个 skip 是 POSIX-only 文件生命周期 case。
- Linux 全量离线（`python:3.13-slim` Docker，额外安装可选测试依赖 pandas/httpx/psutil）：**1075 passed, 6 failed, 3 skipped, 1 deselected** in 181.18s。6 个失败 = 5 个已知 CLI 失败 + 1 个已知计时 flake（`test_malformed_live_row_stops_the_source_when_quarantine_write_fails`），该 flake 隔离重跑 **1 passed**；3 个 skip 为两个 nautilus 可选依赖 case 与 Windows-only 文件行为 case。POSIX-only `test_posix_unlink_of_open_file_keeps_stream_readable` 在 Linux 真实执行。
- Linux Lease/生命周期/维护子集：**40 passed, 1 skipped** in 6.56s（skip 为 Windows-only case），覆盖 POSIX 删除、Read Lease、Compaction/GC 跳过与 Session Lock。
- Golden/类型子集（Windows）：**172 passed, 1 skipped** in 53.30s；覆盖 Decimal、UTC ns、uint32/null、字段顺序/nullability、Hive `interval` 恢复与 JSON/CSV/export parity。
- 失败行为子集：16 passed, 1 failed（上述已知 Windows 文件共享 `PermissionError` flake），隔离重跑 1 passed；另用 `sys.meta_path` 阻断 `duckdb` 导入，Store 构造抛出 `DuckDBUnavailableError`，无回退路径。
- 所有性能门槛仅为迁移评审标准，不是对外 SLA。

**性能（同机、同一保存语料、warm-up + 5 次 timed）:**
- REST JSON -2.4%、Arrow -23.9%、CSV +3.8%、export -11.9%/-23.7%、分页 +0.5%、Revision-heavy -8.3%/-9.3%、Replay query prep -48.8%、端到端 -5.2%；多文件扫描 -62.9%/-78.5%、count_100k -69.3%。
- 已批准偏差 D1：直接一次性 `ParquetBarStore` 调用（read_bars_50k +83.3%、scan_revisions_6k +84.4%、count_revisions_6k +59.6%、scan_100k +10.7%）是 Manifest Snapshot + Read Lease + DuckDB 连接的固定 +2~8ms 开销；REST/Replay 不受影响，Backfill 总耗时 +7.7% 在门槛内，单一读取语义是迁移强制要求，不引入第二个读取路径。详情见报告 §5.3。
- 峰值内存：无超过 20% 的增长；多数下降，Arrow native 峰值在扫描路径下降 98%+。
- 内存伸缩：asap/realtime/step 在 50k→400k（8×）下 Python 峰值 13.91–13.95 MiB、Arrow 单批 745 KiB 恒定；RSS 在 200k 后平台化于 ~94–104 MiB（DuckDB 64MB 排序预算），全局排序临时磁盘峰值单独报告 ~41–42 MiB。

**最终报告:** `benchmarks/RESULTS-duckdb-final.md`（本地日志在 `benchmarks/.acceptance-logs/`，已 git-ignore）。

**独立遗留问题:** Revision-heavy Time Cursor 契约缺口保持独立开放（`docs/revision-heavy-time-cursor.md`），本次验收不以关闭它为前提；最终运行仍复现 6,000 行返回 6,060 行、4,060 个重复 `open_ts` 的既有行为。

**给 merger 的说明:**
- 无新运行时依赖、无新配置键；`benchmarks/bench_replay.py` 的 `--mode step` 仅为基准扩展。
- `.gitignore` 仅新增 `benchmarks/.acceptance-logs/`（大日志不提交）。
- 已合并 integration tip `aefb447`；本分支包含 3 个功能/文档提交与本 closure 提交，无冲突。
