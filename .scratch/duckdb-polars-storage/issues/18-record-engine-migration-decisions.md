# 18: 发布三份 ADR 并同步公共文档

**What to build:** 让未来维护者和外部消费者能从正式文档理解为什么采用 Manifest-aware DuckDB、Polars Quarantine 和有界 Replay，以及哪些公共语义保持或有意改变。文档必须与最终代码一致，而不是只记录工具选型。

**Blocked by:** 17 迁移剩余 Store 调用方并删除旧生产路径。

**Status:** resolved

- [x] 发布 Manifest-aware DuckDB reads ADR，记录 Manifest Snapshot、Dataset Read Lease、显式文件列表、REST/Replay 语义差异和 Arrow 边界。
- [x] 发布 Polars batch Normalization with Quarantine ADR，记录批量边界、失败原子性、Reason Code、Rule Version、fail-closed 和 Retention 的权衡。
- [x] 发布 Bounded Replay with explicit failure termination ADR，记录有界读取、seek 代际、取消、资源约束和 `reason=error` 的公共兼容影响。
- [x] 每份 ADR 都说明真实备选方案、选择理由、代价和后果，不只描述最终依赖清单。
- [x] Data Contract 同步描述 Manifest Rule Version 元数据、Quarantine、Read Lease、Replay 失败终止和稳定错误行为。
- [x] 移除 DESIGN 中“所有读取使用 pyarrow.dataset”的过时实现陈述，并准确说明 Store 门面与 DuckDB/Polars/Arrow 的职责边界。
- [x] README 在最终有界实现验收后更新 Replay 全量物化的已知限制、依赖要求、资源配置和运维维护方式。
- [x] GLOSSARY 保持领域定义且无实现细节，统一使用 Normalization、Validation、Quarantine、Manifest Snapshot 和 Dataset Read Lease。
- [x] 文档明确非分页输出无全序承诺、REST raw Revisions、Replay 五键排序，以及 Revision-heavy Time Cursor 仍是独立契约问题。
- [x] 文档不再建议 DuckDB 1.x、Dataset 根目录 glob、生产旧引擎开关或把读取异常当成空结果。

## Comments

**Branch:** `ticket/18-record-engine-migration-decisions`（worktree `E:/total-project/mdgw-worktrees/duckdb-18`）

**Commits:**
- `48b3ba9` docs(adr): record DuckDB Polars storage migration decisions
- 本 ticket 的 closure 提交（ticket 状态、勾选与本文）

**文档变更:**
- 新增 `docs/adr/0013-manifest-aware-duckdb-reads.md`：Manifest Snapshot、Dataset Read Lease、显式 Data File 列表（禁止 Dataset 根目录 glob）、REST raw Revisions 与 Replay close-time/Revision/五键语义差异、Arrow 边界，以及直接 `pyarrow.dataset` 扫描、DuckDB 原生权威表/第二 Catalog、根目录 glob、生产 `use_duckdb` 开关四类真实备选方案与代价。
- 新增 `docs/adr/0014-polars-batch-normalization-with-quarantine.md`：Collector 批量边界、窄 Arrow 列与 Python 精确 Decimal 校验、整批/整文件失败原子性、稳定 Reason Code/Source Stage、Rule Version 与 Data Contract Version 分工、fail-closed Quarantine 写入、30 天默认 Retention/0 永久/dry-run 整月 GC，以及标量逐行规则、单行 DataFrame、仅日志、Quarantine 失败后继续处理四类备选方案。
- 新增 `docs/adr/0015-bounded-replay-with-explicit-failure-termination.md`：有界批次与有界预取、同一固定 Snapshot 上的 seek 查询代际、Credits/Clock 语义、取消与 Reader/连接/Lease 生命周期、`reason=error` 的 `{code,message}`、`schema_version_mismatch`/`internal_error`、精确 `bars_sent`、SDK `ReplayFailedError` 与兼容性影响；拒绝全量物化、失败当空回放、轮询整个结果集。
- `docs/data-contract.md`：补齐 Manifest Snapshot/Dataset Read Lease（Compaction/GC 跳过与报告、60 秒心跳窗口）、独立 Quarantine Dataset（路径、分区、12 字段 schema、Reason Code/Source Stage、fail-closed、Retention/GC）、Replay 失败终止的精确错误消息与 SDK 行为；明确 REST 返回全部 raw Revisions、非分页输出不承诺全序（也不承诺物理存储顺序），并链接 `docs/revision-heavy-time-cursor.md`；修正 `rule_versions` 中过时的 “scalar Backfill/archive rules” 表述。
- `DESIGN.md`：删除“读取一律走 `pyarrow.dataset`”及相关过时陈述；新增 Store 门面 → Manifest Snapshot/Read Lease → DuckDB 显式文件列表 → Arrow 边界说明，明确 PyArrow 仍用于写入、Arrow IPC/ZMQ、export、Compaction 与测试；补充 Polars 批量 Normalization + Quarantine、有界 Replay/失败终止、资源与维护；更新 `[storage.duckdb]`/`[storage.quarantine]`/`[replay]` 等配置样例、依赖、风险、已拒绝方案与仓库结构。
- `README.md`：新增 Runtime requirements、`[storage.duckdb]`/`[replay]`/`[storage.quarantine]` 资源与运维章节、`mdgw quarantine-gc` retention/GC 命令、新 ADR 链接；删除 “Replay 在发布前物化整个范围” 的已知限制，改成有界流 + `reason=error` + 临时磁盘的真实限制；保留并更新 Quarantine 非公共 API、非分页无全序、raw Revisions 等限制。
- `GLOSSARY.md`：已在集成分支包含 Normalization、Validation、Quarantine、Quarantine Reason、Manifest Snapshot、Dataset Read Lease 六个领域定义且均有 `_Avoid_` 行，未引入 Cleaning Layer；本 ticket 核对后无需改动。
- `docs/adr/0012-single-instrument-pagination.md`：仅把“非分页继续按存储顺序”改为“不承诺全序，也不承诺物理存储顺序”。
- `benchmarks/RESULTS.md`：把 PyArrow 时代的 Query 数字标注为历史基线，避免继续暗示当前读取仍走 PyArrow。
- `config/gateway.toml`：只修正 `[storage.duckdb]` 的过时注释（原注释称 JSON/CSV/export 仍走 PyArrow）；无配置键或默认值变更。

**测试:**
- 命令：`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q`
- 最终全量离线结果：**1078 passed, 5 failed, 1 skipped, 1 deselected** in 204.55s。5 个失败均为 ticket 01 起已知的 Typer/Click CLI 问题（4 个 `test_archive_backfill.py` CLI 测试 + `test_cli_flags.py::test_run_rejects_an_unknown_service_role`），无新增失败。
- 第一次全量运行出现已知计时 flake `tests/unit/test_ccxt_feed_handler.py::test_malformed_live_row_stops_the_source_when_quarantine_write_fails`（ticket 12/14 已记录）；隔离重跑 **1 passed**，第二次全量运行恢复基线。
- 文档本地链接检查通过；配置默认值经 `GatewayConfig()` 与 `mdgw quarantine-gc --help` 实测核对（`[storage.duckdb]` 1GB/4/10GB/65536、`[replay]` 4/1.0/4096/2、`[storage.quarantine]` 1/polars-1/30）。

**发现的代码/文档不一致（未改代码，按只看文档规则上报）:**
- `[storage.quarantine].rule_version` 的配置模型注释称其会盖到 Quarantine Records 与新 Manifest 条目，但 `mdgw backfill --archive` 的 `_build_store` 未传 `rule_versions`，`import_archive` 也未传 `rule_version=config.storage.quarantine.rule_version`。因此显式覆盖该配置时，archive 的 Manifest/Quarantine lineage 仍使用默认 `polars-1`；默认配置下两者一致。建议后续单独修复或明确配置语义。

**给 merger 的说明:**
- 已合并 `integration/duckdb-polars-storage` tip `4c468b1`，无冲突。
- 无生产代码/测试行为变更、无新依赖、无新配置键；仅有 `config/gateway.toml` 注释修正。
- 分支 `integration/bus-topology` 已占用 `docs/adr/0013-standalone-message-bus.md`；本 ticket 按任务指定发布 `0013-manifest-aware-duckdb-reads.md`。若两条集成线最终汇合，需要统一 ADR 编号。
