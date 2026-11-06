# 01: 用 Bars-only Storage Composition 打通只读读取路径

**What to build:** 网关维护者可以通过一个深的 Storage Composition 模块，从已验证的 GatewayConfig 和可选 root override 获得 Bars Store。api 与 replay Service Role 使用该组合获得各自独立的读取对象，不创建 QuarantineStore；实际 root 的独立性校验在任何文件系统副作用之前完成，现有 REST API、Replay Protocol、Dataset 布局和角色生命周期保持不变。

**Blocked by:** None (can start immediately)

**Status:** resolved

- [x] Bars-only 组合接受已验证的 GatewayConfig 与可选 root override，每次返回新的 ParquetBarStore，不创建或缓存 QuarantineStore，不引入全局 singleton。
- [x] 实际 storage root 保持“显式 override 优先于配置值”的语义；Bars Dataset、Instrument Snapshot、High-Water Mark 与 DuckDB 临时目录的派生位置保持当前契约。
- [x] 默认 Quarantine root 跟随实际 storage root，显式 Quarantine root 继续原值优先且不跟随 override；相对显式路径的现有锚点不变。
- [x] Bars-only 组合也会在构造 Store 前校验本次实际 root 组合；Quarantine 等于或位于实际 Bars Dataset 内时，在任何目录、descriptor、Manifest 或 DuckDB 临时目录创建前失败。
- [x] 校验覆盖父目录片段、相对路径、root override 和绕过模型校验构造的配置；比较不要求路径预先存在，也不新增反向包含禁令。
- [x] api 与 replay 的默认生产构造路径全部迁移到 Bars-only 组合，原位置的重复 Store 参数映射被删除，不增加切换开关或旧路径后备。
- [x] api 与 replay 仍只拥有 Bars 读取依赖，其公共构造方式、启动、状态、就绪、指标、关闭、REST API 和 Replay 行为不变。
- [x] DuckDB 资源配置与当前转换结果一致；未配置临时目录时仍位于实际 Store root 下，DuckDBConfig 与 DuckDBOptions 不合并，配置导入的懒加载方向不倒退。
- [x] ParquetBarStore 的公共构造接口、子类行为和直接构造方式不变；不为原本没有注入参数的 Service Role 扩张公共接口。
- [x] 新增测试穿过 Storage Composition 的 interface，使用真实临时目录与真实 DuckDB 行为，覆盖按需创建、新实例、root 优先级、路径布局、允许布局和失败前零写入。
- [x] 现有 api、replay、REST、Replay、Store contract 与多进程读取测试保持通过；测试不断言私有字段或内部调用次数。
- [x] 组合模块不创建 Feed Handler、不接管对象生命周期、不引入存储 adapter，也不修改 GLOSSARY 或 ADR。

## Comments

**Branch:** `ticket/01-bars-only-storage-composition` (worktree
`E:/total-project/mdgw-worktrees/storage-01`), based on
`integration/storage-composition` at `a1ae1d8`.

**Commits:** `756349b` (bars-only Storage Composition module and interface
tests), `24858bb` (api/replay migration). Merged with `--no-ff` into
`integration/storage-composition` as `200ed4b`. This closure commit follows.

**What was added/changed:**

- `src/mdgw/store/composition.py`: new internal `StorageComposition`
  (`StorageBundle` for joint composition). `bars()` maps a validated
  `GatewayConfig` plus optional actual-root override to a fresh
  `ParquetBarStore`; `quarantine()` and `bars_and_quarantine()` exist for the
  later production composition points. It resolves the effective root
  (explicit override, else configured root), derives the Bars Dataset, default
  Quarantine root and the one effective Normalization Rule Version, delegates
  DuckDB conversion to `storage.duckdb.to_options()`, and re-runs the ADR-0014
  one-way containment check against the normalized actual roots before
  constructing any Store. Nothing is cached; the Bars-only path never creates
  a QuarantineStore; there is no global singleton, lifecycle ownership, Feed
  Handler construction or storage adapter.
- `src/mdgw/service/api.py`, `src/mdgw/service/replay.py`: default production
  Store construction now goes through
  `StorageComposition(config, root=data_root).bars()`; the duplicated Store
  parameter mapping is removed. Public constructors, lifecycle, REST and
  Replay behavior are unchanged.
- `tests/integration/test_storage_composition.py` (20 tests): real temporary
  directories, real DuckDB and real Parquet. Covers Bars-only and joint
  composition, fresh Store instances, root priority, explicit/default
  Quarantine anchors, Bars and DuckDB temp layout, parent-fragment/relative/
  root-override/model-bypass independence failures with zero filesystem
  writes, allowed non-nested layouts, Quarantine independence from Bars
  physical settings, Rule Version propagation, and api/replay actual-root
  composition.

No public `ParquetBarStore`/`QuarantineStore` constructor changes; no GLOSSARY,
ADR or spec changes; no new Service Role public injection parameters.

**Tests:**

Post-merge, main workspace:

`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q tests/integration/test_storage_composition.py tests/integration/test_api_service.py tests/unit/test_replay_service_role.py tests/unit/test_config.py tests/contract/test_parquet_dataset.py`

-> **84 passed, 0 failed** in 3.58s (20 + 3 + 3 + 56 + 2 collected per file).

Full suite, same interpreter, `pytest -q`:

- Baseline on this host: 6 failed, 1093 passed, 1 skipped — 5 known Typer/CLI
  failures plus `test_queue_overflow_is_not_quarantined`.
- Ticket branch run: 5 failed, 1114 passed.
- Post-merge audit run: **5 failed, 1114 passed, 1 skipped, 1 deselected** in
  214.84s. The failures are the 5 known Typer/CLI ones
  (`test_archive_backfill.py` x4 and
  `test_cli_flags.py::test_run_rejects_an_unknown_service_role`); the known
  queue-overflow failure did not reproduce. No new failures; +21 passed over
  baseline (20 new tests plus the non-reproducing flaky one).

**Deviations / gaps for later tickets:**

- Only api and replay are migrated. GatewayRuntime/collector write path,
  compaction/Bars GC, Quarantine GC and archive CLI/import still construct
  Stores directly; that is tickets 02-04. `quarantine()` and
  `bars_and_quarantine()` already exist and are interface-tested, so later
  tickets mainly switch call sites and delete duplicate mappings.
- api/replay now run the actual-root containment check at startup even though
  they never create Quarantine. A config/override combination that puts
  Quarantine inside the actual Bars Dataset now fails fast at api/replay
  startup (intended by this ticket, before any filesystem write).
- api/replay Stores now receive an explicit
  `rule_versions=(config.storage.quarantine.rule_version,)`. The current
  default set has one member (`polars-1`), so default behavior is identical; a
  custom configured version now reaches manifest stamps from these roles too.
- Instrument Snapshot and High-Water Mark paths are still derived inside
  `ParquetBarStore` and were not asserted directly by the new composition
  tests; their unchanged behavior is covered by the existing suite.
