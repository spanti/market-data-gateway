# 02: 用 Bars + Quarantine 组合打通采集写入与血缘

**What to build:** collector 与 GatewayRuntime 可以通过一次 Storage Composition 获得相互独立的 Bars Store 和 QuarantineStore，并让同一次默认生产组合中的 Bars Manifest、Quarantine Record 与默认构建的 Feed Handler 使用同一个有效 Normalization Rule Version。两个 Dataset 的物理与语义独立性、现有显式注入和采集原子性保持不变。

**Blocked by:** 01: 用 Bars-only Storage Composition 打通只读读取路径

**Status:** resolved

- [x] Bars + Quarantine 组合返回命名明确的 ParquetBarStore 与 QuarantineStore；每次组合返回新对象，但同一次采集组合中的管线与默认 Feed Handler 共享其应有的同一个 QuarantineStore。
- [x] 联合组合在创建任一对象前完成两种实际 root 的解析与独立性校验；校验失败时不会留下 Bars root、DuckDB 临时目录、Dataset descriptor、Manifest 或 Quarantine 状态。
- [x] collector 与 GatewayRuntime 的所有默认生产构造路径迁移到联合组合，重复 Bars / Quarantine 参数映射被删除，不保留双路径或组合开关。
- [x] 同一次组合创建的写入对象使用配置中的同一个有效 Normalization Rule Version；未配置时继续使用当前默认值，自定义值不依赖调用方各自补传。
- [x] 默认与自定义 Rule Version 均能通过真实写入到达 Bars Manifest 和 Quarantine Record，并到达现有默认 Feed Handler 构建路径。
- [x] Rule Version 继续只是审计元数据，不参与 Data Contract 兼容性判断，不新增规则实现或引擎开关。
- [x] CollectorCore 的 Store / Quarantine 显式注入、archive 的 Quarantine 注入以及现有测试替身继续有效；组合模块只决定它创建的对象和默认构建路径。
- [x] 已持有 Quarantine 的注入 Feed Handler 不被替换，现有只填补缺失依赖的行为保持不变；Storage Composition 不创建或拥有 Feed Handler。
- [x] Bars 与 Quarantine 的 Schema Version、Manifest、Data File、Retention 和物理格式默认值保持独立；Quarantine 不继承 Bars 的 compression、row group size 或 format version。
- [x] 实时批次、Instrument Validation、Quarantine fail-closed 写入、Bar Aggregation、High-Water Mark 和既有 Backfill 行为不变。
- [x] 新增联合组合测试覆盖对象独立性、同组合 Quarantine 身份、失败前零写入、默认与自定义 Rule Version，以及真实临时目录中的持久化结果。
- [x] 现有采集、Normalization、Quarantine、CCXT runtime、Rule Version lineage、High-Water Mark 与 GatewayRuntime 端到端测试保持通过，显式注入测试无需改写语义。
- [x] 不引入全局 registry、生命周期管理者或存储 adapter，不修改公共 Store 构造接口、GLOSSARY 或 ADR。

## Comments

**Branch:** `ticket/02-collector-storage-composition` (worktree
`E:/total-project/mdgw-worktrees/storage-02`), based on
`integration/storage-composition` at `121fd70`.

**Commits:** `c2bce2e` (collector/runtime/Core production migration),
`c65e0c1` (joint-composition interface tests), `eba52b1` (merge of the
integration tip `121fd70` into the ticket branch). Merged with `--no-ff` into
`integration/storage-composition` as `f7469d3` (parents `121fd70` and
`eba52b1`). This closure commit follows.

**What was changed:**

- `src/mdgw/runtime.py`: `GatewayRuntime` obtains its Bars Store and
  QuarantineStore from one
  `StorageComposition(config, root=data_root).bars_and_quarantine()`.
  `self.quarantine` is `bundle.quarantine`, the same instance passed to
  `CollectorCore` and attached to the Feed Handler with
  `composition.rule_version`. The direct `ParquetBarStore` parameter mapping
  is deleted.
- `src/mdgw/service/collector.py`: `CollectorService` composes the joint
  bundle once, attaches `bundle.quarantine` to its default-built or injected
  Feed Handler, passes `rule_version=composition.rule_version` to the default
  builder/attach seam, and hands the same `bundle.bars` and `quarantine`
  instances to `CollectorCore`. The direct `QuarantineStore` mapping is
  deleted.
- `src/mdgw/service/collector_core.py`: the duplicated default
  `ParquetBarStore` construction and parameter mapping are deleted.
  Composition fills only missing dependencies: both missing -> joint bundle;
  only Quarantine injected -> composed Bars; only Store injected -> injected
  Store kept and default Quarantine composed anchored on the injected Store
  root; both injected -> both kept. `self.rule_version` comes from the
  composition and is used for `QuarantineRecord` stamps.
- `tests/integration/test_storage_composition.py`: 11 new test functions / 12
  collected cases (one 2-case parametrization); the file goes from 21 to 33
  collected tests. Covers runtime/service root-override independence failure
  with zero filesystem writes, shared Quarantine identity, default and custom
  Rule Version reaching real Bars Manifest entries, Quarantine Records and the
  default feed-handler seam, injected-store anchoring, explicit configured
  Quarantine root precedence, and CollectorCore injection combinations
  (including rejection of a Quarantine nested inside an injected Store's Bars
  layout).

**Scope audit:** `git diff --stat 121fd70..HEAD` shows exactly the four files
above (422 insertions, 47 deletions). No CLI, archive, docs, Store-constructor,
GLOSSARY, ADR or spec changes. The production diff was inspected; no real
defects found.

**Tests:**

Post-merge, main workspace, targeted:

`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q tests/integration/test_storage_composition.py tests/integration/test_normalization_quarantine.py tests/integration/test_rule_version_lineage.py tests/integration/test_runtime_pipeline.py tests/unit/test_collector_service.py tests/integration/test_ccxt_feed_runtime.py tests/integration/test_hwm_backfill.py tests/characterization/test_collector_characterization.py`

-> **81 passed, 0 failed** in 13.30s.

Full offline suite, same interpreter, `pytest -q`:

- Measured baseline on this host: **6 failed, 1093 passed, 1 skipped** — the 5
  known Typer/CLI failures plus the CCXT queue-overflow failure.
- Ticket-03 post-merge audit state: 5 failed, 1121 passed, 1 skipped,
  1 deselected.
- Post-merge audit run for this ticket: **5 failed, 1133 passed, 1 skipped,
  1 deselected** in 226.00s (0:03:46). The 12 new collected tests all pass
  (1121 + 12 = 1133). The CCXT queue-overflow failure did not reproduce in
  this run. The 5 failures are the known Typer/CLI ones:
  - `tests/integration/test_archive_backfill.py::test_cli_backfill_requires_archive_mode`
  - `tests/integration/test_archive_backfill.py::test_cli_backfill_rejects_unsupported_venue_and_market_type`
  - `tests/integration/test_archive_backfill.py::test_cli_backfill_requires_start`
  - `tests/integration/test_archive_backfill.py::test_cli_backfill_rejects_a_missing_archive_dir`
  - `tests/unit/test_cli_flags.py::test_run_rejects_an_unknown_service_role`
- Skipped: `tests/contract/test_storage_file_lifecycle.py:184` (POSIX
  unlink-of-open-file semantics; executed by the Linux CI job). Deselected:
  the `network`-marked test excluded by `addopts = "-ra -m 'not network'"`.
- No new failures. The implementation handoff described 15 new interface
  tests; the measured addition is 11 test functions / 12 collected cases, as
  above.

**Deviations / gaps for tickets 04-05:**

- `CollectorCore` now constructs a `StorageComposition` even when both Stores
  are injected. With an injected Store and no injected Quarantine, the default
  Quarantine is anchored on the injected Store root and the actual-root
  containment check runs at composition time. A configured Quarantine root
  nested inside that injected Store's Bars Dataset is therefore rejected with
  `ValueError` (`test_collector_core_rejects_a_quarantine_inside_an_injected_store`).
  Previously such a programmatic layout was accepted silently; normal configs
  are already rejected by model validation, so this affects only
  injected/programmatic layouts. This is intended by the validate-before-
  construct requirement.
- Archive backfill CLI still constructs `ParquetBarStore` and a default
  `QuarantineStore` directly, and `archive/importer.py` keeps its direct-call
  default Quarantine. Migrating the CLI to the joint composition and
  propagating the same effective Rule Version to archive import is ticket 04.
- Ticket 05 remains responsible for the final production creation-point audit,
  removal of the remaining duplicate mappings, documentation and full
  acceptance.
