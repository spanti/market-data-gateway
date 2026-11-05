# 11: 打通 Normalization Rule Version 血缘

**What to build:** 让运维人员能从 Manifest 和 Quarantine 判断一份数据由哪些 Normalization 规则产生，并在 Compaction 后保留完整来源血缘。Rule Version 只用于审计，不成为第二个公共 Schema 权威。

**Blocked by:** 09 在 Collector 批量边界落地 Polars Normalization 和 Quarantine 核心。

**Status:** resolved

- [x] 每个新 Bars Data File 的 Manifest 条目记录产生该批次所使用的 Normalization Rule Version 集合。
- [x] 每条 Quarantine Record 记录失败输入所适用的 Rule Version，与独立 Quarantine Schema Version 和 Data Contract Version 明确区分。
- [x] 旧 Data Files 能表示空集合或明确的 legacy/unknown，不得伪造历史规则版本，也不得因此无法读取。
- [x] Compaction 不重做 Normalization，输出条目的 Rule Version 集合是所有来源条目版本的并集。
- [x] 多次 Compaction 不会重复、丢失或伪造版本信息，也不会改变 Bars 的字段值和 Revision。
- [x] 查询层继续只根据 Data Contract Version 判断 Schema 兼容性，旧 Rule Version 不自动导致读取拒绝。
- [x] 若规则变化会改变公共数据语义，验证与文档要求升级 Data Contract，而不能只增加 Rule Version。
- [x] 版本元数据扩展不会破坏现有 Manifest 读取、Compaction、GC、Read Lease 和状态输出。
- [x] 测试覆盖单版本、多版本、legacy 来源、重复 Compaction、Quarantine 血缘和不受支持的 Data Contract Version。

## Comments

- Branch: `ticket/11-normalization-rule-version-lineage`; worktree
  `E:/total-project/mdgw-worktrees/duckdb-11`.
- Commit: `a8e57b0` feat(storage): track Normalization Rule Version lineage;
  this docs commit closes the ticket. `integration/duckdb-polars-storage`
  (tip `e85ce7e`) was merged before closing and was already contained.

### Rule Version source of truth

- New module `src/mdgw/normalize/version.py` is the only definition:
  `NORMALIZATION_RULE_VERSION = "polars-1"` for live Polars batch
  Normalization, `SCALAR_NORMALIZATION_RULE_VERSION = "scalar-1"` for the
  scalar reference rules used by Backfill/archive import, the corresponding
  `*_RULE_VERSIONS` sets, and `normalize_rule_versions()` (sorted, unique,
  tolerant read-path coercion).
- `QuarantineConfig.rule_version` defaults to the live constant, so Quarantine
  Records and new Bars Manifest entries use one value. `CollectorCore` and
  `GatewayRuntime` configure their Stores with that value, so an operator
  override stays coherent across both datasets.
- Backfill and archive import pass the scalar set explicitly to
  `write_bars(rule_versions=...)`; the live sink keeps the Store default.
  This is audit metadata, not an engine switch.
- `QuarantineStore` now rejects an empty `rule_version` before any I/O. The
  module docstring and `docs/data-contract.md` state that
  `rule_version` (audit), `quarantine_schema_version` (Quarantine envelope)
  and the Bar Data Contract Version are three separate values.

### Manifest metadata and Compaction

- Every new entry carries `rule_versions`, a sorted de-duplicated JSON list.
- `normalize_manifest_entry` defaults a missing/invalid value to `[]`
  (legacy/unknown) and preserves present values, so old Manifests stay
  readable and no historical version is invented.
- `_write_compacted_file` takes the union of the eligible entries' sets and
  copies the Parquet rows unchanged; repeated Compaction is idempotent for
  both lineage and Bar values/Revisions.
- Readers still decide compatibility from the Data Contract Version alone.
  The public `/api/v1/status` payload was intentionally left byte-shape
  compatible (the exact-shape characterization tests pin it); lineage is
  visible in `_manifest.json`.

### Tests

Command: `D:/Anaconda3/envs/nautilus/python.exe -m pytest -q` from the
worktree after merging the integration tip. Result: **960 passed, 5 failed,
1 skipped, 1 deselected** in ~164 s. The 5 failures are the known pre-existing
Typer/Click baseline failures; no new failure.

New coverage (12 tests):

- `tests/contract/test_normalization_rule_lineage.py` (9): single-version
  stamping, explicit multi-version sorting/de-duplication, missing/empty/
  explicit legacy markers, unknown Rule Version reads while an unsupported
  Data Contract Version still raises, Compaction union (including
  legacy+known and legacy-only), repeated Compaction, and unchanged Bar
  values/Revisions.
- `tests/integration/test_rule_version_lineage.py` (3): live Collector
  Quarantine + Manifest share the source of truth, a configured live Rule
  Version reaches both datasets, and Backfill stamps the scalar Rule Version.

Two ticket-09 assertions that hard-coded the old placeholder `"1"` now use
the source-of-truth constant.

### Merger notes

- No new dependency. Only ticket-11 files plus a one-line
  `rule_versions=...` pass-through in `archive/importer.py` (the write call,
  not ticket 10's archive quarantine stages); ticket 10 may see a trivial
  conflict there if it edits the same call.
- The status payload shape is unchanged; no config keys were removed.
