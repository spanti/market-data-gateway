# 04: 将 archive backfill CLI 接入统一组合并保持直接调用兼容

**What to build:** archive backfill CLI 通过 Bars + Quarantine Storage Composition 获得存储对象，并把同一有效 Normalization Rule Version 传给 archive import，使 Manifest 与 Quarantine 血缘一致。直接调用 archive import 的调用方仍可省略 GatewayConfig 和 Quarantine，并继续得到 Store root 下的默认 Quarantine；显式注入与整文件失败语义保持不变。

**Blocked by:** 02: 用 Bars + Quarantine 组合打通采集写入与血缘；03: 将 Compaction 与 GC 运维命令接入按需组合

**Status:** resolved

- [x] archive backfill CLI 使用联合组合创建 Bars Store 与 QuarantineStore，不再独立重复两者的配置映射或 Rule Version 默认逻辑。
- [x] CLI 将同一次组合的有效 Rule Version 传给 archive import；默认与自定义值均同时到达新 Bars Manifest 条目和 Quarantine Record。
- [x] CLI 继续使用配置化 Quarantine root、独立 Schema Version 与 Retention，root override 和显式 Quarantine root 的既有优先级不变。
- [x] archive import 的公共接口继续接受可选 Quarantine；直接调用者未注入时仍使用 Store root 下的 quarantine，不被要求提供 GatewayConfig。
- [x] 直接调用的默认 Quarantine Schema Version、Retention、物理格式和 Rule Version 默认行为保持兼容，不因 CLI 组合统一而被静默修改。
- [x] 显式注入 Quarantine 的直接调用与测试保持有效，Quarantine 写入失败的 fail-closed 行为不被吞掉或改写。
- [x] archive checksum、ZIP/CSV 结构、时间单位、整文件原子性、稳定 Reason Code、来源定位和 sanitized 错误行为不变。
- [x] archive backfill 的 High-Water Mark、Bar Aggregation、Revision merge 与实际 root 派生路径不变。
- [x] CLI 参数错误继续先于存储组合发生并产生零目录副作用；实际 root 组合无效时也在任何写入前失败。
- [x] 现有直接 archive 调用的默认 Quarantine 测试保持原样通过，CLI 配置化 Quarantine、自定义 Rule Version 和注入失败测试继续覆盖各自公共行为。
- [x] 新增测试证明 CLI 使用同一次联合组合的对象与 Rule Version，而不是通过私有字段或内部调用次数推断组合方式。
- [x] 不改变 archive 直接调用、CLI、Store、GLOSSARY 或 ADR 的公共契约，不引入新的 adapter 或全局生命周期管理者。

## Comments

**Branch:** `ticket/04-archive-storage-composition` (worktree
`E:/total-project/mdgw-worktrees/storage-04`), based on
`integration/storage-composition` at `ffbafd8`.

**Commit:** `6f5a4ae` (single commit, "refactor(cli): compose archive
backfill storage through one call"). Merged with `--no-ff` into
`integration/storage-composition` as `347923f`. This closure commit follows.

**What was added/changed:**

- `src/mdgw/cli.py`: archive backfill now obtains both Stores from one
  `StorageComposition(config, root=root).bars_and_quarantine()` call. The
  Rule Version passed to `import_archive` is `composition.rule_version`, the
  same value the composition uses to construct the Bars Store, so new
  Manifest entries and Quarantine records share one lineage identity for both
  default and custom values. `_build_store` was deleted (archive backfill was
  its last user) together with the `QuarantineStore` and `ParquetBarStore`
  imports; no direct Store construction remains in `cli.py`. High-Water Mark
  state is still anchored at `bundle.bars.root`, preserving the actual-root
  path derivation.
- `src/mdgw/archive/importer.py` was not touched: `quarantine` remains
  optional and defaults to `<store.root>/quarantine`, and `rule_version`
  still defaults to `NORMALIZATION_RULE_VERSION`; direct callers need no
  `GatewayConfig`. Explicit-quarantine injection and fail-closed
  `QuarantineWriteError` handling are unchanged.
- Tests: 4 new public CLI tests in
  `tests/integration/test_archive_backfill.py`:
  `test_cli_backfill_rejects_end_before_start_without_creating_storage`,
  `test_cli_backfill_rejects_an_override_induced_nested_layout_without_writes`,
  `test_cli_archive_backfill_uses_the_configured_quarantine_root` (asserts the
  Quarantine record's explicit root/schema/custom Rule Version and the same
  Rule Version in the Bars Manifest from one CLI run), and
  `test_cli_archive_backfill_default_quarantine_follows_the_root_override`.
  `_write_cli_fixture` gained optional `data_root`, `quarantine_root` and
  `quarantine_schema_version` knobs. No existing tests were removed.

Only two files changed: `src/mdgw/cli.py` (12 insertions, 31 deletions) and
`tests/integration/test_archive_backfill.py` (211 insertions, 8 deletions).
No Store constructor, GLOSSARY, ADR, spec or other ticket changes.

**Scope audit:** `git diff --stat ffbafd8..HEAD` shows exactly those two
files. The `cli.py` diff contains exactly one joint `bars_and_quarantine()`
call in archive backfill, passes `rule_version=composition.rule_version`,
deletes `_build_store` and the `QuarantineStore`/`ParquetBarStore` imports,
and leaves no direct Store construction. `git diff ffbafd8..HEAD --
src/mdgw/archive/importer.py` is empty. A `src/` search finds no remaining
`_build_store`; direct `ParquetBarStore(...)`/`QuarantineStore(...)`
construction survives only in `store/composition.py` and the archive
importer's documented direct-call default, and all production creation sites
(api, replay, collector, GatewayRuntime, compaction/GC, archive CLI) use
`StorageComposition`. No real defects found in the diff.

**Tests:**

Post-merge, main workspace, targeted:

`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q tests/integration/test_archive_backfill.py tests/integration/test_storage_composition.py tests/integration/test_mdgw_cli.py tests/integration/test_quarantine_retention_gc.py tests/integration/test_rule_version_lineage.py tests/integration/test_normalization_quarantine.py tests/integration/test_multiprocess_services.py`

-> **93 passed, 4 failed** in 27.01s. The 4 failures are the known
Typer/CLI archive tests listed below; the four new tests pass.

The four new tests in isolation:

`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q tests/integration/test_archive_backfill.py -k "configured_quarantine_root or default_quarantine_follows or end_before_start_without or override_induced_nested"`

-> **4 passed, 30 deselected** in 3.25s.

Full offline suite, same interpreter, `pytest -q`:

- Baseline at `ffbafd8` (recorded by the ticket-02 closure audit on this
  host): **5 failed, 1133 passed, 1 skipped, 1 deselected**.
- Post-merge run for this ticket: **5 failed, 1137 passed, 1 skipped,
  1 deselected** in 219.09s (0:03:39). The 4 added tests account for the
  +4 passed; the same five known failures remain and no new failure
  appeared. The CCXT quarantine/queue timing tests did not fail in this run.
- The five failures are the known deterministic Typer/CLI ones:
  - `tests/integration/test_archive_backfill.py::test_cli_backfill_requires_archive_mode`
  - `tests/integration/test_archive_backfill.py::test_cli_backfill_rejects_unsupported_venue_and_market_type`
  - `tests/integration/test_archive_backfill.py::test_cli_backfill_requires_start`
  - `tests/integration/test_archive_backfill.py::test_cli_backfill_rejects_a_missing_archive_dir`
  - `tests/unit/test_cli_flags.py::test_run_rejects_an_unknown_service_role`
- Skipped: `tests/contract/test_storage_file_lifecycle.py:184` (POSIX
  unlink-of-open-file semantics; executed by the Linux CI job). Deselected:
  the `network`-marked test excluded by `addopts = "-ra -m 'not network'"`.

**Deviations / gaps for ticket 05:**

- End-before-start validation now runs before composition (previously
  `_build_store` and the default `QuarantineStore` were constructed first).
  This is required by the zero-side-effect checkbox, and the new test asserts
  that no data root or DuckDB temp directory is created.
- Tests and implementation are in one commit (`6f5a4ae`), not separate
  red/green commits.
- A configured Quarantine root nested inside the actual Bars root now fails
  in archive backfill during composition, before any write. The previous
  direct construction did not run this independence check. This matches the
  spec's actual-root validation and is covered by
  `test_cli_backfill_rejects_an_override_induced_nested_layout_without_writes`.
- Ticket 05 remains responsible for the final production creation-point
  audit, documentation updates, cross-platform (Windows/POSIX) verification,
  and recording final baseline-relative acceptance results. No `_build_store`
  or duplicate Store-mapping remnants remain in `src/`.
