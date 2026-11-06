# 03: 将 Compaction 与 GC 运维命令接入按需组合

**What to build:** 运维人员执行 Compaction、Bars GC 和 Quarantine GC 时，CLI 通过 Storage Composition 获得恰好需要的存储对象：前两者使用 Bars-only，后者使用 Quarantine-only。命令行参数、root 传播、dry-run、报告、Read Lease 保护和 Retention 行为保持不变，无效命令仍在任何目录写入之前失败。

**Blocked by:** 01: 用 Bars-only Storage Composition 打通只读读取路径

**Status:** resolved

- [x] Compaction 与 Bars GC 的默认生产路径使用 Bars-only 组合，不再各自重复 Store 配置映射。
- [x] Quarantine GC 使用 Quarantine-only 组合，不创建 ParquetBarStore、DuckDB 临时目录或 Bars Dataset 状态。
- [x] Quarantine-only 组合每次返回新的 QuarantineStore，并在构造前根据本次实际 root 组合执行相同的 Dataset 独立性校验。
- [x] CLI root override 优先级与配置语义保持不变；默认 Quarantine 跟随实际 root，显式 Quarantine root 继续原值优先。
- [x] mdgw up 继续把解析后的 root 传给子进程，collector、api、replay 的启动和运维行为不因命令组合迁移而改变。
- [x] CLI 参数校验继续发生在 Storage Composition 之前；无效时间、Retention 或其他参数错误不会创建数据 root、descriptor、Manifest 或临时目录。
- [x] Compaction 的不可变 Data File、原子 Manifest 切换、Rule Version 集合并集、Read Lease 重查与报告语义不变。
- [x] Bars GC 的 Retention、dry-run、Manifest 失活、删除与 Read Lease 保护语义不变；Quarantine GC 的独立 Schema、整月清理、当前月跳过和 dry-run 行为不变。
- [x] 本次迁移不合并 DuckDBConfig 与 DuckDBOptions，不改变两个 Store 的公共构造接口，也不要求 benchmark 或测试数据构建器改用组合模块。
- [x] 新增或调整测试穿过 Storage Composition 与 CLI 公共接口，覆盖按需对象创建、root 传播、参数错误零写入和各命令的实际运维结果，不重复断言完整构造参数列表。
- [x] 现有 CLI flags、多进程 Service Role、Compaction、Bars GC、Quarantine Retention/GC、Manifest 与 Read Lease 测试保持通过。
- [x] archive CLI 可在本 ticket 中暂时继续使用其现有 Quarantine 默认创建方式并保持测试通过；其完整联合组合迁移由 ticket 04 完成。
- [x] 不新增生产组合开关、存储 adapter、全局生命周期管理者、GLOSSARY 术语或 ADR。

## Comments

**Branch:** `ticket/03-maintenance-storage-composition` (worktree
`E:/total-project/mdgw-worktrees/storage-03`), based on
`integration/storage-composition` at `6a04e34`.

**Commit:** `eab98a1` (single commit, "refactor(cli): compose maintenance
stores on demand"). Merged with `--no-ff` into
`integration/storage-composition` as `d040fba`. This closure commit follows.

**What was added/changed:**

- `src/mdgw/cli.py`: `compact` and `gc` now obtain their Store via
  `StorageComposition(gateway_config, root=root_path).bars()` instead of the
  duplicated `_build_store` mapping. `quarantine-gc` now obtains a fresh
  `QuarantineStore` via `.quarantine()`, so it no longer constructs a
  `ParquetBarStore`, creates a DuckDB temp directory or touches Bars state.
  The `--older-than` duration parse in `compact` and the non-negative
  Retention validation in `quarantine-gc` now run before composition; the
  `--dry-run`/`--apply` conflict check in `gc` already ran before config and
  composition. `quarantine-gc`'s default Retention now comes from the
  validated config value (`QuarantineStore` normalizes identically with
  `int()`; the config field is `Field(default=30, ge=0)`), preserving prior
  semantics. Root-override precedence and explicit Quarantine-root anchoring
  are delegated to composition and unchanged.
- Archive backfill is untouched: `_build_store` (defined at `cli.py:684`) is
  used only by archive backfill (`cli.py:799`), which still constructs its
  default `QuarantineStore` directly. This is the explicitly allowed
  ticket-04 gap.
- Tests: 7 new tests — 2 in `tests/integration/test_mdgw_cli.py` (invalid
  duration and conflicting flags fail before any storage root is created), 4
  in `tests/integration/test_quarantine_retention_gc.py` (default Quarantine
  root follows the root override, explicit Quarantine root keeps priority,
  nested-layout rejection, and parameter validation before composition), and
  1 in `tests/integration/test_storage_composition.py` (each `quarantine()`
  call returns a new Store with no construction-time filesystem effect).

No Store constructor, GLOSSARY, ADR or spec changes; only `cli.py` and three
test files changed (222 insertions, 12 deletions).

**Scope audit:** `git diff --stat 6a04e34..HEAD` shows exactly the four files
above. `git diff 6a04e34..HEAD -- src/mdgw/cli.py` confirms no archive-CLI
migration, no `_build_store` use outside archive/backfill, and no changes to
`up`, collector, api, replay or Store construction interfaces. No real
defects found in the diff.

**Tests:**

Post-merge, main workspace, targeted:

`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q tests/integration/test_mdgw_cli.py tests/integration/test_quarantine_retention_gc.py tests/contract/test_compaction_gc.py tests/integration/test_storage_composition.py tests/integration/test_multiprocess_services.py`

-> **55 passed, 0 failed** in 25.31s.

Full offline suite, same interpreter, `pytest -q`:

- Measured baseline on this host: **6 failed, 1093 passed, 1 skipped** — the 5
  known Typer/CLI failures plus the CCXT queue-overflow failure.
- Ticket-01 post-merge audit state: 5 failed, 1114 passed, 1 skipped,
  1 deselected.
- Post-merge audit run for this ticket: **5 failed, 1121 passed, 1 skipped,
  1 deselected** in 217.88s (0:03:37). The 7 new tests all pass
  (1114 + 7 = 1121). The CCXT queue-overflow failure did not reproduce, as in
  the ticket-01 audit. The 5 failures are the known Typer/CLI ones:
  - `tests/integration/test_archive_backfill.py::test_cli_backfill_requires_archive_mode`
  - `tests/integration/test_archive_backfill.py::test_cli_backfill_rejects_unsupported_venue_and_market_type`
  - `tests/integration/test_archive_backfill.py::test_cli_backfill_requires_start`
  - `tests/integration/test_archive_backfill.py::test_cli_backfill_rejects_a_missing_archive_dir`
  - `tests/unit/test_cli_flags.py::test_run_rejects_an_unknown_service_role`
- Skipped: `tests/contract/test_storage_file_lifecycle.py:184` (POSIX
  unlink-of-open-file semantics; executed by the Linux CI job). Deselected:
  the `network`-marked test excluded by `addopts = "-ra -m 'not network'"`.
- No new failures.

**Deviations / gaps for tickets 04-05:**

- Archive backfill still uses `_build_store` and a directly constructed
  default `QuarantineStore`; migrating it to the joint
  `bars_and_quarantine()` composition and propagating the same effective Rule
  Version to archive import is ticket 04.
- `quarantine-gc` now runs the actual-root independence check through
  composition even when `--retention-days 0` returns early. This is required
  by the ticket (validate before constructing the QuarantineStore) and fails
  before any filesystem write; an otherwise no-op keep-forever run can now
  reject a layout whose Quarantine root sits inside the actual Bars Dataset.
- `gc` still composes the Bars Store before checking `retention.enabled`
  (pre-existing ordering, unchanged by this ticket).
- Ticket 05 remains responsible for removing the last `_build_store` /
  duplicate-mapping remnants, auditing all production creation points, and
  documentation/acceptance.

