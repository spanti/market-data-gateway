# 05: 收缩旧组合实现并完成文档与全量验收

**What to build:** 网关的全部生产存储创建都经过已确认的按需 Storage Composition，不再存在重复的配置映射、旧组合后备或切换开关。维护者能从设计与使用文档理解实际 root 和写入前校验，测试与验收结果证明公共行为、直接 Store 使用和跨平台路径语义没有非预期变化。

**Blocked by:** 04: 将 archive backfill CLI 接入统一组合并保持直接调用兼容

**Status:** resolved

- [x] 审计全部生产 Bars Store 与 Quarantine Store 创建点；除 Storage Composition 和 archive 直接调用的已确认兼容默认外，不再存在重复的 GatewayConfig 到 Store 参数映射。
- [x] 若实施期间新增了等价生产组合点，也已迁移到相同规则；最终没有旧组合路径、引擎开关、组合开关或生产测试专用后备。
- [x] Bars-only、Quarantine-only、Bars + Quarantine 三种操作均按 spec 工作，且不存在绕过实际 root 校验的生产调用。
- [x] api、replay、collector、GatewayRuntime、Compaction、Bars GC、Quarantine GC 和 archive CLI 的默认生产路径均使用正确的按需组合。
- [x] ParquetBarStore 与 QuarantineStore 的公共构造接口保持兼容；测试、benchmark、示例、characterization 数据构建器和 Store 子类没有被无收益地批量迁移。
- [x] 显式注入、archive 直接调用默认 Quarantine、Feed Handler 缺失依赖填补、Rule Version 审计含义和 Dataset 独立性均与 spec 一致。
- [x] DuckDBConfig 与 DuckDBOptions 仍未合并，配置懒加载方向未倒退，Quarantine 物理格式配置没有意外继承 Bars。
- [x] 设计与使用文档同步说明 Storage Composition、root override、默认与显式 Quarantine root、实际 root 独立性校验和失败前零写入；文档不存在第二套规则。
- [x] 未把内部 Storage Composition 名称加入 GLOSSARY，未新增 ADR，未实现其他架构候选或无关修复。
- [x] Storage Composition interface 测试与现有 GatewayRuntime、Service Role、CLI、REST、Replay、archive、Normalization、Quarantine、Rule Version、Manifest 和 Data Contract 测试全部按预期执行。
- [x] 在 Windows 与可用 POSIX 环境执行实际 root、相对路径、父目录片段和失败前零写入测试；路径比较不依赖某一操作系统的分隔符或当前目录假设。
- [x] 实施开始时的当前完整离线基线与最终结果均已记录；最终相对基线无新增失败，命令、通过数、失败、跳过和任何波动均如实记录，未用历史结果替代实测。
- [x] ticket 的 Comments 记录迁移审计、文档变更、测试命令与结果、跨平台执行范围以及任何未执行验证；未执行的验证不得写成通过。
- [x] 工作区已有未提交修改得到保留，父 spec 的状态和内容不因本 ticket 收尾而被擅自修改。

## Comments

**Branch:** `ticket/05-storage-composition-acceptance` (worktree
`E:/total-project/mdgw-worktrees/storage-05`), based on
`integration/storage-composition` at `7513974`.

**Commit:** `720f02a` (single commit, "docs(storage): document storage
composition and effective root"). Merged with `--no-ff` into
`integration/storage-composition` as `dacbfac`. This closure commit follows.

**Scope audit:** `git diff --stat 7513974..HEAD` shows exactly `README.md`,
`DESIGN.md` and `docs/data-contract.md` (58 insertions, 14 deletions). No
`src/`, `tests/`, `.scratch/`, `GLOSSARY.md`, ADR, spec or project-config
changes. No conflicts during the merge. No production defects found.

**Migration audit conclusion:**

- Production Store creation points are exactly three: two in
  `src/mdgw/store/composition.py` (`ParquetBarStore`, `QuarantineStore`) and
  the archive importer's documented direct-call default
  (`src/mdgw/archive/importer.py`: `quarantine or
  QuarantineStore(Path(store.root) / "quarantine")`). `ParquetBarStore` is
  constructed nowhere else in `src/`; no duplicate `GatewayConfig` -> Store
  parameter mapping remains.
- Composition is on demand per command: `api`, `replay`, `compact` and `gc`
  use `.bars()`; `quarantine-gc` uses `.quarantine()`; `collector`,
  `GatewayRuntime` and `backfill --archive` use `.bars_and_quarantine()`.
  Every call returns new objects; there is no cache, global registry,
  lifecycle ownership, Feed Handler construction, or engine/composition
  switch.
- No old path, fallback or switch remains: no `_build_store`, storage
  engine/backend switch, `use_composition`, or legacy/fallback Store path in
  `src/`. All three composition operations run `_check_independence()`
  against the normalized actual roots before constructing any Store, so a
  root override, `..` fragment or programmatically built config cannot
  bypass validation; the check only enforces the existing one-way ADR-0014
  containment.
- Public constructors and direct Store use are unchanged: the spec diff
  (`2323edd..HEAD`) does not touch `store/parquet_dataset.py`,
  `quarantine.py` or `config.py`; tests, benchmarks, examples,
  characterization builders and Store subclasses were not migrated.
- Injection seams are preserved: `CollectorCore` keeps explicitly injected
  Store/Quarantine/Feed Handler and composes only missing objects (a default
  Quarantine still anchors on an injected Store root, and an explicit
  configured Quarantine root still wins). `import_archive` still accepts an
  optional Quarantine and needs no `GatewayConfig`.
- Rule Version semantics are unchanged: `composition.rule_version` (default
  `polars-1` or configured `[storage.quarantine].rule_version`) reaches Bars
  Manifest entries, Quarantine records and default-built Feed Handlers; it
  remains audit metadata only, with the Data Contract Version as the sole
  Schema authority. Quarantine keeps independent schema version, retention
  and physical-format defaults (composition passes only `schema_version` and
  `retention_days`).
- `DuckDBConfig.to_options()` still lazily imports `DuckDBOptions`; the two
  types remain separate and Quarantine does not inherit Bars physical format
  settings.

**Docs changed:** `README.md` (effective root, on-demand composition,
default vs explicit Quarantine root, zero-write pre-construction validation,
direct `import_archive` default); `DESIGN.md` section 5.8 (Quarantine default
follows the effective root), new section 5.9 (Storage Composition and
effective root), section 6 config comments (`temp_directory` default,
Quarantine root), section 7 module tree; `docs/data-contract.md` (effective
root, explicit root used as written, actual-root re-check before any write,
direct-call default). The design and usage docs now state one rule.

**Tests:**

Measured pre-change baseline at implementation start on `2323edd`
(`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q`):
**6 failed, 1093 passed, 1 skipped, 1 deselected in 220.66s**. Failures: the
four archive Typer/CLI tests,
`tests/unit/test_ccxt_feed_handler.py::test_queue_overflow_is_not_quarantined`,
and `tests/unit/test_cli_flags.py::test_run_rejects_an_unknown_service_role`.

Post-merge full offline suite, main workspace, same interpreter,
`pytest -q`:
**5 failed, 1137 passed, 1 skipped, 1 deselected in 222.60s (0:03:42)**.
Failures:
- `tests/integration/test_archive_backfill.py::test_cli_backfill_requires_archive_mode`
- `tests/integration/test_archive_backfill.py::test_cli_backfill_rejects_unsupported_venue_and_market_type`
- `tests/integration/test_archive_backfill.py::test_cli_backfill_requires_start`
- `tests/integration/test_archive_backfill.py::test_cli_backfill_rejects_a_missing_archive_dir`
- `tests/unit/test_cli_flags.py::test_run_rejects_an_unknown_service_role`

The CCXT `test_queue_overflow_is_not_quarantined` passed in this observed
run, so only five of the six measured-baseline failures appeared. The
implementer's post-merge Windows run observed **6 failed, 1136 passed,
1 skipped, 1 deselected in 223.54s**, with that CCXT test failing. The CCXT
queue-overflow/quarantine tests are the known-flaky timing family; both runs
contain only measured-baseline failures and no new deterministic failure.
Skipped: `tests/contract/test_storage_file_lifecycle.py:184` (POSIX
unlink-of-open-file semantics, executed on Linux). Deselected: the
`network`-marked test excluded by `addopts = "-ra -m 'not network'"`.

Explicit focused files, post-merge Windows:
`pytest -q tests/integration/test_storage_composition.py tests/integration/test_examples_docs_smoke.py`
-> **45 passed in 12.85s** (no failures or skips).

Implementer's focused Windows suites: **4 failed, 93 passed in 28.48s**; the
4 failures are the known archive CLI ones. Ruff: `src/` has 0 findings; test
files have the same 35 pre-existing findings in the same three files
(17 `test_archive_backfill.py`, 13 `test_mdgw_cli.py`, 5
`test_quarantine_retention_gc.py`) as baseline; the new
`test_storage_composition.py` has 0 findings.

**Linux/POSIX Docker (implementer-reported, recorded as run):**

- The exact command using the PyPI index did not execute: pip failed while
  installing build dependencies with
  `SSL: UNEXPECTED_EOF_WHILE_READING` fetching `setuptools` from
  `pypi.org`. A Tsinghua mirror attempt was rejected with HTTP 403 on a
  wheel, so the same `python:3.13-slim` image, mount and suite were run with
  the Aliyun PyPI mirror substituted. Only the package index changed.
- First Aliyun-mirror full run was anomalous: **38 failed, 1100 passed,
  3 skipped, 1 deselected in 3676.75s (1:01:16)**, dominated by
  characterization/timing failures that pass on rerun; it is recorded as an
  environmental outlier, not as the acceptance result.
- Clean rerun in a persistent container of the same image: **6 failed,
  1132 passed, 3 skipped, 1 deselected, 1 warning in 196.01s**. Failures:
  the four known archive CLI Typer cases,
  `test_run_rejects_an_unknown_service_role`, and
  `tests/unit/test_ccxt_feed_handler.py::test_malformed_live_row_stops_the_source_when_quarantine_write_fails`
  (the known-flaky CCXT quarantine timing family; it failed on this run,
  passed in isolation, and passes on Windows). Skips: two cases requiring
  the absent `nautilus_trader` and one Windows file-sharing behavior case.
  Deselected: the network-marked test.
- No verification is claimed beyond what actually ran; the exact
  PyPI-index form of the Docker command was not executed to completion
  because of the container network SSL failure.

**Cross-platform path safety (implementer-reported):** explicit POSIX run in
the same Linux container over the `tests/integration/test_storage_composition.py`
root override, nested-layout rejection, parent-directory fragment, relative
CWD and zero-write cases plus `tests/contract/test_storage_file_lifecycle.py`
including `test_posix_unlink_of_open_file_keeps_stream_readable` ->
**20 passed, 20 deselected in 3.24s**. On Windows the explicit
`test_storage_composition.py` run above covers the same path cases; the
composition compares normalized `Path.resolve()` values rather than OS
separators or current-directory assumptions.

**Residual notes:** The named failures are pre-existing (Typer/Click exit-code
behavior and the CCXT timing family) and were deliberately left untouched.
The parent spec `.scratch/storage-composition/spec.md` and its status were
not modified (last touched by `a1ae1d8` when added). The pre-existing
`.gitignore` modification and untracked `handoff.md` in the main workspace
were preserved and not committed. ADR-0014 still phrases the default
Quarantine root as `<storage.root>/quarantine`; that remains accurate when
no `--root` override is given, and the ADR was left unchanged as a historical
record while the operational docs describe the effective-root rule.

## Post-review fixes

Two-axis review of `d574487`: no hard standards violations and no spec
implementation defects; 1 test-coverage gap plus 4 standards judgement calls
were recorded.

Fix branch `ticket/storage-review-fixes` (worktree
`E:/total-project/mdgw-worktrees/storage-review`), based on `d574487`:
- `3312eed` refactor(config): share the quarantine independence check
- `db659f9` refactor(collector): extract default store resolution
- `e94954d` refactor(cli): use the composition-owned effective root
- `193b739` test(quarantine): reuse _write_config for GC layout cases
- `068fc3e` test(storage): cover snapshot and HWM under the effective root

Merged with `--no-ff` as `6164256` ("merge: storage composition code-review
fixes"). Scope audit `git diff --stat d574487..HEAD`: exactly six files,
141 insertions, 92 deletions — `src/mdgw/config.py`,
`src/mdgw/store/composition.py`, `src/mdgw/service/collector_core.py`,
`src/mdgw/cli.py`, `tests/integration/test_quarantine_retention_gc.py`,
`tests/integration/test_storage_composition.py`. One shared
`ensure_quarantine_is_independent` helper now backs both config and
composition with the same `ValueError` message; injected-Store resolution and
CLI effective-root behavior are unchanged. No docs, spec, ticket, GLOSSARY or
ADR changes; no behavior change beyond the refactor.

Post-fix full offline suite in the main workspace
(`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q`):
**5 failed, 1138 passed, 1 skipped, 1 deselected in 224.43s (0:03:44)**.
Failures are only the known pre-existing ones: the four
`tests/integration/test_archive_backfill.py` Typer/CLI cases and
`tests/unit/test_cli_flags.py::test_run_rejects_an_unknown_service_role`.
Focused files: `test_config.py` 56 passed,
`test_storage_composition.py` 34 passed,
`test_quarantine_retention_gc.py` 7 passed, `test_mdgw_cli.py` 7 passed,
`test_multiprocess_services.py` 5 passed. Ruff: changed `src/` files 0
findings; changed test files only the pre-existing
`test_quarantine_retention_gc.py` findings.

Intentionally not changed: `api`/`replay` central `rule_versions` ownership;
nested-layout tests remain explicit rather than parametrized.
