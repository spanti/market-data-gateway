# 10: 覆盖 Feed/Archive Quarantine 与 Retention/GC

**What to build:** 将 Quarantine 从 Collector 核心扩展到 CCXT 单行解析和归档验证，并提供可运维的指标、默认 30 天 Retention 与 dry-run 整月 GC。不同来源保持各自已批准的失败原子性，不把所有数据问题都当成同一种清洗失败。

**Blocked by:** 09 在 Collector 批量边界落地 Polars Normalization 和 Quarantine 核心。

**Status:** resolved

- [x] CCXT 单行无法解析时，只有成功写入 Quarantine 才能跳过并继续该来源；隔离写入失败时不得越过该记录。
- [x] CCXT 的闭合识别、修正 Bars、重连、历史分页和队列行为保持不变，Venue 协议逻辑不迁移到 Polars。
- [x] 归档 checksum、ZIP/CSV 结构、关键字段和来源时间单位失败时，整个归档文件不写入规范 Dataset，并留下可定位来源的 Quarantine 记录。
- [x] 队列溢出继续通过 HWM 缺口与 Backfill 修复，不生成虚假的 Validation Quarantine Reason。
- [x] Quarantine 指标按稳定 Source Stage 和 Reason Code 聚合，不使用异常文本、文件路径、request ID 等高基数标签。
- [x] Quarantine Retention 默认 30 天，可配置，0 表示永久保留；容量与写入失败状态可观测并触发告警。
- [x] GC 默认 dry-run，只删除超过 Retention 的完整 UTC 月份，跳过当前月，不引入自动后台维护任务。
- [x] GC 不影响 Bars、Instrument Reference Data、HWM 或正常 Dataset Manifest，也不会让 Quarantine 进入正常查询。
- [x] 测试覆盖隔离成功后来源继续、隔离失败时停止消费、归档整文件原子性、永久保留、月份边界和当前月保护。

## Comments

**Branch:** `ticket/10-quarantine-sources-retention` (worktree `E:/total-project/mdgw-worktrees/duckdb-10`)

**Commits:**
- `5d405d2` feat(quarantine): cover CCXT/archive sources and add retention GC
- `f426c7b` merge: integration/duckdb-polars-storage into ticket 10 (resolved `src/mdgw/config.py` and `src/mdgw/archive/importer.py` conflicts with ticket 11 rule-version lineage)

**Source-stage / reason mapping:**
- CCXT live malformed row: `source_stage=ccxt_live_row`, `reason_code=malformed_row`, location `ccxt:<symbol>:live:row=<i>`.
- CCXT history malformed row: `source_stage=ccxt_history_row`, `reason_code=malformed_row`, location `ccxt:<symbol>:history:row=<i>`; readable timestamps still advance pagination.
- Archive checksum (missing CHECKSUM, unparseable digest, SHA-256 mismatch): `source_stage=archive_file`, `reason_code=archive_checksum_mismatch`, location `archive:file=<zip>` with filename/period/instrument details.
- Archive ZIP/CSV structure (bad ZIP, CSV count/name, UTF-8, column count, close_time): `reason_code=archive_format_error`.
- Archive source-time-unit mismatch/magnitude: `reason_code=archive_time_unit_unsupported`.
- Archive key-field or scalar Normalization failure: `reason_code=archive_field_error`.
- Queue overflow keeps the existing dropped-Bar/HWM-gap/Backfill path; it never writes a Quarantine record.
- A failed Quarantine write is fail-closed: CCXT stops `iter_bars()`/`fetch_history()` with `QuarantineWriteError` (no reconnect past the row); archive reports an extra `ArchiveReport.failures` entry and writes no Bars.

**Retention / GC rules:**
- `storage.quarantine.retention_days` defaults to 30 (`0` = keep forever); `retention_days=0` makes `QuarantineStore.gc()` a no-op.
- `QuarantineStore.gc(retention_days, apply=False, now=None)` defaults to dry-run; only complete UTC months whose month end is `<= cutoff` are eligible; the current UTC month is always skipped; the Manifest is switched atomically before unlinking and empty `year=/month=` dirs are pruned.
- Operator command: `mdgw quarantine-gc [--retention-days N] [--apply|--dry-run] [--config PATH] [--root PATH]`; default is dry-run and no background task is scheduled.
- GC only touches `<storage.root>/quarantine`; Bars Data Files/Manifest, the Instrument Snapshot and the HWM are untouched. Quarantine stays outside Bars/REST/Replay.
- `QuarantineStore.stats()` now exposes `retention_days`, `active_bytes`, `active_file_count`, `write_failures`, `gc_runs` and `last_gc`; Collector metrics add `mdgw_collector_quarantine_active_files`, `..._active_bytes`, `..._retention_days` plus the existing stage/reason record and write-failure counters.

**Tests:** `D:/Anaconda3/envs/nautilus/python.exe -m pytest -q`
Result after merging the integration tip: **984 passed, 5 failed, 1 skipped, 1 deselected**. The 5 failures are the known pre-existing Typer/Click issues (`test_cli_backfill_requires_archive_mode`, `test_cli_backfill_rejects_unsupported_venue_and_market_type`, `test_cli_backfill_requires_start`, `test_cli_backfill_rejects_a_missing_archive_dir`, `test_run_rejects_an_unknown_service_role`).
