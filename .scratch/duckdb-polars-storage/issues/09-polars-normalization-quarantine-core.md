# 09: 在 Collector 批量边界落地 Polars Normalization 和 Quarantine 核心

**What to build:** 让现有 Collector 的一批 Raw Bars 通过 Polars 完成与旧实现等价的 Normalization/Validation，然后才进入 Instrument 校验、Revision merge 和 Bar Aggregation。失败批次不发布且留下独立 Quarantine 审计记录，Quarantine 写入失败时整批 fail-closed。

**Blocked by:** 07 迁移单 Instrument Time Cursor 分页；08 迁移 CSV、export 与流式失败行为。

**Status:** resolved

- [x] Polars 在现有 Collector 批量边界运行，不为每条实时消息创建单行 DataFrame，也不改变 Feed Handler 的协议状态机。
- [x] 批量结果与旧标量 Normalization 逐字段等价，包括 InstrumentId、Bar Interval、closed-Bar 检查、UTC、close/event/ingest 时间、Decimal scale 和固定 envelope 字段。
- [x] float-shaped 数值、未闭合 Bars、非法 Interval 和超额小数精度继续被拒绝，不发生隐式 float 转换或舍入。
- [x] Normalization 和 Instrument Validation 在 Aggregation 状态推进之前完成；任何失败都保持 Aggregation、HWM、freshness 和持久化结果不动。
- [x] 建立独立、版本化 Quarantine Dataset，记录 Source Stage、稳定 Reason Code、来源定位、结构化详情和可获得的原始记录表示。
- [x] Quarantine 使用独立 Schema，不会被正常 Bars Store、REST 或 Replay 注册、扫描或导出。
- [x] Quarantine 写入失败时对应批次不得被确认消费、跳过或发布，必须暴露明确错误和失败指标。
- [x] Reason Code 与 Source Stage 为稳定低基数词汇，异常文本仅作为 sanitized 诊断详情，不能用于程序分支或指标标签。
- [x] 保留旧标量实现仅用于本阶段差分测试，不增加生产 Normalization 引擎开关；最终删除由收缩 ticket 完成。
- [x] 覆盖空批、单行、多行、混合 Instrument、失败重试和 Quarantine 故障的端到端 Collector 测试。

## Comments

- Branch: `ticket/09-polars-normalization-quarantine-core`; worktree
  `E:/total-project/mdgw-worktrees/duckdb-09`.
- Commits: `e826ec7` feat(collector): normalize live batches with Polars and
  quarantine rejections; the docs commit below closes the ticket. Merged
  `integration/duckdb-polars-storage` (tip `d14aebd`) before closing; it was
  already contained.

### Polars Normalization seam

- `CollectorCore.ingest()` drains one Feed Handler batch, then calls
  `mdgw.normalize.batch.normalize_batch(raws, feed=..., ingest_ts=...)` exactly
  once. There is no per-message DataFrame and no Normalization engine switch;
  `mdgw.normalize.bar.normalize_bar` remains unchanged for Backfill, archive
  and differential tests.
- The batch first validates field types (`float` prices, over-scale /
  too-many-integer-digit decimals, non-finite decimals, non-int64 counters and
  non-UTC-convertible timestamps) without constructing an untyped Decimal
  Series. Surviving values become one typed Arrow table
  (`decimal128(38,18)`, `timestamp[ns, tz=UTC]`, `string`, `int64`) and enter
  Polars via `pl.from_arrow`.
- Polars computes InstrumentId validity (`(?s)^[^:]+:.+$`), Bar Interval
  membership, `close_ns = open_ns + interval_ns` and the closed-Bar rule
  (`close_ns <= ingest_ns`), then stamps the fixed envelope
  (`schema_version=1`, `data_type="bar"`, `feed`, `seq=None`,
  `aggregation_source="external"`). Valid rows become `Bar` objects from the
  Polars output; decimals are never float64 and are never silently rescaled.
- Rejection precedence follows the scalar rule: invalid InstrumentId, invalid
  Interval, unreadable timestamp, unclosed Bar, first decimal field in
  `open/high/low/close/volume` order, then other malformed fields.

### Batch atomicity and failure semantics

- Any rejection from Normalization or Instrument validation rejects the WHOLE
  batch before `merge_bar_batch`/`BarAggregator`. No valid row is published or
  persisted; Aggregation windows/retention, HWM, freshness and the Bars
  Dataset stay untouched, so the batch is retried after fixing the input or
  reference data.
- Every rejected row gets one Quarantine record. A batch can mix stages:
  normalization failures use `live_normalize`, unknown Instruments use
  `live_instrument_validate`. `NormalizationRejected` batches raise
  `BatchNormalizationError` (a `ValueError`, with a legacy-readable message
  such as `Bar is not closed: ...`); an unknown-Instrument-only batch keeps the
  existing `UnknownInstrumentError` type.
- `QuarantineStore.write_records()` is fail-closed: Parquet files are written
  first, the Quarantine Manifest is appended second. Any write or append
  failure removes the new files, increments `write_failures`, raises
  `QuarantineWriteError` and the batch is neither acknowledged, skipped nor
  published.

### Quarantine Dataset

- Module `src/mdgw/quarantine.py`. Default root `<storage.root>/quarantine`,
  configurable through `[storage.quarantine]` (`root`, independent
  `schema_version`, `rule_version`); a configured root inside
  `<storage.root>/bars` is rejected.
- Own descriptor `_quarantine_dataset.json` (`schema_version = 1`, own Arrow
  schema, `year/month` Hive partitioning) and own manifest
  `_quarantine_manifest.json`. Immutable Parquet files live under
  `year=YYYY/month=MM` and are named `quarantine-...parquet`, never
  `data-...parquet` and never with Bar identity partitions.
- Record schema is the agreed contract: `quarantine_schema_version` uint16,
  `recorded_ts` UTC ns, `source_stage`, `reason_code`, `rule_version`, nullable
  `feed`/`instrument_id`/`interval`/`open_ts`, `source_location`, JSON
  `details`, nullable JSON `raw_record`. Live locations look like
  `live:batch:<ingest_ns_iso>:row=<index>`. Long diagnostic strings are
  truncated; exception text is never a branch or a metric label.
- The Bars Store (`<root>/bars`), REST Bars routes and Replay only consult the
  Bars Manifest, so they never register, scan or export Quarantine records.
- `QuarantineStore.stats()` exposes records written, write failures, last
  write timestamp, active file count and per-`source_stage`/`reason_code`
  counts. `CollectorService.render_metrics()` converts those stats to deltas
  (`mdgw_collector_quarantine_records_total{source_stage,reason_code}` and
  `mdgw_collector_quarantine_write_failures_total`) and `status_payload()` adds
  a `quarantine` section; the core is not coupled to the MetricsRegistry.

### Tests

Command: `D:/Anaconda3/envs/nautilus/python.exe -m pytest -q` from the
worktree after merging the integration tip. Result: **948 passed, 5 failed,
1 skipped, 1 deselected** in ~159 s. The 5 failures are the known pre-existing
Typer/Click baseline failures (`tests/integration/test_archive_backfill.py`
x4 and `tests/unit/test_cli_flags.py::test_run_rejects_an_unknown_service_role`);
no new failure and no temp-directory flake.

New coverage (35 tests):

- `tests/unit/test_polars_batch_normalization.py` (17): differential equality
  with `normalize_bar` for empty/one/many/mixed-Instrument batches, timezone and
  envelope equivalence, exact decimal precision, and rejection of float,
  unclosed, invalid Interval, over-scale decimal, invalid InstrumentId and
  naive timestamps; mixed batches report every rejection.
- `tests/unit/test_quarantine_store.py` (6): independent descriptor/manifest,
  independent schema version, UTC month partitioning, record content,
  stats-by-stage/reason, atomic manifest-failure cleanup, vocabulary rejection
  and detail truncation.
- `tests/integration/test_normalization_quarantine.py` (8): mixed rejection
  batch leaves Aggregation/HWM/freshness/Dataset untouched and retries;
  unknown-Instrument batch quarantines then retries; Quarantine write failure
  is fail-closed for both stages; empty batch; Quarantine invisible to Bars
  Store/REST/Replay; CollectorService metrics/status records and failures.
- `tests/unit/test_config.py` (+4): QuarantineConfig defaults/root resolution,
  custom root/version, empty rule version and Bars-Dataset containment
  rejection.

### Merger notes

- No new dependency (`polars` and `pyarrow` were already runtime
  dependencies). No production Normalization engine switch.
- `GatewayRuntime` now exposes `runtime.quarantine`; `CollectorCore` accepts an
  optional `quarantine=` store for tests and exposes `core.quarantine`.
- Ticket 10 owns retention/GC, CCXT/archive quarantine stages and operator
  commands; this ticket only defines the shared store, schema and vocabulary.
