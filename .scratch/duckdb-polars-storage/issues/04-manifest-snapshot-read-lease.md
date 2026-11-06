# 04: 为现有读取器引入 Manifest Snapshot 与 Dataset Read Lease

**What to build:** 在继续使用 PyArrow 的情况下，让一次 HTTP 请求或 Replay Session 读取固定 Manifest Snapshot，并通过 Dataset Read Lease 阻止 Compaction/GC 删除仍在使用的 Data Files。先证明快照与维护协议，再让 DuckDB 接入同一协议。

**Blocked by:** 01 建立旧实现的差分测试与性能基线；02 完成 DuckDB 2.0/Polars 类型与生命周期 Spike。

**Status:** resolved

- [x] Store 只从 Manifest 选择活动 Data Files，并继续使用 Instrument、Interval、分区与时间范围进行打开前裁剪；不得引入根目录 glob。
- [x] Lease 流程覆盖候选选择、Lease 获取、Manifest 代际复核和变化时释放重试，读取不会混合不同 Manifest 代际。
- [x] 同一 HTTP 请求的 count、存在性探测和实际读取共享 Snapshot 与 Lease；不同分页请求不持有跨请求 Lease。
- [x] Replay Session 在整个生命周期使用固定 Snapshot，包括暂停与 seek，并兼容现有分区保护行为。
- [x] Lease 持有到 Reader 完成、取消或失败并关闭之后；客户端断开、异常和重复关闭也能释放。
- [x] 长时间读取使用 heartbeat，能够识别和处理进程异常退出后的过期 Lease。
- [x] Compaction/GC 遇到有效 Lease 时跳过并报告，未受保护数据仍按 Manifest-first 原则维护。
- [x] 使用并发测试证明维护切换发生在候选选择、Lease 获取和读取中的不同阶段时，结果始终是一致的新快照或旧快照。
- [x] 保留现有 Schema Version 校验、稳定错误映射和 raw Revision 行为，不引入 DuckDB 生产查询。
- [x] 现有 Store、REST、Replay 和维护测试保持通过，并新增成功、异常、取消与过期 Lease 的生命周期测试。

## Comments

**Branch:** `ticket/04-manifest-snapshot-read-lease` (worktree
`E:/total-project/mdgw-worktrees/duckdb-04`).

**Commits:** implementation `dbd5b9e`; ticket closure commit follows.
Integration tip `2e620aa` merged into the branch (already up to date).

**Files changed (production):**
`src/mdgw/store/parquet_dataset.py` (ManifestSnapshot, BarReadQuery,
StoreRead, `open_read`, legacy helper delegation),
`src/mdgw/store/session_locks.py` (DatasetReadLease, scoped lock records),
`src/mdgw/store/manifest.py` (`manifest_generation`),
`src/mdgw/store/maintenance.py` (lease matching + pre-switch/pre-unlink
re-checks + GC re-activation),
`src/mdgw/api/routes/bars.py` (one StoreRead per request),
`src/mdgw/replay/session.py` and `src/mdgw/replay/source.py` (one fixed
Snapshot + Lease per Session). No DuckDB production query and no engine
switch were added.

**Snapshot / lease protocol (exact):**
1. `store.open_read(...)` normalizes active Manifest entries and selects
   candidates by Instrument, Interval, partition and file `[start_ns,
   end_ns)` range before any Data File is opened. It never globs the Dataset
   root. The lease scope is the exact selected partitions plus their months.
2. `DatasetReadLease.acquire` writes the existing
   `_sessions/<lease-id>.lock` heartbeat file (Replay uses the session id),
   extended with `partitions` and `months` records, and starts a heartbeat
   thread (`read_lease_heartbeat_s`, default
   `min(5, max(0.05, session_lock_timeout_s / 3))`).
3. The Manifest generation is re-read (SHA-256 of the `_manifest.json`
   bytes, or `missing`). If it changed, the lease is released and the
   selection is retried, bounded by `READ_LEASE_ATTEMPTS = 3`; otherwise the
   generation and selected entries are pinned in `ManifestSnapshot`.
4. `StoreRead.count()`, `iter_batches()` and `read_bars()` all prune and scan
   from that pinned Snapshot. A `FileNotFoundError` while opening before any
   row is emitted (and before a second scan) triggers the retained one-shot
   refresh/re-acquire; after output starts it is a read failure.
5. Release: context exit, exception, explicit close and repeated close all
   call `DatasetReadLease.close()` (idempotent, stops the heartbeat, unlinks
   the lock); REST Arrow/CSV also registers a Starlette `BackgroundTask` so a
   disconnect before the body starts cannot leak the lease. Each paginated
   request opens its own StoreRead (no cross-request lease).
6. Replay acquires one Snapshot+Lease for the union of its subscriptions and
   holds it through the initial read, pause, seek and publishing; the worker's
   `finally` releases it after End of Session or stop.
7. Compaction/GC read `active_session_locks` (heartbeat within
   `session_lock_timeout_s`), skip and report protected partitions/months,
   re-check the lease set immediately before `write_manifest` and again
   immediately before the unlink. A GC month skipped after the Manifest
   switch has its entries re-activated and is reported in `skipped_locked`,
   so the data is never silently orphaned. `partition_is_locked` matches
   exact partition keys for read leases (month-only locks fall back to month
   matching) and preserves the legacy subscription matching; an unscoped
   legacy lock still protects the whole Dataset. Expired locks are ignored by
   heartbeat age, so a crashed process cannot block maintenance.

**Tests added:** `tests/contract/test_read_lease.py`,
`tests/contract/test_store_snapshot_read.py`,
`tests/integration/test_read_lease_api.py`,
`tests/integration/test_replay_read_lease.py`, plus race/lifecycle cases in
`tests/contract/test_manifest_read_safety.py` and
`tests/contract/test_compaction_gc.py`. Existing replay test proxies learned
the `open_read` seam without removing their gates.

**Test command and result:**
`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q` →
**826 passed, 5 failed, 1 skipped, 1 deselected**. The 5 failures are the
known pre-existing Typer/Click baseline failures. One earlier full run also
hit the unrelated timing-sensitive
`tests/contract/test_duckdb_compat.py::test_temp_directory_limit_raises_explicit_error_and_cleans_up`
(ticket 02); it passes in isolation and on the repeated full run.

**Deviations from STORE-READ-CONTRACT.md shape:**
- `open_read` also accepts `queries=[BarReadQuery(...)]` so a Replay Session
  can acquire one lease for the union of subscriptions while scanning each
  subscription on the same Snapshot. `StoreRead` methods accept optional
  filters (defaulting to the acquisition query) for the same reason.
- `SessionLock` gained `partitions`/`months`; `active_session_locks` accepts
  fractional timeouts (needed by lease tests).
- `ParquetBarStore.__init__` gained optional `read_lease_heartbeat_s`;
  `ReplayBarSource.read` gained an optional `read=` StoreRead for one-shot
  callers.
