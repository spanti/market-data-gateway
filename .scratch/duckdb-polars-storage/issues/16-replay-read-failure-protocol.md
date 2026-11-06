# 16: 完成 Replay 失败终止协议与 SDK 行为

**What to build:** 让所有 Replay Mode 在 Parquet、Manifest、Schema 或查询资源失败时向客户端发送明确的失败终止，而不是把异常转换成空 Bars 后正常 exhausted。客户端和 SDK 能区分正常完成、主动停止与读取失败，并获得准确的已发送数量。

**Blocked by:** 15 交付有界 step Replay 与 Step Credit 竞态保证。

**Status:** resolved

- [x] 扩展现有 End of Session 事件，支持 `reason=error`，并在失败时附带稳定错误对象和实际 `bars_sent`。
- [x] `exhausted` 与 `stopped` 的现有形状和含义保持不变，失败错误对象只在 `reason=error` 时出现。
- [x] Schema Version 不匹配映射为现有 `schema_version_mismatch`，其他未分类读取失败映射为 `internal_error`。
- [x] 公共错误消息经过 sanitization，不包含 SQL、Data File 路径、堆栈或原始 DuckDB/Polars 异常。
- [x] 损坏 Parquet、Manifest/外部文件异常、Schema 不兼容、资源耗尽和查询取消都不能被转换成空回放或正常 exhausted。
- [x] 失败前已经发布的 Bars 不回滚、不重发，`bars_sent` 与实际发布数量一致。
- [x] 失败终止后当前查询代际失效，Reader、连接、执行线程和 Dataset Read Lease 全部释放。
- [x] SDK 将 `reason=error` 作为失败向调用方报告，不再视为正常回放完成；正常 exhausted/stopped 行为保持。
- [x] 测试覆盖 asap/realtime/step 的初始读取失败、部分发布后的失败、seek 读取失败、资源失败和控制命令竞态。
- [x] 公共契约测试证明空 Dataset 与读取失败具有可区分的外部行为，REST 错误协议不受此变化影响。

## Comments

**Branch:** `ticket/16-replay-read-failure-protocol` (worktree `E:/total-project/mdgw-worktrees/duckdb-16`)

**Commits:**
- `e217318` feat(replay): terminate failed reads with reason=error
- 本 ticket 的 docs closure 提交（docs(replay): close ticket 16 replay read failure protocol）

**End of Session 错误协议:**
- `reason="error"` 复用现有 End of Session，新增稳定 `error` 对象，仅在该 reason 下出现：
  `{"op":"end_of_session","session_id":"rpl-...","reason":"error","bars_sent":N,"error":{"code":"...","message":"..."}}`
- `exhausted` / `stopped` 事件形状与含义完全不变（没有 `error` 键）。`bars_sent` 在 `_cond` 下读取，等于失败前实际发布数；失败前 Bars 不回滚、不重发。
- 分类：`mdgw.core.errors.SchemaVersionMismatch` → `schema_version_mismatch`（message `Dataset schema version is not supported`）；其余 DuckDB I/O/OOM、Manifest/Data File、Arrow 及未分类异常 → `internal_error`（message `replay read failed`）。公共消息固定且 sanitized；真实异常保留在 `session.failure` 并由 `logger.exception` 记录。
- `_eos_published` 保证任何路径恰好一个 EOS：正常完成、失败边界、stop 与失败竞态、EOS 发送异常、迟到清理异常都不会重复发送。
- stop 与读取失败竞态时以 `stopped` 终止（不带 `error`）；seek 取代的旧代际失败由 `_generation_stop_reason` 丢弃，不产生 error/EOS。
- 失败后 Session 为 terminal `finished`，控制命令被拒绝、`stop` 幂等成功；`_run_worker` 的 finally 在 EOS 前关闭 StoreRead（Reader、DuckDB 连接、Dataset Read Lease），prefetch producer 线程 join，Store 可立即复用。
- 更新 `docs/data-contract.md` Replay Protocol 的 End of Session 章节（错误形状、稳定码、sanitization、`bars_sent`、资源释放与 SDK 行为）。

**SDK:**
- 新增并导出 `mdgw_client.ReplayFailedError(ClientError)`，携带 `code`、`message`、`bars_sent`。
- `ReplaySession.next_event()` / `__iter__` 收到 `reason="error"` EOS 时标记 `ended` 并抛出该失败；后续 `next_event()` 继续抛出同一失败（sticky），不会变成 `StopIteration`。`end_of_session` 仍返回完整事件。正常 `exhausted`/`stopped` 仍以 `StopIteration` 结束。

**失败注入与测试:**
- 真实损坏 Parquet（asap 初始失败、ReplayService、恢复文件后 Store/连接/lease 可复用）。
- `_dataset.json` 的 `schema_version=2`（真实 Schema 不兼容）。
- Snapshot 后删除 Data File（Manifest/外部文件异常）。
- 真实 DuckDB 资源耗尽：`memory_limit=2MB`、`max_temp_directory_size=1MB`、20,000 行全局排序触发 `OutOfMemoryException`。
- 脚本化 `StoreRead`：三种模式初始失败、部分发布后失败（`bars_sent` 精确）、realtime/step seek 代际失败、旧代际失败被丢弃、stop 与初始/seek 读取失败竞态。
- 公共契约：空 Dataset（REST 200 空结果、Replay `exhausted` 无 error）与读取失败（REST 500 稳定 error envelope、Replay `reason=error` + `internal_error`）外部可区分；REST 响应不泄漏 DuckDB/Parquet/路径文本。
- SDK 端到端（真实 ReplayServer）：损坏 Parquet → `ReplayFailedError(internal_error)`；Schema 不匹配 → `schema_version_mismatch`；空回放 → `StopIteration`。

**测试:**
- 命令：`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q`
- 全量离线结果（integration tip `175bfef`）：**1088 passed, 5 failed, 1 skipped, 1 deselected** in 199.07s。5 个失败均为 ticket 01 起已知的 Typer/Click CLI 问题（4 个 `test_archive_backfill.py` CLI 测试 + `test_cli_flags.py::test_run_rejects_an_unknown_service_role`），无新增失败。
- Replay/SDK 相关子集（read-failure-boundary/streaming/realtime/step/asap/controls/read-lease/client-replay/client-errors）：**146 passed**。

**给 merger 的说明:** 无新依赖、无配置变更；公开 REST 协议不变。Replay 公共协议按 ticket 新增 `reason="error"` 与 `error` 对象（既有 exhausted/stopped 不变），SDK 新增导出 `ReplayFailedError`。
