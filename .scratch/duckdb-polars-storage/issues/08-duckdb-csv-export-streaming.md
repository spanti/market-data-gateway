# 08: 迁移 CSV、export 与流式失败行为

**What to build:** 让 CSV 历史响应和 Parquet/Arrow export 使用 DuckDB Manifest Snapshot 读取，并定义完整的流式失败行为：响应开始前返回稳定错误，响应开始后必须被客户端识别为不完整传输，而不是正常截断。

**Blocked by:** 05 打通 DuckDB → Store → REST Arrow 的 tracer bullet。

**Status:** resolved

- [x] CSV 逐批读取并输出，保持精确 Decimal 文本、九位纳秒时间、null 表示、字段顺序和 Content-Type。
- [x] CSV 不增加全局排序，保留全部 raw Revisions，并对空结果、多文件和大结果进行旧新差分验证。
- [x] Parquet/Arrow export 在同一 Manifest Snapshot 中读取全部匹配数据，先完整生成临时文件再发送，保持既有文件名、Content-Type 和清理行为。
- [x] 响应开始前的失败使用现有稳定 HTTP 状态、错误 envelope 和 request ID，不泄漏 DuckDB/Polars 异常或路径。
- [x] Arrow/CSV 响应开始后的失败会异常终止传输，不拼接 JSON 错误、不吞异常，也不产生貌似成功的正常 EOF。
- [x] 客户端能把过早 EOF、缺失 Footer 或不完整分块识别为失败，而不是完整空结果或完整文件。
- [x] 客户端断开、生成失败和 export 发送失败都会关闭 Reader、释放连接与 Read Lease，并清理临时文件。
- [x] 资源耗尽和外部文件错误不会被静默截断；正常维护由 Read Lease 阻止删除使用中的 Data Files。
- [x] 大型流式响应不默认物化到进程内存，峰值内存和临时磁盘相对基线没有未经批准的增长。

## Comments

### Branch and commits

- Branch: `ticket/08-duckdb-csv-export-streaming`
- Worktree: `E:/total-project/mdgw-worktrees/duckdb-08`
- Feature commit: `28e26de feat(api): stream CSV and export through the DuckDB manifest scan`
- Integration merge: `47ea558 merge: integration/duckdb-polars-storage into ticket 08`
  (integration tip `c874382`, ticket 06's DuckDB JSON count/read seam). The only
  conflict was the stale "CSV/export keep PyArrow" comment and the per-format
  boundary test; both were resolved by combining ticket 06's JSON seam with
  ticket 08's CSV/export seam.
- No new dependencies and no configuration changes.

### CSV and export seam

Both surfaces now call `StoreRead.iter_arrow_batches()`; JSON and the ordered
paginated page reader keep their own Store seams (ticket 06). There is no
runtime switch.

- **CSV** (`_csv_body`): yields the unchanged header chunk first, then each
  bounded DuckDB Arrow batch is rendered by the existing `_batch_to_csv` and
  yielded immediately. No global ORDER BY, no dedup, all raw Revisions. The
  SQL and bound parameters are ticket 05's (`_build_scan_sql`), unchanged.
- **Export** (`_write_export_file`): one `StoreRead` (one Manifest Snapshot and
  Read Lease) feeds `iter_arrow_batches(limit=...)` into the existing Parquet /
  Arrow IPC file writer. The temporary file is complete before
  `StreamingResponse` is constructed, so filename, Content-Type,
  Content-Length and the pre-response error envelope are unchanged.

### Streaming-failure protocol

- **Before the response starts**: Store/Snapshot acquisition failures keep
  their stable status and envelope (`409 schema_version_mismatch`), and any
  export generation failure stays the existing middleware `500 internal_error`
  envelope with `X-Request-ID`. Response bodies contain neither engine
  exceptions, SQL nor file paths.
- **After the response starts**: CSV/Arrow failures propagate out of the body
  iterator. No JSON is appended, the exception is not swallowed and no
  `more_body=false` message is sent; uvicorn closes the chunked connection, so
  a real HTTP client observes `IncompleteRead`/`ConnectionError` instead of a
  short valid CSV or a complete Arrow stream. Tests use a real ASGI server and
  corrupt a Data File, delete one externally, or lower DuckDB's memory limit.
- **Cleanup**: `_CleanupStreamingResponse` runs an idempotent cleanup callback
  in a `finally` around the whole ASGI response call. This covers normal
  completion, client disconnect (`http.disconnect`), a failing `send()`
  (Starlette does not finalize the body iterator on that path) and a scanner
  exception. Cleanup closes the DuckDB Reader, releases the Read Lease and
  (for export) closes/removes the temporary file. `StoreRead.close()` interrupts
  an in-flight query from another thread; failed/cancelled connections are not
  returned to the one-idle cache.
- **Resource/external failures**: `OutOfMemoryException`, corrupted pages and
  missing externally-deleted files are explicit failures, never a truncated
  success. While a streaming response holds the lease, compaction skips the
  partition (`skipped_locked`) and can merge it after the lease is released;
  the existing ticket-04 maintenance tests plus a new streaming-lease test
  cover this.

### Tests

Command (from the worktree):

```
D:/Anaconda3/envs/nautilus/python.exe -m pytest -q
```

Result after merging the integration tip: **903 passed, 5 failed, 1 skipped,
1 deselected** in ~157 s. The 5 failures are the known pre-existing
Typer/Click baseline failures (`tests/integration/test_archive_backfill.py` x4
and `tests/unit/test_cli_flags.py::test_run_rejects_an_unknown_service_role`);
no new failure. Before the integration merge the ticket branch had 878 passed
(854 ticket-05 baseline + 24 new tests).

New file `tests/integration/test_duckdb_csv_export.py` (24 tests):

- CSV parity with the pre-migration PyArrow Store read on empty, multi-file
  raw-Revision/duplicate and 5,000-row multi-batch results; exact wire text for
  decimals, nine-digit nanoseconds, pre-epoch instants, nulls, field order,
  Content-Type/Disposition; `limit`; one response chunk per bounded batch.
- Export parity (Parquet and Arrow) for raw Revisions/duplicates plus
  Content-Length/Disposition and `limit`; one Snapshot and one DuckDB scan per
  request.
- Real-HTTP failure tests for corrupt, externally missing and
  memory-exhausted Data Files: status 200 then an incomplete chunked transfer
  (no JSON error, no engine/path leak), followed by a successful bounded
  request (proves the Reader/connection was released).
- Export pre-response envelopes for the same three causes with request-ID echo,
  lease release and temporary-directory removal.
- Client disconnect during CSV streaming and during export sending; send()
  failure during CSV/Arrow streaming and during export; streaming-lease
  maintenance skip and post-release compaction.
- CSV schema mismatch keeps the `409` envelope; a failed export does not poison
  a later bounded export.

Order-sensitive CSV/export assertions in the existing characterization tests
were changed to exact-content/multiplicity comparisons (sorted canonical
messages or row sets), because non-paginated output has no ordering contract;
decimal, timestamp, null and header assertions are unchanged.

### Performance (same machine and corpus)

Corpus: `C:/Users/10107/AppData/Local/Temp/mdgw-duckdb-baseline/query`
(50,000 rows), in-process `GatewayRuntime`. Method: interleaved new
(DuckDB) vs same-session old-path control (the same routes with
`StoreRead.iter_arrow_batches` mapped to the PyArrow `iter_batches`),
8 timed passes per format per session x 3 sessions (24 samples), reported as
medians; M0 baseline numbers are the recorded ones.

| Scenario | M0 baseline | New median (min-max) | Old-path control median | Delta vs control |
| --- | ---: | ---: | ---: | ---: |
| `rest.csv_50k` | 1.2805 s (spread 4.3%) | 1.3478 s (1.2964-2.0851; session spreads 6.6-55.9%) | 1.3538 s | -0.4% |
| `rest.export_parquet_50k` | 0.0790 s (11.7%) | 0.0712 s (0.0624-0.1006; 24-35% per session) | 0.0737 s | -3.4% |
| `rest.export_arrow_50k` | 0.0849 s (58.0%) | 0.0923 s (0.0576-0.1547; 52-90% per session) | 0.0909 s | +1.5% |

Against the recorded baseline the deltas are +5.3%, -9.9% and +8.7%, all
inside the 10% gate. Machine noise is high on this Windows host; the
interleaved control is the fairer comparison.

Memory (median of 3 memory passes per path; RSS delta vs the same-session old
control):

| Scenario | tracemalloc peak | Arrow native peak | RSS delta vs old |
| --- | ---: | ---: | ---: |
| CSV 50k | 65.93 MiB (baseline 62.71, +5.1%) | 0.26 MiB (baseline 21.69) | +3.7% |
| Parquet export | 3.13 MiB (baseline 3.13) | 6.65 MiB (baseline 29.57) | +10.5% |
| Arrow export | 12.10 MiB (baseline 12.10) | 0.00 MiB sampled (baseline 21.69) | -22.6% |

All are inside the 20% memory gate; the large Arrow-native reductions come
from streaming batches instead of materialising the scan. Export temporary
files are unchanged (Parquet 1,921,158 B vs 1,921,194 B baseline; Arrow
9,204,418 B identical), and the scan has no sort/aggregate, so no DuckDB temp
spill is produced.

### Merger notes

- `src/mdgw/api/routes/bars.py`: CSV/export call sites, `_ExportFileBody` and
  `_CleanupStreamingResponse`. The now-unused route-level `_iter_batches`
  helper was removed; `StoreRead.iter_batches()` itself remains for JSON,
  pagination and differential tests.
- The cleanup callback is invoked from the ASGI response wrapper, not from the
  body generator, which is what makes disconnect/send-failure cleanup
  deterministic. Keep that wrapper if the response construction changes.
