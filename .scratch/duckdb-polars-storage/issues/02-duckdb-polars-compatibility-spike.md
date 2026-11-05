# 02: 完成 DuckDB 2.0/Polars 类型与生命周期 Spike

**What to build:** 用不接入生产代码的真实验证程序确定 DuckDB 2.0.x LTS 和候选 Polars 版本能否无损承载项目 Data Contract，并证明流式读取、取消和资源生命周期可满足后续设计。输出明确的 go/no-go 结论，而不是根据库文档推断兼容性。

**Blocked by:** None (can start immediately).

**Status:** resolved

- [x] 在项目使用的 Python 3.13 环境验证 DuckDB 2.0.x LTS 和候选 Polars 版本，记录精确版本与平台；DuckDB 1.x 不得作为后备。
- [x] 验证 `decimal128(38,18)` 的正负高精度值、边界值、精确文本、null 和超额 scale 拒绝，任何路径都不得经过 float64 或静默舍入。
- [x] 验证 UTC 纳秒时间包含 1ns、999ns、1000ns、123456789ns、epoch 前后和 null，覆盖 Parquet → DuckDB → Arrow → Polars 及反向路径。
- [x] 验证 uint32 最小/最大值、nullable `seq`、nullable `trade_count`、空结果和完整 `BAR_SCHEMA` 的字段顺序与 nullability。
- [x] 证明 Relation API 的 Arrow RecordBatch Reader 真正逐批输出，并确认未使用会先复制完整结果的 DBAPI reader。
- [x] 对长时间查询验证取消、Reader 提前关闭、重复取消和连接复用，确认旧查询停止且连接、线程与临时文件能够释放。
- [x] 在低 memory limit、明确 temp directory、不同线程数下执行排序/聚合，记录流式行为、临时文件、资源错误和清理结果。
- [x] 验证文件在首次读取前和部分批次输出后被删除/重命名的行为；不得把结果当作事务保证，Read Lease 仍须覆盖整个 Reader 生命周期。
- [x] 在 Windows 与 Linux 上执行关键文件生命周期实验，或明确记录无法覆盖的平台并给出阻塞后续实施的验证安排。
- [x] 形成书面 go/no-go 结论；任何精度损失、隐式浮点转换、不可取消旧查询或资源泄漏都视为失败，不得通过降低契约绕过。
- [x] 不修改生产读取器、Normalization 或公共 API。

## Comments

**Status:** resolved — GO on Windows, with mandatory constraints recorded in
`.scratch/duckdb-polars-storage/spike-report.md`.

- Branch: `ticket/02-compatibility-spike` (worktree `E:/total-project/mdgw-worktrees/duckdb-02`).
- Commits: `c0e3898` (spike evidence, tests, dependencies, report);
  `1410b79` (poll for asynchronous temp-file cleanup; 15/15 stress runs);
  merge `a6f3a7d` merged `integration/duckdb-polars-storage` with no conflicts.
- Exact versions: Python 3.13.18, DuckDB 2.0.0, Polars 2.1.0, PyArrow 26.0.0;
  Windows 11 10.0.26200 x86-64, 16 logical CPUs.

GO/NO-GO per hard gate:

- Versions / no DuckDB 1.x fallback: **GO**.
- `decimal128(38,18)` exact text, boundaries, null: **GO**; over-scale is
  rejected at the typed Arrow boundary. Mandatory: never pass decimals as SQL
  text/parameters or untyped Python values (DuckDB and Polars silently rescale
  those; Arrow rejects them).
- UTC nanoseconds incl. 1ns/999ns/1000ns/123456789ns/pre-epoch/null: **GO**;
  requires `timezone=UTC` on the connection, otherwise Arrow exports the local
  timezone (instant still correct).
- uint32 min/max, nullable `seq`/`trade_count`, empty results, full
  `BAR_SCHEMA`: **GO**; DuckDB/Polars mark all Arrow fields nullable and omit
  `interval`/widen strings, so the Store must restore
  `select(BAR_SCHEMA.names).cast(BAR_SCHEMA)`.
- Relation Arrow RecordBatchReader streaming: **GO**; 100M rows with a 64MB
  limit returned batches in milliseconds, no temp files; DBAPI reader (extra
  copy) and deprecated `fetch_arrow_reader()` are not used.
- Cancellation, repeated cancel, early Reader close, connection reuse:
  **GO**; `fetchall` raises `InterruptException`, Reader iteration raises
  `OSError: INTERRUPT ...`, both explicit; temp files and threads are released
  and the connection is reusable. Caveat: Python-UDF queries delay
  interruption; do not put UDFs on the scan path.
- Low memory / explicit temp dir / varying threads: **GO**; sorts spilled to
  the configured directory (157–194MB), cleaned up after completion, cancel
  and close; explicit `OutOfMemoryException` on temp-limit exhaustion; same
  results for threads 1/2/4.
- File delete/rename before first read and after partial output: **GO as a
  design input, not a transaction guarantee**. Deleting an unopened file in an
  explicit list fails mid-stream after partial output; deleting an already-open
  file may go unnoticed. Read Lease must cover the whole Reader lifetime.
- Windows/Linux: Windows experiments executed locally. Linux was unavailable
  here; the POSIX-only test is skipped on Windows and runs in the Linux GitHub
  Actions job (`.github/workflows/tests.yml`). No Linux result is claimed.

Test commands and results:

```bash
D:/Anaconda3/envs/nautilus/python.exe -m pytest tests/contract/test_duckdb_compat.py \
  tests/contract/test_polars_compat.py tests/contract/test_storage_file_lifecycle.py -q
# 39 passed, 1 skipped (Linux-only POSIX test)

D:/Anaconda3/envs/nautilus/python.exe -m pytest -q
# 791 passed, 5 failed, 1 skipped, 1 deselected
# The 5 failures are the pre-existing Typer/Click baseline; unrelated and unchanged.
```

Dependencies added: `duckdb>=2.0,<3`, `polars==2.1.0` in `pyproject.toml` and
`environment.yml`. No production reader, Normalization or public API was
modified.
