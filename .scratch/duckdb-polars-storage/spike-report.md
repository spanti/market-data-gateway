# M1 Spike report: DuckDB 2.0.x LTS + Polars compatibility and lifecycle

Date: 2026-11-05
Branch: `ticket/02-compatibility-spike`
Ticket: `.scratch/duckdb-polars-storage/issues/02-duckdb-polars-compatibility-spike.md`
Verdict: **GO**, with mandatory constraints and one pending Linux-only verification
(see "Platform gap" below). No precision or lifecycle gate failed on Windows.

## Environment

| Item | Value |
| --- | --- |
| Platform | Windows 11 Home China 10.0.26200 SP0, x86-64 |
| CPU | AMD64 Family 25 Model 97 Stepping 2, 16 logical CPUs |
| Python | 3.13.18 (Anaconda, MSC v.1942 64-bit) |
| Interpreter | `D:/Anaconda3/envs/nautilus/python.exe` |
| DuckDB | 2.0.0 |
| Polars | 2.1.0 (chosen and pinned) |
| PyArrow | 26.0.0 |
| Default machine timezone | `Asia/Shanghai` |

All evidence below comes from real programs against real Parquet files and a
real DuckDB connection. No compatibility conclusion in this report is inferred
from documentation alone.

## How to reproduce

```bash
cd E:/total-project/mdgw-worktrees/duckdb-02
D:/Anaconda3/envs/nautilus/python.exe -m pytest tests/contract/test_duckdb_compat.py -q
D:/Anaconda3/envs/nautilus/python.exe -m pytest tests/contract/test_polars_compat.py -q
D:/Anaconda3/envs/nautilus/python.exe -m pytest tests/contract/test_storage_file_lifecycle.py -q
# Full offline suite:
D:/Anaconda3/envs/nautilus/python.exe -m pytest -q
```

Result at the time of writing:

- Contract spike tests: **39 passed, 1 skipped** (the Linux-only POSIX test).
- Full offline suite after merging the integration tip: **791 passed, 5 failed,
  1 skipped, 1 deselected**.
  The 5 failures are the pre-existing Typer/Click baseline failures
  (`tests/integration/test_archive_backfill.py` ×4 and
  `tests/unit/test_cli_flags.py::test_run_rejects_an_unknown_service_role`);
  they are unrelated to this ticket and were not modified.

The executable evidence lives in:

- `tests/contract/test_duckdb_compat.py` (25 tests)
- `tests/contract/test_polars_compat.py` (8 tests)
- `tests/contract/test_storage_file_lifecycle.py` (6 Windows tests + 1 Linux-only)

## Verdict per hard gate

### 1. Python 3.13 + DuckDB 2.0.x LTS, no 1.x fallback — GO

- `sys.version_info[:2] == (3, 13)`; exact interpreter 3.13.18.
- `duckdb.__version__ == "2.0.0"`; the test asserts major version 2, so a
  regression to 1.x fails the suite.
- `pyproject.toml` now requires `duckdb>=2.0,<3`; `environment.yml` mirrors it.
  There is no engine switch and no 1.x fallback path.
- `polars==2.1.0` is pinned in `pyproject.toml` and `environment.yml`.

### 2. `decimal128(38,18)` exactness and over-scale rejection — GO on the typed path

Tested values (positive/negative boundary, minimum unit, zero, null, a
float64-unsafe 38-digit value):

```
 99999999999999999999.999999999999999999
-99999999999999999999.999999999999999999
-0.000000000000000001
 0.000000000000000000
 12345678901234567890.123456789012345678
 NULL
```

Evidence:

- Parquet `decimal128(38,18)` → DuckDB: `relation.types == ["DECIMAL(38,18)"]`.
- DuckDB → Arrow: `decimal128(38, 18)`; every value's exact decimal text is
  preserved, including null.
- Arrow → Polars: `pl.Decimal(precision=38, scale=18)`; exact text preserved.
- Reverse Polars → Arrow → Parquet → DuckDB is exact as well.
- No stage exposes `float`/`double`/`float64`. A separate test proves the
  chosen values do not survive a float64 detour (`Decimal(float(value)) != value`),
  so an accidental float conversion would be detected by the exact-text gate.
- Over-scale input (`0.0000000000000000001`, `1.1234567890123456789`, and a
  21-integer-digit value) is rejected by Arrow with `ArrowInvalid: Rescaling
  Decimal value would cause data loss`. Typed Arrow/Parquet is the only
  sanctioned decimal path.

**Mandatory constraint (must be enforced by M3/M4):** DuckDB and Polars both
silently rescale untyped decimal input. Measured:

```
DuckDB:  '1.1234567890123456789'::DECIMAL(38,18) -> 1.123456789012345679
DuckDB:  '0.0000000000000000001'::DECIMAL(38,18) -> 0E-18
Polars:  pl.Series([Decimal('1.1234567890123456789')], dtype=pl.Decimal(38,18))
         -> 1.123456789012345679   (strict=True does not help)
```

Therefore decimal values must enter DuckDB only as typed Arrow/Parquet
columns and must be validated for scale before reaching Polars. SQL text or
Decimal-parameter casts for contract decimals are forbidden. The spike does
not weaken the contract: the typed pipeline is lossless and over-scale cannot
enter it.

### 3. UTC nanoseconds — GO, with explicit `timezone=UTC` configuration

Values tested: `1`, `999`, `1000`, `123456789`, `0`, `-1`, `-999`, `-1000`,
`-123456789`, `null` (before and after epoch, non-microsecond multiples).

Evidence:

- Parquet stores `timestamp[ns, tz=UTC]`.
- DuckDB reads it as `TIMESTAMPTZ_NS` (the required type).
- With `duckdb.connect(config={"timezone": "UTC"})`, Arrow export is exactly
  `timestamp[ns, tz=UTC]` and every int64 nanosecond value round-trips exactly,
  including negatives and null.
- Polars reads it as `Datetime(time_unit='ns', time_zone='UTC')`; casting to
  `Int64` returns the exact input values; `to_arrow()` returns
  `timestamp[ns, tz=UTC]`.
- Reverse Polars → Arrow → Parquet → DuckDB is exact.
- Without the config, the instant is still exact but the exported Arrow type
  carries the machine timezone (`Asia/Shanghai` on this machine). The
  compatibility test asserts the explicit UTC type, so a missing UTC config
  fails the gate.

### 4. uint32, nullable `seq`/`trade_count`, empty results, full `BAR_SCHEMA` — GO, with schema restoration

- `uint32` values `0`, `4294967295`, and null round-trip as DuckDB `UINTEGER`,
  Arrow `uint32`, and Polars `UInt32`.
- `seq` `int64` values `0`, `2**63-1`, null and `trade_count` max/null are
  preserved.
- Empty Parquet (0 rows): the Relation Arrow reader yields zero batches but
  exposes the correct schema; Arrow table, Polars frame and re-imported
  DuckDB result all keep the decimal/time/uint32 types and 0 rows.
- Full `BAR_SCHEMA` (18 fields) field order and nullability: DuckDB's Arrow
  export marks **every field nullable** and omits the Hive partition column
  `interval`. The Store must restore both. A final
  `table.select(BAR_SCHEMA.names).cast(BAR_SCHEMA)` yields a schema that
  `equals(BAR_SCHEMA, check_metadata=False)`, with all values unchanged.
  Polars behaves the same way and additionally widens strings to
  `large_string`; the same cast restores the contract.
  This is lossless schema restoration, not data normalization.

### 5. Relation Arrow RecordBatch Reader streams incrementally — GO

- The sanctioned API is `relation.to_arrow_reader(batch_size=...)` (Arrow
  RecordBatchReader). `fetch_arrow_reader()` is deprecated in DuckDB 2.0 and
  emits a `DeprecationWarning`.
- The DBAPI result path (`connection.execute(...).to_arrow_reader()`) is not
  used: it emits `UserWarning: Arrow output on a DB-API result makes an extra
  copy of the rows`. A test asserts the warning on the DBAPI path and asserts
  that the Relation path emits no warning.
- Multi-batch: a 200,000-row query at `batch_size=10_000` yields exactly 20
  batches, all rows present.
- Not materialised: with `memory_limit=64MB`,
  `max_temp_directory_size=16MB`, a 100,000,000-row
  `range` + `md5` query opens the reader in ~0.06 s and returns the first
  three 10,000-row batches in ~0.08 s, with no temp files. That result cannot
  be held in 64 MB. A full 30,000,000-row consumption produced 3,000 batches,
  first batch at 0.09 s, total 5.33 s, peak process RSS 111 MB, and no temp
  files. Opening a reader starts the query (the connection refuses a second
  query until the reader is drained or closed), so the Store must keep the
  connection exclusive for the Reader's lifetime.

### 6. Cancellation, repeated cancellation, early Reader close, connection reuse — GO, with caveats

Measured behaviour:

- `connection.interrupt()` during a pure-SQL long query (`fetchall`) produced
  `duckdb.InterruptException: INTERRUPT Error: Interrupted!` at the interrupt
  time (0.70 s after start). The connection was immediately reusable.
- Interrupting a query while iterating its Relation reader produced
  `OSError: INTERRUPT Error: Interrupted!` after 0.40 s (not
  `InterruptException`; the error must be classified by message, not class).
  The connection was reusable and `reader.close()` was safe.
- Repeated `interrupt()` on an idle connection is harmless. Repeated
  interrupts aimed at one query yield one explicit failure, never a hang.
- Early `reader.close()` during a query that had spilled 6 temp blocks:
  close took ~0.04 s, DuckDB temp data files were removed immediately, the
  connection was reusable, and 5 repeated open/read-one-batch/close cycles did
  not grow the process thread count (stable at 88; after `connection.close()`
  it dropped to 23). Closing a Reader twice is harmless.
- Caveat: cancellation of a query that calls a Python UDF is delayed until the
  UDF work finishes (2000 rows × 20 ms took 40 s despite an interrupt at
  0.7 s). Production scans use Parquet/SQL only, so this is not a gate
  failure; do not put Python UDFs on the Replay path.
- Caveat: `to_arrow_reader()` blocks until the first chunk is ready. For a
  streaming scan that is milliseconds; for a blocking sort it is the entire
  sort (measured 2.1–2.6 s for 3M rows). This is time-to-first-batch, not
  full-result materialisation.

### 7. Low memory, explicit temp directory, varying threads — GO

- `temp_directory`, `memory_limit`, `threads` were set explicitly on every
  test connection; `current_setting('temp_directory')` matches.
- At `memory_limit=64MB`, a 3M-row sort spilled 7 temp blocks (~157–194 MB) in
  the configured directory. Data temp files were removed immediately after the
  query completed or was cancelled; the per-connection owner marker was
  removed on `connection.close()`, leaving the directory empty.
- Sort and aggregate results were identical with `threads=1`, `2`, and `4`
  (4 threads used 128 MB to stay above the parallel hash/sort overhead).
  Sorted prefixes matched an independent Python `hashlib.md5` computation.
- `threads=4` at 64 MB produced a clear `OutOfMemoryException` ("failed to pin
  block ..."), not truncation; the connection remained usable.
- `max_temp_directory_size='1MB'` forced `OutOfMemoryException` mentioning the
  temp-directory limit. Temp data files were cleaned, the connection was
  reusable, and no partial result was returned.
- Direct process RSS is not asserted in the offline suite (psutil is optional
  and skipped in CI), but the 64 MB limit vs. 3 GB+ result size is the proof
  that the reader is bounded.

### 8. File deletion/rename before first read and after partial batches — GO as a design input; no transaction guarantee

All experiments used explicit file lists, never a Dataset-root glob.

| Scenario | Observed behaviour (Windows 11, DuckDB 2.0.0) |
| --- | --- |
| File deleted/renamed before the first read | Explicit `IOException` / `OSError` ("No files found that match the pattern" for a single path, "Cannot open file ... No such file" for an explicit list). Connection reusable. |
| Single already-open file deleted/renamed after the first batch | Unlink/rename succeeded; the stream continued from the open handle and delivered all rows. (Windows opened the file with delete sharing; the OS kept the handle alive.) |
| Second file in an explicit list not yet opened, deleted/renamed after partial output | The reader first emitted 280,000 of 400,000 rows from file 1, then failed with `OSError: Cannot open file "...b.parquet": ... No such file`. It did **not** report clean exhaustion. |
| Reader created (query started), single file deleted before the first `read_next_batch()` | The first read still succeeded because the handle was already open. |

Conclusion: DuckDB opens Parquet files lazily and gives no external-file
transaction guarantee. Deleting a file that is already open may go unnoticed
(depending on OS sharing), while deleting a not-yet-opened file fails
mid-stream after partial output. Both outcomes are compatible with the design
only because a Read Lease covers the entire Reader lifetime: maintenance must
not delete data files until the Reader finishes, is cancelled, or fails. A
mid-stream failure must abort the response as an incomplete transfer, never be
appended to a JSON error or mistaken for a short result. This is exactly the
M2 Read Lease requirement; it is not a new failure.

### 9. Windows and Linux file lifecycle — Windows GO; Linux not executed here

- Windows 11 experiments above are executed locally and encoded in
  `tests/contract/test_storage_file_lifecycle.py` (Windows-specific test plus
  platform-agnostic no-silent-truncation tests).
- No Linux host was available in this environment, so **no Linux result is
  claimed**. The gap is recorded, not waived.
- Verification arrangement: `.github/workflows/tests.yml` runs the offline
  suite on `ubuntu-latest` with Python 3.13 and installs the new dependencies
  from `pyproject.toml`. The file
  `test_posix_unlink_of_open_file_keeps_stream_readable` is skipped on Windows
  and executes only on Linux. The two platform-agnostic lifecycle tests
  (`unopened later file fails mid-stream`, `delete of an already open file
  never silently truncates`) also run on Linux. This ticket is not complete
  for the merger until that Linux CI job passes; if it exposes different
  deletion semantics, the Read Lease design is unaffected because it does not
  rely on OS deletion behaviour, but the report must be amended.

## Polars version decision

`polars==2.1.0` was accepted. It passes every typed-path gate:

- `decimal128(38,18)` exact text, null, and reverse path;
- `Datetime('ns','UTC')` exact nanoseconds including pre-epoch;
- `uint32` min/max/null and `int64` nullable;
- empty results keep contract dtypes;
- full `BAR_SCHEMA` restorable with `cast(BAR_SCHEMA)`.

No older candidate was needed. The one hazard, silent rescaling of untyped
Python decimals, is an input-validation obligation for M4, not a reason to
downgrade Polars: Arrow-typed columns cannot carry an over-scale value.

## Mandatory constraints for M2–M6

1. Connect with `timezone="UTC"` (config or `SET TimeZone='UTC'`) on every
   DuckDB connection used for contract data. Assert `timestamp[ns, tz=UTC]`
   at the Arrow boundary.
2. Use `relation.to_arrow_reader(batch_size=...)` only. Never use
   `fetch_arrow_reader()` or a DBAPI `execute(...).to_arrow_reader()`.
3. Keep the DuckDB connection exclusive to one Reader at a time; close or
   drain the Reader before any other query on that connection.
4. Restore the contract schema after DuckDB/Polars export:
   `select(BAR_SCHEMA.names).cast(BAR_SCHEMA)` (adds `interval`). This is
   required because both engines report every field nullable; Polars also
   widens strings to `large_string`.
5. Decimals enter and leave DuckDB only as typed Arrow/Parquet columns. Do not
   cast decimal text/parameters inside SQL. Validate scale before Polars
   conversion; Polars `strict=True` does not reject over-scale values.
6. Hold the Read Lease until the Reader is finished, cancelled, or failed and
   closed. External deletion must be treated as an explicit read failure, not
   as an empty or short result.
7. Classify interrupts by message (`INTERRUPT`/`Interrupted`) rather than a
   single exception class (`InterruptException` for `fetchall`, `OSError` for
   Reader iteration). Treat a cancelled generation as invalid regardless of
   whether the Reader raises or stops.
8. Do not put Python UDFs on the production scan path: they delay interruption
   and are not needed for Parquet reads.
9. Configure `memory_limit`, `threads`, `temp_directory`, and
   `max_temp_directory_size` explicitly. Resource errors are explicit
   exceptions and temp data files are cleaned up, but temp disk is not a
   second data copy and must not be treated as durable state.

## Scope confirmation

No production reader, Normalization code, or public API was modified in this
ticket. `pyproject.toml` and `environment.yml` gained the two runtime
dependencies; `tests/contract/` gained the compatibility suites; this report
and the ticket file are the documentation changes.
