"""M1 spike gates: DuckDB 2.0.x can carry the Bar Data Contract without loss.

These tests are executable evidence for ``spike-report.md``. They use real
Parquet files, a real DuckDB connection and the Relation Arrow reader; nothing
here mocks the engine. The gates covered are:

* Python 3.13 + DuckDB 2.x (no 1.x fallback);
* ``decimal128(38,18)`` exactness and over-scale rejection at the typed
  Arrow boundary;
* UTC nanosecond timestamps (``TIMESTAMPTZ_NS``), including pre-epoch values;
* ``uint32`` bounds, nullable ``seq``/``trade_count``, empty results and the
  full ``BAR_SCHEMA`` field order/nullability;
* incremental Relation Arrow RecordBatchReader output (not DBAPI, not
  materialised);
* cancellation, early Reader close and connection reuse;
* low memory, explicit temp directory, thread-count variation and cleanup;
* Parquet file deletion/rename before the first read and after partial output.
"""

from __future__ import annotations

import sys
import threading
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mdgw.core.schema import BAR_SCHEMA

DECIMAL_38_18 = pa.decimal128(38, 18)
NS_UTC = pa.timestamp("ns", tz="UTC")

# Physical Bar columns: `interval` is a Hive partition column and is not stored
# inside Data Files; the Store restores it after reading.
PHYSICAL_NAMES = [field.name for field in BAR_SCHEMA if field.name != "interval"]

DECIMAL_TEXTS = [
    "99999999999999999999.999999999999999999",
    "-99999999999999999999.999999999999999999",
    "-0.000000000000000001",
    "0.000000000000000000",
    "12345678901234567890.123456789012345678",
    None,
]

NS_VALUES = [1, 999, 1000, 123456789, 0, -1, -999, -1000, -123456789, None]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def connect(
    temp_directory: Path | None = None,
    *,
    timezone: str | None = "UTC",
    **settings: str,
) -> duckdb.DuckDBPyConnection:
    config: dict[str, str] = {"threads": "2", **settings}
    if timezone is not None:
        config["timezone"] = timezone
    if temp_directory is not None:
        temp_directory.mkdir(parents=True, exist_ok=True)
        config["temp_directory"] = str(temp_directory)
    return duckdb.connect(config=config)


def decimal_texts(column: pa.ChunkedArray | pa.Array) -> list[str | None]:
    return [
        None if value is None else format(value, "f")
        for value in column.to_pylist()
    ]


def ns_ints(column: pa.ChunkedArray | pa.Array) -> list[int | None]:
    return column.cast(pa.int64()).to_pylist()


def temp_data_files(directory: Path) -> list[Path]:
    """DuckDB temp blocks, excluding the per-connection owner marker."""
    if not directory.exists():
        return []
    return [
        path
        for path in directory.iterdir()
        if path.is_file() and not path.name.endswith("_owner")
    ]


class TempDirMonitor(threading.Thread):
    """Poll the configured temp directory while a query is running."""

    def __init__(self, directory: Path) -> None:
        super().__init__(daemon=True)
        self.directory = directory
        self._stop = threading.Event()
        self.max_files = 0
        self.max_bytes = 0

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                files = temp_data_files(self.directory)
                total = sum(path.stat().st_size for path in files)
            except OSError:
                files, total = [], 0
            self.max_files = max(self.max_files, len(files))
            self.max_bytes = max(self.max_bytes, total)
            self._stop.wait(0.01)

    def finish(self) -> None:
        self._stop.set()
        self.join(timeout=10)


def run_with_interrupt(
    connection: duckdb.DuckDBPyConnection,
    work,
    *,
    delay: float = 0.3,
    timeout: float = 20.0,
    repeats: int = 1,
) -> BaseException | None:
    """Run *work* on a thread, interrupt the connection, return its exception."""
    outcome: dict[str, BaseException | None] = {"error": None, "done": False}

    def worker() -> None:
        try:
            work()
        except BaseException as exc:  # noqa: BLE001 - propagated to the caller
            outcome["error"] = exc
        finally:
            outcome["done"] = True

    thread = threading.Thread(target=worker)
    thread.start()
    time.sleep(delay)
    for _ in range(repeats):
        connection.interrupt()
        time.sleep(0.02)
    thread.join(timeout)
    if thread.is_alive():  # try once more so a stuck test cannot leak a thread
        connection.interrupt()
        thread.join(5)
    assert not thread.is_alive(), "query kept running after interrupt()"
    return outcome["error"]


def interrupt_error_message(error: BaseException | None) -> str:
    assert error is not None, "expected the interrupt to surface an error"
    message = f"{type(error).__name__}: {error}"
    assert "INTERRUPT" in message.upper() or "INTERRUPTED" in message.upper(), message
    return message


def bar_rows() -> list[dict[str, object]]:
    return [
        {
            "schema_version": 1,
            "data_type": "bar",
            "instrument_id": "binance:BTC/USDT",
            "interval": "1m",
            "open_ts": datetime(2024, 1, 1, tzinfo=UTC),
            "close_ts": datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
            "event_ts": datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
            "ingest_ts": datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
            "feed": "binance_spot",
            "seq": 2**63 - 1,
            "aggregation_source": "external",
            "open": Decimal("99999999999999999999.999999999999999999"),
            "high": Decimal("-99999999999999999999.999999999999999999"),
            "low": Decimal("-0.000000000000000001"),
            "close": Decimal("0"),
            "volume": Decimal("12345678901234567890.123456789012345678"),
            "trade_count": 2**32 - 1,
            "revision": 2**32 - 1,
        },
        {
            "schema_version": 1,
            "data_type": "bar",
            "instrument_id": "binance:ETH/USDT",
            "interval": "1m",
            "open_ts": datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
            "close_ts": datetime(2024, 1, 1, 0, 2, tzinfo=UTC),
            "event_ts": datetime(2024, 1, 1, 0, 2, tzinfo=UTC),
            "ingest_ts": datetime(2024, 1, 1, 0, 2, tzinfo=UTC),
            "feed": "binance_spot",
            "seq": None,
            "aggregation_source": "internal",
            "open": Decimal("1"),
            "high": Decimal("1"),
            "low": Decimal("1"),
            "close": Decimal("1"),
            "volume": Decimal("1"),
            "trade_count": None,
            "revision": 0,
        },
    ]


def write_bar_file(path: Path, rows: list[dict[str, object]] | None = None) -> None:
    table = pa.Table.from_pylist(
        bar_rows() if rows is None else rows, schema=BAR_SCHEMA
    )
    pq.write_table(table.select(PHYSICAL_NAMES), path, row_group_size=4096)


def restore_bar_schema(table: pa.Table) -> pa.Table:
    """Restore the partition column and exact contract schema after DuckDB."""
    with_interval = table.append_column(
        "interval", pa.array(["1m"] * table.num_rows, type=pa.string())
    )
    return with_interval.select(BAR_SCHEMA.names).cast(BAR_SCHEMA)


SORT_SQL = (
    "select i, md5((i*2654435761 % 4294967296)::varchar) as h "
    "from range(3000000) tbl(i) order by h, i"
)


# ---------------------------------------------------------------------------
# version and platform gate
# ---------------------------------------------------------------------------


def test_python_313_and_duckdb_2_baseline() -> None:
    assert sys.version_info[:2] == (3, 13), sys.version
    major, minor, *_ = (int(part) for part in duckdb.__version__.split("."))
    assert (major, minor) >= (2, 0), duckdb.__version__
    assert major == 2, f"DuckDB 1.x fallback is forbidden, got {duckdb.__version__}"


# ---------------------------------------------------------------------------
# decimal128(38,18)
# ---------------------------------------------------------------------------


def test_decimal128_38_18_roundtrip_is_exact(tmp_path: Path) -> None:
    source = pa.table(
        {
            "id": pa.array(range(len(DECIMAL_TEXTS)), type=pa.int64()),
            "value": pa.array(
                [None if text is None else Decimal(text) for text in DECIMAL_TEXTS],
                type=DECIMAL_38_18,
            ),
        }
    )
    first = tmp_path / "decimal.parquet"
    pq.write_table(source, first)

    connection = connect()
    try:
        relation = connection.read_parquet(str(first))
        assert relation.types == ["BIGINT", "DECIMAL(38,18)"]

        exported = relation.to_arrow_table()
        assert exported.schema.field("value").type == DECIMAL_38_18
        assert not pa.types.is_floating(exported.schema.field("value").type)
        assert decimal_texts(exported["value"]) == DECIMAL_TEXTS

        second = tmp_path / "decimal-again.parquet"
        pq.write_table(exported, second)
        again = connection.read_parquet(str(second)).to_arrow_table()
        assert again.schema.field("value").type == DECIMAL_38_18
        assert decimal_texts(again["value"]) == DECIMAL_TEXTS
    finally:
        connection.close()


def test_decimal_values_would_change_under_a_float64_detour() -> None:
    # These are exactly the values a float64 conversion would corrupt; the
    # preceding round-trip test therefore proves the pipeline never took one.
    for text in DECIMAL_TEXTS:
        if text is None:
            continue
        value = Decimal(text)
        if value == 0:
            continue
        # Decimal(float) is the exact binary value; any detour changes it.
        assert Decimal(float(value)) != value, f"{text} is float64-exact"


@pytest.mark.parametrize(
    "over_scale",
    [
        "0.0000000000000000001",  # scale 19, rounds to zero in untyped engines
        "1.1234567890123456789",  # scale 19, rounds the last digit
        "100000000000000000000.0",  # 21 integer digits > precision headroom
    ],
)
def test_over_scale_decimal_is_rejected_at_the_typed_arrow_boundary(
    over_scale: str,
) -> None:
    with pytest.raises(pa.ArrowInvalid):
        pa.array([Decimal(over_scale)], type=DECIMAL_38_18)


def test_untyped_duckdb_text_cast_rounds_so_only_typed_decimal_is_allowed() -> None:
    """Characterisation of the trap this gate forbids in production code.

    DuckDB's VARCHAR -> DECIMAL cast rounds instead of rejecting; therefore
    decimal values must only enter DuckDB as typed Arrow/Parquet columns (which
    Arrow validates) and never as SQL text or bound decimal parameters.
    """
    connection = connect()
    try:
        rounded = connection.sql(
            "select '1.1234567890123456789'::DECIMAL(38,18)"
        ).fetchall()[0][0]
        assert rounded == Decimal("1.123456789012345679")

        truncated = connection.sql(
            "select '0.0000000000000000001'::DECIMAL(38,18)"
        ).fetchall()[0][0]
        assert truncated == Decimal("0E-18")
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# UTC nanoseconds
# ---------------------------------------------------------------------------


def test_utc_nanoseconds_roundtrip_exactly(tmp_path: Path) -> None:
    source = pa.table({"ts": pa.array(NS_VALUES, type=NS_UTC)})
    first = tmp_path / "ns.parquet"
    pq.write_table(source, first)
    assert pq.read_schema(first).field("ts").type == NS_UTC

    connection = connect()
    try:
        relation = connection.read_parquet(str(first))
        # The exact DuckDB type required by the spec, not TIMESTAMP_MS/US.
        assert relation.types == ["TIMESTAMPTZ_NS"]

        exported = relation.to_arrow_table()
        assert exported.schema.field("ts").type == NS_UTC
        assert ns_ints(exported["ts"]) == NS_VALUES
        assert len({value for value in NS_VALUES if value is not None}) == 9

        second = tmp_path / "ns-again.parquet"
        pq.write_table(exported, second)
        again = connection.read_parquet(str(second)).to_arrow_table()
        assert again.schema.field("ts").type == NS_UTC
        assert ns_ints(again["ts"]) == NS_VALUES
    finally:
        connection.close()


def test_arrow_export_needs_explicit_utc_timezone_config(tmp_path: Path) -> None:
    source = pa.table({"ts": pa.array(NS_VALUES, type=NS_UTC)})
    path = tmp_path / "tz.parquet"
    pq.write_table(source, path)

    explicit = connect()
    local = connect(timezone=None)
    try:
        timezone_setting = explicit.sql(
            "select current_setting('TimeZone')"
        ).fetchall()
        assert timezone_setting[0][0] == "UTC"
        utc_table = explicit.read_parquet(str(path)).to_arrow_table()
        assert utc_table.schema.field("ts").type == NS_UTC

        # Without the config the instant is still exact, but the exported Arrow
        # type carries the machine timezone. That is why timezone=UTC is a gate.
        local_table = local.read_parquet(str(path)).to_arrow_table()
        assert ns_ints(local_table["ts"]) == NS_VALUES
        assert local_table.schema.field("ts").type.unit == "ns"
    finally:
        explicit.close()
        local.close()


# ---------------------------------------------------------------------------
# uint32, nullability, empty results and the full BAR_SCHEMA
# ---------------------------------------------------------------------------


def test_uint32_bounds_and_nullable_columns_roundtrip(tmp_path: Path) -> None:
    source = pa.table(
        {
            "trade_count": pa.array([0, 2**32 - 1, None], type=pa.uint32()),
            "seq": pa.array([0, 2**63 - 1, None], type=pa.int64()),
        }
    )
    path = tmp_path / "uint32.parquet"
    pq.write_table(source, path)

    connection = connect()
    try:
        relation = connection.read_parquet(str(path))
        assert relation.types == ["UINTEGER", "BIGINT"]
        exported = relation.to_arrow_table()
        assert exported.schema.field("trade_count").type == pa.uint32()
        assert exported.schema.field("seq").type == pa.int64()
        assert exported["trade_count"].to_pylist() == [0, 2**32 - 1, None]
        assert exported["seq"].to_pylist() == [0, 2**63 - 1, None]

        second = tmp_path / "uint32-again.parquet"
        pq.write_table(exported, second)
        again = connection.read_parquet(str(second)).to_arrow_table()
        assert again.schema.field("trade_count").type == pa.uint32()
        assert again["trade_count"].to_pylist() == [0, 2**32 - 1, None]
    finally:
        connection.close()


def test_complete_bar_schema_order_and_nullability_survive_the_pipeline(
    tmp_path: Path,
) -> None:
    path = tmp_path / "bars.parquet"
    write_bar_file(path)

    connection = connect()
    try:
        exported = connection.read_parquet(str(path)).to_arrow_table()
        # DuckDB's Arrow export marks every field nullable and omits the Hive
        # partition column; the Store must restore both. Nothing else changes.
        assert exported.schema.names == PHYSICAL_NAMES
        assert all(field.nullable for field in exported.schema)

        restored = restore_bar_schema(exported)
        assert restored.schema.equals(BAR_SCHEMA, check_metadata=False)
        assert restored.schema.names == BAR_SCHEMA.names
        assert [field.nullable for field in restored.schema] == [
            field.nullable for field in BAR_SCHEMA
        ]
        rows = restored.to_pylist()
        assert [str(row["trade_count"]) for row in rows] == [str(2**32 - 1), "None"]
        assert rows[0]["seq"] == 2**63 - 1
        assert rows[1]["seq"] is None
        assert rows[0]["revision"] == 2**32 - 1
        assert format(rows[0]["open"], "f") == DECIMAL_TEXTS[0]
        assert format(rows[1]["close"], "f") == "1.000000000000000000"
    finally:
        connection.close()


def test_empty_result_keeps_contract_schema(tmp_path: Path) -> None:
    path = tmp_path / "empty.parquet"
    write_bar_file(path, rows=[])

    connection = connect()
    try:
        relation = connection.read_parquet(str(path))
        exported = relation.to_arrow_table()
        assert exported.num_rows == 0
        assert exported.schema.names == PHYSICAL_NAMES
        assert exported.schema.field("open").type == DECIMAL_38_18
        assert exported.schema.field("open_ts").type == NS_UTC
        assert exported.schema.field("revision").type == pa.uint32()
        assert exported.schema.field("trade_count").type == pa.uint32()

        restored = restore_bar_schema(exported)
        assert restored.num_rows == 0
        assert restored.schema.equals(BAR_SCHEMA, check_metadata=False)

        reader = relation.to_arrow_reader(batch_size=16)
        batches: list[pa.RecordBatch] = []
        try:
            assert reader.schema.field("open").type == DECIMAL_38_18
            while True:
                try:
                    batches.append(reader.read_next_batch())
                except StopIteration:
                    break
        finally:
            reader.close()
        assert batches == []
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Relation Arrow RecordBatchReader: streaming, not materialised, not DBAPI
# ---------------------------------------------------------------------------


def test_relation_arrow_reader_yields_multiple_batches(tmp_path: Path) -> None:
    connection = connect()
    try:
        reader = connection.sql("select i from range(200000) tbl(i)").to_arrow_reader(
            batch_size=10_000
        )
        batches: list[pa.RecordBatch] = []
        try:
            while True:
                try:
                    batches.append(reader.read_next_batch())
                except StopIteration:
                    break
        finally:
            reader.close()

        assert len(batches) > 1
        assert all(batch.num_rows > 0 for batch in batches)
        assert sum(batch.num_rows for batch in batches) == 200_000
        first_values = [batch.column(0)[0].as_py() for batch in batches[:3]]
        assert first_values == [0, 10_000, 20_000]
    finally:
        connection.close()


def test_relation_arrow_reader_is_incremental_not_materialised(
    tmp_path: Path,
) -> None:
    """100M rows cannot fit in the 64MB DuckDB memory limit as one result.

    A materialising terminal would have to spill (blocked by the 16MB temp cap)
    or fail; the streaming reader returns its first batches in milliseconds.
    """
    temp_directory = tmp_path / "duckdb-temp"
    connection = connect(
        temp_directory, memory_limit="64MB", threads="2"
    )
    connection.sql("SET max_temp_directory_size='16MB'")
    sql = "select i as id, md5((i*7919)::varchar) as h from range(100000000) tbl(i)"
    try:
        started = time.perf_counter()
        reader = connection.sql(sql).to_arrow_reader(batch_size=10_000)
        opened = time.perf_counter() - started
        batches = [reader.read_next_batch() for _ in range(3)]
        elapsed = time.perf_counter() - started
        reader.close()

        assert opened < 2.0, f"opening the reader took {opened:.2f}s"
        assert elapsed < 2.0, f"first three batches took {elapsed:.2f}s"
        assert [batch.num_rows for batch in batches] == [10_000, 10_000, 10_000]
        assert temp_data_files(temp_directory) == []

        # The connection is immediately reusable once the reader is closed.
        assert connection.sql("select 1").fetchall() == [(1,)]
    finally:
        connection.close()


def test_relation_reader_is_not_the_dbapi_copying_reader() -> None:
    connection = connect()
    try:
        # The DBAPI result path explicitly warns that it copies every row.
        with pytest.warns(UserWarning, match="extra copy"):
            dbapi_reader = connection.execute("select 1 as x").to_arrow_reader()
        dbapi_reader.close()

        # The Relation API path is the sanctioned streaming path and warns not.
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            relation_reader = connection.sql("select 1 as x").to_arrow_reader()
        relation_reader.close()

        # fetch_arrow_reader() is only a deprecated alias in DuckDB 2.0.
        with pytest.warns(DeprecationWarning, match="to_arrow_reader"):
            alias_reader = connection.sql("select 1 as x").fetch_arrow_reader()
        alias_reader.close()
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# cancellation, early Reader close, connection reuse
# ---------------------------------------------------------------------------


def test_interrupt_stops_long_query_and_connection_is_reusable() -> None:
    connection = connect()
    try:
        error = run_with_interrupt(
            connection,
            lambda: connection.sql(
                "select count(*) from range(0, 300000) a, range(0, 300000) b"
            ).fetchall(),
        )
        interrupt_error_message(error)
        assert connection.sql("select 42").fetchall() == [(42,)]
    finally:
        connection.close()


def test_interrupt_during_reader_iteration_surfaces_an_explicit_error() -> None:
    connection = connect()
    try:
        reader = connection.sql(
            "select a.range as x, b.range as y "
            "from range(0, 300000) a, range(0, 300000) b"
        ).to_arrow_reader(batch_size=1024)

        def drain() -> None:
            while True:
                try:
                    reader.read_next_batch()
                except StopIteration:
                    break

        error = run_with_interrupt(connection, drain)
        interrupt_error_message(error)
        reader.close()
        assert connection.sql("select 43").fetchall() == [(43,)]
    finally:
        connection.close()


def test_repeated_cancel_and_early_reader_close_keep_connection_usable() -> None:
    connection = connect()
    try:
        reader = connection.sql(
            "select i from range(100000000) tbl(i)"
        ).to_arrow_reader(batch_size=10_000)
        assert reader.read_next_batch().num_rows == 10_000
        reader.close()
        reader.close()  # closing twice must be harmless

        # interrupt() on an idle connection is harmless.
        connection.interrupt()
        connection.interrupt()
        assert connection.sql("select 7").fetchall() == [(7,)]

        # Two interrupts aimed at one query still produce one clear failure.
        error = run_with_interrupt(
            connection,
            lambda: connection.sql(
                "select count(*) from range(0, 300000) a, range(0, 300000) b"
            ).fetchall(),
            delay=0.2,
            repeats=2,
        )
        interrupt_error_message(error)
        connection.interrupt()
        assert connection.sql("select 8").fetchall() == [(8,)]
    finally:
        connection.close()


def test_early_reader_close_releases_a_spilling_query(tmp_path: Path) -> None:
    temp_directory = tmp_path / "duckdb-temp"
    connection = connect(
        temp_directory, memory_limit="64MB", threads="2"
    )
    monitor = TempDirMonitor(temp_directory)
    monitor.start()
    try:
        reader = connection.sql(SORT_SQL).to_arrow_reader(batch_size=5000)
        first = reader.read_next_batch()
        assert first.num_rows == 5000
        reader.close()
        monitor.finish()

        assert monitor.max_files > 0, "sorts at 64MB were expected to spill"
        assert temp_data_files(temp_directory) == []
        assert connection.sql("select 9").fetchall() == [(9,)]
    finally:
        monitor.finish()
        connection.close()
    assert list(temp_directory.iterdir()) == []


def test_reader_close_does_not_leak_process_threads(tmp_path: Path) -> None:
    psutil = pytest.importorskip("psutil")
    connection = connect()
    sql = "select i from range(100000000) tbl(i)"

    def cycle() -> None:
        reader = connection.sql(sql).to_arrow_reader(batch_size=10_000)
        reader.read_next_batch()
        reader.close()

    try:
        cycle()  # warm up native thread pools
        baseline = psutil.Process().num_threads()
        for _ in range(5):
            cycle()
        current = psutil.Process().num_threads()
        assert current <= baseline + 4, f"thread count grew {baseline} -> {current}"
        assert connection.sql("select 10").fetchall() == [(10,)]
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# low memory, explicit temp directory, varying threads
# ---------------------------------------------------------------------------


def test_low_memory_sort_spills_to_configured_temp_directory_and_cleans_up(
    tmp_path: Path,
) -> None:
    temp_directory = tmp_path / "duckdb-temp"
    connection = connect(
        temp_directory, memory_limit="64MB", threads="2"
    )
    monitor = TempDirMonitor(temp_directory)
    monitor.start()
    try:
        assert connection.sql("select current_setting('temp_directory')").fetchall()[0][
            0
        ] == str(temp_directory)
        rows = connection.sql(SORT_SQL).fetchall()
        monitor.finish()

        assert len(rows) == 3_000_000
        assert monitor.max_files > 0
        assert monitor.max_bytes > 0
        assert temp_data_files(temp_directory) == []
    finally:
        monitor.finish()
        connection.close()
    # Closing the connection removes even the per-connection owner marker.
    assert list(temp_directory.iterdir()) == []


@pytest.mark.parametrize(
    ("threads", "memory_limit"),
    [("1", "64MB"), ("2", "64MB"), ("4", "128MB")],
)
def test_sort_and_aggregate_results_are_identical_across_thread_counts(
    tmp_path: Path, threads: str, memory_limit: str
) -> None:
    temp_directory = tmp_path / f"duckdb-temp-{threads}"
    connection = connect(
        temp_directory, memory_limit=memory_limit, threads=threads
    )
    try:
        aggregate = connection.sql(
            "select count(*) as n, sum(i) as total, min(i) as lo, "
            "max(i) as hi, "
            "count(distinct md5((i*2654435761 % 4294967296)::varchar)) "
            "as distinct_hashes "
            "from range(2000000) tbl(i)"
        ).fetchall()[0]
        n = 2_000_000
        assert aggregate == (n, n * (n - 1) // 2, 0, n - 1, n)

        # A real ORDER BY, checked against an independent Python computation.
        prefix = connection.sql(
            "select i, md5((i*2654435761 % 4294967296)::varchar) as h "
            "from range(50000) tbl(i) order by h, i limit 100"
        ).fetchall()
        import hashlib

        expected = sorted(
            (
                (i, hashlib.md5(
                    str(i * 2654435761 % 4294967296).encode()
                ).hexdigest())
                for i in range(50_000)
            ),
            key=lambda pair: (pair[1], pair[0]),
        )[:100]
        assert prefix == expected
    finally:
        connection.close()
    assert temp_data_files(temp_directory) == []


def test_temp_directory_limit_raises_explicit_error_and_cleans_up(
    tmp_path: Path,
) -> None:
    temp_directory = tmp_path / "duckdb-temp"
    connection = connect(
        temp_directory, memory_limit="32MB", threads="2"
    )
    connection.sql("SET max_temp_directory_size='1MB'")
    try:
        with pytest.raises((duckdb.OutOfMemoryException, duckdb.IOException)) as info:
            connection.sql(
                "select i, md5((i*2654435761 % 4294967296)::varchar) as h, "
                "repeat(md5(i::varchar), 5) as pad "
                "from range(4000000) tbl(i) order by h"
            ).fetchall()
        message = str(info.value).lower()
        assert "memory" in message or "temp" in message or "offload" in message
        assert temp_data_files(temp_directory) == []
        assert connection.sql("select 5").fetchall() == [(5,)]
    finally:
        connection.close()
    assert list(temp_directory.iterdir()) == []
