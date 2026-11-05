"""M1 spike: Parquet file deletion/rename while a DuckDB Reader is alive.

These are deliberately *behavioural* tests, not transaction guarantees. The
observed result is the input to the Read Lease design: DuckDB opens data files
lazily, an already-open file may survive deletion (the OS keeps the handle
alive), and an unopened file in an explicit list fails mid-stream after some
batches were already delivered. A Read Lease must therefore cover the whole
Reader lifetime, and a partial stream failure must surface as an error rather
than as a clean end of results.

Windows runs these locally. The POSIX-only variant is skipped here and is
covered by the Linux GitHub Actions job (``.github/workflows/tests.yml``).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

FILE_ROWS = 300_000
ROW_GROUP_SIZE = 20_000
BATCH_SIZE = 5_000


def connect() -> duckdb.DuckDBPyConnection:
    # One thread keeps file-opening order deterministic across platforms.
    return duckdb.connect(config={"timezone": "UTC", "threads": "1"})


def write_file(path: Path, tag: str, rows: int = FILE_ROWS) -> None:
    table = pa.table(
        {
            "tag": pa.array([tag] * rows, type=pa.string()),
            "i": pa.array(range(rows), type=pa.int64()),
            "payload": pa.array([f"{tag}-{i:07d}-" + "x" * 40 for i in range(rows)]),
        }
    )
    pq.write_table(table, path, row_group_size=ROW_GROUP_SIZE)


def drain(reader) -> tuple[int, dict[str, int]]:
    rows = 0
    tags: dict[str, int] = {}

    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            return rows, tags
        rows += batch.num_rows
        for tag in batch.column(0).to_pylist():
            tags[tag] = tags.get(tag, 0) + 1


def is_file_open_error(error: BaseException) -> bool:
    message = str(error).lower()
    return any(
        marker in message
        for marker in ("cannot open file", "no such file", "no files found")
    )


def test_delete_before_first_read_is_an_explicit_io_error(tmp_path: Path) -> None:
    path = tmp_path / "delete-before.parquet"
    write_file(path, "a")

    connection = connect()
    try:
        relation = connection.read_parquet([str(path)])
        path.unlink()
        with pytest.raises((OSError, duckdb.IOException)) as info:
            relation.to_arrow_reader(batch_size=BATCH_SIZE)
        assert is_file_open_error(info.value), str(info.value)
        assert connection.sql("select 1").fetchall() == [(1,)]
    finally:
        connection.close()


@pytest.mark.parametrize("action", ["delete", "rename"])
def test_unopened_later_file_fails_midstream_after_partial_output(
    tmp_path: Path, action: str
) -> None:
    first = tmp_path / "a.parquet"
    second = tmp_path / "b.parquet"
    write_file(first, "a")
    write_file(second, "b")

    connection = connect()
    try:
        reader = connection.read_parquet([str(first), str(second)]).to_arrow_reader(
            batch_size=BATCH_SIZE
        )
        try:
            head = reader.read_next_batch()
            assert set(head.column(0).to_pylist()) == {"a"}
            rows = head.num_rows
            tags = {"a": head.num_rows}

            moved = tmp_path / "b-moved.parquet"
            if action == "delete":
                second.unlink()
            else:
                os.replace(second, moved)

            with pytest.raises((OSError, duckdb.IOException)) as info:
                while True:
                    try:
                        batch = reader.read_next_batch()
                    except StopIteration:
                        pytest.fail(
                            "reader reported normal exhaustion after deleting an "
                            "unopened data file; that would be silent truncation"
                        )
                    rows += batch.num_rows
                    for tag in batch.column(0).to_pylist():
                        tags[tag] = tags.get(tag, 0) + 1
        finally:
            reader.close()

        # Partial output was already delivered; the stream ended in an error.
        assert 0 < rows < 2 * FILE_ROWS
        assert tags.get("b", 0) == 0
        assert is_file_open_error(info.value), str(info.value)
        assert connection.sql("select 2").fetchall() == [(2,)]
    finally:
        connection.close()


def test_delete_of_already_open_file_never_silently_truncates(
    tmp_path: Path,
) -> None:
    """Whether unlink succeeds is platform-specific; truncation is not allowed."""
    path = tmp_path / "single.parquet"
    write_file(path, "a")

    connection = connect()
    try:
        reader = connection.read_parquet([str(path)]).to_arrow_reader(
            batch_size=BATCH_SIZE
        )
        head = reader.read_next_batch()
        rows = head.num_rows
        tags = {"a": head.num_rows}
        unlink_succeeded = True
        try:
            path.unlink()
        except PermissionError:
            # Windows without FILE_SHARE_DELETE; the Reader keeps the handle.
            unlink_succeeded = False

        error: BaseException | None = None
        try:
            more, more_tags = drain(reader)
            rows += more
            for tag, count in more_tags.items():
                tags[tag] = tags.get(tag, 0) + count
        except (OSError, duckdb.IOException) as exc:
            error = exc
        finally:
            reader.close()

        if error is None:
            assert rows == FILE_ROWS, "clean completion must deliver every row"
        else:
            assert rows < FILE_ROWS, "an error must never masquerade as full output"
            assert is_file_open_error(error), str(error)

        # After the Reader is gone the path can be removed on every platform.
        if unlink_succeeded:
            assert not path.exists()
        else:
            path.unlink()
        assert connection.sql("select 3").fetchall() == [(3,)]
    finally:
        connection.close()


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="POSIX unlink-of-open-file semantics; executed by the Linux CI job",
)
def test_posix_unlink_of_open_file_keeps_stream_readable(tmp_path: Path) -> None:
    """Linux-only arrangement for the Windows gap: CI verifies POSIX behaviour."""
    path = tmp_path / "posix.parquet"
    write_file(path, "a")

    connection = connect()
    try:
        reader = connection.read_parquet([str(path)]).to_arrow_reader(
            batch_size=BATCH_SIZE
        )
        head = reader.read_next_batch()
        rows = head.num_rows

        # POSIX unlink always succeeds while the descriptor is open.
        path.unlink()
        assert not path.exists()

        more, _ = drain(reader)
        reader.close()
        assert rows + more == FILE_ROWS
        assert connection.sql("select 4").fetchall() == [(4,)]
    finally:
        connection.close()


@pytest.mark.skipif(
    sys.platform != "win32",
    reason="records the Windows file-sharing behaviour observed by the spike",
)
def test_windows_delete_after_first_batch_is_permitted_by_duckdb(
    tmp_path: Path,
) -> None:
    path = tmp_path / "windows.parquet"
    write_file(path, "a")

    connection = connect()
    try:
        reader = connection.read_parquet([str(path)]).to_arrow_reader(
            batch_size=BATCH_SIZE
        )
        head = reader.read_next_batch()
        rows = head.num_rows
        try:
            path.unlink()
        except PermissionError:
            # Refusal is acceptable; silent truncation is not. Close first so
            # the test leaves the filesystem clean.
            reader.close()
            path.unlink()
            return

        more, _ = drain(reader)
        reader.close()
        assert rows + more == FILE_ROWS
        assert not path.exists()
    finally:
        connection.close()


def test_rename_of_open_file_never_silently_truncates(tmp_path: Path) -> None:
    path = tmp_path / "single.parquet"
    moved = tmp_path / "moved.parquet"
    write_file(path, "a")

    connection = connect()
    try:
        reader = connection.read_parquet([str(path)]).to_arrow_reader(
            batch_size=BATCH_SIZE
        )
        head = reader.read_next_batch()
        rows = head.num_rows
        os.replace(path, moved)

        error: BaseException | None = None
        try:
            more, _ = drain(reader)
            rows += more
        except (OSError, duckdb.IOException) as exc:
            error = exc
        finally:
            reader.close()

        if error is None:
            assert rows == FILE_ROWS
        else:
            assert rows < FILE_ROWS
            assert is_file_open_error(error), str(error)
        assert not path.exists()
        assert moved.exists()
        assert connection.sql("select 5").fetchall() == [(5,)]
    finally:
        connection.close()
