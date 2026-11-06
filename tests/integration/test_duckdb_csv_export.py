# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[suspicious-url-open-usage]
"""CSV history and Parquet/Arrow export on the DuckDB Manifest Snapshot.

Both surfaces use `StoreRead.iter_arrow_batches()`, the same seam as the REST
Arrow response. The reference is the Store's one DuckDB read.

Non-paginated output has no ordering contract, so every row comparison is a
multiset: exact field text plus duplicate counts, ignoring physical scan
order. Failure tests observe the real HTTP transfer: a failure after the
response starts must reach the client as an incomplete transfer, never as a
short but normally-terminated body and never as an appended JSON error.
"""

from __future__ import annotations

import asyncio
import csv
import http.client
import io
import json
import struct
import threading
import time
import urllib.request
from collections import Counter
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.core.bar import Bar
from mdgw.core.envelope import decimal_to_str
from mdgw.core.schema import BAR_SCHEMA
from mdgw.core.time import datetime_to_ns, ns_to_iso
from mdgw.store.duckdb_reader import DuckDBOptions
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import active_session_locks

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path
INSTRUMENT = "binance:BTC/USDT"
OTHER_INSTRUMENT = "binance:ETH/USDT"
START = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-duckdb-csv-export"
BARS = "/api/v1/bars"
EXPORT = "/api/v1/bars/export"
EXPORT_DIR_GLOB = "mdgw-export-*"

TIMESTAMP_COLUMNS = ("open_ts", "close_ts", "event_ts", "ingest_ts")
DECIMAL_COLUMNS = ("open", "high", "low", "close", "volume")


# -- fixtures ---------------------------------------------------------------


def _bar(  # ruff: ignore[too-many-arguments] - explicit fixture fields
    minute: int,
    *,
    instrument_id: str = INSTRUMENT,
    interval: str = "1m",
    revision: int = 0,
    open_value: str = "65000.1",
    seq: int | None = None,
    trade_count: int | None = 128,
    open_ts: datetime | None = None,
) -> Bar:
    if open_ts is None:
        open_ts = START + timedelta(minutes=minute)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=instrument_id,
        interval=interval,
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=seq,
        aggregation_source="external",
        open=Decimal(open_value),
        high=Decimal("65010.0"),
        low=Decimal("64990.0"),
        close=Decimal("65005.5"),
        volume=Decimal("12.34"),
        trade_count=trade_count,
        revision=revision,
    )


def _write(store: ParquetBarStore, bars: list[Bar]) -> None:
    store.write_bars(bars, asset_class="crypto")


def _multi_file_revision_store(root: Path) -> ParquetBarStore:
    """Three Data Files with raw Revisions and a duplicated row."""
    store = ParquetBarStore(root)
    _write(store, [_bar(0, revision=0), _bar(0, revision=0)])
    _write(store, [_bar(0, revision=1), _bar(1, revision=0)])
    _write(store, [_bar(0, revision=2), _bar(2, revision=3)])
    return store


@contextmanager
def _serve(app: Any) -> Generator[AsgiServer]:
    server = AsgiServer()
    try:
        server.start(app, host=HOST, port=0, thread_name=THREAD_NAME)
        yield server
    finally:
        server.stop()
    assert [
        thread
        for thread in threading.enumerate()
        if thread.name == THREAD_NAME
    ] == []


def _url(base_url: str, path: str, **query: object) -> str:
    return f"{base_url}{path}?{urlencode(query)}"


def _get(url: str) -> tuple[int, dict[str, str], bytes]:
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.status, dict(response.headers), response.read()


def _header(headers: dict[str, str], name: str) -> str:
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    raise KeyError(name)


def _media_type(headers: dict[str, str]) -> str:
    for key, value in headers.items():
        if key.lower() == "content-type":
            return value.split(";", 1)[0].strip().lower()
    return ""


# -- wire-contract helpers --------------------------------------------------


def _wire_row(row: dict[str, Any]) -> tuple[str, ...]:
    """Render one logical row exactly as the CSV/JSON wire contract does."""
    values: list[str] = []
    for name in BAR_SCHEMA.names:
        value = row[name]
        if value is None:
            values.append("")
        elif name in TIMESTAMP_COLUMNS:
            values.append(ns_to_iso(datetime_to_ns(value)))
        elif name in DECIMAL_COLUMNS:
            values.append(decimal_to_str(value))
        else:
            values.append(str(value))
    return tuple(values)


def _store_rows(
    store: ParquetBarStore,
    *,
    instrument_id: str | None = INSTRUMENT,
    interval: str = "1m",
    limit: int | None = None,
) -> Counter:
    """Multiset of rows from the Store read."""
    table = store.read_bars(
        instrument_id=instrument_id, interval=interval, limit=limit
    )
    return Counter(_wire_row(row) for row in table.to_pylist())


def _csv_rows(body: bytes) -> tuple[list[str], list[tuple[str, ...]]]:
    text = body.decode("utf-8")
    lines = text.splitlines()
    header = lines[0].split(",")
    reader = csv.DictReader(io.StringIO(text))
    rows = [
        tuple(record[name] for name in BAR_SCHEMA.names) for record in reader
    ]
    return header, rows


def _export_rows(table: pa.Table) -> Counter:
    return Counter(_wire_row(row) for row in table.to_pylist())


def _open_export(format_name: str, body: bytes) -> pa.Table:
    if format_name == "parquet":
        return pq.read_table(pa.BufferReader(body))
    return pa.ipc.open_file(pa.BufferReader(body)).read_all()


def _assert_no_engine_leak(body: bytes, store: ParquetBarStore) -> None:
    lowered = body.lower()
    forbidden = (
        b"duckdb",
        b"polars",
        b"select ",
        b"read_parquet",
        b"tprotocolexception",
        b"out of memory",
        b".parquet",
        str(store.dataset_root).encode("utf-8"),
    )
    for needle in forbidden:
        assert needle.lower() not in lowered


def _assert_no_error_leak(body: bytes, store: ParquetBarStore) -> None:
    """A streaming failure must not append a JSON error or a 500 message."""
    _assert_no_engine_leak(body, store)
    lowered = body.lower()
    assert b'"error"' not in lowered
    assert b"internal server error" not in lowered


def _corrupt_data_pages(path: Path) -> None:
    """Keep the Parquet magic and footer; zero the (already written) pages."""
    raw = path.read_bytes()
    footer_length = struct.unpack("<I", raw[-8:-4])[0]
    tail = raw[-(footer_length + 8) :]
    path.write_bytes(raw[:4] + b"\x00" * (len(raw) - len(tail) - 4) + tail)


def _manifest_files(store: ParquetBarStore) -> list[dict]:
    manifest = json.loads(
        (store.dataset_root / "_manifest.json").read_text(encoding="utf-8")
    )
    return list(manifest["files"])


def _break_data_file(store: ParquetBarStore, *, how: str) -> None:
    entries = _manifest_files(store)
    victim = store.dataset_root / entries[-1]["path"]
    if how == "corrupt":
        _corrupt_data_pages(victim)
    else:
        victim.unlink()


def _failing_store(root: Path, how: str) -> ParquetBarStore:
    """A Store whose scan fails: bad file, missing file or resource limit."""
    if how == "resource":
        store = ParquetBarStore(
            root,
            duckdb=DuckDBOptions(memory_limit="1MB", arrow_batch_size=10_000),
        )
        _write(store, [_bar(minute) for minute in range(5000)])
        return store
    # The healthy first file is enough for a later bounded read.
    store = ParquetBarStore(root)
    _write(store, [_bar(minute) for minute in range(5)])
    _write(store, [_bar(10 + minute) for minute in range(5)])
    _break_data_file(store, how=how)
    return store


# -- real HTTP transfer observation -----------------------------------------


def _read_streaming(
    url: str,
) -> tuple[int, dict[str, str], bytes, BaseException | None]:
    """Read a chunked response until EOF, returning the transport failure."""
    parts = urlsplit(url)
    connection = http.client.HTTPConnection(
        parts.hostname, parts.port, timeout=30
    )
    body = bytearray()
    error: BaseException | None = None
    try:
        connection.request("GET", parts.path + "?" + parts.query)
        response = connection.getresponse()
        status = response.status
        headers = dict(response.getheaders())
        try:
            while True:
                chunk = response.read(1024)
                if not chunk:
                    break
                body.extend(chunk)
        except BaseException as exc:  # ruff: ignore[blind-except] - caller asserts
            error = exc
            partial = getattr(exc, "partial", None)
            if partial:
                body.extend(partial)
        return status, headers, bytes(body), error
    finally:
        connection.close()


# -- direct ASGI driving ----------------------------------------------------


def _scope(path: str, *, request_id: str | None = None) -> dict[str, Any]:
    parts = urlsplit(path)
    headers = []
    if request_id is not None:
        headers.append((b"x-request-id", request_id.encode("ascii")))
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": parts.path,
        "raw_path": parts.path.encode(),
        "query_string": parts.query.encode(),
        "headers": headers,
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "root_path": "",
    }


class _CallResult:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []
        self.error: BaseException | None = None

    async def run(  # ruff: ignore[too-many-arguments] - test-only driver
        self,
        app: Any,
        path: str,
        *,
        request_id: str | None = None,
        pause_on_first_body: bool = False,
        fail_on_body: bool = False,
        disconnect: asyncio.Event | None = None,
        first_body: asyncio.Event | None = None,
    ) -> None:
        request_sent = False

        async def receive() -> dict[str, Any]:
            nonlocal request_sent
            if not request_sent:
                request_sent = True
                return {
                    "type": "http.request",
                    "body": b"",
                    "more_body": False,
                }
            if disconnect is None:
                await asyncio.Event().wait()
            await disconnect.wait()
            return {"type": "http.disconnect"}

        async def send(message: dict[str, Any]) -> None:
            self.messages.append(message)
            if fail_on_body and message["type"] == "http.response.body":
                msg = "simulated transport failure"
                raise OSError(msg)
            if (
                pause_on_first_body
                and message["type"] == "http.response.body"
                and message.get("body")
            ):
                if first_body is not None:
                    first_body.set()
                await asyncio.Event().wait()

        try:
            await app(
                _scope(path, request_id=request_id), receive, send
            )
        except BaseException as exc:  # ruff: ignore[blind-except] - caller checks
            self.error = exc

    def body(self) -> bytes:
        return b"".join(
            message.get("body", b"")
            for message in self.messages
            if message["type"] == "http.response.body"
        )

    def start(self) -> dict[str, Any]:
        return next(
            message
            for message in self.messages
            if message["type"] == "http.response.start"
        )

    def normal_end(self) -> bool:
        return any(
            message["type"] == "http.response.body"
            and not message.get("more_body", False)
            for message in self.messages
        )


def _run_call(
    app: Any,
    path: str,
    *,
    request_id: str | None = None,
) -> _CallResult:
    call = _CallResult()
    asyncio.run(
        call.run(app, path, request_id=request_id)
    )
    return call


def _wait_until(predicate, *, timeout: float = 10.0, description: str = "state"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    msg = f"timed out waiting for {description}"
    raise AssertionError(msg)


class _PausedStream:
    """Drive one ASGI request until its first body chunk, then hold it open."""

    def __init__(self, app: Any, path: str, *, thread_name: str) -> None:
        self._app = app
        self._path = path
        self.loop = asyncio.new_event_loop()
        self.disconnect = asyncio.Event()
        self.first_body = asyncio.Event()
        self.result = _CallResult()
        self.thread = threading.Thread(
            target=self._run, name=thread_name
        )

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(
            self.result.run(
                self._app,
                self._path,
                disconnect=self.disconnect,
                first_body=self.first_body,
                pause_on_first_body=True,
            )
        )
        self.loop.close()

    def start(self) -> _PausedStream:
        self.thread.start()
        return self

    def wait_ready(self, store: ParquetBarStore) -> None:
        _wait_until(
            lambda: active_session_locks(store.dataset_root),
            description="streaming lease",
        )
        _wait_until(
            lambda: self.first_body.is_set() or not self.thread.is_alive(),
            description="first response body",
        )

    def stop(self) -> _CallResult:
        try:
            if self.thread.is_alive():
                self.loop.call_soon_threadsafe(self.disconnect.set)
            self.thread.join(10)
        finally:
            if self.thread.is_alive():
                self.loop.call_soon_threadsafe(self.disconnect.set)
        assert not self.thread.is_alive()
        return self.result


# ===========================================================================
# Content parity
# ===========================================================================


def test_csv_matches_the_store_for_empty_and_multifile_results(
    tmp_path: Path,
) -> None:
    empty = ParquetBarStore(tmp_path / "empty")
    with _serve(create_app(empty)) as server:
        status, headers, body = _get(
            _url(server.base_url, BARS, interval="1m", format="csv")
        )
    assert status == 200
    assert _media_type(headers) == "text/csv"
    header, rows = _csv_rows(body)
    assert header == BAR_SCHEMA.names
    assert rows == []
    assert Counter(rows) == _store_rows(empty, instrument_id=None)

    multi = _multi_file_revision_store(tmp_path / "multi")
    with _serve(create_app(multi)) as server:
        status, _, body = _get(
            _url(
                server.base_url,
                BARS,
                interval="1m",
                instrument=INSTRUMENT,
                format="csv",
            )
        )
    assert status == 200
    _, rows = _csv_rows(body)
    expected = _store_rows(multi)
    assert len(rows) == 6
    assert Counter(rows) == expected
    revision_index = BAR_SCHEMA.names.index("revision")
    assert sorted(int(row[revision_index]) for row in rows) == [
        0,
        0,
        0,
        1,
        2,
        3,
    ]


def test_csv_matches_the_store_for_a_large_multibatch_result(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(
        tmp_path, duckdb=DuckDBOptions(arrow_batch_size=512)
    )
    for chunk in range(5):
        _write(
            store,
            [
                _bar(minute, revision=minute % 3)
                for minute in range(chunk * 1000, (chunk + 1) * 1000)
            ],
        )
    with _serve(create_app(store)) as server:
        status, _, body = _get(
            _url(
                server.base_url,
                BARS,
                interval="1m",
                instrument=INSTRUMENT,
                format="csv",
            )
        )
        _, _, limited_body = _get(
            _url(
                server.base_url,
                BARS,
                interval="1m",
                instrument=INSTRUMENT,
                format="csv",
                limit=123,
            )
        )

    assert status == 200
    _, rows = _csv_rows(body)
    assert len(rows) == 5000
    assert Counter(rows) == _store_rows(store)
    # Raw Revisions are never deduplicated.
    revision_index = BAR_SCHEMA.names.index("revision")
    assert Counter(int(row[revision_index]) for row in rows) == Counter(
        {
            0: sum(1 for minute in range(5000) if minute % 3 == 0),
            1: sum(1 for minute in range(5000) if minute % 3 == 1),
            2: sum(1 for minute in range(5000) if minute % 3 == 2),
        }
    )
    # `limit` still caps the raw rows, without introducing an ordering.
    _, limited_rows = _csv_rows(limited_body)
    assert len(limited_rows) == 123
    assert set(limited_rows) <= set(rows)


def test_csv_keeps_exact_decimals_nanoseconds_nulls_and_field_order(
    tmp_path: Path,
) -> None:
    precise = "0.000000000000000001"
    pre_epoch = datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
    store = ParquetBarStore(tmp_path)
    _write(
        store,
        [
            _bar(
                0,
                open_value=precise,
                seq=None,
                trade_count=None,
                open_ts=pre_epoch,
            ),
            _bar(1),
        ],
    )
    with _serve(create_app(store)) as server:
        status, _, body = _get(
            _url(server.base_url, BARS, interval="1m", format="csv")
        )

    assert status == 200
    header, rows = _csv_rows(body)
    assert header == BAR_SCHEMA.names
    assert len(rows) == 2
    assert all(len(row) == len(BAR_SCHEMA.names) for row in rows)
    assert Counter(rows) == _store_rows(store)

    open_index = BAR_SCHEMA.names.index("open")
    seq_index = BAR_SCHEMA.names.index("seq")
    trade_count_index = BAR_SCHEMA.names.index("trade_count")
    open_ts_index = BAR_SCHEMA.names.index("open_ts")
    exact = next(row for row in rows if row[open_index] == precise)
    assert not exact[seq_index]
    assert not exact[trade_count_index]
    assert exact[open_ts_index] == "1969-12-31T23:59:59.999999000Z"
    assert all(
        len(row[index].split(".", 1)[1]) == 10  # nine digits plus `Z`
        for row in rows
        for index in (
            BAR_SCHEMA.names.index(name) for name in TIMESTAMP_COLUMNS
        )
    )
    assert "e-" not in body.decode("utf-8").lower()


def test_csv_yields_one_chunk_per_bounded_duckdb_batch(tmp_path: Path) -> None:
    store = ParquetBarStore(
        tmp_path, duckdb=DuckDBOptions(arrow_batch_size=1)
    )
    _write(store, [_bar(minute) for minute in range(5)])
    call = _run_call(
        create_app(store), f"{BARS}?interval=1m&format=csv"
    )

    assert call.error is None
    assert call.start()["status"] == 200
    chunks = [
        message.get("body", b"")
        for message in call.messages
        if message["type"] == "http.response.body" and message.get("body")
    ]
    # Header plus one chunk per single-row Arrow batch: the response is
    # assembled incrementally, not materialised in full.
    assert len(chunks) >= 6
    _, rows = _csv_rows(call.body())
    assert len(rows) == 5


@pytest.mark.parametrize("format_name", ["parquet", "arrow"])
def test_export_matches_the_store_for_revisions_and_duplicates(
    tmp_path: Path, format_name: str
) -> None:
    store = _multi_file_revision_store(tmp_path)
    expected = _store_rows(store)
    with _serve(create_app(store)) as server:
        status, headers, body = _get(
            _url(
                server.base_url,
                EXPORT,
                interval="1m",
                instrument=INSTRUMENT,
                format=format_name,
            )
        )
        _, _, limited_body = _get(
            _url(
                server.base_url,
                EXPORT,
                interval="1m",
                instrument=INSTRUMENT,
                format=format_name,
                limit=4,
            )
        )

    assert status == 200
    assert int(_header(headers, "Content-Length")) == len(body)
    if format_name == "parquet":
        assert _media_type(headers) == "application/vnd.apache.parquet"
        assert (
            _header(headers, "Content-Disposition")
            == 'attachment; filename="1m-bars.parquet"'
        )
    else:
        assert _media_type(headers) == "application/vnd.apache.arrow.file"
        assert (
            _header(headers, "Content-Disposition")
            == 'attachment; filename="1m-bars.arrow"'
        )
    table = _open_export(format_name, body)
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 6
    assert _export_rows(table) == expected
    limited = _open_export(format_name, limited_body)
    assert limited.schema == BAR_SCHEMA
    assert limited.num_rows == 4
    assert set(_export_rows(limited)) <= set(expected)


# ===========================================================================
# Streaming failure protocol
# ===========================================================================


@pytest.mark.parametrize("how", ["corrupt", "missing", "resource"])
def test_csv_failure_after_the_response_starts_is_an_incomplete_transfer(
    tmp_path: Path, how: str
) -> None:
    store = _failing_store(tmp_path, how)
    with _serve(create_app(store)) as server:
        status, headers, body, error = _read_streaming(
            _url(server.base_url, BARS, interval="1m", format="csv")
        )

    assert status == 200
    assert _media_type(headers) == "text/csv"
    assert _header(headers, "Transfer-Encoding").lower() == "chunked"
    # The client sees a broken chunked stream, not a short but valid CSV.
    assert error is not None
    assert isinstance(error, (http.client.IncompleteRead, ConnectionError))
    _assert_no_error_leak(body, store)
    assert active_session_locks(store.dataset_root) == []

    if how == "resource":
        # The configured limit still applies; the files themselves are intact
        # and a Store with normal resources reads them all.
        fresh = ParquetBarStore(store.root)
        assert fresh.read_bars(interval="1m").num_rows == 5000
        assert active_session_locks(fresh.dataset_root) == []
    else:
        # The failed Reader/connection must not poison the next request: a
        # bounded read of the healthy file still succeeds.
        with _serve(create_app(store)) as server:
            status, _, body = _get(
                _url(
                    server.base_url,
                    BARS,
                    interval="1m",
                    end=ns_to_iso(
                        datetime_to_ns(START + timedelta(minutes=6))
                    ),
                    format="csv",
                )
            )
        assert status == 200
        _, rows = _csv_rows(body)
        assert len(rows) == 5


def test_arrow_failure_after_the_response_starts_is_an_incomplete_transfer(
    tmp_path: Path,
) -> None:
    store = _failing_store(tmp_path, "corrupt")
    with _serve(create_app(store)) as server:
        status, headers, body, error = _read_streaming(
            _url(server.base_url, BARS, interval="1m", format="arrow")
        )

    assert status == 200
    assert _media_type(headers) == "application/vnd.apache.arrow.stream"
    # The stream format has no footer, so truncation at a batch boundary can
    # still parse; the HTTP chunked transfer is the authoritative failure
    # signal and the client must have seen it.
    assert error is not None
    assert isinstance(error, (http.client.IncompleteRead, ConnectionError))
    assert b'"error"' not in body
    assert active_session_locks(store.dataset_root) == []


@pytest.mark.parametrize("how", ["corrupt", "missing", "resource"])
def test_export_generation_failure_stays_a_pre_response_envelope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    how: str,
) -> None:
    _use_local_export_directory(monkeypatch, tmp_path)
    store = _failing_store(tmp_path / "dataset", how)
    request_id = f"export-failure-{how}"
    call = _run_call(
        create_app(store),
        f"{EXPORT}?interval=1m&format=parquet",
        request_id=request_id,
    )

    assert call.error is None
    start = call.start()
    assert start["status"] == 500
    body = call.body()
    payload = json.loads(body)
    assert payload["error"]["code"] == "internal_error"
    assert payload["error"]["message"] == "internal server error"
    assert payload["error"]["request_id"] == request_id
    _assert_no_engine_leak(body, store)
    assert active_session_locks(store.dataset_root) == []
    assert list(tmp_path.glob(EXPORT_DIR_GLOB)) == []


def test_csv_schema_mismatch_before_the_response_keeps_its_stable_envelope(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])
    descriptor = store.dataset_root / "_dataset.json"
    payload = json.loads(descriptor.read_text(encoding="utf-8"))
    payload["schema_version"] = 2
    descriptor.write_text(json.dumps(payload), encoding="utf-8")

    call = _run_call(
        create_app(store),
        f"{BARS}?interval=1m&format=csv",
        request_id="csv-schema-mismatch",
    )

    assert call.error is None
    assert call.start()["status"] == 409
    error = json.loads(call.body())["error"]
    assert error["code"] == "schema_version_mismatch"
    assert error["request_id"] == "csv-schema-mismatch"
    assert error["details"] == {}
    assert active_session_locks(store.dataset_root) == []


# ===========================================================================
# Lifecycle: disconnect, reader, lease and temporary files
# ===========================================================================


def _use_local_export_directory(
    monkeypatch: pytest.MonkeyPatch, root: Path
) -> None:
    """Keep export temp directories observable inside the test directory."""
    counter = [0]
    root.mkdir(parents=True, exist_ok=True)

    def _mkdtemp(*, prefix: str = "", **_kwargs: Any) -> str:
        counter[0] += 1
        path = root / f"{prefix}{counter[0]}"
        path.mkdir()
        return str(path)

    monkeypatch.setattr(
        "mdgw.api.routes.bars.tempfile.mkdtemp", _mkdtemp
    )


def test_client_disconnect_during_a_csv_stream_releases_lease_and_reader(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(
        tmp_path, duckdb=DuckDBOptions(arrow_batch_size=1)
    )
    _write(store, [_bar(minute) for minute in range(500)])
    app = create_app(store)
    stream = _PausedStream(
        app, f"{BARS}?interval=1m&format=csv", thread_name="mdgw-csv-disconnect"
    ).start()
    stream.wait_ready(store)
    result = stream.stop()
    assert result.error is None
    _wait_until(
        lambda: active_session_locks(store.dataset_root) == [],
        description="lease release after disconnect",
    )

    with store.open_read(interval="1m") as read:
        batches = list(read.iter_arrow_batches())
    assert sum(batch.num_rows for batch in batches) == 500


def test_client_disconnect_during_export_removes_the_temp_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _use_local_export_directory(monkeypatch, tmp_path)
    store = ParquetBarStore(tmp_path / "dataset")
    _write(store, [_bar(minute) for minute in range(200)])
    app = create_app(store)
    stream = _PausedStream(
        app,
        f"{EXPORT}?interval=1m&format=parquet",
        thread_name="mdgw-export-disconnect",
    ).start()
    stream.wait_ready(store)
    result = stream.stop()
    assert result.error is None
    _wait_until(
        lambda: active_session_locks(store.dataset_root) == [],
        description="lease release after export disconnect",
    )
    assert list(tmp_path.glob(EXPORT_DIR_GLOB)) == []


def test_maintenance_skips_data_files_held_by_a_streaming_csv_lease(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(
        tmp_path, duckdb=DuckDBOptions(arrow_batch_size=1)
    )
    _write(store, [_bar(minute) for minute in range(250)])
    _write(store, [_bar(minute) for minute in range(250, 500)])
    app = create_app(store)
    stream = _PausedStream(
        app,
        f"{BARS}?interval=1m&format=csv",
        thread_name="mdgw-csv-maintenance",
    ).start()
    stream.wait_ready(store)

    files_before = _manifest_files(store)
    now_ns = time.time_ns() + 1_000_000_000
    locked = store.compact(older_than=timedelta(seconds=0), now_ns=now_ns)
    assert locked.skipped_locked, locked
    assert len(_manifest_files(store)) == len(files_before)

    result = stream.stop()
    assert result.error is None
    _wait_until(
        lambda: active_session_locks(store.dataset_root) == [],
        description="lease release before maintenance retry",
    )
    merged = store.compact(older_than=timedelta(seconds=0), now_ns=now_ns)
    assert merged.merged_partitions, merged
    with store.open_read(interval="1m") as read:
        rows = sum(
            batch.num_rows for batch in read.iter_arrow_batches()
        )
    assert rows == 500


@pytest.mark.parametrize("format_name", ["csv", "arrow"])
def test_send_failure_during_a_stream_releases_lease_and_reader(
    tmp_path: Path, format_name: str
) -> None:
    store = ParquetBarStore(
        tmp_path, duckdb=DuckDBOptions(arrow_batch_size=1)
    )
    _write(store, [_bar(minute) for minute in range(500)])
    call = _CallResult()
    asyncio.run(
        call.run(
            create_app(store),
            f"{BARS}?interval=1m&format={format_name}",
            fail_on_body=True,
        )
    )

    assert call.error is not None
    assert call.start()["status"] == 200
    # The transfer cannot look like a normally finished response.
    assert not call.normal_end()
    _wait_until(
        lambda: active_session_locks(store.dataset_root) == [],
        description="lease release after send failure",
    )
    with store.open_read(interval="1m") as read:
        rows = sum(batch.num_rows for batch in read.iter_arrow_batches())
    assert rows == 500


def test_send_failure_during_export_cleans_temp_file_and_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _use_local_export_directory(monkeypatch, tmp_path)
    store = ParquetBarStore(tmp_path / "dataset")
    _write(store, [_bar(minute) for minute in range(200)])
    call = _CallResult()
    asyncio.run(
        call.run(
            create_app(store),
            f"{EXPORT}?interval=1m&format=parquet",
            fail_on_body=True,
        )
    )

    assert call.error is not None
    assert not call.normal_end()
    _wait_until(
        lambda: active_session_locks(store.dataset_root) == [],
        description="lease release after export send failure",
    )
    assert list(tmp_path.glob(EXPORT_DIR_GLOB)) == []


def test_export_failure_does_not_poison_a_later_bounded_read(
    tmp_path: Path,
) -> None:
    store = _failing_store(tmp_path, "corrupt")
    app = create_app(store)
    failing = _run_call(app, f"{EXPORT}?interval=1m&format=parquet")
    assert failing.start()["status"] == 500
    assert active_session_locks(store.dataset_root) == []

    end = ns_to_iso(datetime_to_ns(START + timedelta(minutes=6)))
    healed = _run_call(
        app, f"{EXPORT}?interval=1m&format=parquet&end={end}"
    )
    assert healed.error is None
    assert healed.start()["status"] == 200
    table = pq.read_table(pa.BufferReader(healed.body()))
    assert table.schema == BAR_SCHEMA
    assert sorted(table.column("open_ts").cast(pa.int64()).to_pylist()) == [
        datetime_to_ns(START + timedelta(minutes=minute))
        for minute in range(5)
    ]


# ===========================================================================
# Snapshot seam
# ===========================================================================


class _RecordingRead:
    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.calls: list[str] = []
        self.generations: list[str] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def iter_batches(self, *args, **kwargs):
        self.calls.append("iter_batches")
        yield from self._inner.iter_batches(*args, **kwargs)

    def iter_arrow_batches(self, *args, **kwargs):
        self.calls.append("iter_arrow_batches")
        self.generations.append(self._inner.snapshot.generation)
        yield from self._inner.iter_arrow_batches(*args, **kwargs)

    def close(self) -> None:
        self.calls.append("close")
        self._inner.close()

    def __enter__(self):
        self._inner.__enter__()
        return self

    def __exit__(self, *exc_info: object):
        return self._inner.__exit__(*exc_info)


class _RecordingStore(ParquetBarStore):
    def __init__(self, root: Path, **kwargs: Any) -> None:
        super().__init__(root, **kwargs)
        self.reads: list[_RecordingRead] = []

    def open_read(self, **kwargs):
        read = _RecordingRead(super().open_read(**kwargs))
        self.reads.append(read)
        return read


@pytest.mark.parametrize("format_name", ["parquet", "arrow"])
def test_export_uses_one_snapshot_and_one_duckdb_scan_per_request(
    tmp_path: Path, format_name: str
) -> None:
    store = _RecordingStore(tmp_path)
    _write(store, [_bar(0), _bar(1)])
    with _serve(create_app(store)) as server:
        status, _, _ = _get(
            _url(server.base_url, EXPORT, interval="1m", format=format_name)
        )

    assert status == 200
    assert len(store.reads) == 1
    read = store.reads[0]
    assert read.calls == ["iter_arrow_batches"]
    assert len(set(read.generations)) == 1
    assert read.closed
    assert active_session_locks(store.dataset_root) == []


def test_csv_and_export_use_the_single_duckdb_scan(
    tmp_path: Path,
) -> None:
    store = _RecordingStore(tmp_path)
    _write(store, [_bar(0), _bar(1)])
    with _serve(create_app(store)) as server:
        _get(_url(server.base_url, BARS, interval="1m", format="csv"))
        csv_read = store.reads[-1]
        _get(_url(server.base_url, EXPORT, interval="1m", format="parquet"))
        export_read = store.reads[-1]

    assert csv_read.calls[0] == "iter_arrow_batches"
    assert export_read.calls == ["iter_arrow_batches"]
    assert csv_read.closed
    assert export_read.closed
    assert all(
        "iter_batches" not in read.calls
        for read in (csv_read, export_read)
    )
