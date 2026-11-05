"""REST Arrow history through the DuckDB Relation Arrow Reader.

The public `/api/v1/bars?format=arrow` response is compared with the
pre-migration PyArrow Store read: exact per-row content, duplicate counts and
the exact Data Contract schema. Non-paginated output has no ordering contract,
so row comparisons are order insensitive. Other surfaces (JSON, CSV, export)
are asserted to keep using the existing PyArrow scan.
"""

# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[suspicious-url-open-usage]
from __future__ import annotations

import asyncio
import json
import struct
import threading
import time
import urllib.request
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode, urlsplit

import pyarrow as pa
import pytest

from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.core.bar import Bar
from mdgw.core.schema import BAR_SCHEMA
from mdgw.core.time import datetime_to_ns, ns_to_iso
from mdgw.store.duckdb_reader import DuckDBOptions
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import active_session_locks

if TYPE_CHECKING:
    from collections.abc import Generator

UTC = timezone.utc
INSTRUMENT = "binance:BTC/USDT"
OTHER_INSTRUMENT = "binance:ETH/USDT"
START = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-duckdb-arrow"
BARS = "/api/v1/bars"
EXPORT = "/api/v1/bars/export"


def _bar(
    minute: int,
    *,
    instrument_id: str = INSTRUMENT,
    interval: str = "1m",
    revision: int = 0,
    base: datetime = START,
    open_value: str = "65000.1",
) -> Bar:
    open_ts = base + timedelta(minutes=minute)
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
        seq=None,
        aggregation_source="external",
        open=Decimal(open_value),
        high=Decimal("65010.0"),
        low=Decimal("64990.0"),
        close=Decimal("65005.5"),
        volume=Decimal("12.34"),
        trade_count=128,
        revision=revision,
    )


def _write(store: ParquetBarStore, bars: list[Bar]) -> None:
    store.write_bars(bars, asset_class="crypto")


def _url(base_url: str, path: str, **query: object) -> str:
    return f"{base_url}{path}?{urlencode(query)}"


def _get(url: str) -> tuple[int, dict[str, str], bytes]:
    with urllib.request.urlopen(url, timeout=30) as response:
        return response.status, dict(response.headers), response.read()


def _media_type(headers: dict[str, str]) -> str:
    for key, value in headers.items():
        if key.lower() == "content-type":
            return value.split(";", 1)[0].strip().lower()
    return ""


def _arrow_table(body: bytes) -> pa.Table:
    return pa.ipc.open_stream(pa.BufferReader(body)).read_all()


def _rows(table: pa.Table) -> Counter:
    return Counter(
        tuple(row[name] for name in BAR_SCHEMA.names)
        for row in table.to_pylist()
    )


def _old_store_rows(
    store: ParquetBarStore,
    *,
    instrument_id: str = INSTRUMENT,
    interval: str = "1m",
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> pa.Table:
    return store.read_bars(
        instrument_id=instrument_id,
        interval=interval,
        start_ns=start_ns,
        end_ns=end_ns,
    )


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


# -- scan boundary ----------------------------------------------------------


class _RecordingRead:
    """Delegating StoreRead that records which Store scan a route chose."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def count(self, *args, **kwargs):
        self.calls.append("count")
        return self._inner.count(*args, **kwargs)

    def read_bars(self, *args, **kwargs):
        self.calls.append("read_bars")
        return self._inner.read_bars(*args, **kwargs)

    def iter_batches(self, *args, **kwargs):
        self.calls.append("iter_batches")
        yield from self._inner.iter_batches(*args, **kwargs)

    def iter_arrow_batches(self, *args, **kwargs):
        self.calls.append("iter_arrow_batches")
        yield from self._inner.iter_arrow_batches(*args, **kwargs)

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


def test_only_the_arrow_route_chooses_the_duckdb_scan(tmp_path: Path) -> None:
    store = _RecordingStore(tmp_path)
    _write(store, [_bar(0)])
    with _serve(create_app(store)) as server:
        _get(_url(server.base_url, BARS, interval="1m", format="arrow"))
        arrow_call = list(store.reads[-1].calls)

        _get(_url(server.base_url, BARS, interval="1m"))
        json_call = list(store.reads[-1].calls)

        _get(_url(server.base_url, BARS, interval="1m", format="csv"))
        csv_call = list(store.reads[-1].calls)

        _get(_url(server.base_url, EXPORT, interval="1m", format="parquet"))
        export_call = list(store.reads[-1].calls)

    assert arrow_call == ["iter_arrow_batches"]
    assert "iter_batches" not in arrow_call
    assert "iter_batches" in csv_call
    assert "iter_batches" in export_call
    assert "count" in json_call
    assert all(
        "iter_arrow_batches" not in call
        for call in (json_call, csv_call, export_call)
    )


# -- content and schema parity ---------------------------------------------


def test_arrow_rest_empty_dataset_is_a_schema_only_stream(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    with _serve(create_app(store)) as server:
        status, headers, body = _get(
            _url(server.base_url, BARS, interval="1m", format="arrow")
        )

    assert status == 200
    assert _media_type(headers) == "application/vnd.apache.arrow.stream"
    table = _arrow_table(body)
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 0


def test_arrow_rest_bounded_empty_result_keeps_the_contract_schema(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])
    with _serve(create_app(store)) as server:
        status, _, body = _get(
            _url(
                server.base_url,
                BARS,
                interval="1m",
                start="2025-01-01T00:00:00Z",
                format="arrow",
            )
        )

    assert status == 200
    table = _arrow_table(body)
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 0


def test_arrow_rest_matches_the_pyarrow_store_for_multi_file_revisions(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    # Three Data Files, three Revisions of the same identity and a duplicate
    # row: every raw Revision and every duplicate must survive.
    _write(store, [_bar(0, revision=0), _bar(0, revision=0)])
    _write(store, [_bar(0, revision=1), _bar(1, revision=0)])
    _write(store, [_bar(0, revision=2), _bar(2, revision=3)])
    expected = _old_store_rows(store)

    with _serve(create_app(store)) as server:
        status, _, body = _get(
            _url(
                server.base_url,
                BARS,
                interval="1m",
                instrument=INSTRUMENT,
                format="arrow",
            )
        )

    assert status == 200
    table = _arrow_table(body)
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == expected.num_rows == 6
    assert _rows(table) == _rows(expected)
    assert sorted(row["revision"] for row in table.to_pylist()) == [
        0,
        0,
        0,
        1,
        2,
        3,
    ]


def test_arrow_rest_honours_half_open_open_ts_bounds(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(minute) for minute in range(5)])

    def iso(minute: int) -> str:
        return ns_to_iso(datetime_to_ns(START + timedelta(minutes=minute)))

    with _serve(create_app(store)) as server:
        status, _, body = _get(
            _url(
                server.base_url,
                BARS,
                interval="1m",
                start=iso(1),
                end=iso(4),
                format="arrow",
            )
        )

    assert status == 200
    table = _arrow_table(body)
    assert [value.minute for value in table.column("open_ts").to_pylist()] == [
        1,
        2,
        3,
    ]


def test_arrow_rest_prunes_cross_partition_data(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0, interval="1m")])
    _write(store, [_bar(0, interval="5m")])
    _write(store, [_bar(0, instrument_id=OTHER_INSTRUMENT)])
    expected = _old_store_rows(store, instrument_id=INSTRUMENT, interval="1m")

    with _serve(create_app(store)) as server:
        status, _, body = _get(
            _url(
                server.base_url,
                BARS,
                interval="1m",
                instrument=INSTRUMENT,
                format="arrow",
            )
        )

    assert status == 200
    table = _arrow_table(body)
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["instrument_id"] == INSTRUMENT
    assert row["interval"] == "1m"
    assert _rows(table) == _rows(expected)


def test_arrow_rest_preserves_decimals_and_pre_epoch_instants(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    precise = "0.000000000000000001"
    pre_epoch = datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
    store.write_bars(
        [
            _bar(0, open_value=precise),
            Bar(
                schema_version=1,
                data_type="bar",
                instrument_id=INSTRUMENT,
                interval="1m",
                open_ts=pre_epoch,
                close_ts=pre_epoch + timedelta(minutes=1),
                event_ts=pre_epoch + timedelta(minutes=1),
                ingest_ts=pre_epoch + timedelta(minutes=1),
                feed="fake",
                seq=None,
                aggregation_source="external",
                open=Decimal("-1"),
                high=Decimal("1"),
                low=Decimal("-1"),
                close=Decimal("0"),
                volume=Decimal("1"),
                trade_count=None,
                revision=0,
            ),
        ],
        asset_class="crypto",
    )
    expected = _old_store_rows(store)

    with _serve(create_app(store)) as server:
        status, _, body = _get(
            _url(server.base_url, BARS, interval="1m", format="arrow")
        )

    assert status == 200
    table = _arrow_table(body)
    assert table.schema == BAR_SCHEMA
    assert _rows(table) == _rows(expected)
    opens = sorted(value.as_py() for value in table.column("open"))
    assert Decimal(precise) in opens
    instants = table.column("open_ts").cast(pa.int64()).to_pylist()
    assert datetime_to_ns(pre_epoch) in instants
    assert datetime_to_ns(START) in instants


# -- failure and lifecycle ---------------------------------------------------


def _scope(path: str) -> dict[str, Any]:
    parts = urlsplit(path)
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": parts.path,
        "raw_path": parts.path.encode(),
        "query_string": parts.query.encode(),
        "headers": [],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "root_path": "",
    }


class _CollectingCall:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []
        self.error: BaseException | None = None
        self.first_body = threading.Event()

    async def run(self, app: Any, path: str, *, pause_on_first_body: bool) -> None:
        async def receive() -> dict[str, Any]:
            await asyncio.Event().wait()
            msg = "receive must not be awaited after the request body"
            raise AssertionError(msg)

        async def send(message: dict[str, Any]) -> None:
            self.messages.append(message)
            if (
                pause_on_first_body
                and message["type"] == "http.response.body"
                and message.get("body")
            ):
                self.first_body.set()
                await asyncio.Event().wait()

        try:
            await app(_scope(path), receive, send)
        except BaseException as exc:  # ruff: ignore[BLE001] - asserted by the caller
            self.error = exc

    def body(self) -> bytes:
        return b"".join(
            message.get("body", b"")
            for message in self.messages
            if message["type"] == "http.response.body"
        )

    def normal_end(self) -> bool:
        return any(
            message["type"] == "http.response.body"
            and not message.get("more_body", False)
            for message in self.messages
        )


def _run_call(app: Any, path: str, *, pause_on_first_body: bool = False):
    call = _CollectingCall()
    asyncio.run(call.run(app, path, pause_on_first_body=pause_on_first_body))
    return call


def _corrupt_data_pages(path: Path) -> None:
    """Keep the Parquet magic and footer; zero the (already written) pages."""
    raw = path.read_bytes()
    footer_length = struct.unpack("<I", raw[-8:-4])[0]
    tail = raw[-(footer_length + 8) :]
    path.write_bytes(raw[:4] + b"\x00" * (len(raw) - len(tail) - 4) + tail)


def test_arrow_rest_aborts_midstream_on_a_scan_failure(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0), _bar(1)])
    _write(store, [_bar(2), _bar(3)])
    manifest = json.loads(
        (store.dataset_root / "_manifest.json").read_text(encoding="utf-8")
    )
    victim = store.dataset_root / manifest["files"][1]["path"]
    _corrupt_data_pages(victim)

    app = create_app(store)
    call = _run_call(app, f"{BARS}?interval=1m&format=arrow")

    assert call.error is not None
    assert any(
        message["type"] == "http.response.start" for message in call.messages
    )
    assert not call.normal_end()
    assert b'"error"' not in call.body()
    assert b"internal server error" not in call.body().lower()
    assert active_session_locks(store.dataset_root) == []


def test_client_disconnect_during_the_duckdb_stream_releases_the_lease(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(
        tmp_path, duckdb=DuckDBOptions(arrow_batch_size=1)
    )
    _write(store, [_bar(minute) for minute in range(500)])
    app = create_app(store)

    loop = asyncio.new_event_loop()
    disconnect = asyncio.Event()
    first_body = asyncio.Event()
    errors: list[BaseException] = []

    async def receive() -> dict[str, Any]:
        await disconnect.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        if (
            message["type"] == "http.response.body"
            and message.get("body")
            and not first_body.is_set()
        ):
            first_body.set()
            await asyncio.Event().wait()

    def drive() -> None:
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(
                app(_scope(f"{BARS}?interval=1m&format=arrow"), receive, send)
            )
        except BaseException as exc:  # ruff: ignore[BLE001] - asserted below
            errors.append(exc)
        finally:
            loop.close()

    thread = threading.Thread(target=drive, name="mdgw-duckdb-disconnect")
    thread.start()
    try:
        _wait_until(
            lambda: active_session_locks(store.dataset_root),
            description="streaming lease",
        )
        _wait_until(
            lambda: first_body.is_set() or not thread.is_alive(),
            description="first response body",
        )
        loop.call_soon_threadsafe(disconnect.set)
        thread.join(10)
    finally:
        if thread.is_alive():
            loop.call_soon_threadsafe(disconnect.set)
    assert not thread.is_alive()
    assert errors == []
    _wait_until(
        lambda: active_session_locks(store.dataset_root) == [],
        description="lease release after disconnect",
    )

    # The cancelled Reader must not leave a connection or lease that blocks a
    # later read through the same Store.
    with store.open_read(interval="1m") as read:
        batches = list(read.iter_arrow_batches())
    assert sum(batch.num_rows for batch in batches) == 500


def _wait_until(predicate, *, timeout: float = 10.0, description: str = "state"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    msg = f"timed out waiting for {description}"
    raise AssertionError(msg)


@pytest.mark.parametrize("format_name", ["arrow", "csv"])
def test_streaming_read_failure_aborts_instead_of_ending_normally(
    tmp_path: Path, format_name: str
) -> None:
    """A broken Data File aborts the transfer instead of ending normally."""
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])
    _write(store, [_bar(1)])
    manifest = json.loads(
        (store.dataset_root / "_manifest.json").read_text(encoding="utf-8")
    )
    _corrupt_data_pages(store.dataset_root / manifest["files"][1]["path"])

    call = _run_call(create_app(store), f"{BARS}?interval=1m&format={format_name}")

    assert call.error is not None
    assert not call.normal_end()
    body = call.body()
    if body:
        assert b'"error"' not in body
        assert b"internal server error" not in body.lower()
    assert active_session_locks(store.dataset_root) == []


def test_export_failure_before_the_response_uses_the_error_envelope(
    tmp_path: Path,
) -> None:
    """Export materializes before sending, so a failure stays a normal error."""
    store = ParquetBarStore(tmp_path)
    _write(store, [_bar(0)])
    _write(store, [_bar(1)])
    manifest = json.loads(
        (store.dataset_root / "_manifest.json").read_text(encoding="utf-8")
    )
    _corrupt_data_pages(store.dataset_root / manifest["files"][1]["path"])

    call = _run_call(
        create_app(store), f"{EXPORT}?interval=1m&format=parquet"
    )

    assert call.error is None
    start = next(
        message
        for message in call.messages
        if message["type"] == "http.response.start"
    )
    assert start["status"] == 500
    assert json.loads(call.body())["error"]["code"] == "internal_error"
    assert active_session_locks(store.dataset_root) == []
