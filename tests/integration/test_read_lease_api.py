"""REST requests hold one Snapshot + Lease for their whole duration.

The app is driven through its ASGI interface so a test can observe the lease
while a streaming body is open and simulate a client disconnect. Assertions
are on public HTTP behaviour (status, body, lock directory), not on internal
call counts alone.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from mdgw.api.app import create_app
from mdgw.core.bar import Bar
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import active_session_locks

UTC = timezone.utc
INSTRUMENT = "binance:BTC/USDT"
START = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
BARS = "/api/v1/bars"
EXPORT = "/api/v1/bars/export"


def _bar(minute: int, *, interval: str = "1m") -> Bar:
    open_ts = START + timedelta(minutes=minute)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=INSTRUMENT,
        interval=interval,
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=None,
        aggregation_source="external",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1"),
        trade_count=1,
        revision=0,
    )


class _RecordingRead:
    """Delegating StoreRead that records how a request uses its Snapshot."""

    def __init__(self, owner: _RecordingStore, inner: Any) -> None:
        self._owner = owner
        self._inner = inner

    @property
    def snapshot(self):
        return self._inner.snapshot

    @property
    def lease(self):
        return self._inner.lease

    def _record(self, event: str) -> None:
        self._owner.events.append((event, self._inner.snapshot.generation))

    def count(self, *args, **kwargs):
        value = self._inner.count(*args, **kwargs)
        self._record("count")
        hook = self._owner.after_count
        if hook is not None:
            self._owner.after_count = None
            hook()
        return value

    def count_arrow(self, *args, **kwargs):
        value = self._inner.count_arrow(*args, **kwargs)
        self._record("count_arrow")
        hook = self._owner.after_count
        if hook is not None:
            self._owner.after_count = None
            hook()
        return value

    def read_bars(self, *args, **kwargs):
        self._record("read_bars")
        return self._inner.read_bars(*args, **kwargs)

    def read_arrow_bars(self, *args, **kwargs):
        self._record("read_arrow_bars")
        return self._inner.read_arrow_bars(*args, **kwargs)

    def read_earliest_arrow_bars(self, *args, **kwargs):
        self._record("read_earliest_arrow_bars")
        return self._inner.read_earliest_arrow_bars(*args, **kwargs)

    def iter_batches(self, *args, **kwargs):
        self._record("iter_batches")
        yield from self._inner.iter_batches(*args, **kwargs)

    def iter_arrow_batches(self, *args, **kwargs):
        self._record("iter_arrow_batches")
        yield from self._inner.iter_arrow_batches(*args, **kwargs)

    def close(self) -> None:
        self._record("close")
        self._inner.close()

    def __enter__(self):
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()


class _RecordingStore(ParquetBarStore):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.opens: list[_RecordingRead] = []
        self.events: list[tuple[str, str]] = []
        self.after_count = None

    def open_read(self, **kwargs):
        inner = super().open_read(**kwargs)
        wrapper = _RecordingRead(self, inner)
        self.opens.append(wrapper)
        self.events.append(("open", inner.snapshot.generation))
        return wrapper


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


async def _call_app(
    app: Any,
    path: str,
    *,
    disconnect_event: asyncio.Event | None = None,
    first_body: asyncio.Event | None = None,
) -> list[dict[str, Any]]:
    sent: list[dict[str, Any]] = []
    request_sent = False

    async def receive() -> dict[str, Any]:
        nonlocal request_sent
        if not request_sent:
            request_sent = True
            return {"type": "http.request", "body": b"", "more_body": False}
        if disconnect_event is None:
            await asyncio.Event().wait()
        await disconnect_event.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)
        if (
            first_body is not None
            and message["type"] == "http.response.body"
            and message.get("body")
        ):
            first_body.set()
            await asyncio.Event().wait()

    await app(_scope(path), receive, send)
    return sent


def _body(messages: list[dict[str, Any]]) -> bytes:
    return b"".join(
        message.get("body", b"")
        for message in messages
        if message["type"] == "http.response.body"
    )


def _request(app: Any, path: str) -> tuple[int, bytes]:
    messages = asyncio.run(_call_app(app, path))
    start = next(
        message for message in messages if message["type"] == "http.response.start"
    )
    return start["status"], _body(messages)


def _wait_until(predicate, *, timeout: float = 5.0, description: str = "state"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {description}")


def _store_with_rows(tmp_path: Path) -> _RecordingStore:
    store = _RecordingStore(tmp_path)
    store.write_bars([_bar(0)], asset_class="crypto")
    return store


def test_json_request_uses_one_snapshot_and_one_lease(tmp_path: Path) -> None:
    store = _store_with_rows(tmp_path)
    app = create_app(store)

    status, body = _request(app, f"{BARS}?interval=1m&instrument={INSTRUMENT}&limit=5")

    assert status == 200
    assert [event for event, _ in store.events] == [
        "open",
        "count_arrow",
        "read_earliest_arrow_bars",
        "close",
    ]
    generations = {generation for _, generation in store.events}
    assert len(generations) == 1
    assert len(json.loads(body)["bars"]) == 1
    assert active_session_locks(store.dataset_root) == []

    # A different paginated request is a different Snapshot and Lease.
    status, _ = _request(
        app, f"{BARS}?interval=1m&instrument={INSTRUMENT}&limit=1"
    )
    assert status == 200
    assert len(store.opens) == 2
    assert active_session_locks(store.dataset_root) == []


def test_non_paginated_json_counts_and_reads_on_one_snapshot(
    tmp_path: Path,
) -> None:
    store = _store_with_rows(tmp_path)
    app = create_app(store)

    def append_a_row() -> None:
        # Runs inside the request, after the DuckDB count and before the read.
        store.write_bars([_bar(1)], asset_class="crypto")

    store.after_count = append_a_row

    status, body = _request(app, f"{BARS}?interval=1m")

    assert status == 200
    assert [event for event, _ in store.events] == [
        "open",
        "count_arrow",
        "read_arrow_bars",
        "close",
    ]
    assert len({generation for _, generation in store.events}) == 1
    payload = json.loads(body)
    assert set(payload) == {"bars"}
    # The row appended after the count must not leak into the same response.
    assert len(payload["bars"]) == 1
    assert store.count_bars(interval="1m") == 2
    assert active_session_locks(store.dataset_root) == []


def test_json_count_and_read_cannot_see_a_write_between_them(
    tmp_path: Path,
) -> None:
    store = _store_with_rows(tmp_path)
    app = create_app(store)

    def append_a_row() -> None:
        # Runs inside the request, after the count and before the read.
        store.write_bars([_bar(1)], asset_class="crypto")

    store.after_count = append_a_row

    status, body = _request(
        app, f"{BARS}?interval=1m&instrument={INSTRUMENT}&limit=10"
    )

    assert status == 200
    payload = json.loads(body)
    # The row appended after the count must not leak into the same response.
    assert len(payload["bars"]) == 1
    assert payload["page"] == {"has_more": False, "next_start": None}
    assert store.count_bars(interval="1m") == 2


def test_limit_zero_existence_probe_uses_the_count_snapshot(
    tmp_path: Path,
) -> None:
    store = _RecordingStore(tmp_path)  # Empty Dataset: count is 0.
    app = create_app(store)

    def append_a_row() -> None:
        store.write_bars([_bar(0)], asset_class="crypto")

    store.after_count = append_a_row

    status, body = _request(
        app, f"{BARS}?interval=1m&instrument={INSTRUMENT}&limit=0"
    )

    assert status == 200
    # The probe must report the Snapshot it counted, not the row appended
    # between the count and the existence read.
    assert json.loads(body) == {
        "bars": [],
        "page": {"has_more": False, "next_start": None},
    }


def test_arrow_stream_holds_the_lease_until_the_body_finishes(
    tmp_path: Path,
) -> None:
    store = _store_with_rows(tmp_path)
    app = create_app(store)

    status, body = _request(app, f"{BARS}?interval=1m&format=arrow")

    assert status == 200
    assert body
    assert [event for event, _ in store.events][0] == "open"
    assert [event for event, _ in store.events][-1] == "close"
    assert active_session_locks(store.dataset_root) == []


def test_export_uses_one_snapshot_and_releases_the_lease(tmp_path: Path) -> None:
    store = _store_with_rows(tmp_path)
    app = create_app(store)

    status, body = _request(app, f"{EXPORT}?interval=1m&format=parquet")

    assert status == 200
    assert body
    assert [event for event, _ in store.events][0] == "open"
    assert [event for event, _ in store.events][-1] == "close"
    assert len(store.opens) == 1
    assert active_session_locks(store.dataset_root) == []


def test_client_disconnect_during_an_arrow_stream_releases_the_lease(
    tmp_path: Path,
) -> None:
    store = _RecordingStore(tmp_path)
    # Enough rows to keep the response iterating after the first chunk.
    for minute in range(200):
        store.write_bars([_bar(minute)], asset_class="crypto")
    app = create_app(store)

    loop = asyncio.new_event_loop()
    disconnect = asyncio.Event()
    first_body = asyncio.Event()
    errors: list[BaseException] = []

    def drive() -> None:
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(
                _call_app(
                    app,
                    f"{BARS}?interval=1m&format=arrow",
                    disconnect_event=disconnect,
                    first_body=first_body,
                )
            )
        except BaseException as exc:  # noqa: BLE001 - asserted below
            errors.append(exc)
        finally:
            loop.close()

    thread = threading.Thread(target=drive, name="mdgw-asgi-disconnect")
    thread.start()
    try:
        # The lease is acquired before the streaming response starts.
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
        disconnect.set() if not thread.is_alive() else None
    assert not thread.is_alive()
    assert errors == []
    _wait_until(
        lambda: active_session_locks(store.dataset_root) == [],
        description="lease release after disconnect",
    )
