# ruff: file-ignore[implicit-namespace-package]
"""Admin Export Job API over a real FastAPI app, socket and temp Dataset.

The tests pin the operator-facing contract: create/list/get/cancel/download,
ownership and role policy, CSRF, quotas, audit fail-closed, Range/206/416,
safe attachment names, exact SHA-256/size, expiry, cancellation, restart
recovery and the untouched Dataset. The CSV assertions use known literal
rows, so an accidental decimal/timestamp/column-order change fails.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import operator
import re
import sqlite3
import threading
import time
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

import pytest

from mdgw.admin.context import AdminContext
from mdgw.admin.db import AdminStore
from mdgw.admin.export.models import (
    CANCELLED,
    CANCELLING,
    EXPIRED,
    FAILED,
    QUEUED,
    RUNNING,
    SUCCEEDED,
    ExportJobRecord,
)
from mdgw.admin.export.service import ExportService
from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.config import (
    AdminAdaptersConfig,
    AdminConfig,
    AdminExportConfig,
)
from mdgw.core.schema import BAR_SCHEMA
from mdgw.core.time import ns_to_iso
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import active_session_locks
from tests.characterization.dataset_builder import BASE, BarDatasetBuilder, make_bar

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-admin-export"
SECOND = 1_000_000_000
DAY_NS = 86_400 * SECOND
CLOCK_BASE_NS = 1_800_000_000 * SECOND
ROLES = ("SuperAdmin", "Operator", "Auditor")
JOBS = "/api/v1/admin/export-jobs"
NS_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z")
INSTRUMENT = "binance:BTC/USDT"
START = datetime(2026, 1, 1, tzinfo=UTC)
END_EARLY = datetime(2026, 2, 1, tzinfo=UTC)
END_LATE = datetime(2026, 3, 1, tzinfo=UTC)


# -- dataset / expected CSV -------------------------------------------------

ROWS = [
    make_bar(
        open_ts=BASE,
        open="65000.1",
        high="65000.200000000000000001",
        low="64999.9",
        close="65000.15",
        volume="0.123456789012345678",
        trade_count=3,
        seq=10,
        revision=0,
    ),
    make_bar(
        open_ts=BASE,
        open="65000.2",
        high="65000.3",
        low="65000.05",
        close="65000.25",
        volume="0.5",
        trade_count=4,
        seq=10,
        revision=1,
    ),
    make_bar(
        open_ts=BASE.replace(minute=1),
        open="100.000",
        high="101.5",
        low="99.25",
        close="100.5",
        volume="2.5",
        trade_count=None,
        seq=None,
        revision=0,
    ),
]

_COLUMN_TEXT: dict[str, str] = {
    "schema_version": "1",
    "data_type": "bar",
    "instrument_id": INSTRUMENT,
    "interval": "1m",
    "open_ts": "2026-01-01T00:00:00.000000000Z",
    "close_ts": "2026-01-01T00:01:00.000000000Z",
    "event_ts": "2026-01-01T00:01:00.000000000Z",
    "ingest_ts": "2026-01-01T00:01:00.000000000Z",
    "feed": "characterization",
    "seq": "10",
    "aggregation_source": "external",
    "open": "65000.1",
    "high": "65000.200000000000000001",
    "low": "64999.9",
    "close": "65000.15",
    "volume": "0.123456789012345678",
    "trade_count": "3",
    "revision": "0",
}


def _expected_line(**overrides: str) -> str:
    values = {**_COLUMN_TEXT, **overrides}
    return ",".join(values[name] for name in BAR_SCHEMA.names)


EXPECTED_CSV = "\n".join(
    [
        ",".join(BAR_SCHEMA.names),
        _expected_line(),
        _expected_line(
            open="65000.2",
            high="65000.3",
            low="65000.05",
            close="65000.25",
            volume="0.5",
            trade_count="4",
            revision="1",
        ),
        _expected_line(
            open_ts="2026-01-01T00:01:00.000000000Z",
            close_ts="2026-01-01T00:02:00.000000000Z",
            event_ts="2026-01-01T00:02:00.000000000Z",
            ingest_ts="2026-01-01T00:02:00.000000000Z",
            seq="",
            open="100.0",
            high="101.5",
            low="99.25",
            close="100.5",
            volume="2.5",
            trade_count="",
        ),
        "",
    ]
)


def _build_dataset(root: Path) -> ParquetBarStore:
    builder = BarDatasetBuilder(root)
    builder.add(ROWS[:2])
    builder.add(ROWS[2:])
    builder.store.close()
    return ParquetBarStore(root)


# -- HTTP client ------------------------------------------------------------


@dataclass
class Result:
    status: int
    headers: list[tuple[str, str]]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body)

    def header(self, name: str) -> str | None:
        for key, value in self.headers:
            if key.lower() == name.lower():
                return value
        return None


class Client:
    def __init__(self, server: AsgiServer) -> None:
        self.server = server

    @property
    def origin(self) -> str:
        return f"http://{HOST}:{self.server.bound_port}"

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> Result:
        connection = http.client.HTTPConnection(
            HOST, self.server.bound_port, timeout=30
        )
        try:
            connection.request(
                method, path, body=body, headers=dict(headers or {})
            )
            response = connection.getresponse()
            payload = response.read()
            return Result(
                int(response.status), list(response.getheaders()), payload
            )
        finally:
            connection.close()

    def get(self, path: str, **kwargs: Any) -> Result:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> Result:
        return self.request("POST", path, **kwargs)


class Clock:
    def __init__(self, now_ns: int = CLOCK_BASE_NS) -> None:
        self.now_ns = now_ns

    def __call__(self) -> int:
        return self.now_ns

    def advance(self, delta_ns: int) -> None:
        self.now_ns += delta_ns


@dataclass
class Credential:
    token: str
    csrf: str
    subject: str
    role: str


def _credential(
    context: AdminContext, role: str, *, subject: str | None = None
) -> Credential:
    issued = context.auth.sessions.create(
        subject=subject or f"subject-{role.lower()}",
        display_name=f"{role} User",
        email=None,
        access_role=role,
    )
    return Credential(
        token=issued.token,
        csrf=issued.csrf_token,
        subject=issued.record.subject,
        role=role,
    )


def _headers(credential: Credential, *, csrf: bool = False) -> dict[str, str]:
    headers = {"Cookie": f"mdgw_session={credential.token}"}
    if csrf:
        headers["X-CSRF-Token"] = credential.csrf
    return headers


def _write_headers(client: Client, credential: Credential) -> dict[str, str]:
    return {
        **_headers(credential, csrf=True),
        "Origin": client.origin,
        "Content-Type": "application/json",
    }


def _migrate_admin_db(tmp_path: Path) -> None:
    """Create/migrate the management DB for direct-service tests."""
    database = AdminStore(tmp_path / "admin" / "admin.db")
    database.close()


def _config(
    tmp_path: Path,
    *,
    export: AdminExportConfig | None = None,
    export_executor: bool = True,
) -> AdminConfig:
    return AdminConfig(
        enabled=True,
        db_path=tmp_path / "admin" / "admin.db",
        adapters=AdminAdaptersConfig(
            oidc=False, audit_store=True, export_executor=export_executor
        ),
        export=export
        or AdminExportConfig(root=tmp_path / "data" / "exports"),
    )


@contextmanager
def _running(
    tmp_path: Path,
    *,
    config: AdminConfig | None = None,
    clock: Clock | None = None,
) -> Generator[tuple[AsgiServer, Client, AdminContext, ParquetBarStore]]:
    data_root = tmp_path / "data"
    store = _build_dataset(data_root)
    context = AdminContext(
        config=config or _config(tmp_path),
        storage_root=data_root,
        store=store,
        clock=clock,
    )
    app = create_app(store, admin=context)
    server = AsgiServer()
    try:
        server.start(app, host=HOST, port=0, thread_name=THREAD_NAME)
        yield server, Client(server), context, store
    finally:
        server.stop()
        store.close()


def _json_body(**overrides: Any) -> bytes:
    body = {
        "instrument_id": INSTRUMENT,
        "interval": "1m",
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-02-15T00:00:00Z",
        "format": "csv",
    }
    body.update(overrides)
    return json.dumps(body).encode("utf-8")


def _create(
    client: Client, credential: Credential, **overrides: Any
) -> Result:
    headers = _write_headers(client, credential)
    return client.post(JOBS, headers=headers, body=_json_body(**overrides))


def _wait_status(
    client: Client,
    credential: Credential,
    job_id: str,
    statuses: set[str],
    *,
    timeout: float = 30.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        response = client.get(
            f"{JOBS}/{job_id}", headers=_headers(credential)
        )
        assert response.status == 200, response.body
        last = response.json()
        if last["status"] in statuses:
            return last
        time.sleep(0.02)
    msg = f"job {job_id} did not reach {statuses}: {last}"
    raise AssertionError(msg)


# -- create / range policy --------------------------------------------------


def test_create_returns_202_queued_job_and_migrates_to_schema_8(
    tmp_path: Path,
) -> None:
    with _running(tmp_path) as (_server, client, context, _store):
        credential = _credential(context, "Operator")
        response = _create(client, credential)

        assert context.db is not None
        assert context.db.schema_version == 8

    assert response.status == 202, response.body
    job = response.json()
    assert job["job_id"].startswith("exp_")
    assert job["status"] == QUEUED
    assert job["instrument_id"] == INSTRUMENT
    assert job["interval"] == "1m"
    assert job["start"] == "2026-01-01T00:00:00.000000000Z"
    assert job["end"] == "2026-02-15T00:00:00.000000000Z"
    assert job["range_days"] == 45
    assert job["format"] == "csv"
    assert job["created_by"] == {
        "subject": credential.subject,
        "display_name": "Operator User",
    }
    assert job["progress"] == {
        "processed_rows": 0,
        "processed_bytes": 0,
        "total_rows": None,
        "percent": None,
        "indeterminate": True,
    }
    assert job["result"] is None
    assert job["error"] is None
    assert job["cancelled_at"] is None
    assert job["expires_at"] is None
    assert job["download_url"] is None
    assert NS_TIMESTAMP.fullmatch(job["created_at"])
    assert set(job) == {
        "job_id",
        "status",
        "instrument_id",
        "interval",
        "start",
        "end",
        "range_days",
        "format",
        "created_at",
        "created_by",
        "progress",
        "result",
        "error",
        "cancelled_at",
        "expires_at",
        "download_url",
    }


def test_range_policy_uses_invalid_parameter_not_quota(tmp_path: Path) -> None:
    with _running(tmp_path) as (_server, client, context, _store):
        credential = _credential(context, "Operator")

        short = _create(
            client,
            credential,
            start="2026-01-01T00:00:00Z",
            end="2026-01-30T00:00:00Z",
        )
        long = _create(
            client,
            credential,
            start="2026-01-01T00:00:00Z",
            end="2027-01-03T00:00:00Z",
        )
        inverted = _create(
            client,
            credential,
            start="2026-02-01T00:00:00Z",
            end="2026-01-01T00:00:00Z",
        )
        bad_format = _create(client, credential, format="parquet")
        bad_interval = _create(client, credential, interval="2m")
        bad_instrument = _create(client, credential, instrument_id="BTCUSDT")
        bad_time = _create(client, credential, start="not-a-time")
        missing = client.post(
            JOBS,
            headers=_write_headers(client, credential),
            body=b"{}",
        )

    assert short.status == 400
    assert short.json()["error"]["details"]["reason"] == "use_direct_download"
    assert long.status == 400
    assert long.json()["error"]["details"]["reason"] == "range_too_large"
    for response, field in (
        (inverted, "end"),
        (bad_format, "format"),
        (bad_interval, "interval"),
        (bad_instrument, "instrument_id"),
        (bad_time, "start"),
        (missing, "instrument_id"),
    ):
        assert response.status == 400, response.body
        envelope = response.json()["error"]
        assert envelope["code"] == "invalid_parameter"
        assert envelope["details"]["field"] == field


# -- success / CSV bytes / download ----------------------------------------


def test_successful_export_preserves_all_revisions_and_exact_csv(
    tmp_path: Path,
) -> None:
    clock = Clock()
    with _running(tmp_path, clock=clock) as (_server, client, context, store):
        credential = _credential(context, "Operator")
        before = _dataset_snapshot(store.dataset_root)
        created = _create(client, credential).json()
        job = _wait_status(client, credential, created["job_id"], {SUCCEEDED})
        assert _dataset_snapshot(store.dataset_root) == before

        download = client.get(
            f"{JOBS}/{created['job_id']}/download",
            headers=_headers(credential),
        )
        by_id = client.get(
            f"{JOBS}/{created['job_id']}", headers=_headers(credential)
        ).json()

    assert job["status"] == SUCCEEDED
    assert job["progress"] == {
        "processed_rows": 3,
        "processed_bytes": job["result"]["size_bytes"],
        "total_rows": None,
        "percent": None,
        "indeterminate": True,
    }
    result = job["result"]
    assert result["content_type"] == "text/csv; charset=utf-8"
    assert result["row_count"] == 3
    assert len(result["sha256"]) == 64
    assert result["sha256"] == result["sha256"].lower()
    assert job["expires_at"] == _ns(CLOCK_BASE_NS + 24 * 3600 * SECOND)
    assert job["download_url"] == f"{JOBS}/{created['job_id']}/download"

    assert download.status == 200, download.body
    assert download.header("Content-Type") == "text/csv; charset=utf-8"
    assert download.header("Content-Length") == str(result["size_bytes"])
    assert download.header("Accept-Ranges") == "bytes"
    assert download.header("ETag") == f'"{result["sha256"]}"'
    disposition = download.header("Content-Disposition") or ""
    assert disposition.startswith("attachment;")
    assert 'filename="' in disposition
    assert "filename*=UTF-8''" in disposition
    assert "/" not in disposition
    assert "\\" not in disposition

    # Byte-for-byte: exact schema order, timestamps, decimals, all revisions.
    text = download.body.decode("utf-8")
    assert text.splitlines()[0] == ",".join(BAR_SCHEMA.names)
    assert Counter(text.splitlines()) == Counter(EXPECTED_CSV.splitlines())
    assert len(download.body) == result["size_bytes"]
    assert hashlib.sha256(download.body).hexdigest() == result["sha256"]

    # The same file is used by the metadata endpoint, and no server path is
    # exposed on the wire.
    assert by_id["status"] == SUCCEEDED
    assert str(store.dataset_root) not in download.body.decode("utf-8")
    assert str(store.dataset_root) not in json.dumps(job)


def _ns(value: int) -> str:
    return ns_to_iso(value)


def test_half_open_range_includes_start_and_excludes_end(
    tmp_path: Path,
) -> None:
    # Direct service check with a one-minute minimum makes the Store's
    # half-open contract observable without changing the public 31-day policy.
    data_root = tmp_path / "data"
    store = _build_dataset(data_root)
    _migrate_admin_db(tmp_path)
    config = _config(
        tmp_path,
        export=AdminExportConfig(
            root=data_root / "exports", min_days=1, max_days=366
        ),
    )
    service = ExportService(
        config=config,
        storage_root=data_root,
        db_path=tmp_path / "admin" / "admin.db",
        store=store,
    )
    service.start()
    try:
        windows = (
            # end is exclusive: the 00:00 Bar is outside this window.
            ("2025-12-31T00:00:00Z", "2026-01-01T00:00:00Z", 0),
            # start is inclusive: both revisions at 00:00 and the 00:01 row.
            ("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", 3),
            # only the 00:01 row; the 00:00 revisions are before start.
            ("2026-01-01T00:01:00Z", "2026-01-02T00:01:00Z", 1),
        )
        for start, end, expected_rows in windows:
            request = service.parse_request(
                {
                    "instrument_id": INSTRUMENT,
                    "interval": "1m",
                    "start": start,
                    "end": end,
                    "format": "csv",
                }
            )
            job_id = service.new_job_id()
            service.create(
                request, job_id=job_id, subject="op", display_name="Op"
            )
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                job = service.get(job_id)
                if job.status == SUCCEEDED:
                    break
                time.sleep(0.02)
            assert job.status == SUCCEEDED
            assert job.result["row_count"] == expected_rows
    finally:
        service.close()
        store.close()


def test_atomic_publish_keeps_temp_files_outside_the_dataset(
    tmp_path: Path,
) -> None:
    opened = threading.Event()
    release = threading.Event()

    def blocking_opener(**kwargs: Any) -> Any:
        read = store.open_read(**kwargs)
        opened.set()
        release.wait(20)
        return read

    with _running(tmp_path) as (_server, client, context, store):
        service = context.exports
        service.set_read_opener(blocking_opener)
        credential = _credential(context, "Operator")
        before = _dataset_snapshot(store.dataset_root)
        export_root = tmp_path / "data" / "exports"
        job_id = _create(client, credential).json()["job_id"]
        try:
            assert opened.wait(10)
            # Nothing is published yet; the in-progress bytes are a temp file
            # in the Export root, never inside the Dataset.
            assert list(export_root.glob("*.csv")) == []
            temps = list(export_root.glob(".*.csv.tmp"))
            assert len(temps) == 1
            assert store.dataset_root not in temps[0].parents
            assert _dataset_snapshot(store.dataset_root) == before
        finally:
            release.set()
        job = _wait_status(client, credential, job_id, {SUCCEEDED})
        files = list(export_root.glob("*.csv"))
        temps = list(export_root.glob(".*.csv.tmp"))
        assert _dataset_snapshot(store.dataset_root) == before

    assert len(files) == 1
    assert files[0].stat().st_size == job["result"]["size_bytes"]
    assert temps == []


def test_download_before_completion_is_job_not_succeeded(
    tmp_path: Path,
) -> None:
    with _running(tmp_path) as (_server, client, context, _store):
        credential = _credential(context, "Operator")
        queued_id = "exp_waiting"
        context.exports.export_store.insert(
            _record(queued_id, credential.subject),
            max_running_per_user=5,
            global_concurrency=5,
            storage_quota_bytes=0,
            now_ns=CLOCK_BASE_NS,
        )
        response = client.get(
            f"{JOBS}/{queued_id}/download", headers=_headers(credential)
        )
        anonymous = client.get(f"{JOBS}/{queued_id}/download")

    assert response.status == 409
    envelope = response.json()["error"]
    assert envelope["code"] == "conflict"
    assert envelope["details"] == {
        "reason": "job_not_succeeded",
        "status": QUEUED,
    }
    assert anonymous.status == 401


def test_download_supports_single_http_ranges(tmp_path: Path) -> None:
    with _running(tmp_path) as (_server, client, context, _store):
        credential = _credential(context, "Operator")
        created = _create(client, credential).json()
        job = _wait_status(client, credential, created["job_id"], {SUCCEEDED})
        size = job["result"]["size_bytes"]
        path = f"{JOBS}/{created['job_id']}/download"
        body = client.get(path, headers=_headers(credential)).body

        first = client.get(
            path,
            headers={**_headers(credential), "Range": "bytes=0-9"},
        )
        tail = client.get(
            path,
            headers={**_headers(credential), "Range": "bytes=-5"},
        )
        open_ended = client.get(
            path,
            headers={**_headers(credential), "Range": "bytes=5-"},
        )
        bad = [
            client.get(
                path,
                headers={**_headers(credential), "Range": value},
            )
            for value in (
                f"bytes={size}-",
                "bytes=abc",
                "bytes=9-2",
                "bytes=0-1,3-4",
                "items=0-1",
                "bytes=-0",
            )
        ]

    assert first.status == 206
    assert first.body == body[:10]
    assert first.header("Content-Range") == f"bytes 0-9/{size}"
    assert first.header("Content-Length") == "10"
    assert first.header("Accept-Ranges") == "bytes"

    assert tail.status == 206
    assert tail.body == body[-5:]
    assert tail.header("Content-Range") == f"bytes {size - 5}-{size - 1}/{size}"

    assert open_ended.status == 206
    assert open_ended.body == body[5:]
    assert (
        open_ended.header("Content-Range")
        == f"bytes 5-{size - 1}/{size}"
    )

    for response in bad:
        assert response.status == 416, response.body
        envelope = response.json()["error"]
        assert envelope["code"] == "invalid_parameter"
        assert envelope["details"]["reason"] == "range_not_satisfiable"
        assert envelope["request_id"] == response.header("X-Request-ID")


# -- ownership / roles ------------------------------------------------------


def test_ownership_scopes_list_get_cancel_and_download(  # ruff: ignore[too-many-locals] - explicit denials
    tmp_path: Path,
) -> None:
    with _running(tmp_path) as (_server, client, context, _store):
        owner = _credential(context, "Operator", subject="operator-a")
        other = _credential(context, "Auditor", subject="auditor-b")
        admin = _credential(context, "SuperAdmin", subject="super-1")
        created = _create(client, owner).json()
        job_id = created["job_id"]
        _wait_status(client, owner, job_id, {SUCCEEDED})
        queued_id = "exp_ownerqueued"
        context.exports.export_store.insert(
            _record(queued_id, owner.subject),
            max_running_per_user=5,
            global_concurrency=5,
            storage_quota_bytes=0,
            now_ns=CLOCK_BASE_NS,
        )

        other_list = client.get(JOBS, headers=_headers(other)).json()
        admin_list = client.get(JOBS, headers=_headers(admin)).json()
        other_detail = client.get(
            f"{JOBS}/{job_id}", headers=_headers(other)
        )
        other_cancel = client.post(
            f"{JOBS}/{job_id}/cancel",
            headers=_write_headers(client, other),
        )
        other_download = client.get(
            f"{JOBS}/{job_id}/download", headers=_headers(other)
        )
        other_cancel_queued = client.post(
            f"{JOBS}/{queued_id}/cancel",
            headers=_write_headers(client, other),
        )
        admin_cancel_queued = client.post(
            f"{JOBS}/{queued_id}/cancel",
            headers=_write_headers(client, admin),
        )
        admin_detail = client.get(
            f"{JOBS}/{job_id}", headers=_headers(admin)
        )
        admin_download = client.get(
            f"{JOBS}/{job_id}/download", headers=_headers(admin)
        )
        missing = client.get(
            f"{JOBS}/exp_unknown", headers=_headers(owner)
        )

    assert other_list["items"] == []
    assert {item["job_id"] for item in admin_list["items"]} == {
        job_id,
        queued_id,
    }
    for denied in (
        other_detail,
        other_cancel,
        other_download,
        other_cancel_queued,
    ):
        assert denied.status == 403, denied.body
        assert denied.json()["error"]["details"]["reason"] == "ownership"
    assert admin_detail.status == 200
    assert admin_download.status == 200
    assert admin_cancel_queued.status == 202
    assert admin_cancel_queued.json()["status"] == CANCELLING
    assert missing.status == 404
    assert missing.json()["error"]["code"] == "job_not_found"
    assert missing.json()["error"]["details"] == {"job_id": "exp_unknown"}


def test_every_role_can_create_but_anonymous_and_unsafe_checks_fail(
    tmp_path: Path,
) -> None:
    config = _config(
        tmp_path,
        export=AdminExportConfig(
            root=tmp_path / "data" / "exports",
            max_running_per_user=10,
            global_concurrency=10,
        ),
    )
    with _running(tmp_path, config=config) as (_server, client, context, _store):
        created = {}
        for role in ROLES:
            credential = _credential(context, role)
            response = _create(client, credential)
            assert response.status == 202, response.body
            created[role] = response.json()["job_id"]

        anonymous = client.get(JOBS)
        no_csrf = client.post(
            JOBS,
            headers={"Origin": client.origin},
            body=_json_body(),
        )
        bad_origin = client.post(
            JOBS,
            headers={
                **_headers(_credential(context, "Operator"), csrf=True),
                "Origin": "http://evil.example",
            },
            body=_json_body(),
        )
        anonymous_cancel = client.post(f"{JOBS}/{created['Operator']}/cancel")

    assert anonymous.status == 401
    assert anonymous.json()["error"]["code"] == "unauthenticated"
    assert no_csrf.status == 401  # no session at all
    assert bad_origin.status == 403
    assert bad_origin.json()["error"]["details"]["reason"] == "origin"
    assert anonymous_cancel.status == 401
    assert len(created) == 3


def test_csrf_is_required_for_create_and_cancel(tmp_path: Path) -> None:
    with _running(tmp_path) as (_server, client, context, _store):
        credential = _credential(context, "Operator")
        missing = client.post(
            JOBS,
            headers={**_headers(credential), "Origin": client.origin},
            body=_json_body(),
        )
        job_id = _create(client, credential).json()["job_id"]
        cancel_missing = client.post(
            f"{JOBS}/{job_id}/cancel",
            headers={**_headers(credential), "Origin": client.origin},
        )

    for response in (missing, cancel_missing):
        assert response.status == 403, response.body
        assert response.json()["error"]["details"]["reason"] == "csrf"


# -- quotas -----------------------------------------------------------------


def test_per_user_running_quota_is_enforced(tmp_path: Path) -> None:
    opened = threading.Event()
    release = threading.Event()

    def blocking_opener(**kwargs: Any) -> Any:
        read = store.open_read(**kwargs)
        opened.set()
        release.wait(20)
        return read

    with _running(tmp_path) as (_server, client, context, store):
        service = context.exports
        service.set_read_opener(blocking_opener)
        credential = _credential(context, "Operator")
        try:
            first = _create(client, credential)
            assert first.status == 202
            assert opened.wait(10)
            second = _create(client, credential)
        finally:
            release.set()
            _wait_status(client, credential, first.json()["job_id"], {SUCCEEDED})

    assert second.status == 429, second.body
    envelope = second.json()["error"]
    assert envelope["code"] == "quota_exceeded"
    assert envelope["details"] == {
        "quota": "running_jobs",
        "limit": 1,
        "current": 1,
    }


def test_global_concurrency_quota_is_enforced(tmp_path: Path) -> None:
    opened = threading.Event()
    release = threading.Event()

    def blocking_opener(**kwargs: Any) -> Any:
        read = store.open_read(**kwargs)
        opened.set()
        release.wait(20)
        return read

    config = _config(
        tmp_path,
        export=AdminExportConfig(
            root=tmp_path / "data" / "exports",
            global_concurrency=1,
            max_running_per_user=2,
        ),
    )
    with _running(tmp_path, config=config) as (_server, client, context, store):
        service = context.exports
        service.set_read_opener(blocking_opener)
        first_user = _credential(context, "Operator", subject="op-1")
        second_user = _credential(context, "Auditor", subject="aud-1")
        try:
            assert _create(client, first_user).status == 202
            assert opened.wait(10)
            blocked = _create(client, second_user)
        finally:
            release.set()

    assert blocked.status == 429, blocked.body
    assert blocked.json()["error"]["details"] == {
        "quota": "global_concurrency",
        "limit": 1,
        "current": 1,
    }


def test_global_storage_quota_is_enforced_after_a_completed_job(
    tmp_path: Path,
) -> None:
    config = _config(
        tmp_path,
        export=AdminExportConfig(
            root=tmp_path / "data" / "exports",
            storage_quota_bytes=10,
        ),
    )
    with _running(tmp_path, config=config) as (_server, client, context, _store):
        first_user = _credential(context, "Operator", subject="op-1")
        second_user = _credential(context, "Auditor", subject="aud-1")
        created = _create(client, first_user).json()
        _wait_status(client, first_user, created["job_id"], {SUCCEEDED})
        blocked = _create(client, second_user)

    assert blocked.status == 429, blocked.body
    details = blocked.json()["error"]["details"]
    assert details["quota"] == "global_storage"
    assert details["limit"] == 10
    assert details["current"] > 10


# -- idempotency ------------------------------------------------------------


def test_idempotency_key_replays_the_original_job(tmp_path: Path) -> None:
    with _running(tmp_path) as (_server, client, context, _store):
        credential = _credential(context, "Operator")
        headers = {
            **_write_headers(client, credential),
            "Idempotency-Key": "create-1",
        }
        first = client.post(JOBS, headers=headers, body=_json_body())
        second = client.post(JOBS, headers=headers, body=_json_body())
        different = client.post(
            JOBS,
            headers=headers,
            body=_json_body(interval="5m"),
        )
        listing = client.get(JOBS, headers=_headers(credential)).json()
        _wait_status(
            client, credential, first.json()["job_id"], {SUCCEEDED}
        )

    assert first.status == 202
    assert second.status == 202
    assert second.header("Idempotent-Replay") == "true"
    assert second.json()["job_id"] == first.json()["job_id"]
    assert different.status == 409
    assert (
        different.json()["error"]["details"]["reason"]
        == "idempotency_key_reuse"
    )
    assert len(listing["items"]) == 1


# -- cancellation -----------------------------------------------------------


def test_cancel_queued_job_transitions_to_cancelling_then_cancelled(
    tmp_path: Path,
) -> None:
    opened = threading.Event()
    release = threading.Event()

    def blocking_opener(**kwargs: Any) -> Any:
        read = store.open_read(**kwargs)
        opened.set()
        release.wait(20)
        return read

    with _running(tmp_path) as (_server, client, context, store):
        service = context.exports
        service.set_read_opener(blocking_opener)
        credential = _credential(context, "Operator")
        job_id = _create(client, credential).json()["job_id"]
        try:
            assert opened.wait(10)
            cancel = client.post(
                f"{JOBS}/{job_id}/cancel",
                headers=_write_headers(client, credential),
            )
            assert cancel.status == 202
            assert cancel.json()["status"] == CANCELLING
        finally:
            release.set()
        job = _wait_status(client, credential, job_id, {CANCELLED})
        cancel_again = client.post(
            f"{JOBS}/{job_id}/cancel",
            headers=_write_headers(client, credential),
        )
        files = list((tmp_path / "data" / "exports").glob("*.csv"))

    assert job["status"] == CANCELLED
    assert job["cancelled_at"] is not None
    assert job["result"] is None
    assert cancel_again.status == 409
    assert (
        cancel_again.json()["error"]["details"]["reason"]
        == "job_not_cancellable"
    )
    assert files == []


def test_cancelling_a_finished_job_is_a_conflict(tmp_path: Path) -> None:
    with _running(tmp_path) as (_server, client, context, _store):
        credential = _credential(context, "Operator")
        job_id = _create(client, credential).json()["job_id"]
        _wait_status(client, credential, job_id, {SUCCEEDED})
        response = client.post(
            f"{JOBS}/{job_id}/cancel",
            headers=_write_headers(client, credential),
        )

    assert response.status == 409
    assert response.json()["error"]["code"] == "conflict"
    assert (
        response.json()["error"]["details"]["reason"] == "job_not_cancellable"
    )


# -- expiry -----------------------------------------------------------------


def test_expiry_returns_410_removes_only_the_export_file(
    tmp_path: Path,
) -> None:
    clock = Clock()
    with _running(tmp_path, clock=clock) as (_server, client, context, store):
        credential = _credential(context, "Operator")
        job_id = _create(client, credential).json()["job_id"]
        job = _wait_status(client, credential, job_id, {SUCCEEDED})
        export_root = tmp_path / "data" / "exports"
        result_file = export_root / job["result"]["file_name"]
        assert result_file.is_file()

        bars_before = _dataset_snapshot(store.dataset_root)
        quarantine = tmp_path / "quarantine"
        quarantine.mkdir()
        sentinel = quarantine / "keep.parquet"
        sentinel.write_bytes(b"quarantine-data")

        clock.advance(24 * 3600 * SECOND)
        # The fake clock also drives session idle/absolute timeout; sign in
        # again at the new time so this observes expiry, not 401.
        credential = _credential(
            context, "Operator", subject=credential.subject
        )
        expired = client.get(
            f"{JOBS}/{job_id}/download", headers=_headers(credential)
        )
        after = client.get(
            f"{JOBS}/{job_id}", headers=_headers(credential)
        ).json()

        assert not result_file.exists()
        assert sentinel.read_bytes() == b"quarantine-data"
        assert _dataset_snapshot(store.dataset_root) == bars_before

    assert expired.status == 410
    envelope = expired.json()["error"]
    assert envelope["code"] == "export_expired"
    assert envelope["details"]["job_id"] == job_id
    assert envelope["details"]["expires_at"] == job["expires_at"]
    assert after["status"] == EXPIRED
    assert after["download_url"] is None


def _dataset_snapshot(dataset_root: Path) -> dict[str, bytes]:
    snapshot: dict[str, bytes] = {}
    for path in sorted(dataset_root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(dataset_root).as_posix()
        if relative.startswith(("_sessions/", "duckdb_")):
            continue
        snapshot[relative] = path.read_bytes()
    return snapshot


# -- audit + fail-closed ----------------------------------------------------


def test_create_and_cancel_append_requested_and_terminal_events(
    tmp_path: Path,
) -> None:
    clock = Clock()
    with _running(tmp_path, clock=clock) as (_server, client, context, _store):
        credential = _credential(context, "Operator")
        job_id = _create(client, credential).json()["job_id"]
        _wait_status(client, credential, job_id, {SUCCEEDED})
        # Seed one queued job (not submitted to the worker) so cancel is
        # deterministic and both requested/terminal events are appended.
        queued_id = "exp_auditqueued"
        context.exports.export_store.insert(
            _record(queued_id, credential.subject),
            max_running_per_user=5,
            global_concurrency=5,
            storage_quota_bytes=0,
            now_ns=CLOCK_BASE_NS,
        )
        cancelled = client.post(
            f"{JOBS}/{queued_id}/cancel",
            headers=_write_headers(client, credential),
        )
        assert cancelled.status == 202

        create_events = client.get(
            "/api/v1/admin/audit-events"
            f"?{urlencode({'job_id': job_id, 'limit': '50'})}",
            headers=_headers(credential),
        ).json()["items"]
        cancel_events = client.get(
            "/api/v1/admin/audit-events"
            f"?{urlencode({'job_id': queued_id, 'limit': '50'})}",
            headers=_headers(credential),
        ).json()["items"]

    creates = sorted(
        (
            event
            for event in create_events
            if event["action"] == "export_job.create"
        ),
        key=operator.itemgetter("sequence"),
    )
    cancels = sorted(
        (
            event
            for event in cancel_events
            if event["action"] == "export_job.cancel"
        ),
        key=operator.itemgetter("sequence"),
    )
    assert [event["result"] for event in creates] == ["requested", "succeeded"]
    assert [event["result"] for event in cancels] == ["requested", "succeeded"]
    assert {event["target"]["type"] for event in create_events} == {
        "export_job"
    }
    assert {event["target"]["id"] for event in create_events} == {job_id}
    assert {event["target"]["id"] for event in cancel_events} == {queued_id}


def test_audit_unavailable_blocks_create_and_cancel_with_zero_execution(
    tmp_path: Path,
) -> None:
    with _running(tmp_path) as (_server, client, context, _store):
        credential = _credential(context, "Auditor")
        queued_id = "exp_blocked"
        context.exports.export_store.insert(
            _record(queued_id, credential.subject),
            max_running_per_user=5,
            global_concurrency=5,
            storage_quota_bytes=0,
            now_ns=CLOCK_BASE_NS,
        )
        before = context.exports.get(queued_id)
        context.audit.close()

        create = _create(client, credential)
        cancel = client.post(
            f"{JOBS}/{queued_id}/cancel",
            headers=_write_headers(client, credential),
        )
        after = context.exports.get(queued_id)
        listing = client.get(JOBS, headers=_headers(credential)).json()

    for response in (create, cancel):
        assert response.status == 503, response.body
        envelope = response.json()["error"]
        assert envelope["code"] == "audit_unavailable"
        assert envelope["details"]["executed"] is False
    assert before is not None
    assert after is not None
    assert after.status == QUEUED
    assert after.cancel_requested is False
    assert len(listing["items"]) == 1  # only the seeded job


def _record(job_id: str, subject: str) -> ExportJobRecord:
    return ExportJobRecord(
        job_id=job_id,
        status=QUEUED,
        instrument_id=INSTRUMENT,
        interval="1m",
        start_ns=CLOCK_BASE_NS,
        end_ns=CLOCK_BASE_NS + 32 * DAY_NS,
        range_days=32,
        format="csv",
        created_by_subject=subject,
        created_by_display_name="Auditor User",
        created_at_ns=CLOCK_BASE_NS,
    )


# -- capabilities / adapter gating -----------------------------------------


def test_capabilities_announce_export_job_details(tmp_path: Path) -> None:
    with _running(tmp_path) as (_server, client, context, _store):
        document = client.get("/api/v1/admin/capabilities").json()
        schema_version = context.db.schema_version

    adapters = {
        entry["name"]: entry["state"] for entry in document["adapters"]
    }
    assert adapters["export_executor"] == "available"
    assert document["capabilities"]["export_job"] == {
        "state": "enabled",
        "formats": ["csv"],
        "min_days": 31,
        "max_days": 366,
        "retention_s": 86400,
        "max_running_per_user": 1,
    }
    assert schema_version == 8


def test_export_routes_fail_closed_when_the_adapter_is_disabled(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, export_executor=False)
    with _running(tmp_path, config=config) as (_server, client, context, _store):
        document = client.get("/api/v1/admin/capabilities").json()
        credential = _credential(context, "Operator")
        response = _create(client, credential)

    adapters = {
        entry["name"]: entry["state"] for entry in document["adapters"]
    }
    assert adapters["export_executor"] == "unsupported"
    assert document["capabilities"]["export_job"] == {"state": "unsupported"}
    assert response.status == 503
    assert response.json()["error"]["code"] == "control_unavailable"
    assert response.json()["error"]["details"]["source"] == "export_executor"


# -- executor seam: lease, restart, stop order ------------------------------


def test_executor_holds_a_dataset_read_lease_while_running(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    store = _build_dataset(data_root)
    _migrate_admin_db(tmp_path)
    opened = threading.Event()
    release = threading.Event()

    def blocking_opener(**kwargs: Any) -> Any:
        read = store.open_read(**kwargs)
        opened.set()
        release.wait(20)
        return read

    service = ExportService(
        config=_config(tmp_path),
        storage_root=data_root,
        db_path=tmp_path / "admin" / "admin.db",
        store=store,
        read_opener=blocking_opener,
    )
    service.start()
    try:
        request = service.parse_request(
            {
                "instrument_id": INSTRUMENT,
                "interval": "1m",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-02-15T00:00:00Z",
                "format": "csv",
            }
        )
        job_id = service.new_job_id()
        service.create(
            request,
            job_id=job_id,
            subject="operator-a",
            display_name="Operator A",
        )
        assert opened.wait(15)
        locks = active_session_locks(store.dataset_root)
        assert [lock.session_id for lock in locks] == [job_id]
        assert service.get(job_id).status == RUNNING
        release.set()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if service.get(job_id).status == SUCCEEDED:
                break
            time.sleep(0.02)
        assert service.get(job_id).status == SUCCEEDED
    finally:
        release.set()
        service.close()
        store.close()

    assert active_session_locks(store.dataset_root) == []


def test_startup_cleanup_removes_only_orphan_export_files(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    store = _build_dataset(data_root)
    _migrate_admin_db(tmp_path)
    export_root = data_root / "exports"
    export_root.mkdir(parents=True)
    orphan = export_root / (
        "binance_BTC_USDT-1m-exp_0123456789abcdef0123456789abcdef.csv"
    )
    orphan.write_bytes(b"orphan")
    unrelated = export_root / "keep.csv"
    unrelated.write_bytes(b"keep")
    temp = export_root / ".exp_deadbeef-deadbeef.csv.tmp"
    temp.write_bytes(b"temp")

    service = ExportService(
        config=_config(tmp_path),
        storage_root=data_root,
        db_path=tmp_path / "admin" / "admin.db",
        store=store,
    )
    try:
        service.start()
        assert not orphan.exists()
        assert not temp.exists()
        assert unrelated.read_bytes() == b"keep"
    finally:
        service.close()
        store.close()


def test_restart_marks_unconfirmed_running_jobs_interrupted(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    store = _build_dataset(data_root)
    _migrate_admin_db(tmp_path)
    config = _config(
        tmp_path,
        export=AdminExportConfig(
            root=data_root / "exports", max_running_per_user=2
        ),
    )
    service = ExportService(
        config=config,
        storage_root=data_root,
        db_path=tmp_path / "admin" / "admin.db",
        store=store,
    )
    try:
        request = service.parse_request(
            {
                "instrument_id": INSTRUMENT,
                "interval": "1m",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-02-15T00:00:00Z",
                "format": "csv",
            }
        )
        job_id = service.new_job_id()
        service.create(
            request, job_id=job_id, subject="operator-a", display_name="A"
        )
        assert service.export_store.mark_running(job_id, CLOCK_BASE_NS)
        # A second job stays queued across the restart and must resume.
        queued_id = service.new_job_id()
        service.create(
            request, job_id=queued_id, subject="operator-a", display_name="A"
        )
        service.close()

        restarted = ExportService(
            config=config,
            storage_root=data_root,
            db_path=tmp_path / "admin" / "admin.db",
            store=store,
        )
        try:
            restarted.start()
            job = restarted.get(job_id)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                resumed = restarted.get(queued_id)
                if resumed.status == SUCCEEDED:
                    break
                time.sleep(0.02)
        finally:
            restarted.close()
    finally:
        service.close()
        store.close()

    assert job is not None
    assert job.status == FAILED
    assert job.error is not None
    assert job.error["code"] == "interrupted"
    assert job.result is None
    assert resumed is not None
    assert resumed.status == SUCCEEDED
    assert resumed.result["row_count"] == 3


def test_context_close_stops_the_executor_before_closing_the_store(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    store = _build_dataset(data_root)
    context = AdminContext(
        config=_config(tmp_path), storage_root=data_root, store=store
    )
    try:
        service = context.exports
        assert service.available
        worker_threads = [
            thread
            for thread in threading.enumerate()
            if thread.name.startswith("mdgw-export-")
        ]
        assert worker_threads
        context.close()

        assert service.closed
        with pytest.raises(sqlite3.ProgrammingError):
            service.export_store.queued_job_ids()
        # The Bars Store is still open here: the admin cleanup ran before the
        # service that owns the Store would close it (ApiService.stop order).
        assert store.count_bars(instrument_id=INSTRUMENT, interval="1m") == 3
    finally:
        store.close()

    remaining = [
        thread
        for thread in threading.enumerate()
        if thread.name.startswith("mdgw-export-")
    ]
    assert remaining == []


# -- listing / pagination ---------------------------------------------------


def test_list_is_owner_scoped_newest_first_and_keyset_paginated(
    tmp_path: Path,
) -> None:
    clock = Clock()
    config = _config(
        tmp_path,
        export=AdminExportConfig(
            root=tmp_path / "data" / "exports",
            max_running_per_user=10,
            global_concurrency=10,
        ),
    )
    with _running(tmp_path, config=config, clock=clock) as (
        _server,
        client,
        context,
        _store,
    ):
        first_user = _credential(context, "Operator", subject="op-1")
        second_user = _credential(context, "Auditor", subject="aud-1")
        superadmin = _credential(context, "SuperAdmin", subject="super-1")
        job_ids = []
        for _ in range(3):
            clock.advance(SECOND)
            job_ids.append(_create(client, first_user).json()["job_id"])
        clock.advance(SECOND)
        other_id = _create(client, second_user).json()["job_id"]

        own = client.get(JOBS, headers=_headers(first_user)).json()
        own_page = client.get(
            f"{JOBS}?{urlencode({'limit': '2'})}",
            headers=_headers(first_user),
        ).json()
        second_page = client.get(
            f"{JOBS}?"
            f"{urlencode({'limit': '2', 'cursor': own_page['page']['next_cursor']})}",
            headers=_headers(first_user),
        ).json()
        all_jobs = client.get(JOBS, headers=_headers(superadmin)).json()
        bad_cursor = client.get(
            f"{JOBS}?cursor=not-base64!!", headers=_headers(first_user)
        )

    assert [item["job_id"] for item in own["items"]] == list(
        reversed(job_ids)
    )
    assert [item["job_id"] for item in own_page["items"]] == list(
        reversed(job_ids)
    )[:2]
    assert own_page["page"]["has_more"] is True
    assert [item["job_id"] for item in second_page["items"]] == [job_ids[0]]
    assert second_page["page"] == {"next_cursor": None, "has_more": False}
    assert {item["job_id"] for item in all_jobs["items"]} == {
        *job_ids,
        other_id,
    }
    assert bad_cursor.status == 400
    assert bad_cursor.json()["error"]["details"]["field"] == "cursor"
