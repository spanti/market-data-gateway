# ruff: file-ignore[implicit-namespace-package,hardcoded-password-string,too-many-arguments,too-many-statements,no-self-use,unused-method-argument,too-many-locals]
"""Backfill Job Admin API over a real FastAPI app and real Collector runner.

The API and the Collector are separate objects in these tests: the API owns a
read-only ``ParquetBarStore`` whose ``write_bars`` raises, while a real
``BackfillJobRunner`` (driven through a real loopback control plane) writes a
temporary Dataset. This pins the boundary that the API never writes Parquet or
the HWM.
"""

from __future__ import annotations

import http.client
import json
import socket
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from mdgw.admin.context import AdminContext
from mdgw.admin.jobs.models import (
    FAILED,
    QUEUED,
    BackfillJobRecord,
    empty_counters,
)
from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.backfill_job_plan import plan_for_interval
from mdgw.config import (
    AdminAdaptersConfig,
    AdminConfig,
    AdminControlConfig,
    load_config,
)
from mdgw.core.bar import RawBar
from mdgw.core.time import datetime_to_ns, ns_to_iso
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.reference import (
    InstrumentStore,
    export_instrument_snapshot,
    load_instrument_reference,
)
from mdgw.service.backfill_jobs import BackfillJobRunner
from mdgw.service.collector_core import CollectorCore
from mdgw.service.control_plane import (
    BACKFILL_JOB_CANCEL_ACTION,
    BACKFILL_JOB_START_ACTION,
    BACKFILL_JOB_STATUS_ACTION,
    ControlPlaneServer,
    collector_control_registry,
)
from mdgw.state.high_water_mark import HighWaterMarkStore
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-admin-backfill-api"
TOKEN_ENV = "MDGW_TEST_CONTROL_TOKEN"
TOKEN = "backfill-control-token"
BTC = "binance:BTC/USDT"
ETH = "binance:ETH/USDT"
JOBS = "/api/v1/admin/backfill-jobs"
SECOND = 1_000_000_000
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
TERMINAL = {
    "succeeded",
    "partially_succeeded",
    "failed",
    "cancelled",
}


def _at(minute: int = 0, hour: int = 0, day: int = 1) -> datetime:
    return datetime(2024, 1, day, hour, minute, tzinfo=UTC)


def _bar_input(minute: int, *, day: int = 1) -> BarInput:
    return BarInput(
        open_ts=_at(minute, day=day),
        open="100",
        high="101",
        low="99",
        close="100",
        volume="1",
        trade_count=1,
    )


def _history() -> list[BarInput]:
    return [_bar_input(minute) for minute in range(60)]


class Clock:
    def __init__(self) -> None:
        self.now_ns = 1_700_000_000 * SECOND

    def __call__(self) -> int:
        self.now_ns += SECOND
        return self.now_ns


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

    def post(self, path: str, body: Any = None, **kwargs: Any) -> Result:
        data = None if body is None else json.dumps(body).encode("utf-8")
        return self.request("POST", path, body=data, **kwargs)


class Credential:
    def __init__(self, context: AdminContext, role: str) -> None:
        issued = context.auth.sessions.create(
            subject=f"subject-{role.lower()}",
            display_name=f"{role} User",
            email=None,
            access_role=role,
        )
        self.token = issued.token
        self.csrf = issued.csrf_token
        self.subject = issued.record.subject
        self.role = role

    def headers(
        self,
        *,
        csrf: bool = True,
        origin: str | bool = True,
        client: Client,
        idempotency_key: str | None = "idem-default",
    ) -> dict[str, str]:
        headers = {
            "Cookie": f"mdgw_session={self.token}",
            "Content-Type": "application/json",
        }
        if csrf:
            headers["X-CSRF-Token"] = self.csrf
        if origin is True:
            headers["Origin"] = client.origin
        elif isinstance(origin, str):
            headers["Origin"] = origin
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        return headers


class ReadOnlyBarStore(ParquetBarStore):
    """The API-side Store; any write is a test failure."""

    def write_bars(self, *args: Any, **kwargs: Any) -> Any:
        msg = "the Admin API must never write the Bars Dataset"
        raise AssertionError(msg)


class BlockingFeed:
    """History source that blocks the chunk starting at ``block_from_ns``."""

    name = "blocking"
    instrument_ids = (BTC,)
    supports_history = True

    def __init__(
        self,
        history: Sequence[BarInput],
        *,
        block_from_ns: int | None = None,
    ) -> None:
        self._history = tuple(history)
        self.block_from_ns = block_from_ns
        self.release = threading.Event()
        self.calls = 0

    def iter_bars(self) -> Iterator[Any]:
        return iter(())

    def feed_status(self) -> tuple[Any, ...]:
        return ()

    def fetch_history(
        self,
        instrument_id: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> Iterable[Any]:
        start_ns = datetime_to_ns(start)
        end_ns = datetime_to_ns(end)
        self.calls += 1
        if self.block_from_ns is not None and start_ns >= self.block_from_ns:
            self.release.wait(timeout=20)
        return tuple(
            bar
            for bar in (self._raw(entry) for entry in self._history)
            if start_ns <= datetime_to_ns(bar.open_ts) < end_ns
        )

    def _raw(self, entry: BarInput) -> RawBar:
        return RawBar(
            instrument_id=BTC,
            interval="1m",
            open_ts=entry.open_ts,
            open=Decimal(entry.open),
            high=Decimal(entry.high),
            low=Decimal(entry.low),
            close=Decimal(entry.close),
            volume=Decimal(entry.volume),
            trade_count=entry.trade_count,
            revision=entry.revision,
        )


class RealCollector:
    """A real Collector job runner behind a real control-plane server."""

    def __init__(
        self,
        root: Path,
        *,
        feed: Any,
        port: int = 0,
    ) -> None:
        self.config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
        self.store = ParquetBarStore(root / "bars")
        self.hwm = HighWaterMarkStore(root / "bars" / "state.db")
        core = CollectorCore(
            feed_handler=feed,
            config=self.config,
            store=self.store,
            asset_class_for=lambda _instrument_id: "crypto",
        )
        core.hwm = self.hwm
        self.runner = BackfillJobRunner(core=core, config=self.config)
        self.runner.start()
        self.server = ControlPlaneServer(
            host=HOST,
            port=port,
            token=TOKEN,
            registry=collector_control_registry(),
            max_message_bytes=65_536,
            request_timeout_s=5.0,
        )
        self.server.register("ping", lambda _payload: {"pong": True})
        self.server.register(
            BACKFILL_JOB_START_ACTION, self.runner.handle_start
        )
        self.server.register(
            BACKFILL_JOB_STATUS_ACTION, self.runner.handle_status
        )
        self.server.register(
            BACKFILL_JOB_CANCEL_ACTION, self.runner.handle_cancel
        )
        self.server.start()

    @property
    def port(self) -> int:
        return self.server.port

    def stop(self) -> None:
        self.server.stop()
        self.runner.close()
        self.hwm.close()
        self.store.close()


class RejectingCollector:
    """A control plane that refuses every start without executing anything."""

    def __init__(self, *, port: int = 0) -> None:
        self.server = ControlPlaneServer(
            host=HOST,
            port=port,
            token=TOKEN,
            registry=collector_control_registry(),
            max_message_bytes=65_536,
            request_timeout_s=5.0,
        )
        self.server.register("ping", lambda _payload: {"pong": True})
        self.server.register(BACKFILL_JOB_START_ACTION, self._start)
        self.server.register(
            BACKFILL_JOB_STATUS_ACTION,
            lambda payload: {"job_id": payload.job_id, "state": "unknown"},
        )
        self.server.register(
            BACKFILL_JOB_CANCEL_ACTION,
            lambda payload: {"job_id": payload.job_id, "state": "unknown"},
        )
        self.server.start()

    @property
    def port(self) -> int:
        return self.server.port

    @staticmethod
    def _start(payload: Any) -> dict[str, Any]:
        return {
            "job_id": payload.job_id,
            "accepted": False,
            "reason": "unsupported_interval",
            "state": "unknown",
        }

    def stop(self) -> None:
        self.server.stop()


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((HOST, 0))
        return int(probe.getsockname()[1])


def _instrument_store(tmp_path: Path) -> InstrumentStore:
    entries = []
    for instrument_id in (BTC, ETH):
        venue, symbol = instrument_id.split(":", 1)
        entries.append(
            {
                "instrument_id": instrument_id,
                "venue": venue,
                "symbol": symbol,
                "asset_class": "crypto",
                "instrument_type": "spot",
                "base": symbol.split("/")[0],
                "quote": symbol.split("/")[1],
                "tick_size": "0.01",
                "lot_size": "0.0001",
                "multiplier": "1",
                "timezone": "UTC",
                "listing_ts": "2020-01-01T00:00:00Z",
            }
        )
    reference_dir = tmp_path / "reference"
    reference_dir.mkdir(parents=True, exist_ok=True)
    (reference_dir / "instruments.yaml").write_text(
        yaml.safe_dump(entries), encoding="utf-8"
    )
    reference = load_instrument_reference(reference_dir)
    export = export_instrument_snapshot(reference, tmp_path / "snapshots")
    return InstrumentStore(export.path)


@contextmanager
def _running_admin(
    tmp_path: Path,
    *,
    port: int,
    db_name: str = "admin.db",
):
    config = AdminConfig(
        enabled=True,
        db_path=tmp_path / "admin" / db_name,
        adapters=AdminAdaptersConfig(
            oidc=False, audit_store=True, collector_control=True
        ),
        control=AdminControlConfig(
            collector_host=HOST,
            collector_port=port,
            token_env=TOKEN_ENV,
            timeout_s=1.0,
        ),
    )
    store = ReadOnlyBarStore(tmp_path / "bars")
    instruments = _instrument_store(tmp_path)
    context = AdminContext(
        config=config,
        storage_root=tmp_path,
        store=store,
        instrument_store=instruments,
        environ={TOKEN_ENV: TOKEN},
        clock=Clock(),
    )
    app = create_app(store, instrument_store=instruments, admin=context)
    server = AsgiServer()
    try:
        server.start(app, host=HOST, port=0, thread_name=THREAD_NAME)
        yield server, Client(server), context
    finally:
        server.stop()
        context.close()
        store.close()


def _create(
    client: Client,
    credential: Credential,
    *,
    instrument_id: str = BTC,
    interval: str = "1m",
    start: str | None = None,
    end: str | None = None,
    reason: str = "recover exchange outage",
    idempotency_key: str | None = "idem-create",
    confirm: bool | None = True,
) -> Result:
    body: dict[str, Any] = {
        "instrument_id": instrument_id,
        "interval": interval,
        "start": start or _iso(_at(0)),
        "end": end or _iso(_at(0, hour=1)),
        "reason": reason,
    }
    if confirm is not None:
        body["confirm"] = confirm
    return client.post(
        JOBS,
        body,
        headers=credential.headers(
            client=client, idempotency_key=idempotency_key
        ),
    )


def _iso(value: datetime) -> str:
    return ns_to_iso(datetime_to_ns(value))


def _wait_job(
    client: Client,
    credential: Credential,
    job_id: str,
    predicate,
    *,
    timeout: float = 20.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        response = client.get(
            f"{JOBS}/{job_id}",
            headers=credential.headers(client=client, csrf=False),
        )
        assert response.status == 200, response.body
        last = response.json()
        if predicate(last):
            return last
        time.sleep(0.02)
    msg = f"condition not reached; last={last}"
    raise AssertionError(msg)


def _audit(client: Client, credential: Credential) -> list[dict[str, Any]]:
    response = client.get(
        "/api/v1/admin/audit-events?order=asc&limit=200",
        headers=credential.headers(client=client, csrf=False),
    )
    return response.json()["items"]


def _seed_record(
    job_id: str,
    *,
    status: str = QUEUED,
    instrument_id: str = BTC,
    interval: str = "1m",
    subject: str = "seed-subject",
) -> BackfillJobRecord:
    now = 1_700_000_000 * SECOND
    return BackfillJobRecord(
        job_id=job_id,
        status=status,
        instrument_id=instrument_id,
        interval=interval,
        start_ns=datetime_to_ns(_at(0)),
        end_ns=datetime_to_ns(_at(0, hour=1)),
        plan=plan_for_interval(interval),
        current_range=None,
        counters=empty_counters(),
        requested_by_subject=subject,
        requested_by_display_name="Seed User",
        requested_by_access_role="Operator",
        reason="seed",
        retry_of_job_id=None,
        created_at_ns=now,
        started_at_ns=None,
        finished_at_ns=None,
        error=None,
        resolved_gap_ids=(),
        dispatched_at_ns=None,
        updated_at_ns=now,
    )


# -- create / execute / read ------------------------------------------------


def test_create_dispatches_to_the_collector_and_persists_progress(
    tmp_path: Path,
) -> None:
    collector = RealCollector(tmp_path, feed=FakeFeedHandler(
        instrument_id=BTC, bars=(), history=_history()
    ))
    try:
        with _running_admin(tmp_path, port=collector.port) as (
            _server,
            client,
            context,
        ):
            operator = Credential(context, "Operator")
            created = _create(client, operator)

            assert created.status == 202, created.body
            body = created.json()
            job_id = body["job_id"]
            assert job_id.startswith("bf_")
            assert body["status"] == "queued"
            assert body["instrument_id"] == BTC
            assert body["interval"] == "1m"
            assert body["plan"] == {
                "mode": "native_interval",
                "source_intervals": ["1m"],
                "target_interval": "1m",
                "description": "补采 1m Bar",
            }
            assert body["current_range"] is None
            assert body["counters"] == empty_counters()
            assert body["requested_by"] == {
                "subject": operator.subject,
                "display_name": "Operator User",
                "access_role": "Operator",
            }
            assert body["retry_of_job_id"] is None
            assert body["error"] is None
            assert body["resolved_gap_ids"] == []

            final = _wait_job(
                client,
                operator,
                job_id,
                lambda state: state["status"] in TERMINAL,
            )

            assert final["status"] == "succeeded"
            assert final["counters"] == {
                "scanned_bars": 60,
                "gap_bars": 0,
                "backfilled_bars": 60,
                "written_bars": 77,
                "rejected_bars": 0,
            }
            assert final["current_range"] is None
            assert final["started_at"] is not None
            assert final["finished_at"] is not None
            assert final["error"] is None

            # The Collector wrote the real Dataset; the API Store refused
            # every write (ReadOnlyBarStore would have raised).
            read_store = ParquetBarStore(tmp_path / "bars")
            try:
                assert len(
                    read_store.read_bars(
                        instrument_id=BTC, interval="1m"
                    ).to_pylist()
                ) == 60
                assert len(
                    read_store.read_bars(
                        instrument_id=BTC, interval="5m"
                    ).to_pylist()
                ) == 12
                assert len(
                    read_store.read_bars(
                        instrument_id=BTC, interval="15m"
                    ).to_pylist()
                ) == 4
                assert len(
                    read_store.read_bars(
                        instrument_id=BTC, interval="1h"
                    ).to_pylist()
                ) == 1
            finally:
                read_store.close()

            listing = client.get(
                f"{JOBS}?status=succeeded&instrument_id={BTC}&interval=1m",
                headers=operator.headers(client=client, csrf=False),
            )
            assert listing.status == 200
            assert [item["job_id"] for item in listing.json()["items"]] == [
                job_id
            ]

            events = [event for event in _audit(client, operator)
                      if event["action"] == "backfill_job.create"]
            assert [event["result"] for event in events] == [
                "requested",
                "succeeded",
            ]
            assert {event["job_id"] for event in events} == {job_id}

            capabilities = client.get("/api/v1/admin/capabilities").json()
            assert capabilities["capabilities"]["backfill_job"] == {
                "state": "enabled",
                "plan_modes": [
                    "native_interval",
                    "one_minute_plus_aggregation",
                ],
            }
    finally:
        collector.stop()


def test_higher_interval_wire_plan_is_always_aggregation(tmp_path: Path) -> None:
    collector = RealCollector(
        tmp_path,
        feed=FakeFeedHandler(instrument_id=BTC, bars=(), history=_history()),
    )
    try:
        with _running_admin(tmp_path, port=collector.port) as (
            _server,
            client,
            context,
        ):
            operator = Credential(context, "Operator")
            created = _create(client, operator, interval="1h")

            assert created.status == 202, created.body
            body = created.json()
            assert body["plan"] == {
                "mode": "one_minute_plus_aggregation",
                "source_intervals": ["1m"],
                "target_interval": "1h",
                "description": "补采 1m + 内部 Bar Aggregation",
            }

            final = _wait_job(
                client,
                operator,
                body["job_id"],
                lambda state: state["status"] in TERMINAL,
            )
            assert final["status"] == "succeeded"
            assert final["counters"]["written_bars"] == 77
    finally:
        collector.stop()


# -- validation / conflict / not found -------------------------------------


def test_create_validation_conflict_and_not_found(tmp_path: Path) -> None:
    feed = BlockingFeed(_history(), block_from_ns=0)
    collector = RealCollector(tmp_path, feed=feed)
    try:
        with _running_admin(tmp_path, port=collector.port) as (
            _server,
            client,
            context,
        ):
            operator = Credential(context, "Operator")

            invalid_instrument = _create(
                client, operator, instrument_id="not-an-instrument"
            )
            assert invalid_instrument.status == 400
            assert invalid_instrument.json()["error"]["details"][
                "reason"
            ] == "invalid_instrument"

            unknown_instrument = _create(
                client, operator, instrument_id="binance:NOPE/USDT"
            )
            assert unknown_instrument.status == 400
            assert unknown_instrument.json()["error"]["details"][
                "reason"
            ] == "unknown_instrument"

            bad_interval = _create(client, operator, interval="2h")
            assert bad_interval.status == 400
            assert bad_interval.json()["error"]["details"]["field"] == (
                "interval"
            )

            bad_range = _create(
                client,
                operator,
                start=_iso(_at(0, hour=2)),
                end=_iso(_at(0, hour=1)),
            )
            assert bad_range.status == 400
            assert bad_range.json()["error"]["details"]["reason"] == (
                "invalid_range"
            )

            too_large = _create(
                client,
                operator,
                start=_iso(datetime(2023, 1, 1, tzinfo=UTC)),
                end=_iso(datetime(2025, 1, 1, tzinfo=UTC)),
            )
            assert too_large.status == 400
            assert too_large.json()["error"]["details"]["reason"] == (
                "range_too_large"
            )

            no_reason = client.post(
                JOBS,
                {
                    "instrument_id": BTC,
                    "interval": "1m",
                    "start": _iso(_at(0)),
                    "end": _iso(_at(0, hour=1)),
                    "confirm": True,
                },
                headers=operator.headers(client=client),
            )
            assert no_reason.status == 400
            assert no_reason.json()["error"]["details"]["field"] == "reason"

            no_confirm = _create(client, operator, confirm=False)
            assert no_confirm.status == 400
            assert no_confirm.json()["error"]["details"]["field"] == "confirm"

            missing_key = _create(client, operator, idempotency_key=None)
            assert missing_key.status == 400
            assert missing_key.json()["error"]["details"]["field"] == (
                "Idempotency-Key"
            )

            first = _create(client, operator, idempotency_key="idem-active")
            assert first.status == 202
            active_id = first.json()["job_id"]
            _wait_job(
                client,
                operator,
                active_id,
                lambda state: state["status"] == "running",
            )
            conflict = _create(
                client, operator, idempotency_key="idem-conflict"
            )
            assert conflict.status == 409
            assert conflict.json()["error"]["details"] == {
                "reason": "active_job",
                "active_job_id": active_id,
            }

            other_interval = _create(
                client,
                operator,
                interval="5m",
                idempotency_key="idem-other-interval",
            )
            assert other_interval.status == 202

            missing = client.get(
                f"{JOBS}/bf_missing",
                headers=operator.headers(client=client, csrf=False),
            )
            assert missing.status == 404
            assert missing.json()["error"]["code"] == "job_not_found"
            assert missing.json()["error"]["details"] == {
                "job_id": "bf_missing"
            }

            bad_status = client.get(
                f"{JOBS}?status=nonsense",
                headers=operator.headers(client=client, csrf=False),
            )
            bad_interval_filter = client.get(
                f"{JOBS}?interval=2h",
                headers=operator.headers(client=client, csrf=False),
            )
            bad_cursor = client.get(
                f"{JOBS}?cursor=not-a-cursor",
                headers=operator.headers(client=client, csrf=False),
            )
            bad_limit = client.get(
                f"{JOBS}?limit=0",
                headers=operator.headers(client=client, csrf=False),
            )
            for response in (
                bad_status,
                bad_interval_filter,
                bad_cursor,
                bad_limit,
            ):
                assert response.status == 400
                assert response.json()["error"]["code"] == "invalid_parameter"
    finally:
        feed.release.set()
        collector.stop()


# -- cancellation -----------------------------------------------------------


def test_cancel_is_cooperative_and_never_retracts_written_revisions(
    tmp_path: Path,
) -> None:
    history = [
        _bar_input(minute, day=day) for day in (1, 2) for minute in range(60)
    ]
    release = threading.Event()
    feed = BlockingFeed(
        history, block_from_ns=datetime_to_ns(_at(0, day=2))
    )
    feed.release = release
    collector = RealCollector(tmp_path, feed=feed)
    try:
        with _running_admin(tmp_path, port=collector.port) as (
            _server,
            client,
            context,
        ):
            operator = Credential(context, "Operator")
            created = _create(
                client,
                operator,
                start=_iso(_at(0, day=1)),
                end=_iso(_at(0, day=3)),
                idempotency_key="idem-cancel-create",
            )
            assert created.status == 202, created.body
            job_id = created.json()["job_id"]
            running = _wait_job(
                client,
                operator,
                job_id,
                lambda state: state["status"] == "running"
                and state["current_range"] is not None,
            )
            # Progress is persisted from the polled Collector document.
            assert running["counters"]["scanned_bars"] >= 1440
            assert running["current_range"]["start"].endswith("Z")
            assert running["current_range"]["end"].endswith("Z")

            cancelled = client.post(
                f"{JOBS}/{job_id}/cancel",
                {"reason": "no longer needed"},
                headers=operator.headers(
                    client=client, idempotency_key="idem-cancel"
                ),
            )
            assert cancelled.status == 202, cancelled.body
            assert cancelled.json()["status"] == "cancelling"

            replay = client.post(
                f"{JOBS}/{job_id}/cancel",
                {"reason": "no longer needed"},
                headers=operator.headers(
                    client=client, idempotency_key="idem-cancel"
                ),
            )
            assert replay.status == 202
            assert replay.header("Idempotent-Replay") == "true"

            # The 202 records the cancel request; the API poller delivers it
            # to the Collector asynchronously. Release the blocked feed only
            # once the Runner has actually observed it, otherwise a fast
            # completion can legitimately win the race and report
            # partially_succeeded instead of cancelled.
            deadline = time.monotonic() + 10.0
            while collector.runner.status(job_id)["state"] != "cancelling":
                assert time.monotonic() < deadline, (
                    "the Collector did not observe the cancellation request"
                )
                time.sleep(0.01)

            release.set()
            final = _wait_job(
                client,
                operator,
                job_id,
                lambda state: state["status"] in TERMINAL,
            )
            assert final["status"] == "cancelled"

            # Day 1 was written before cancellation and is retained.
            read_store = ParquetBarStore(tmp_path / "bars")
            try:
                rows = read_store.read_bars(
                    instrument_id=BTC, interval="1m"
                ).to_pylist()
            finally:
                read_store.close()
            assert len(rows) == 60

            again = client.post(
                f"{JOBS}/{job_id}/cancel",
                {"reason": "too late"},
                headers=operator.headers(
                    client=client, idempotency_key="idem-cancel-2"
                ),
            )
            assert again.status == 409
            assert again.json()["error"]["details"]["reason"] == (
                "job_not_cancellable"
            )

            events = [event for event in _audit(client, operator)
                      if event["action"] == "backfill_job.cancel"]
            assert [event["result"] for event in events] == [
                "requested",
                "succeeded",
            ]
    finally:
        release.set()
        collector.stop()


# -- retry ------------------------------------------------------------------


def test_retry_creates_a_new_linked_job_without_mutating_the_old_one(
    tmp_path: Path,
) -> None:
    collector = RealCollector(
        tmp_path,
        feed=FakeFeedHandler(
            instrument_id=BTC, bars=(), supports_history=False
        ),
    )
    try:
        with _running_admin(tmp_path, port=collector.port) as (
            _server,
            client,
            context,
        ):
            operator = Credential(context, "Operator")
            created = _create(
                client, operator, idempotency_key="idem-retry-create"
            )
            job_id = created.json()["job_id"]
            failed = _wait_job(
                client,
                operator,
                job_id,
                lambda state: state["status"] in TERMINAL,
            )
            assert failed["status"] == "failed"
            assert failed["error"]["code"] == "history_unsupported"

            retried = client.post(
                f"{JOBS}/{job_id}/retry",
                {"reason": "retry after failure"},
                headers=operator.headers(
                    client=client, idempotency_key="idem-retry"
                ),
            )
            assert retried.status == 202, retried.body
            new_job = retried.json()
            assert new_job["job_id"] != job_id
            assert new_job["retry_of_job_id"] == job_id
            assert new_job["status"] == "queued"
            assert new_job["reason"] == "retry after failure"

            old = client.get(
                f"{JOBS}/{job_id}",
                headers=operator.headers(client=client, csrf=False),
            ).json()
            assert old == failed

            replay = client.post(
                f"{JOBS}/{job_id}/retry",
                {"reason": "retry after failure"},
                headers=operator.headers(
                    client=client, idempotency_key="idem-retry"
                ),
            )
            assert replay.status == 202
            assert replay.header("Idempotent-Replay") == "true"
            assert replay.json()["job_id"] == new_job["job_id"]

            events = [event for event in _audit(client, operator)
                      if event["action"] == "backfill_job.retry"]
            assert [event["result"] for event in events] == [
                "requested",
                "succeeded",
            ]
            assert {event["job_id"] for event in events} == {
                new_job["job_id"]
            }
    finally:
        collector.stop()


def test_retry_of_a_succeeded_job_is_rejected(tmp_path: Path) -> None:
    collector = RealCollector(
        tmp_path,
        feed=FakeFeedHandler(instrument_id=BTC, bars=(), history=_history()),
    )
    try:
        with _running_admin(tmp_path, port=collector.port) as (
            _server,
            client,
            context,
        ):
            operator = Credential(context, "Operator")
            created = _create(
                client, operator, idempotency_key="idem-succeeded"
            )
            job_id = created.json()["job_id"]
            _wait_job(
                client,
                operator,
                job_id,
                lambda state: state["status"] == "succeeded",
            )

            retried = client.post(
                f"{JOBS}/{job_id}/retry",
                {"reason": "again"},
                headers=operator.headers(
                    client=client, idempotency_key="idem-succeeded-retry"
                ),
            )
            assert retried.status == 409
            assert retried.json()["error"]["details"]["reason"] == (
                "job_succeeded"
            )
    finally:
        collector.stop()


# -- collector unavailable / rejection -------------------------------------


def test_collector_unavailable_keeps_the_job_queued_and_retries_dispatch(
    tmp_path: Path,
) -> None:
    port = _free_port()
    with _running_admin(tmp_path, port=port) as (_server, client, context):
        operator = Credential(context, "Operator")
        created = _create(client, operator, idempotency_key="idem-offline")
        assert created.status == 202
        job_id = created.json()["job_id"]

        queued = _wait_job(
            client,
            operator,
            job_id,
            lambda state: state["error"] is not None,
        )
        assert queued["status"] == "queued"
        # A closed loopback port surfaces as timeout on some Windows setups;
        # both are bounded transport failures and neither is an execution.
        assert queued["error"]["code"] in {
            "collector_unavailable",
            "timeout",
        }
        assert queued["started_at"] is None

        collector = RealCollector(
            tmp_path,
            feed=FakeFeedHandler(
                instrument_id=BTC, bars=(), history=_history()
            ),
            port=port,
        )
        try:
            final = _wait_job(
                client,
                operator,
                job_id,
                lambda state: state["status"] in TERMINAL,
            )
            assert final["status"] == "succeeded"
            assert final["error"] is None
        finally:
            collector.stop()


def test_cancel_of_a_queued_job_without_a_collector_finishes_cancelled(
    tmp_path: Path,
) -> None:
    port = _free_port()
    with _running_admin(tmp_path, port=port) as (_server, client, context):
        operator = Credential(context, "Operator")
        created = _create(client, operator, idempotency_key="idem-queued-cancel")
        job_id = created.json()["job_id"]
        _wait_job(
            client,
            operator,
            job_id,
            lambda state: state["error"] is not None,
        )

        cancelled = client.post(
            f"{JOBS}/{job_id}/cancel",
            {"reason": "never mind"},
            headers=operator.headers(
                client=client, idempotency_key="idem-queued-cancel-action"
            ),
        )
        assert cancelled.status == 202, cancelled.body
        assert cancelled.json()["status"] == "cancelling"

        final = _wait_job(
            client,
            operator,
            job_id,
            lambda state: state["status"] in TERMINAL,
        )
        assert final["status"] == "cancelled"
        assert final["started_at"] is None


def test_a_collector_rejection_never_becomes_running(tmp_path: Path) -> None:
    collector = RejectingCollector()
    try:
        with _running_admin(tmp_path, port=collector.port) as (
            _server,
            client,
            context,
        ):
            operator = Credential(context, "Operator")
            created = _create(client, operator, idempotency_key="idem-reject")
            job_id = created.json()["job_id"]

            rejected = _wait_job(
                client,
                operator,
                job_id,
                lambda state: state["error"] is not None,
            )
            assert rejected["status"] == "queued"
            assert rejected["error"]["code"] == "unsupported_interval"
            assert rejected["started_at"] is None
            assert rejected["finished_at"] is None
    finally:
        collector.stop()


# -- restart ----------------------------------------------------------------


def test_restart_turns_an_unconfirmed_running_job_into_interrupted(
    tmp_path: Path,
) -> None:
    port = _free_port()
    release = threading.Event()
    feed = BlockingFeed(_history(), block_from_ns=0)
    feed.release = release
    first_collector = RealCollector(tmp_path, feed=feed, port=port)
    try:
        with _running_admin(tmp_path, port=port) as (
            _server,
            client,
            context,
        ):
            operator = Credential(context, "Operator")
            created = _create(client, operator, idempotency_key="idem-restart")
            job_id = created.json()["job_id"]
            running = _wait_job(
                client,
                operator,
                job_id,
                lambda state: state["status"] == "running",
            )
            assert running["finished_at"] is None
    finally:
        release.set()
        first_collector.stop()

    # A fresh Collector has an empty in-memory job registry: `unknown`.
    second_collector = RealCollector(
        tmp_path,
        feed=FakeFeedHandler(instrument_id=BTC, bars=(), history=_history()),
        port=port,
    )
    try:
        with _running_admin(tmp_path, port=port) as (
            _server,
            client,
            context,
        ):
            operator = Credential(context, "Operator")
            final = _wait_job(
                client,
                operator,
                job_id,
                lambda state: state["status"] in TERMINAL,
            )
            assert final["status"] == "failed"
            assert final["error"]["code"] == "interrupted"
            assert final["finished_at"] is not None
    finally:
        second_collector.stop()


def test_restart_resubmits_a_queued_job(tmp_path: Path) -> None:
    port = _free_port()
    with _running_admin(tmp_path, port=port) as (_server, client, context):
        operator = Credential(context, "Operator")
        created = _create(
            client, operator, idempotency_key="idem-queued-restart"
        )
        job_id = created.json()["job_id"]
        queued = _wait_job(
            client,
            operator,
            job_id,
            lambda state: state["error"] is not None,
        )
        assert queued["status"] == "queued"

    collector = RealCollector(
        tmp_path,
        feed=FakeFeedHandler(instrument_id=BTC, bars=(), history=_history()),
        port=port,
    )
    try:
        with _running_admin(tmp_path, port=port) as (
            _server,
            client,
            context,
        ):
            operator = Credential(context, "Operator")
            final = _wait_job(
                client,
                operator,
                job_id,
                lambda state: state["status"] in TERMINAL,
            )
            assert final["status"] == "succeeded"
            assert final["retry_of_job_id"] is None
    finally:
        collector.stop()


# -- idempotency ------------------------------------------------------------


def test_create_idempotency_replays_and_rejects_body_reuse(
    tmp_path: Path,
) -> None:
    collector = RealCollector(
        tmp_path,
        feed=FakeFeedHandler(instrument_id=BTC, bars=(), history=_history()),
    )
    try:
        with _running_admin(tmp_path, port=collector.port) as (
            _server,
            client,
            context,
        ):
            operator = Credential(context, "Operator")
            first = _create(client, operator, idempotency_key="idem-replay")
            assert first.status == 202
            second = _create(client, operator, idempotency_key="idem-replay")
            assert second.status == 202
            assert second.header("Idempotent-Replay") == "true"
            assert second.json()["job_id"] == first.json()["job_id"]

            reuse = _create(
                client,
                operator,
                reason="a different body",
                idempotency_key="idem-replay",
            )
            assert reuse.status == 409
            assert reuse.json()["error"]["details"]["reason"] == (
                "idempotency_key_reuse"
            )

            listing = client.get(
                JOBS,
                headers=operator.headers(client=client, csrf=False),
            ).json()
            assert len(listing["items"]) == 1
    finally:
        collector.stop()


# -- RBAC / CSRF ------------------------------------------------------------


def test_rbac_csrf_and_idempotency_are_server_enforced(tmp_path: Path) -> None:
    collector = RealCollector(
        tmp_path,
        feed=FakeFeedHandler(instrument_id=BTC, bars=(), history=_history()),
    )
    try:
        with _running_admin(tmp_path, port=collector.port) as (
            _server,
            client,
            context,
        ):
            operator = Credential(context, "Operator")
            auditor = Credential(context, "Auditor")

            denied_role = _create(client, auditor, idempotency_key="idem-auditor")
            assert denied_role.status == 403
            assert denied_role.json()["error"]["details"]["reason"] == "role"

            missing_csrf = client.post(
                JOBS,
                {
                    "instrument_id": BTC,
                    "interval": "1m",
                    "start": _iso(_at(0)),
                    "end": _iso(_at(0, hour=1)),
                    "reason": "x",
                    "confirm": True,
                },
                headers=operator.headers(
                    client=client, csrf=False, idempotency_key="idem-no-csrf"
                ),
            )
            assert missing_csrf.status == 403
            assert missing_csrf.json()["error"]["details"]["reason"] == "csrf"

            wrong_origin = _create(
                client,
                operator,
                idempotency_key="idem-origin",
            )
            assert wrong_origin.status == 202  # sanity: valid request works
            # Reuse a fresh pair to avoid the active-job conflict.
            wrong_origin = client.post(
                JOBS,
                {
                    "instrument_id": ETH,
                    "interval": "1m",
                    "start": _iso(_at(0)),
                    "end": _iso(_at(0, hour=1)),
                    "reason": "x",
                    "confirm": True,
                },
                headers=operator.headers(
                    client=client,
                    origin="http://evil.example",
                    idempotency_key="idem-evil",
                ),
            )
            assert wrong_origin.status == 403
            assert wrong_origin.json()["error"]["details"]["reason"] == "origin"

            auditor_list = client.get(
                JOBS, headers=auditor.headers(client=client, csrf=False)
            )
            assert auditor_list.status == 200

            operator_list = client.get(
                JOBS, headers=operator.headers(client=client, csrf=False)
            )
            assert operator_list.status == 200
    finally:
        collector.stop()


# -- audit fail-closed ------------------------------------------------------


def test_audit_unavailable_blocks_create_cancel_and_retry(tmp_path: Path) -> None:
    # The rejecting Collector keeps the seeded queued job from executing, so
    # the zero-execution assertion is about the blocked management writes.
    collector = RejectingCollector()
    try:
        with _running_admin(tmp_path, port=collector.port) as (
            _server,
            client,
            context,
        ):
            operator = Credential(context, "Operator")
            runtime = context.backfill
            runtime.store.insert(
                _seed_record(
                    "bf_seed_cancel", instrument_id=BTC, interval="1m"
                )
            )
            runtime.store.insert(
                _seed_record(
                    "bf_seed_retry",
                    status=FAILED,
                    instrument_id=ETH,
                    interval="1m",
                )
            )
            before = [
                runtime.store.get("bf_seed_cancel"),
                runtime.store.get("bf_seed_retry"),
            ]
            context.audit.close()

            create = _create(client, operator, idempotency_key="idem-audit-create")
            cancel = client.post(
                f"{JOBS}/bf_seed_cancel/cancel",
                {"reason": "stop"},
                headers=operator.headers(
                    client=client, idempotency_key="idem-audit-cancel"
                ),
            )
            retry = client.post(
                f"{JOBS}/bf_seed_retry/retry",
                {"reason": "again"},
                headers=operator.headers(
                    client=client, idempotency_key="idem-audit-retry"
                ),
            )
            after = [
                runtime.store.get("bf_seed_cancel"),
                runtime.store.get("bf_seed_retry"),
            ]
            listing = client.get(
                JOBS, headers=operator.headers(client=client, csrf=False)
            ).json()

            for response in (create, cancel, retry):
                assert response.status == 503, response.body
                envelope = response.json()["error"]
                assert envelope["code"] == "audit_unavailable"
                assert envelope["details"]["executed"] is False
            # The seeded queued job may record a bounded dispatch error, but
            # no management state transition happened and no job was created.
            assert after[0] is not None
            assert after[0].status == QUEUED
            assert after[0].started_at_ns is None
            assert after[1] == before[1]
            assert len(listing["items"]) == 2
    finally:
        collector.stop()
