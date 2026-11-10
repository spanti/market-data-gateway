# ruff: file-ignore[implicit-namespace-package, hardcoded-password-string, too-many-arguments, no-self-use, unused-method-argument]
"""Persistent Backfill Gap Admin API over a real FastAPI app and SQLite.

The list/detail endpoints are read-only for every authenticated Access Role.
History is served from the Admin management database, so a Collector outage
never hides it. Repeated discovery updates one natural-key row, and no
mutation/edit/delete route exists.
"""

from __future__ import annotations

import http.client
import json
import socket
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mdgw.admin.context import AdminContext
from mdgw.admin.gaps.models import (
    GAP_BACKFILL_LIMIT,
    GAP_HISTORY_UNSUPPORTED,
    SOURCE_COLLECTOR_PROCESS,
)
from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.config import (
    AdminAdaptersConfig,
    AdminConfig,
    AdminControlConfig,
)
from mdgw.core.time import ns_to_iso
from mdgw.service.control_plane import (
    GAPS_SNAPSHOT_ACTION,
    ControlPlaneServer,
    collector_control_registry,
)
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-admin-gaps-api"
TOKEN_ENV = "MDGW_TEST_GAPS_API_TOKEN"
TOKEN = "gaps-api-token"
GAPS = "/api/v1/admin/backfill-gaps"
CAPABILITIES = "/api/v1/admin/capabilities"
BTC = "binance:BTC/USDT"
ETH = "binance:ETH/USDT"
SECOND = 1_000_000_000
BASE_NS = 1_700_000_000 * SECOND
MINUTE_NS = 60 * SECOND
HOUR_NS = 60 * MINUTE_NS

GAP_WIRE_KEYS = {
    "gap_id",
    "status",
    "feed",
    "instrument_id",
    "interval",
    "start",
    "end",
    "reason",
    "first_discovered_at",
    "last_discovered_at",
    "occurrence_count",
    "resolved_at",
    "resolved_by_job_id",
    "related_job_ids",
    "created_at",
    "updated_at",
}


@dataclass
class Result:
    status: int
    headers: list[tuple[str, str]]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body)


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


class Credential:
    def __init__(self, context: AdminContext, role: str) -> None:
        issued = context.auth.sessions.create(
            subject=f"subject-{role.lower()}",
            display_name=f"{role} User",
            email=None,
            access_role=role,
        )
        self.token = issued.token
        self.subject = issued.record.subject

    def headers(self, *, client: Client) -> dict[str, str]:
        return {
            "Cookie": f"mdgw_session={self.token}",
            "Origin": client.origin,
        }


class ReadOnlyBarStore(ParquetBarStore):
    """The API-side Store; any write is a test failure."""

    def write_bars(self, *args: Any, **kwargs: Any) -> Any:
        msg = "the Admin API must never write the Bars Dataset"
        raise AssertionError(msg)


class FakeCollectorSnapshot:
    """A control plane that serves one fixed in-memory Gap snapshot."""

    def __init__(self, gaps: list[dict[str, Any]]) -> None:
        self.gaps = gaps
        self.server = ControlPlaneServer(
            host=HOST,
            port=0,
            token=TOKEN,
            registry=collector_control_registry(),
            max_message_bytes=65_536,
            request_timeout_s=2.0,
        )
        self.server.register(
            GAPS_SNAPSHOT_ACTION,
            lambda _payload: {
                "collector_instance_id": "collector-fake",
                "snapshot_at_ns": BASE_NS,
                "gaps": list(self.gaps),
            },
        )
        self.server.start()

    @property
    def port(self) -> int:
        return self.server.port

    def stop(self) -> None:
        self.server.stop()


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((HOST, 0))
        return int(probe.getsockname()[1])


def _admin_config(tmp_path: Path, control_port: int) -> AdminConfig:
    return AdminConfig(
        enabled=True,
        db_path=tmp_path / "admin" / "admin.db",
        adapters=AdminAdaptersConfig(
            oidc=False, audit_store=True, collector_control=True
        ),
        control=AdminControlConfig(
            collector_host=HOST,
            collector_port=control_port,
            token_env=TOKEN_ENV,
            timeout_s=0.25,
        ),
    )


@contextmanager
def _running_admin(
    tmp_path: Path,
    *,
    control_port: int,
) -> Generator[tuple[Client, AdminContext]]:
    config = _admin_config(tmp_path, control_port)
    store = ReadOnlyBarStore(tmp_path / "bars")
    context = AdminContext(
        config=config,
        storage_root=tmp_path,
        store=store,
        environ={TOKEN_ENV: TOKEN},
    )
    app = create_app(store, admin=context)
    server = AsgiServer()
    try:
        server.start(app, host=HOST, port=0, thread_name=THREAD_NAME)
        yield Client(server), context
    finally:
        server.stop()
        context.close()
        store.close()


def _seed(
    context: AdminContext,
    *,
    feed: str = "binance_spot",
    instrument_id: str = BTC,
    interval: str = "1m",
    reason: str = GAP_HISTORY_UNSUPPORTED,
    start_ns: int = BASE_NS + MINUTE_NS,
    end_ns: int = BASE_NS + 10 * MINUTE_NS,
    observed_at_ns: int = BASE_NS,
    source: str = SOURCE_COLLECTOR_PROCESS,
):
    return context.gaps.service.store.upsert_discovery(
        source=source,
        feed=feed,
        instrument_id=instrument_id,
        interval=interval,
        start_ns=start_ns,
        end_ns=end_ns,
        reason=reason,
        observed_at_ns=observed_at_ns,
    )


def _get_gaps(client: Client, credential: Credential, query: str = "") -> Result:
    return client.get(f"{GAPS}{query}", headers=credential.headers(client=client))


def _items(result: Result) -> list[dict[str, Any]]:
    assert result.status == 200, result.body
    return result.json()["items"]


# -- read access ------------------------------------------------------------


def test_anonymous_requests_get_the_unauthenticated_envelope(
    tmp_path: Path,
) -> None:
    with _running_admin(tmp_path, control_port=_free_port()) as (client, _context):
        response = client.get(GAPS)

    assert response.status == 401
    envelope = response.json()
    assert envelope["error"]["code"] == "unauthenticated"
    assert envelope["error"]["details"] == {"reason": "missing"}


def test_every_role_can_read_the_list_and_the_detail(tmp_path: Path) -> None:
    with _running_admin(tmp_path, control_port=_free_port()) as (client, context):
        gap = _seed(context)
        for role in ("SuperAdmin", "Operator", "Auditor"):
            credential = Credential(context, role)
            listed = _get_gaps(client, credential)
            items = _items(listed)
            assert len(items) == 1
            assert items[0]["gap_id"] == gap.gap_id

            detail = client.get(
                f"{GAPS}/{gap.gap_id}",
                headers=credential.headers(client=client),
            )
            assert detail.status == 200
            assert detail.json()["gap_id"] == gap.gap_id


def test_the_wire_fields_are_exactly_the_contract_and_carry_no_seq(
    tmp_path: Path,
) -> None:
    with _running_admin(tmp_path, control_port=_free_port()) as (client, context):
        gap = _seed(context)
        credential = Credential(context, "Auditor")
        item = _items(_get_gaps(client, credential))[0]

    assert set(item) == GAP_WIRE_KEYS
    assert "seq" not in item
    assert item["status"] == "open"
    assert item["feed"] == "binance_spot"
    assert item["instrument_id"] == BTC
    assert item["interval"] == "1m"
    assert item["reason"] == GAP_HISTORY_UNSUPPORTED
    assert item["start"].endswith("Z")
    assert item["end"].endswith("Z")
    assert item["first_discovered_at"] == item["created_at"]
    assert item["last_discovered_at"] == item["created_at"]
    assert item["occurrence_count"] == 1
    assert item["resolved_at"] is None
    assert item["resolved_by_job_id"] is None
    assert item["related_job_ids"] == []
    assert item["gap_id"] == gap.gap_id


def test_reading_gaps_is_not_audited(tmp_path: Path) -> None:
    with _running_admin(tmp_path, control_port=_free_port()) as (client, context):
        gap = _seed(context)
        credential = Credential(context, "Operator")
        headers = credential.headers(client=client)
        _get_gaps(client, credential)
        client.get(f"{GAPS}/{gap.gap_id}", headers=headers)
        events = client.get(
            "/api/v1/admin/audit-events?limit=200", headers=headers
        ).json()["items"]

    assert events == []


def test_an_unknown_gap_is_404_not_found(tmp_path: Path) -> None:
    with _running_admin(tmp_path, control_port=_free_port()) as (client, context):
        credential = Credential(context, "Operator")
        response = client.get(
            f"{GAPS}/gap_missing", headers=credential.headers(client=client)
        )

    assert response.status == 404
    error = response.json()["error"]
    assert error["code"] == "not_found"
    assert error["details"] == {
        "resource": "backfill_gap",
        "id": "gap_missing",
    }


# -- persistence / discovery ------------------------------------------------


def test_history_survives_an_api_and_store_restart(tmp_path: Path) -> None:
    with _running_admin(tmp_path, control_port=_free_port()) as (client, context):
        open_gap = _seed(context)
        resolved_gap = _seed(
            context,
            instrument_id="binance:SOL/USDT",
            observed_at_ns=BASE_NS + SECOND,
        )
        context.gaps.service.store.resolve_contained(
            feed="binance_spot",
            instrument_id="binance:SOL/USDT",
            interval="1m",
            start_ns=BASE_NS,
            end_ns=BASE_NS + HOUR_NS,
            job_id="bf_before_restart",
            observed_at_ns=BASE_NS + HOUR_NS,
        )
        credential = Credential(context, "Operator")
        before = {
            item["gap_id"]: item
            for item in _items(_get_gaps(client, credential))
        }
        assert before[open_gap.gap_id]["status"] == "open"
        assert before[resolved_gap.gap_id]["status"] == "resolved"

    # A brand-new API process/context opens the same management database.
    with _running_admin(tmp_path, control_port=_free_port()) as (client, context):
        credential = Credential(context, "Auditor")
        items = _items(_get_gaps(client, credential))

    after = {item["gap_id"]: item for item in items}
    assert set(after) == {open_gap.gap_id, resolved_gap.gap_id}
    assert after[open_gap.gap_id] == before[open_gap.gap_id]
    assert after[resolved_gap.gap_id] == before[resolved_gap.gap_id]
    assert after[resolved_gap.gap_id]["status"] == "resolved"
    assert (
        after[resolved_gap.gap_id]["resolved_by_job_id"]
        == "bf_before_restart"
    )


def test_repeated_discovery_updates_one_row_first_last_and_count(
    tmp_path: Path,
) -> None:
    with _running_admin(tmp_path, control_port=_free_port()) as (client, context):
        credential = Credential(context, "Operator")
        first = _seed(context, observed_at_ns=BASE_NS)
        second = _seed(context, observed_at_ns=BASE_NS + HOUR_NS)
        items = _items(_get_gaps(client, credential))

    assert first.gap_id == second.gap_id
    assert len(items) == 1
    assert items[0]["first_discovered_at"] == first.to_wire()[
        "first_discovered_at"
    ]
    assert items[0]["last_discovered_at"] == second.to_wire()[
        "last_discovered_at"
    ]
    assert items[0]["occurrence_count"] == 2


def test_collector_snapshot_is_persisted_while_the_collector_is_down(
    tmp_path: Path,
) -> None:
    collector = FakeCollectorSnapshot(
        [
            {
                "feed": "binance_spot",
                "instrument_id": BTC,
                "interval": "1m",
                "from_ns": BASE_NS + MINUTE_NS,
                "to_ns": BASE_NS + 5 * MINUTE_NS,
                "reason": GAP_BACKFILL_LIMIT,
                "recorded_at_ns": BASE_NS,
            }
        ]
    )
    try:
        with _running_admin(tmp_path, control_port=collector.port) as (
            client,
            context,
        ):
            credential = Credential(context, "Operator")
            # The startup poll already ingested the snapshot.
            items_before = _items(_get_gaps(client, credential))
            assert len(items_before) == 1
            assert items_before[0]["reason"] == GAP_BACKFILL_LIMIT

            collector.stop()
            # A failed poll is not evidence that any gap disappeared.
            assert context.gaps.poll_once() == 0
            items_after = _items(_get_gaps(client, credential))
            assert items_after == items_before
    finally:
        collector.stop()


def test_a_still_listed_collector_event_does_not_undo_a_resolution(
    tmp_path: Path,
) -> None:
    collector = FakeCollectorSnapshot(
        [
            {
                "feed": "binance_spot",
                "instrument_id": BTC,
                "interval": "1m",
                "from_ns": BASE_NS + MINUTE_NS,
                "to_ns": BASE_NS + 5 * MINUTE_NS,
                "reason": GAP_HISTORY_UNSUPPORTED,
                "recorded_at_ns": BASE_NS,
            }
        ]
    )
    try:
        with _running_admin(tmp_path, control_port=collector.port) as (
            client,
            context,
        ):
            credential = Credential(context, "Operator")
            gap = _items(_get_gaps(client, credential))[0]
            context.gaps.service.store.resolve_contained(
                feed="binance_spot",
                instrument_id=BTC,
                interval="1m",
                start_ns=BASE_NS,
                end_ns=BASE_NS + HOUR_NS,
                job_id="bf_success",
                observed_at_ns=BASE_NS + MINUTE_NS,
            )
            # The Collector's in-memory list still contains the old event.
            assert context.gaps.poll_once() == 1
            item = _items(_get_gaps(client, credential))[0]
    finally:
        collector.stop()

    assert item["gap_id"] == gap["gap_id"]
    assert item["status"] == "resolved"
    assert item["resolved_by_job_id"] == "bf_success"
    assert item["occurrence_count"] == 1


# -- filters / pagination ---------------------------------------------------


def _seed_mixed(context: AdminContext) -> dict[str, Any]:
    first = _seed(context, observed_at_ns=BASE_NS + SECOND)
    second = _seed(
        context,
        feed="okx_spot",
        instrument_id=ETH,
        interval="5m",
        reason="history_missing",
        start_ns=BASE_NS + 2 * HOUR_NS,
        end_ns=BASE_NS + 3 * HOUR_NS,
        observed_at_ns=BASE_NS + 2 * SECOND,
    )
    third = _seed(
        context,
        instrument_id="binance:SOL/USDT",
        observed_at_ns=BASE_NS + 3 * SECOND,
    )
    context.gaps.service.store.resolve_contained(
        feed="binance_spot",
        instrument_id="binance:SOL/USDT",
        interval="1m",
        start_ns=BASE_NS,
        end_ns=BASE_NS + HOUR_NS,
        job_id="bf_resolved_seed",
        observed_at_ns=BASE_NS + 10 * SECOND,
    )
    return {"first": first, "second": second, "third": third}


def test_filters_are_exact_and_time_bounds_are_half_open(tmp_path: Path) -> None:
    with _running_admin(tmp_path, control_port=_free_port()) as (client, context):
        seeded = _seed_mixed(context)
        credential = Credential(context, "Operator")

        def ids(query: str) -> list[str]:
            pages = _get_gaps(client, credential, query)
            return [item["gap_id"] for item in _items(pages)]

        assert ids("?status=open") == [
            seeded["second"].gap_id,
            seeded["first"].gap_id,
        ]
        assert ids("?status=resolved") == [seeded["third"].gap_id]
        assert ids("?feed=okx_spot") == [seeded["second"].gap_id]
        assert ids(f"?instrument_id={BTC}") == [seeded["first"].gap_id]
        assert ids("?interval=5m") == [seeded["second"].gap_id]
        assert ids("?reason=history_missing") == [seeded["second"].gap_id]
        # discovered_* filter the first discovery; from inclusive, to exclusive.
        assert ids(
            f"?discovered_from={_iso(BASE_NS + 2 * SECOND)}"
        ) == [seeded["third"].gap_id, seeded["second"].gap_id]
        assert ids(
            f"?discovered_to={_iso(BASE_NS + 2 * SECOND)}"
        ) == [seeded["first"].gap_id]
        # resolved_* filter resolved_at.
        assert ids(
            f"?resolved_from={_iso(BASE_NS + 9 * SECOND)}"
        ) == [seeded["third"].gap_id]
        assert ids(
            f"?resolved_to={_iso(BASE_NS + 9 * SECOND)}"
        ) == []


def test_keyset_pagination_walks_every_gap_once(tmp_path: Path) -> None:
    with _running_admin(tmp_path, control_port=_free_port()) as (client, context):
        seeded = [
            _seed(
                context,
                start_ns=BASE_NS + (index + 1) * MINUTE_NS,
                end_ns=BASE_NS + (index + 2) * MINUTE_NS,
                observed_at_ns=BASE_NS + index * SECOND,
            )
            for index in range(5)
        ]
        credential = Credential(context, "Auditor")

        first = _get_gaps(client, credential, "?limit=2")
        page = first.json()["page"]
        assert len(first.json()["items"]) == 2
        assert page["has_more"] is True
        assert page["next_cursor"]

        seen = [item["gap_id"] for item in first.json()["items"]]
        cursor = page["next_cursor"]
        while cursor is not None:
            response = _get_gaps(
                client, credential, f"?limit=2&cursor={cursor}"
            )
            payload = response.json()
            seen.extend(item["gap_id"] for item in payload["items"])
            cursor = payload["page"]["next_cursor"]

    assert len(seen) == len(set(seen)) == 5
    assert set(seen) == {gap.gap_id for gap in seeded}


def test_invalid_query_parameters_are_400_invalid_parameter(
    tmp_path: Path,
) -> None:
    with _running_admin(tmp_path, control_port=_free_port()) as (client, context):
        credential = Credential(context, "Operator")
        cases = [
            "?status=bogus",
            "?interval=2m",
            "?discovered_from=not-a-time",
            "?resolved_to=2024-13-99",
            "?cursor=not_a_valid_cursor",
            "?limit=0",
            "?limit=201",
            "?limit=abc",
        ]
        for query in cases:
            response = _get_gaps(client, credential, query)
            assert response.status == 400, (query, response.body)
            error = response.json()["error"]
            assert error["code"] == "invalid_parameter", query
            assert "field" in error["details"], query


# -- no mutation ------------------------------------------------------------


def test_no_edit_or_delete_route_exists(tmp_path: Path) -> None:
    with _running_admin(tmp_path, control_port=_free_port()) as (client, context):
        gap = _seed(context)
        credential = Credential(context, "SuperAdmin")
        headers = credential.headers(client=client)
        headers["Content-Type"] = "application/json"
        for method, path in (
            ("POST", GAPS),
            ("PUT", f"{GAPS}/{gap.gap_id}"),
            ("PATCH", f"{GAPS}/{gap.gap_id}"),
            ("DELETE", f"{GAPS}/{gap.gap_id}"),
            ("DELETE", GAPS),
        ):
            response = client.request(
                method, path, headers=headers, body=b"{}"
            )
            assert response.status == 405, (method, path, response.body)
            assert (
                response.json()["error"]["code"] == "method_not_allowed"
            ), (method, path)
        # History is still there and readable.
        assert [item["gap_id"] for item in _items(_get_gaps(client, credential))] == [
            gap.gap_id
        ]


# -- capability -------------------------------------------------------------


def test_persistent_gaps_capability_tracks_the_collector_adapter(
    tmp_path: Path,
) -> None:
    collector = FakeCollectorSnapshot([])
    try:
        with _running_admin(tmp_path, control_port=collector.port) as (
            client,
            _context,
        ):
            document = json.loads(client.get(CAPABILITIES).body)
            capability = document["capabilities"]["persistent_backfill_gaps"]
            adapter = next(
                entry
                for entry in document["adapters"]
                if entry["name"] == "collector_control"
            )
    finally:
        collector.stop()

    assert adapter["state"] == "available"
    assert capability["state"] == "enabled"


def _iso(value: int) -> str:
    return ns_to_iso(value)
