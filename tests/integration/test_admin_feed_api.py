# ruff: file-ignore[implicit-namespace-package,hardcoded-password-string,unused-lambda-argument,unnecessary-default-type-args,too-many-arguments,too-many-statements]
"""Feed Subscription and flow-control Admin API over a real FastAPI app.

The tests use a real loopback control plane (a fake Collector) so Desired,
Observed and Drift are exercised through the actual transport, not a mocked
client. A Collector rejection is drift, never auto-rollback; an unavailable
Collector keeps Desired and records last_error; an API restart re-dispatches
the persisted desired set.
"""

from __future__ import annotations

import http.client
import json
import socket
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

import yaml

from mdgw.admin.audit.models import AuditQuery
from mdgw.admin.context import AdminContext
from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.api.ws_hub import WebSocketHub
from mdgw.config import (
    AdminAdaptersConfig,
    AdminConfig,
    AdminControlConfig,
    AdminProxyConfig,
)
from mdgw.reference import (
    InstrumentStore,
    export_instrument_snapshot,
    load_instrument_reference,
)
from mdgw.service.control_plane import (
    FEED_SUBSCRIPTION_SYNC_ACTION,
    ControlPlaneServer,
)
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-admin-feed-api"
TOKEN_ENV = "MDGW_TEST_CONTROL_TOKEN"
TOKEN = "feed-control-token"
SECOND = 1_000_000_000
BASE_NS = 1_700_000_000 * SECOND
BTC = "binance:BTC/USDT"
ETH = "binance:ETH/USDT"
FEED_PATH = "/api/v1/admin/feed-subscriptions"
FLOW_PATH = "/api/v1/admin/flow-control"


class Clock:
    def __init__(self, now_ns: int = BASE_NS) -> None:
        self.now_ns = now_ns

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
            HOST, self.server.bound_port, timeout=15
        )
        try:
            connection.request(
                method, path, body=body, headers=dict(headers or {})
            )
            response = connection.getresponse()
            payload = response.read()
            result = Result(
                int(response.status), list(response.getheaders()), payload
            )
        finally:
            connection.close()
        return result

    def get(self, path: str, **kwargs: Any) -> Result:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, body: Any, **kwargs: Any) -> Result:
        return self.request("POST", path, body=_json(body), **kwargs)

    def put(self, path: str, body: Any, **kwargs: Any) -> Result:
        return self.request("PUT", path, body=_json(body), **kwargs)

    def delete(self, path: str, **kwargs: Any) -> Result:
        return self.request("DELETE", path, **kwargs)


def _json(value: Any) -> bytes:
    return json.dumps(value).encode("utf-8")


class Credential:
    def __init__(
        self, context: AdminContext, role: str, *, subject: str | None = None
    ) -> None:
        issued = context.auth.sessions.create(
            subject=subject or f"subject-{role.lower()}",
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
        idempotency_key: str | None = None,
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


class FakeCollector:
    """A Collector control-plane stand-in with real framing and validation."""

    def __init__(
        self, *, port: int = 0, reject: dict[str, str] | None = None
    ) -> None:
        self.reject = dict(reject or {})
        self.calls: list[Any] = []
        self.server = ControlPlaneServer(
            host=HOST,
            port=port,
            token=TOKEN,
            max_message_bytes=65_536,
            request_timeout_s=5.0,
        )
        self.server.register("ping", lambda payload: {"pong": True})
        self.server.register(FEED_SUBSCRIPTION_SYNC_ACTION, self._sync)
        self.server.start()

    @property
    def port(self) -> int:
        return self.server.port

    def _sync(self, payload: Any) -> dict[str, Any]:
        self.calls.append(payload)
        entries = []
        for item in payload.subscriptions:
            reason = self.reject.get(item.instrument_id)
            entries.append(
                {
                    "subscription_id": item.subscription_id,
                    "version": item.version,
                    "state": "rejected" if reason else "applied",
                    "reason": reason,
                }
            )
        return {
            "collector_instance_id": "collector-test",
            "subscriptions": entries,
        }

    def stop(self) -> None:
        self.server.stop()


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((HOST, 0))
        return int(probe.getsockname()[1])


def _instrument_store(
    tmp_path: Path, *instrument_ids: str
) -> InstrumentStore:
    entries = []
    for instrument_id in instrument_ids or (BTC, ETH):
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
    collector: FakeCollector | None = None,
    collector_control: bool = True,
    control_port: int | None = None,
    db_name: str = "admin.db",
    trusted_proxies: tuple[str, ...] = (),
) -> Generator[tuple[AsgiServer, Client, AdminContext, WebSocketHub], None, None]:
    port = (
        collector.port
        if collector is not None
        else (control_port if control_port is not None else 0)
    )
    config = AdminConfig(
        enabled=True,
        db_path=tmp_path / "admin" / db_name,
        adapters=AdminAdaptersConfig(
            oidc=False, audit_store=True, collector_control=collector_control
        ),
        control=AdminControlConfig(
            host=HOST,
            port=port,
            token_env=TOKEN_ENV,
            timeout_s=1.0,
        ),
        proxy=AdminProxyConfig(trusted_proxies=trusted_proxies),
    )
    store = ParquetBarStore(tmp_path / "data")
    instruments = _instrument_store(tmp_path)
    context = AdminContext(
        config=config,
        storage_root=tmp_path,
        store=store,
        instrument_store=instruments,
        environ={TOKEN_ENV: TOKEN},
        clock=Clock(),
    )
    hub = WebSocketHub()
    app = create_app(
        store,
        instrument_store=instruments,
        ws_hub=hub,
        admin=context,
    )
    server = AsgiServer()
    try:
        server.start(app, host=HOST, port=0, thread_name=THREAD_NAME)
        yield server, Client(server), context, hub
    finally:
        server.stop()
        store.close()


def _create(
    client: Client,
    credential: Credential,
    *,
    instrument_id: str = BTC,
    interval: str = "1m",
    feed: str = "fake",
    idempotency_key: str | None = None,
) -> Result:
    return client.post(
        FEED_PATH,
        {
            "feed": feed,
            "instrument_id": instrument_id,
            "data_type": "bar",
            "interval": interval,
        },
        headers=credential.headers(
            client=client, idempotency_key=idempotency_key
        ),
    )


def _audit_actions(context: AdminContext, action: str) -> list[str]:
    events = context.audit.store.query(
        AuditQuery(action=action, order="asc", limit=100)
    )
    return [event.result for event in events]


# -- create / read / audit ---------------------------------------------------


def test_operator_creates_reads_and_lists_an_applied_subscription(
    tmp_path: Path,
) -> None:
    collector = FakeCollector()
    try:
        with _running_admin(tmp_path, collector=collector) as (
            _server,
            client,
            context,
            _hub,
        ):
            operator = Credential(context, "Operator")

            created = _create(client, operator)

            assert created.status == 201
            body = created.json()
            assert body["subscription_id"].startswith("fsub_")
            assert created.header("Location") == (
                f"{FEED_PATH}/{body['subscription_id']}"
            )
            assert created.header("ETag") == '"1"'
            assert body["feed"] == "fake"
            assert body["venue"] == "binance"
            assert body["instrument_id"] == BTC
            assert body["data_type"] == "bar"
            assert body["interval"] == "1m"
            assert body["desired"] == {
                "enabled": True,
                "deleted": False,
                "version": 1,
                "updated_at": body["desired"]["updated_at"],
                "updated_by": operator.subject,
            }
            assert body["desired"]["updated_at"].endswith("Z")
            assert body["observed"]["state"] == "applied"
            assert body["observed"]["version"] == 1
            assert body["observed"]["enabled"] is True
            assert body["observed"]["collector_instance_id"] == (
                "collector-test"
            )
            assert body["drift"] == {
                "in_sync": True,
                "desired_version": 1,
                "observed_version": 1,
                "since": body["drift"]["since"],
                "reason": None,
            }
            assert body["last_error"] is None
            assert body["created_by"] == operator.subject

            # The Collector really received the full desired set.
            assert collector.calls[-1].subscriptions[0].instrument_id == BTC

            listing = client.get(
                FEED_PATH, headers=operator.headers(client=client, csrf=False)
            )
            assert listing.status == 200
            payload = listing.json()
            assert [item["subscription_id"] for item in payload["items"]] == [
                body["subscription_id"]
            ]
            assert payload["page"] == {
                "next_cursor": None,
                "has_more": False,
            }

            detail = client.get(
                f"{FEED_PATH}/{body['subscription_id']}",
                headers=operator.headers(client=client, csrf=False),
            )
            assert detail.status == 200
            assert detail.json()["subscription_id"] == body["subscription_id"]
            assert detail.header("ETag") == '"1"'

            missing = client.get(
                f"{FEED_PATH}/fsub_missing",
                headers=operator.headers(client=client, csrf=False),
            )
            assert missing.status == 404
            assert missing.json()["error"]["details"] == {
                "resource": "feed_subscription",
                "id": "fsub_missing",
            }

            assert _audit_actions(context, "feed_subscription.create") == [
                "requested",
                "succeeded",
            ]

            capabilities = client.get("/api/v1/admin/capabilities")
            document = capabilities.json()
            adapters = {
                entry["name"]: entry for entry in document["adapters"]
            }
            assert adapters["collector_control"]["state"] == "available"
            assert document["capabilities"]["feed_subscription"][
                "state"
            ] == "enabled"
            flow = document["capabilities"]["feed_flow_control"]
            assert flow["state"] == "enabled"
            assert flow["api_rate_limit"] == {"hot_apply": True}
            assert flow["ws_slow_consumer"] == {"hot_apply": True}
            assert flow["restart_required_fields"] == []
    finally:
        collector.stop()


def test_duplicate_validation_and_authorization(tmp_path: Path) -> None:
    collector = FakeCollector()
    try:
        with _running_admin(tmp_path, collector=collector) as (
            _server,
            client,
            context,
            _hub,
        ):
            operator = Credential(context, "Operator")
            auditor = Credential(context, "Auditor")
            first = _create(client, operator)
            assert first.status == 201
            subscription_id = first.json()["subscription_id"]

            duplicate = _create(client, operator)
            assert duplicate.status == 409
            assert duplicate.json()["error"]["details"] == {
                "reason": "duplicate",
                "existing_subscription_id": subscription_id,
            }

            unknown = _create(client, operator, instrument_id="binance:NOPE/USDT")
            assert unknown.status == 400
            assert unknown.json()["error"]["code"] == "invalid_parameter"
            assert unknown.json()["error"]["details"]["field"] == (
                "instrument_id"
            )
            assert unknown.json()["error"]["details"]["reason"] == (
                "unknown_instrument"
            )

            bad_interval = _create(client, operator, interval="2m")
            assert bad_interval.status == 400
            assert bad_interval.json()["error"]["code"] == "invalid_parameter"

            bad_type = client.post(
                FEED_PATH,
                {
                    "feed": "fake",
                    "instrument_id": ETH,
                    "data_type": "trade",
                    "interval": "1m",
                },
                headers=operator.headers(client=client),
            )
            assert bad_type.status == 400
            assert bad_type.json()["error"]["code"] == "invalid_parameter"

            auditor_write = _create(client, auditor, instrument_id=ETH)
            assert auditor_write.status == 403
            assert auditor_write.json()["error"]["details"]["reason"] == "role"

            no_csrf = client.post(
                FEED_PATH,
                {
                    "feed": "fake",
                    "instrument_id": ETH,
                    "data_type": "bar",
                    "interval": "1m",
                },
                headers=operator.headers(client=client, csrf=False),
            )
            assert no_csrf.status == 403
            assert no_csrf.json()["error"]["details"]["reason"] == "csrf"

            wrong_origin = client.post(
                FEED_PATH,
                {
                    "feed": "fake",
                    "instrument_id": ETH,
                    "data_type": "bar",
                    "interval": "1m",
                },
                headers=operator.headers(
                    client=client, origin="http://evil.example"
                ),
            )
            assert wrong_origin.status == 403
            assert wrong_origin.json()["error"]["details"]["reason"] == (
                "origin"
            )

            anonymous = client.get(FEED_PATH)
            assert anonymous.status == 401
            assert anonymous.json()["error"]["code"] == "unauthenticated"

            # Auditor may read.
            auditor_read = client.get(
                FEED_PATH, headers=auditor.headers(client=client, csrf=False)
            )
            assert auditor_read.status == 200
    finally:
        collector.stop()


# -- versions / conflicts / tombstone ---------------------------------------


def test_if_match_conflicts_and_tombstone_deletion(tmp_path: Path) -> None:
    collector = FakeCollector()
    try:
        with _running_admin(tmp_path, collector=collector) as (
            _server,
            client,
            context,
            _hub,
        ):
            operator = Credential(context, "Operator")
            created = _create(client, operator)
            subscription_id = created.json()["subscription_id"]
            path = f"{FEED_PATH}/{subscription_id}"

            missing = client.put(
                path,
                {
                    "feed": "fake",
                    "instrument_id": BTC,
                    "interval": "1m",
                    "enabled": False,
                },
                headers=operator.headers(client=client),
            )
            assert missing.status == 400
            assert missing.json()["error"]["details"]["field"] == "If-Match"

            invalid = client.put(
                path,
                {
                    "feed": "fake",
                    "instrument_id": BTC,
                    "interval": "1m",
                    "enabled": False,
                },
                headers={
                    **operator.headers(client=client),
                    "If-Match": "not-a-version",
                },
            )
            assert invalid.status == 400
            assert invalid.json()["error"]["details"]["reason"] == (
                "invalid_if_match"
            )

            stale = client.put(
                path,
                {
                    "feed": "fake",
                    "instrument_id": BTC,
                    "interval": "1m",
                    "enabled": False,
                },
                headers={
                    **operator.headers(client=client),
                    "If-Match": '"99"',
                },
            )
            assert stale.status == 409
            details = stale.json()["error"]["details"]
            assert details["current_version"] == 1
            assert details["current"]["subscription_id"] == subscription_id
            assert details["reason"] == "version_conflict"

            updated = client.put(
                path,
                {
                    "feed": "fake",
                    "instrument_id": BTC,
                    "interval": "1m",
                    "enabled": False,
                },
                headers={
                    **operator.headers(client=client),
                    "If-Match": '"1"',
                },
            )
            assert updated.status == 202
            updated_body = updated.json()
            assert updated_body["desired"]["version"] == 2
            assert updated_body["desired"]["enabled"] is False
            assert updated_body["observed"]["version"] == 2
            assert updated_body["observed"]["state"] == "applied"
            assert updated_body["drift"]["in_sync"] is True
            assert updated.header("ETag") == '"2"'
            assert _audit_actions(context, "feed_subscription.update") == [
                "requested",
                "succeeded",
            ]

            stale_delete = client.delete(
                path,
                headers={
                    **operator.headers(client=client),
                    "If-Match": '"1"',
                },
            )
            assert stale_delete.status == 409

            deleted = client.delete(
                path,
                headers={
                    **operator.headers(client=client),
                    "If-Match": '"2"',
                },
            )
            assert deleted.status == 202
            deleted_body = deleted.json()
            assert deleted_body["desired"]["deleted"] is True
            assert deleted_body["desired"]["enabled"] is False
            assert deleted_body["desired"]["version"] == 3
            assert deleted_body["observed"]["deleted"] is True
            assert deleted_body["observed"]["state"] == "applied"
            assert deleted_body["drift"]["in_sync"] is True

            # Tombstone survives and is filterable; history is not deleted.
            listing = client.get(
                f"{FEED_PATH}?state=deleted",
                headers=operator.headers(client=client, csrf=False),
            )
            assert listing.status == 200
            assert [item["subscription_id"] for item in listing.json()["items"]] == [
                subscription_id
            ]
            assert (
                client.get(
                    path, headers=operator.headers(client=client, csrf=False)
                ).status
                == 200
            )
            assert _audit_actions(context, "feed_subscription.delete") == [
                "requested",
                "succeeded",
            ]
            # Instrument Reference Data is untouched.
            assert context.instrument_store is not None
            assert context.instrument_store.get(BTC) is not None
    finally:
        collector.stop()


# -- drift / rejection / unavailable ---------------------------------------


def test_collector_rejection_is_drift_and_never_rolls_back_desired(
    tmp_path: Path,
) -> None:
    collector = FakeCollector(reject={BTC: "unsupported_instrument"})
    try:
        with _running_admin(tmp_path, collector=collector) as (
            _server,
            client,
            context,
            _hub,
        ):
            operator = Credential(context, "Operator")
            response = _create(client, operator)

            assert response.status == 201
            body = response.json()
            assert body["desired"]["enabled"] is True
            assert body["desired"]["version"] == 1
            assert body["observed"]["state"] == "rejected"
            assert body["observed"]["version"] == 0
            assert body["drift"]["in_sync"] is False
            assert body["drift"]["reason"] == "collector_rejected"
            assert body["drift"]["desired_version"] == 1
            assert body["last_error"]["code"] == "unsupported_instrument"
            assert body["last_error"]["request_id"]

            # Desired is still visible through a fresh read.
            detail = client.get(
                f"{FEED_PATH}/{body['subscription_id']}",
                headers=operator.headers(client=client, csrf=False),
            )
            assert detail.json()["desired"]["enabled"] is True
            assert detail.json()["observed"]["state"] == "rejected"
    finally:
        collector.stop()


def test_an_unreachable_collector_keeps_desired_and_records_drift(
    tmp_path: Path,
) -> None:
    unreachable = _free_port()
    with _running_admin(tmp_path, control_port=unreachable) as (
        _server,
        client,
        context,
        _hub,
    ):
        operator = Credential(context, "Operator")
        response = _create(client, operator)

        assert response.status == 201
        body = response.json()
        assert body["desired"]["enabled"] is True
        assert body["observed"]["state"] in {"unknown", "rejected"}
        assert body["drift"]["in_sync"] is False
        assert body["drift"]["reason"] == "collector_unavailable"
        assert body["last_error"]["code"] == "collector_unavailable"

        listing = client.get(
            FEED_PATH, headers=operator.headers(client=client, csrf=False)
        )
        assert len(listing.json()["items"]) == 1

        capabilities = client.get("/api/v1/admin/capabilities").json()
        adapters = {entry["name"]: entry for entry in capabilities["adapters"]}
        assert adapters["collector_control"]["state"] == "unavailable"
        assert capabilities["capabilities"]["feed_subscription"][
            "state"
        ] == "adapter_unavailable"


def test_restart_reloads_persisted_desired_and_redispatches(
    tmp_path: Path,
) -> None:
    port = _free_port()
    with _running_admin(tmp_path, control_port=port) as (
        _server,
        client,
        context,
        _hub,
    ):
        operator = Credential(context, "Operator")
        created = _create(client, operator)
        assert created.status == 201
        assert created.json()["drift"]["reason"] == "collector_unavailable"
        subscription_id = created.json()["subscription_id"]

    collector = FakeCollector(port=port)
    try:
        with _running_admin(tmp_path, collector=collector) as (
            _server,
            client,
            context,
            _hub,
        ):
            operator = Credential(context, "Operator")
            detail = client.get(
                f"{FEED_PATH}/{subscription_id}",
                headers=operator.headers(client=client, csrf=False),
            )
            assert detail.status == 200
            body = detail.json()
            assert body["desired"]["version"] == 1
            assert body["observed"]["state"] == "applied"
            assert body["observed"]["version"] == 1
            assert body["drift"]["in_sync"] is True
            assert collector.calls
            assert collector.calls[-1].subscriptions[0].subscription_id == (
                subscription_id
            )
    finally:
        collector.stop()


# -- flow control ------------------------------------------------------------


def test_the_two_flow_control_resources_are_independent_and_hot_applied(
    tmp_path: Path,
) -> None:
    collector = FakeCollector()
    try:
        with _running_admin(tmp_path, collector=collector) as (
            _server,
            client,
            context,
            hub,
        ):
            operator = Credential(context, "Operator")
            auditor = Credential(context, "Auditor")
            reading = client.get(
                FLOW_PATH,
                headers=operator.headers(client=client, csrf=False),
            )
            assert reading.status == 200
            flow = reading.json()
            assert set(flow) == {"api_rate_limit", "ws_slow_consumer"}
            assert flow["api_rate_limit"]["version"] == 1
            assert flow["api_rate_limit"]["apply_mode"] == "hot"
            assert flow["api_rate_limit"]["observed"] == {
                "version": 1,
                "state": "applied",
                "observed_at": flow["api_rate_limit"]["observed"][
                    "observed_at"
                ],
            }
            assert flow["api_rate_limit"]["drift"]["in_sync"] is True
            assert flow["ws_slow_consumer"]["version"] == 1
            assert flow["ws_slow_consumer"]["queue_size"] == 1000

            ws_update = client.put(
                f"{FLOW_PATH}/ws-slow-consumer",
                {
                    "enabled": True,
                    "queue_size": 7,
                    "lag_notice_interval_ms": 250,
                },
                headers={
                    **operator.headers(client=client),
                    "If-Match": '"1"',
                },
            )
            assert ws_update.status == 202
            ws_body = ws_update.json()
            assert ws_body["version"] == 2
            assert ws_body["queue_size"] == 7
            assert ws_body["observed"]["version"] == 2
            assert ws_body["observed"]["state"] == "applied"
            assert ws_body["drift"]["in_sync"] is True
            assert hub.queue_size == 7
            assert hub.lag_notice_interval_ms == 250

            stale = client.put(
                f"{FLOW_PATH}/ws-slow-consumer",
                {
                    "enabled": True,
                    "queue_size": 9,
                    "lag_notice_interval_ms": 100,
                },
                headers={
                    **operator.headers(client=client),
                    "If-Match": '"1"',
                },
            )
            assert stale.status == 409
            assert stale.json()["error"]["details"]["current_version"] == 2
            assert stale.json()["error"]["details"]["current"][
                "queue_size"
            ] == 7

            audit_write = client.put(
                f"{FLOW_PATH}/api-rate-limit",
                {
                    "enabled": True,
                    "requests_per_second": 1,
                    "burst": 0,
                    "max_ws_sessions_per_ip": 2,
                },
                headers={
                    **auditor.headers(client=client),
                    "If-Match": '"1"',
                },
            )
            assert audit_write.status == 403

            rate_update = client.put(
                f"{FLOW_PATH}/api-rate-limit",
                {
                    "enabled": True,
                    "requests_per_second": 1,
                    "burst": 0,
                    "max_ws_sessions_per_ip": 2,
                },
                headers={
                    **operator.headers(client=client),
                    "If-Match": '"1"',
                },
            )
            assert rate_update.status == 202
            rate_body = rate_update.json()
            assert rate_body["version"] == 2
            assert rate_body["max_ws_sessions_per_ip"] == 2
            assert rate_body["drift"]["in_sync"] is True

            # The API Rate Limit is really hot-applied; the WS resource was
            # not touched by the other update.
            limited = client.get("/api/v1/instruments")
            assert limited.status == 429
            assert limited.json()["error"]["code"] == "rate_limited"
            assert limited.header("Retry-After") is not None

            persisted = context.feed.flow_control.get()
            assert persisted["ws_slow_consumer"]["version"] == 2
            assert persisted["ws_slow_consumer"]["queue_size"] == 7
            assert persisted["api_rate_limit"]["version"] == 2

            assert _audit_actions(
                context, "flow_control.ws_slow_consumer.update"
            ) == ["requested", "succeeded"]
            assert _audit_actions(
                context, "flow_control.api_rate_limit.update"
            ) == ["requested", "succeeded"]
    finally:
        collector.stop()


def test_api_rate_limit_keys_on_the_trusted_proxy_resolver(
    tmp_path: Path,
) -> None:
    collector = FakeCollector()
    try:
        with _running_admin(
            tmp_path,
            collector=collector,
            trusted_proxies=(HOST,),
        ) as (_server, client, context, _hub):
            operator = Credential(context, "Operator")
            enabled = client.put(
                f"{FLOW_PATH}/api-rate-limit",
                {
                    "enabled": True,
                    "requests_per_second": 0.001,
                    "burst": 1,
                    "max_ws_sessions_per_ip": 4,
                },
                headers={
                    **operator.headers(client=client),
                    "If-Match": '"1"',
                },
            )
            assert enabled.status == 202

            def call(forwarded_for: str) -> Result:
                return client.get(
                    "/api/v1/instruments",
                    headers={"X-Forwarded-For": forwarded_for},
                )

            assert call("203.0.113.10").status == 200
            assert call("203.0.113.10").status == 429
            # A different resolved client behind the same trusted proxy has
            # its own bucket; X-Forwarded-For is not a combined threshold.
            assert call("203.0.113.11").status == 200
    finally:
        collector.stop()


def test_audit_unavailable_blocks_feed_and_flow_writes(tmp_path: Path) -> None:
    collector = FakeCollector()
    try:
        with _running_admin(tmp_path, collector=collector) as (
            _server,
            client,
            context,
            _hub,
        ):
            operator = Credential(context, "Operator")
            context.audit.close()

            blocked = _create(client, operator)
            assert blocked.status == 503
            assert blocked.json()["error"]["code"] == "audit_unavailable"
            assert context.feed.store.all_for_dispatch() == []

            blocked_flow = client.put(
                f"{FLOW_PATH}/api-rate-limit",
                {
                    "enabled": True,
                    "requests_per_second": 1,
                    "burst": 1,
                    "max_ws_sessions_per_ip": 1,
                },
                headers={
                    **operator.headers(client=client),
                    "If-Match": '"1"',
                },
            )
            assert blocked_flow.status == 503
            assert blocked_flow.json()["error"]["code"] == "audit_unavailable"
            assert (
                context.feed.flow_control.get()["api_rate_limit"]["version"]
                == 1
            )
    finally:
        collector.stop()


# -- idempotency and pagination ---------------------------------------------


def test_idempotency_key_replays_and_conflicts(tmp_path: Path) -> None:
    collector = FakeCollector()
    try:
        with _running_admin(tmp_path, collector=collector) as (
            _server,
            client,
            context,
            _hub,
        ):
            operator = Credential(context, "Operator")
            first = _create(client, operator, idempotency_key="create-1")
            assert first.status == 201

            replay = _create(client, operator, idempotency_key="create-1")
            assert replay.status == 201
            assert replay.json()["subscription_id"] == first.json()[
                "subscription_id"
            ]
            assert replay.header("Idempotent-Replay") == "true"
            assert len(context.feed.store.all_for_dispatch()) == 1

            reuse = _create(
                client,
                operator,
                instrument_id=ETH,
                idempotency_key="create-1",
            )
            assert reuse.status == 409
            assert reuse.json()["error"]["details"]["reason"] == (
                "idempotency_key_reuse"
            )
    finally:
        collector.stop()


def test_filters_keyset_pagination_and_bad_parameters(tmp_path: Path) -> None:
    collector = FakeCollector()
    try:
        with _running_admin(tmp_path, collector=collector) as (
            _server,
            client,
            context,
            _hub,
        ):
            operator = Credential(context, "Operator")
            for instrument_id, interval in (
                (BTC, "1m"),
                (BTC, "5m"),
                (ETH, "1h"),
            ):
                assert (
                    _create(
                        client,
                        operator,
                        instrument_id=instrument_id,
                        interval=interval,
                    ).status
                    == 201
                )

            page_one = client.get(
                f"{FEED_PATH}?limit=2",
                headers=operator.headers(client=client, csrf=False),
            )
            assert page_one.status == 200
            first_page = page_one.json()
            assert len(first_page["items"]) == 2
            assert first_page["page"]["has_more"] is True
            cursor = first_page["page"]["next_cursor"]
            assert cursor

            page_two = client.get(
                f"{FEED_PATH}?{urlencode({'limit': 2, 'cursor': cursor})}",
                headers=operator.headers(client=client, csrf=False),
            )
            assert page_two.status == 200
            second_page = page_two.json()
            assert len(second_page["items"]) == 1
            assert second_page["page"]["has_more"] is False
            seen = {
                item["subscription_id"]
                for item in first_page["items"] + second_page["items"]
            }
            assert len(seen) == 3

            filtered = client.get(
                f"{FEED_PATH}?{urlencode({'instrument_id': ETH})}",
                headers=operator.headers(client=client, csrf=False),
            )
            assert [item["instrument_id"] for item in filtered.json()["items"]] == [
                ETH
            ]

            by_interval = client.get(
                f"{FEED_PATH}?{urlencode({'interval': '5m'})}",
                headers=operator.headers(client=client, csrf=False),
            )
            assert [item["interval"] for item in by_interval.json()["items"]] == [
                "5m"
            ]

            bad_state = client.get(
                f"{FEED_PATH}?state=exploded",
                headers=operator.headers(client=client, csrf=False),
            )
            assert bad_state.status == 400
            assert bad_state.json()["error"]["details"]["field"] == "state"

            bad_cursor = client.get(
                f"{FEED_PATH}?cursor=not-a-cursor",
                headers=operator.headers(client=client, csrf=False),
            )
            assert bad_cursor.status == 400
            assert bad_cursor.json()["error"]["details"]["field"] == "cursor"

            bad_limit = client.get(
                f"{FEED_PATH}?limit=0",
                headers=operator.headers(client=client, csrf=False),
            )
            assert bad_limit.status == 400
            assert bad_limit.json()["error"]["code"] == "invalid_parameter"
    finally:
        collector.stop()
