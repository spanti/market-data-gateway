# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[multiple-with-statements]
"""Operator Audit Event query API over a real FastAPI app and socket.

The routes are read-only and authenticated for every role. The tests pin the
contract envelopes (items + opaque keyset page), every documented filter, the
Operation timeline ordering rule, the absence of edit/delete verbs, the
security-event bridge, and fail-closed high-risk behaviour (503, zero
execution) through the actual HTTP exception handlers.
"""

from __future__ import annotations

import http.client
import json
import re
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

from fastapi import APIRouter, Depends

from mdgw.admin.app import FEATURE_ROUTER_FACTORIES, register_feature
from mdgw.admin.audit.models import (
    RESULT_DENIED,
    RESULT_FAILED,
    RESULT_REQUESTED,
    RESULT_SUCCEEDED,
    AuditActor,
    AuditTarget,
)
from mdgw.admin.auth.dependencies import require_role
from mdgw.admin.auth.store import (  # ruff: ignore[typing-only-first-party-import]  # FastAPI resolves this annotation at runtime
    SessionRecord,
)
from mdgw.admin.context import AdminContext
from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.config import AdminAdaptersConfig, AdminConfig
from mdgw.core.time import ns_to_iso
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-admin-audit-api"
SECOND = 1_000_000_000
BASE_NS = 1_700_000_000 * SECOND
ROLES = ("SuperAdmin", "Operator", "Auditor")
NS_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z")

AUDIT_PATH = "/api/v1/admin/audit-events"


class Clock:
    def __init__(self, now_ns: int = BASE_NS) -> None:
        self.now_ns = now_ns

    def __call__(self) -> int:
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


@contextmanager
def _running_admin(
    tmp_path: Path,
    *,
    audit_store: bool = True,
    clock: Clock | None = None,
) -> Generator[tuple[AsgiServer, Client, AdminContext]]:
    config = AdminConfig(
        enabled=True,
        db_path=tmp_path / "admin" / "admin.db",
        adapters=AdminAdaptersConfig(
            oidc=False, audit_store=audit_store
        ),
    )
    store = ParquetBarStore(tmp_path / "data")
    context = AdminContext(
        config=config,
        storage_root=tmp_path,
        store=store,
        clock=clock,
    )
    app = create_app(store, admin=context)
    server = AsgiServer()
    try:
        server.start(app, host=HOST, port=0, thread_name=THREAD_NAME)
        yield server, Client(server), context
    finally:
        server.stop()
        store.close()


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


def _seed(  # ruff: ignore[too-many-arguments]  # explicit event fields
    context: AdminContext,
    *,
    index: int,
    action: str = "feed_subscription.update",
    subject: str = "subject-operator",
    access_role: str | None = "Operator",
    target_type: str = "feed_subscription",
    target_id: str = "fsub_1",
    result: str = RESULT_REQUESTED,
    request_id: str | None = None,
    operation_id: str | None = None,
    job_id: str | None = None,
    instance_id: str | None = None,
    failure_reason: str | None = None,
) -> Any:
    return context.audit.append(
        actor=AuditActor(
            subject=subject,
            display_name=f"{subject} display",
            access_role=access_role,
        ),
        action=action,
        target=AuditTarget(
            type=target_type,
            id=target_id,
            display_name=target_id,
        ),
        result=result,
        request_id=request_id or f"req_{index}",
        reason="seed",
        failure_reason=failure_reason,
        operation_id=operation_id,
        job_id=job_id,
        instance_id=instance_id,
        before={"value": index},
        after={"value": index + 1},
        occurred_at_ns=BASE_NS + index * SECOND,
    )


# -- authentication ---------------------------------------------------------


def test_anonymous_audit_queries_are_rejected_with_the_envelope(
    tmp_path: Path,
) -> None:
    with _running_admin(tmp_path) as (_server, client, _context):
        listing = client.get(AUDIT_PATH)
        detail = client.get(f"{AUDIT_PATH}/evt_missing")

    for response in (listing, detail):
        assert response.status == 401
        envelope = response.json()["error"]
        assert envelope["code"] == "unauthenticated"
        assert envelope["details"]["reason"] == "missing"
        assert envelope["request_id"] == response.header("X-Request-ID")


def test_every_role_can_read_audit_events(tmp_path: Path) -> None:
    with _running_admin(tmp_path) as (_server, client, context):
        _seed(context, index=0)
        for role in ROLES:
            credential = _credential(context, role)

            response = client.get(
                AUDIT_PATH, headers=_headers(credential)
            )

            assert response.status == 200, role
            assert response.json()["items"]


# -- listing, shape, filters ------------------------------------------------


def test_list_returns_the_contract_shape_and_no_store(
    tmp_path: Path,
) -> None:
    with _running_admin(tmp_path) as (_server, client, context):
        seeded = _seed(context, index=0)
        credential = _credential(context, "Auditor")

        response = client.get(AUDIT_PATH, headers=_headers(credential))

    assert response.status == 200
    assert response.header("Cache-Control") == "no-store"
    assert response.header("X-Request-ID")
    payload = response.json()
    assert set(payload) == {"items", "page"}
    assert payload["page"] == {"next_cursor": None, "has_more": False}
    (event,) = payload["items"]
    assert event == seeded.to_wire()
    assert set(event) == {
        "event_id",
        "sequence",
        "occurred_at",
        "actor",
        "action",
        "target",
        "result",
        "failure_reason",
        "reason",
        "request_id",
        "operation_id",
        "job_id",
        "instance_id",
        "diff",
        "hash_chain",
    }
    assert NS_TIMESTAMP.fullmatch(event["occurred_at"])
    assert event["hash_chain"]["algorithm"] == "sha256"
    assert len(event["hash_chain"]["event_hash"]) == 64


def test_documented_filters_are_applied_server_side(
    tmp_path: Path,
) -> None:
    with _running_admin(tmp_path) as (_server, client, context):
        _seed(context, index=0, action="auth.login", result=RESULT_DENIED,
              access_role=None, target_type="session", target_id="anonymous")
        _seed(
            context,
            index=1,
            subject="auditor-1",
            access_role="Auditor",
            action="export_job.create",
            target_type="export_job",
            target_id="exp_1",
            result=RESULT_SUCCEEDED,
            request_id="req_export",
            operation_id="op_export",
            job_id="exp_1",
            instance_id="api",
        )
        _seed(
            context,
            index=2,
            subject="operator-2",
            action="backfill_job.create",
            target_type="backfill_job",
            target_id="bf_1",
            result=RESULT_FAILED,
            failure_reason="control_unavailable",
            request_id="req_backfill",
            job_id="bf_1",
        )
        credential = _credential(context, "Auditor")

        def sequences(**filters: str) -> list[int]:
            query = urlencode({"limit": "50", **filters})
            response = client.get(
                f"{AUDIT_PATH}?{query}", headers=_headers(credential)
            )
            assert response.status == 200, response.body
            return [item["sequence"] for item in response.json()["items"]]

        assert sequences(actor_subject="auditor-1") == [2]
        assert sequences(access_role="Auditor") == [2]
        assert sequences(action="export_job.create") == [2]
        assert sequences(target_type="backfill_job") == [3]
        assert sequences(target_id="bf_1") == [3]
        assert sequences(result=RESULT_DENIED) == [1]
        assert sequences(request_id="req_backfill") == [3]
        assert sequences(operation_id="op_export") == [2]
        assert sequences(job_id="bf_1") == [3]
        assert sequences(instance_id="api") == [2]


def test_time_filter_is_utc_from_inclusive_to_exclusive(
    tmp_path: Path,
) -> None:
    with _running_admin(tmp_path) as (_server, client, context):
        _seed(context, index=0)
        _seed(context, index=1)
        _seed(context, index=2)
        credential = _credential(context, "Auditor")
        query = urlencode(
            {
                "from": ns_to_iso(BASE_NS + SECOND),
                "to": ns_to_iso(BASE_NS + 2 * SECOND),
            }
        )

        response = client.get(
            f"{AUDIT_PATH}?{query}", headers=_headers(credential)
        )

    assert response.status == 200
    assert [item["sequence"] for item in response.json()["items"]] == [2]


def test_invalid_filter_values_are_stable_invalid_parameter_errors(
    tmp_path: Path,
) -> None:
    with _running_admin(tmp_path) as (_server, client, context):
        credential = _credential(context, "Auditor")

        cases = [
            ({"from": "not-a-time"}, "from"),
            ({"to": "2023-13-99T99:99:99Z"}, "to"),
            ({"access_role": "Root"}, "access_role"),
            ({"result": "maybe"}, "result"),
            ({"order": "newest"}, "order"),
        ]
        for query, field in cases:
            response = client.get(
                f"{AUDIT_PATH}?{urlencode(query)}",
                headers=_headers(credential),
            )
            assert response.status == 400, (query, response.body)
            envelope = response.json()["error"]
            assert envelope["code"] == "invalid_parameter"
            assert envelope["details"]["field"] == field


# -- pagination / timeline --------------------------------------------------


def test_keyset_pagination_walks_all_events_without_duplicates(
    tmp_path: Path,
) -> None:
    with _running_admin(tmp_path) as (_server, client, context):
        for index in range(5):
            _seed(context, index=index)
        credential = _credential(context, "Auditor")

        first = client.get(
            f"{AUDIT_PATH}?limit=2", headers=_headers(credential)
        ).json()
        cursor = first["page"]["next_cursor"]
        second = client.get(
            f"{AUDIT_PATH}?limit=2&{urlencode({'cursor': cursor})}",
            headers=_headers(credential),
        ).json()
        third = client.get(
            f"{AUDIT_PATH}?limit=2"
            f"&{urlencode({'cursor': second['page']['next_cursor']})}",
            headers=_headers(credential),
        ).json()

    assert [item["sequence"] for item in first["items"]] == [5, 4]
    assert first["page"]["has_more"] is True
    assert isinstance(cursor, str)
    assert cursor
    assert [item["sequence"] for item in second["items"]] == [3, 2]
    assert second["page"]["has_more"] is True
    assert [item["sequence"] for item in third["items"]] == [1]
    assert third["page"] == {"next_cursor": None, "has_more": False}


def test_limit_and_cursor_validation_use_invalid_parameter(
    tmp_path: Path,
) -> None:
    with _running_admin(tmp_path) as (_server, client, context):
        credential = _credential(context, "Auditor")

        for value in ("0", "201", "abc", "1.5"):
            response = client.get(
                f"{AUDIT_PATH}?limit={value}", headers=_headers(credential)
            )
            assert response.status == 400
            assert response.json()["error"]["details"]["field"] == "limit"

        cursor = client.get(
            f"{AUDIT_PATH}?cursor=not-base64!!", headers=_headers(credential)
        )
        assert cursor.status == 400
        assert cursor.json()["error"]["details"]["field"] == "cursor"


def test_operation_timeline_defaults_to_ascending(
    tmp_path: Path,
) -> None:
    with _running_admin(tmp_path) as (_server, client, context):
        _seed(
            context,
            index=3,
            action="service_instance.start",
            result=RESULT_REQUESTED,
            operation_id="op_1",
            instance_id="api",
        )
        _seed(
            context,
            index=4,
            action="service_instance.start",
            result=RESULT_SUCCEEDED,
            operation_id="op_1",
            instance_id="api",
        )
        credential = _credential(context, "SuperAdmin")

        timeline = client.get(
            f"{AUDIT_PATH}?operation_id=op_1",
            headers=_headers(credential),
        )
        newest_first = client.get(
            f"{AUDIT_PATH}?operation_id=op_1&order=desc",
            headers=_headers(credential),
        )

    assert [item["result"] for item in timeline.json()["items"]] == [
        RESULT_REQUESTED,
        RESULT_SUCCEEDED,
    ]
    assert [item["result"] for item in newest_first.json()["items"]] == [
        RESULT_SUCCEEDED,
        RESULT_REQUESTED,
    ]


# -- detail / no mutation ---------------------------------------------------


def test_event_detail_and_unknown_id(tmp_path: Path) -> None:
    with _running_admin(tmp_path) as (_server, client, context):
        seeded = _seed(context, index=0)
        credential = _credential(context, "Auditor")

        found = client.get(
            f"{AUDIT_PATH}/{seeded.event_id}", headers=_headers(credential)
        )
        missing = client.get(
            f"{AUDIT_PATH}/evt_unknown", headers=_headers(credential)
        )

    assert found.status == 200
    assert found.json() == seeded.to_wire()
    assert missing.status == 404
    envelope = missing.json()["error"]
    assert envelope["code"] == "not_found"
    assert envelope["details"] == {
        "resource": "audit_event",
        "id": "evt_unknown",
    }


def test_there_is_no_edit_or_delete_api(tmp_path: Path) -> None:
    with _running_admin(tmp_path) as (_server, client, context):
        seeded = _seed(context, index=0)
        credential = _credential(context, "SuperAdmin")
        headers = _headers(credential)
        headers["Origin"] = client.origin

        for method in ("PUT", "PATCH", "DELETE"):
            response = client.request(
                method, f"{AUDIT_PATH}/{seeded.event_id}", headers=headers
            )
            assert response.status == 405, (method, response.body)
            assert response.json()["error"]["code"] == "method_not_allowed"

        post_collection = client.request(
            "POST", AUDIT_PATH, headers=headers
        )
        assert post_collection.status == 405


# -- security-event bridge --------------------------------------------------


@contextmanager
def _bridge_probe_routes() -> Generator[None]:
    executed: list[str] = []

    def factory(context: AdminContext) -> APIRouter:
        router = APIRouter()
        superadmin_guard = require_role(context, "SuperAdmin")

        @router.get("/audit-probe/superadmin")
        async def superadmin_probe(
            record: SessionRecord = Depends(superadmin_guard),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
        ) -> dict[str, str]:
            return {"role": record.access_role}

        @router.post("/audit-probe/write")
        async def write_probe() -> dict[str, bool]:
            executed.append("ran")
            return {"ok": True}

        return router

    register_feature(factory)
    try:
        yield
    finally:
        FEATURE_ROUTER_FACTORIES.remove(factory)


def test_auth_and_authz_failures_are_bridged_as_denied_events(
    tmp_path: Path,
) -> None:
    with _bridge_probe_routes():
        with _running_admin(tmp_path) as (_server, client, context):
            auditor = _credential(context, "Auditor")
            operator = _credential(context, "Operator")

            invalid = client.get(
                AUDIT_PATH,
                headers={"Cookie": "mdgw_session=not-a-session"},
            )
            assert invalid.status == 401

            denied_role = client.get(
                "/api/v1/admin/audit-probe/superadmin",
                headers=_headers(operator),
            )
            assert denied_role.status == 403

            denied_csrf = client.request(
                "POST",
                "/api/v1/admin/audit-probe/write",
                headers={**_headers(operator), "Origin": client.origin},
            )
            assert denied_csrf.status == 403
            assert denied_csrf.json()["error"]["details"]["reason"] == "csrf"

            def query(**filters: str) -> list[dict[str, Any]]:
                url = f"{AUDIT_PATH}?{urlencode({'limit': '50', **filters})}"
                response = client.get(url, headers=_headers(auditor))
                assert response.status == 200, response.body
                return response.json()["items"]

            login_failures = query(action="auth.login", result=RESULT_DENIED)
            role_denials = query(
                action="auth.role.denied", result=RESULT_DENIED
            )

    assert len(login_failures) == 1
    assert login_failures[0]["failure_reason"] == "invalid"
    assert login_failures[0]["actor"]["subject"] == "anonymous"
    assert login_failures[0]["target"]["type"] == "session"

    reasons = {item["failure_reason"] for item in role_denials}
    assert reasons == {"role", "csrf"}
    role_event = next(
        item for item in role_denials if item["failure_reason"] == "role"
    )
    assert role_event["actor"]["subject"] == operator.subject
    assert role_event["actor"]["access_role"] == "Operator"
    assert role_event["target"] == {
        "type": "access_role",
        "id": "Operator",
        "display_name": "SuperAdmin",
    }


# -- fail-closed ------------------------------------------------------------


@contextmanager
def _high_risk_probe_routes(
    executed: list[str],
) -> Generator[None]:
    def factory(context: AdminContext) -> APIRouter:
        router = APIRouter()

        @router.post("/audit-probe/high-risk")
        async def high_risk() -> dict[str, str]:
            handle = context.audit.begin(
                actor=AuditActor(
                    subject="subject-operator",
                    display_name="Operator User",
                    access_role="Operator",
                ),
                action="service_instance.start",
                target=AuditTarget(
                    type="service_instance", id="api", display_name="api"
                ),
                request_id="req_high_risk",
                reason="start api",
                operation_id="op_high_risk",
                instance_id="api",
                before={"desired_state": "stopped"},
                after={"desired_state": "running"},
            )
            executed.append("ran")
            handle.succeeded(after={"desired_state": "running"})
            return {"status": "accepted"}

        return router

    register_feature(factory)
    try:
        yield
    finally:
        FEATURE_ROUTER_FACTORIES.remove(factory)


def test_audit_unavailable_returns_503_and_executes_nothing(
    tmp_path: Path,
) -> None:
    executed: list[str] = []
    with _high_risk_probe_routes(executed):
        with _running_admin(tmp_path) as (_server, client, context):
            credential = _credential(context, "Operator")
            headers = {
                **_headers(credential, csrf=True),
                "Origin": client.origin,
            }

            allowed = client.request(
                "POST", "/api/v1/admin/audit-probe/high-risk", headers=headers
            )
            assert allowed.status == 200, allowed.body
            assert executed == ["ran"]

            context.audit.close()
            blocked = client.request(
                "POST", "/api/v1/admin/audit-probe/high-risk", headers=headers
            )

    assert blocked.status == 503
    envelope = blocked.json()["error"]
    assert envelope["code"] == "audit_unavailable"
    assert envelope["details"] == {
        "action": "service_instance.start",
        "target": {"type": "service_instance", "id": "api"},
        "phase": "requested",
        "executed": False,
    }
    assert executed == ["ran"]


# -- capability registration ------------------------------------------------


def test_capabilities_announce_operator_audit_when_enabled(
    tmp_path: Path,
) -> None:
    with _running_admin(tmp_path) as (_server, client, context):
        document = client.get("/api/v1/admin/capabilities").json()
        schema_version = context.db.schema_version

    adapters = {
        entry["name"]: entry["state"] for entry in document["adapters"]
    }
    assert adapters["audit_store"] == "available"
    assert document["capabilities"]["operator_audit"] == {
        "state": "enabled",
        "export": False,
        "edit": False,
        "delete": False,
    }
    # Later tickets add further migrations; this only pins that 003 ran.
    assert schema_version >= 3


def test_operator_audit_stays_unsupported_when_the_adapter_is_disabled(
    tmp_path: Path,
) -> None:
    with _running_admin(tmp_path, audit_store=False) as (
        _server,
        client,
        _context,
    ):
        document = client.get("/api/v1/admin/capabilities").json()

    adapters = {
        entry["name"]: entry["state"] for entry in document["adapters"]
    }
    assert adapters["audit_store"] == "unsupported"
    assert document["capabilities"]["operator_audit"] == {
        "state": "unsupported"
    }
