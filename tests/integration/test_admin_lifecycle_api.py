# ruff: file-ignore[implicit-namespace-package]
"""Service Instance lifecycle API over a real FastAPI app and loopback IPC.

The Supervisor is real: controlled fake child processes and an injected
readiness probe exercise start order, real readiness, drain/force, reload,
idempotency and unknown recovery without any real service process or network.
The API-side client, persistence and HTTP envelope are the production ones.
"""

from __future__ import annotations

import http.client
import json
import socket
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mdgw.admin.audit.models import AuditQuery
from mdgw.admin.audit.store import AuditStore
from mdgw.admin.audit.writer import AuditWriter
from mdgw.admin.context import AdminContext
from mdgw.admin.db import AdminStore
from mdgw.admin.lifecycle.audit import terminal_audit_sink
from mdgw.admin.lifecycle.models import NewOperation, OperationQuery
from mdgw.admin.lifecycle.store import LifecycleStore
from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.config import (
    AdminAdaptersConfig,
    AdminConfig,
    AdminControlConfig,
)
from mdgw.service.supervisor import ChildSpec, Supervisor
from mdgw.service.supervisor_control import SupervisorControlServer
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-admin-lifecycle-api"
TOKEN = "test-lifecycle-token"  # ruff: ignore[hardcoded-password-string]
TOKEN_ENV = "MDGW_TEST_SUPERVISOR_TOKEN"  # ruff: ignore[hardcoded-password-string]
ROLES = ("collector", "api", "replay")
INSTANCES = "/api/v1/admin/service-instances"
OPERATIONS = "/api/v1/admin/operations"
SECOND = 1_000_000_000
NOW = "2026-11-09T12:00:00.000000000Z"


class FakeProcess:
    _next_pid = 9000

    def __init__(self, role: str) -> None:
        FakeProcess._next_pid += 1
        self.role = role
        self.pid = FakeProcess._next_pid
        self.exit_code: int | None = None
        self.terminate_calls = 0

    def poll(self) -> int | None:
        return self.exit_code

    def terminate(self) -> None:
        self.terminate_calls += 1
        self.exit_code = 0

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return 0 if self.exit_code is None else self.exit_code


class World:
    def __init__(self) -> None:
        self.spawned: list[FakeProcess] = []
        self.signalled: list[FakeProcess] = []
        self.ready_urls: set[str] = set()
        self.exit_on_signal = True

    def spawn(self, spec: ChildSpec) -> FakeProcess:
        process = FakeProcess(spec.role)
        self.spawned.append(process)
        return process

    def probe(self, url: str) -> bool:
        return url in self.ready_urls

    def signal(self, process: FakeProcess) -> None:
        self.signalled.append(process)
        if self.exit_on_signal:
            process.exit_code = 0

    def ready_all(self) -> None:
        self.ready_urls.update(
            f"http://127.0.0.1/{role}/readyz" for role in ROLES
        )


def _specs(roles: tuple[str, ...]) -> list[ChildSpec]:
    return [
        ChildSpec(
            role=role,
            command=["fake", role],
            health_url=f"http://127.0.0.1/{role}/readyz",
            ready_timeout_s=3.0,
        )
        for role in roles
    ]


def _unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((HOST, 0))
        return int(probe.getsockname()[1])


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
            HOST, self.server.bound_port, timeout=20
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

    def post(
        self,
        path: str,
        payload: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Result:
        headers = dict(kwargs.pop("headers", {}))
        headers.setdefault("Content-Type", "application/json")
        body = json.dumps(payload).encode("utf-8") if payload is not None else b""
        return self.request("POST", path, headers=headers, body=body, **kwargs)


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


@dataclass
class Credential:
    token: str
    csrf: str
    subject: str
    role: str


def _credential(context: AdminContext, role: str) -> Credential:
    issued = context.auth.sessions.create(
        subject=f"subject-{role.lower()}",
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


def _headers(
    credential: Credential,
    client: Client,
    *,
    csrf: bool = True,
    idempotency_key: str | None = "idem-1",
) -> dict[str, str]:
    headers = {
        "Cookie": f"mdgw_session={credential.token}",
        "Origin": client.origin,
    }
    if csrf:
        headers["X-CSRF-Token"] = credential.csrf
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    return headers


@dataclass
class Stack:
    """One running admin API + Supervisor + loopback control channel."""

    client: Client
    context: AdminContext
    supervisor: Supervisor
    server: SupervisorControlServer
    store: LifecycleStore
    world: World
    db_path: Path
    stop: threading.Event
    supervisor_thread: threading.Thread


@contextmanager
def _stack(
    tmp_path: Path,
    *,
    roles: tuple[str, ...] = ROLES,
    run_supervisor: bool = True,
    control_port: int | None = None,
    supervisor_adapter: bool = True,
) -> Generator[Stack]:
    db_path = tmp_path / "admin" / "admin.db"
    AdminStore(db_path).close()
    store = LifecycleStore(db_path)
    audit = AuditWriter(AuditStore(db_path))
    world = World()
    supervisor = Supervisor(
        _specs(roles),
        spawn=world.spawn,
        probe=world.probe,
        restart_backoff_s=(0.01,),
        poll_interval_s=0.01,
        terminate_timeout_s=0.2,
        signal_child=world.signal,
        lifecycle=store,
        grace_s=0.3,
        on_terminal=terminal_audit_sink(audit),
    )
    control = SupervisorControlServer(
        supervisor, host=HOST, port=0, token=TOKEN
    )
    control.start()
    port = control.bound_port if control_port is None else control_port
    if run_supervisor:
        world.ready_all()
    config = AdminConfig(
        enabled=True,
        db_path=db_path,
        adapters=AdminAdaptersConfig(
            oidc=False, audit_store=True, supervisor=supervisor_adapter
        ),
        control=AdminControlConfig(
            host=HOST,
            port=port,
            token_env=TOKEN_ENV,
            grace_s=0.3,
            timeout_s=2.0,
        ),
    )
    context = AdminContext(
        config=config,
        storage_root=tmp_path,
        environ={TOKEN_ENV: TOKEN},
    )
    parquet = ParquetBarStore(tmp_path / "data")
    app = create_app(parquet, admin=context)
    http_server = AsgiServer()
    http_server.start(app, host=HOST, port=0, thread_name=THREAD_NAME)
    stop = threading.Event()
    thread = threading.Thread(
        target=supervisor.run, args=(stop,), daemon=True
    )
    if run_supervisor:
        thread.start()
        _wait_for(
            lambda: (
                len(store.list_instances()) == len(roles)
                and all(item.ready for item in store.list_instances())
            )
        )
    try:
        yield Stack(
            client=Client(http_server),
            context=context,
            supervisor=supervisor,
            server=control,
            store=store,
            world=world,
            db_path=db_path,
            stop=stop,
            supervisor_thread=thread,
        )
    finally:
        stop.set()
        if thread.is_alive():
            thread.join(timeout=5)
        http_server.stop()
        context.close()
        control.stop()
        audit.close()
        store.close()
        parquet.close()


def _wait_for(predicate, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    msg = "condition was not met before the deadline"
    raise AssertionError(msg)


def _wait_terminal(
    client: Client, credential: Credential, operation_id: str
) -> dict[str, Any]:
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        response = client.get(
            f"{OPERATIONS}/{operation_id}",
            headers={"Cookie": f"mdgw_session={credential.token}"},
        )
        assert response.status == 200, response.body
        operation = response.json()
        if operation["status"] in {"succeeded", "failed", "unknown"}:
            return operation
        time.sleep(0.02)
    msg = "operation did not reach a terminal state"
    raise AssertionError(msg)


# -- read + RBAC ------------------------------------------------------------


def test_service_instances_are_readable_by_every_role(
    tmp_path: Path,
) -> None:
    with _stack(tmp_path) as stack:
        for role in ("SuperAdmin", "Operator", "Auditor"):
            credential = _credential(stack.context, role)
            response = stack.client.get(
                INSTANCES,
                headers={"Cookie": f"mdgw_session={credential.token}"},
            )
            assert response.status == 200, role
            payload = response.json()
            assert set(payload) == {"items", "page"}
            assert payload["page"] == {
                "next_cursor": None,
                "has_more": False,
            }
            assert [item["instance_id"] for item in payload["items"]] == [
                "collector",
                "api",
                "replay",
            ]
            api = next(
                item for item in payload["items"] if item["instance_id"] == "api"
            )
            assert set(api) == {
                "instance_id",
                "service_role",
                "display_name",
                "desired_state",
                "observed_state",
                "health",
                "ready",
                "ready_reason",
                "pid",
                "started_at",
                "last_observed_at",
                "last_error",
                "available_actions",
            }
            assert api["ready"] is True
            assert api["available_actions"] == ["stop", "reload"]
            assert api["pid"] is not None


def test_lifecycle_mutations_are_superadmin_only(tmp_path: Path) -> None:
    with _stack(tmp_path, run_supervisor=False) as stack:
        for role in ("Operator", "Auditor"):
            credential = _credential(stack.context, role)
            response = stack.client.post(
                f"{INSTANCES}/api/start",
                {"reason": "start", "confirm": True},
                headers=_headers(credential, stack.client),
            )
            assert response.status == 403, role
            assert response.json()["error"]["details"]["reason"] == "role"

        superadmin = _credential(stack.context, "SuperAdmin")
        stack.world.ready_all()
        allowed = stack.client.post(
            f"{INSTANCES}/api/start",
            {"reason": "start", "confirm": True},
            headers=_headers(superadmin, stack.client),
        )
        assert allowed.status == 202, allowed.body
        assert allowed.json()["status"] == "accepted"


# -- validation / CSRF / idempotency ---------------------------------------


def test_csrf_and_action_confirmation_are_enforced(tmp_path: Path) -> None:
    with _stack(tmp_path, run_supervisor=False) as stack:
        credential = _credential(stack.context, "SuperAdmin")

        no_csrf = stack.client.post(
            f"{INSTANCES}/api/start",
            {"reason": "start", "confirm": True},
            headers=_headers(credential, stack.client, csrf=False),
        )
        assert no_csrf.status == 403
        assert no_csrf.json()["error"]["details"]["reason"] == "csrf"

        no_key = stack.client.post(
            f"{INSTANCES}/api/start",
            {"reason": "start", "confirm": True},
            headers=_headers(credential, stack.client, idempotency_key=None),
        )
        assert no_key.status == 400
        assert no_key.json()["error"]["details"]["field"] == "Idempotency-Key"

        no_confirm = stack.client.post(
            f"{INSTANCES}/api/start",
            {"reason": "start", "confirm": False},
            headers=_headers(credential, stack.client),
        )
        assert no_confirm.status == 400
        assert no_confirm.json()["error"]["details"]["field"] == "confirm"

        no_name = stack.client.post(
            f"{INSTANCES}/api/stop",
            {"reason": "stop", "confirm": True},
            headers=_headers(credential, stack.client),
        )
        assert no_name.status == 400
        assert (
            no_name.json()["error"]["details"]["field"]
            == "confirm_instance_name"
        )

        empty_reason = stack.client.post(
            f"{INSTANCES}/api/start",
            {"reason": "   ", "confirm": True},
            headers=_headers(credential, stack.client),
        )
        assert empty_reason.status == 400
        assert empty_reason.json()["error"]["details"]["field"] == "reason"


def test_idempotent_replay_and_key_reuse(tmp_path: Path) -> None:
    with _stack(tmp_path, run_supervisor=False) as stack:
        credential = _credential(stack.context, "SuperAdmin")
        stack.world.ready_all()
        headers = _headers(credential, stack.client, idempotency_key="key-1")
        body = {"reason": "start api", "confirm": True}

        first = stack.client.post(f"{INSTANCES}/api/start", body, headers=headers)
        assert first.status == 202
        assert first.header("Idempotent-Replay") is None
        operation_id = first.json()["operation_id"]

        replay = stack.client.post(
            f"{INSTANCES}/api/start", body, headers=headers
        )
        assert replay.status == 202
        assert replay.header("Idempotent-Replay") == "true"
        assert replay.json()["operation_id"] == operation_id

        reuse = stack.client.post(
            f"{INSTANCES}/api/start",
            {"reason": "different reason", "confirm": True},
            headers=headers,
        )
        assert reuse.status == 409
        assert (
            reuse.json()["error"]["details"]["reason"]
            == "idempotency_key_reuse"
        )
        assert stack.store.get_operation(operation_id).status != "unknown"


def test_unknown_instance_and_operation_are_404(tmp_path: Path) -> None:
    with _stack(tmp_path, run_supervisor=False) as stack:
        credential = _credential(stack.context, "SuperAdmin")
        response = stack.client.post(
            f"{INSTANCES}/database/start",
            {"reason": "start", "confirm": True},
            headers=_headers(credential, stack.client),
        )
        assert response.status == 404
        assert response.json()["error"]["details"] == {
            "resource": "service_instance",
            "id": "database",
        }

        missing = stack.client.get(
            f"{OPERATIONS}/op_missing",
            headers={"Cookie": f"mdgw_session={credential.token}"},
        )
        assert missing.status == 404
        assert missing.json()["error"]["details"] == {
            "resource": "operation",
            "id": "op_missing",
        }


# -- start / stop / reload over the real control channel --------------------


def test_start_waits_for_real_readiness_and_records_the_operation(
    tmp_path: Path,
) -> None:
    with _stack(tmp_path, roles=("api",), run_supervisor=False) as stack:
        credential = _credential(stack.context, "SuperAdmin")
        response = stack.client.post(
            f"{INSTANCES}/api/start",
            {"reason": "start api", "confirm": True},
            headers=_headers(credential, stack.client),
        )
        assert response.status == 202, response.body
        operation_id = response.json()["operation_id"]
        assert response.header("Location") == f"{OPERATIONS}/{operation_id}"

        _wait_for(lambda: stack.world.spawned)
        snapshot = stack.store.get_instance("api")
        assert snapshot.pid is not None
        assert snapshot.ready is False
        operation = stack.store.get_operation(operation_id)
        assert operation.status in {"accepted", "running"}

        stack.world.ready_urls.add("http://127.0.0.1/api/readyz")
        finished = _wait_terminal(stack.client, credential, operation_id)

        assert finished["status"] == "succeeded"
        assert finished["result"]["ready"] is True
        assert finished["result"]["observed_state"] == "running"
        registry = stack.client.get(
            INSTANCES,
            headers={"Cookie": f"mdgw_session={credential.token}"},
        ).json()
        assert registry["items"][0]["ready"] is True
        assert registry["items"][0]["available_actions"] == ["stop", "reload"]

        events = stack.context.audit.store.query(AuditQuery(limit=50))
        results = [
            event.result
            for event in events
            if event.operation_id == operation_id
        ]
        assert results == ["succeeded", "requested"]


def test_stop_drains_and_records_forced_false(tmp_path: Path) -> None:
    with _stack(tmp_path) as stack:
        credential = _credential(stack.context, "SuperAdmin")
        response = stack.client.post(
            f"{INSTANCES}/replay/stop",
            {
                "reason": "stop replay",
                "confirm": True,
                "confirm_instance_name": "replay",
            },
            headers=_headers(credential, stack.client),
        )
        assert response.status == 202, response.body
        operation_id = response.json()["operation_id"]

        finished = _wait_terminal(stack.client, credential, operation_id)

        assert finished["status"] == "succeeded"
        assert finished["result"]["forced"] is False
        assert finished["result"]["drained"] is True
        assert finished["result"]["timed_out"] is False
        assert stack.store.get_instance("replay").observed_state == "stopped"
        assert stack.world.signalled


def test_api_reload_returns_202_then_succeeds_as_an_interrupting_restart(
    tmp_path: Path,
) -> None:
    with _stack(tmp_path) as stack:
        credential = _credential(stack.context, "SuperAdmin")
        before = len(stack.world.spawned)
        response = stack.client.post(
            f"{INSTANCES}/api/reload",
            {
                "reason": "reload api config",
                "confirm": True,
                "confirm_instance_name": "api",
            },
            headers=_headers(credential, stack.client),
        )
        assert response.status == 202, response.body
        operation_id = response.json()["operation_id"]

        finished = _wait_terminal(stack.client, credential, operation_id)

        assert finished["status"] == "succeeded"
        assert finished["result"]["interrupting_restart"] is True
        assert finished["result"]["ready"] is True
        assert len(stack.world.spawned) == before + 1
        assert stack.store.get_instance("api").observed_state == "running"


# -- operation queries ------------------------------------------------------


def test_operations_list_filters_and_pages(tmp_path: Path) -> None:
    with _stack(tmp_path, run_supervisor=False) as stack:
        credential = _credential(stack.context, "Auditor")
        for index, kind in enumerate(
            (
                "service_instance.start",
                "service_instance.stop",
                "service_instance.reload",
            )
        ):
            stack.store.create_operation(
                NewOperation(
                    operation_id=f"op_{index}",
                    kind=kind,
                    instance_id="api",
                    service_role="api",
                    actor_subject="subject-superadmin",
                    actor_display_name="Super Admin",
                    actor_access_role="SuperAdmin",
                    reason="seed",
                    request_id=f"req_{index}",
                    requested_at_ns=1_700_000_000 * SECOND + index * SECOND,
                )
            )
        headers = {"Cookie": f"mdgw_session={credential.token}"}

        everything = stack.client.get(
            f"{OPERATIONS}?limit=2", headers=headers
        ).json()
        assert [item["operation_id"] for item in everything["items"]] == [
            "op_2",
            "op_1",
        ]
        assert everything["page"]["has_more"] is True

        rest = stack.client.get(
            f"{OPERATIONS}?limit=2"
            f"&cursor={everything['page']['next_cursor']}",
            headers=headers,
        ).json()
        assert [item["operation_id"] for item in rest["items"]] == ["op_0"]
        assert rest["page"] == {"next_cursor": None, "has_more": False}

        only_reload = stack.client.get(
            f"{OPERATIONS}?kind=service_instance.reload", headers=headers
        ).json()
        assert [item["operation_id"] for item in only_reload["items"]] == [
            "op_2"
        ]

        invalid = stack.client.get(
            f"{OPERATIONS}?status=maybe", headers=headers
        )
        assert invalid.status == 400
        assert invalid.json()["error"]["details"]["field"] == "status"


# -- control unavailable / audit fail-closed / unknown recovery ------------


def test_unreachable_supervisor_is_503_before_acceptance_and_audited(
    tmp_path: Path,
) -> None:
    with _stack(
        tmp_path,
        run_supervisor=False,
        control_port=_unused_port(),
    ) as stack:
        credential = _credential(stack.context, "SuperAdmin")
        response = stack.client.post(
            f"{INSTANCES}/api/start",
            {"reason": "start api", "confirm": True},
            headers=_headers(credential, stack.client),
        )
        assert response.status == 503
        error = response.json()["error"]
        assert error["code"] == "control_unavailable"
        assert error["details"]["source"] == "supervisor"
        assert stack.store.list_instances() == [] or all(
            item.observed_state == "stopped"
            for item in stack.store.list_instances()
        )

        events = stack.context.audit.store.query(
            AuditQuery(action="service_instance.start", limit=50)
        )
        assert [event.result for event in events] == ["failed", "requested"]
        assert events[0].failure_reason == "control_unavailable"


def test_audit_unavailable_blocks_execution(tmp_path: Path) -> None:
    with _stack(tmp_path) as stack:
        credential = _credential(stack.context, "SuperAdmin")
        spawned_before = len(stack.world.spawned)
        operations_before = len(
            stack.store.query_operations(OperationQuery(limit=200))
        )

        stack.context.audit.close()
        response = stack.client.post(
            f"{INSTANCES}/api/reload",
            {
                "reason": "reload api",
                "confirm": True,
                "confirm_instance_name": "api",
            },
            headers=_headers(credential, stack.client),
        )

        assert response.status == 503
        assert response.json()["error"]["code"] == "audit_unavailable"
        assert len(stack.world.spawned) == spawned_before
        assert (
            len(stack.store.query_operations(OperationQuery(limit=200)))
            == operations_before
        )


def test_stale_operations_are_unknown_after_supervisor_restart(
    tmp_path: Path,
) -> None:
    with _stack(tmp_path, run_supervisor=False) as stack:
        stack.store.create_operation(
            NewOperation(
                operation_id="op_stale",
                kind="service_instance.reload",
                instance_id="api",
                service_role="api",
                actor_subject="subject-superadmin",
                actor_display_name="Super Admin",
                actor_access_role="SuperAdmin",
                reason="reload",
                request_id="req_stale",
                requested_at_ns=1_700_000_000 * SECOND,
            )
        )
        stack.store.start_operation(
            "op_stale", now_ns=1_700_000_001 * SECOND
        )
        credential = _credential(stack.context, "Auditor")

        # A new Supervisor process is the only lifecycle executor.
        Supervisor(
            _specs(ROLES),
            spawn=stack.world.spawn,
            probe=stack.world.probe,
            lifecycle=stack.store,
        )

        response = stack.client.get(
            f"{OPERATIONS}/op_stale",
            headers={"Cookie": f"mdgw_session={credential.token}"},
        )
        operation = response.json()
        assert operation["status"] == "unknown"
        assert operation["error"]["code"] == "interrupted"


def test_capabilities_announce_service_lifecycle(tmp_path: Path) -> None:
    with _stack(tmp_path, run_supervisor=False) as stack:
        document = stack.client.get("/api/v1/admin/capabilities").json()
        adapters = {
            entry["name"]: entry["state"]
            for entry in document["adapters"]
        }
        assert adapters["supervisor"] == "available"
        assert document["capabilities"]["service_lifecycle"] == {
            "state": "enabled",
            "actions": ["start", "stop", "reload"],
        }
    with _stack(
        tmp_path / "disabled", run_supervisor=False, supervisor_adapter=False
    ) as disabled:
        document = disabled.client.get(
            "/api/v1/admin/capabilities"
        ).json()
        adapters = {
            entry["name"]: entry["state"]
            for entry in document["adapters"]
        }
        assert adapters["supervisor"] == "unsupported"
        assert document["capabilities"]["service_lifecycle"] == {
            "state": "unsupported"
        }
