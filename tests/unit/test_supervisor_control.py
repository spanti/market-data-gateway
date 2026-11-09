# ruff: file-ignore[implicit-namespace-package]
"""Loopback, token-authenticated Supervisor control channel.

The envelope module (ticket 01) owns schema validation; this module proves the
transport adds only authentication and framing, rejects every malformed or
non-allow-listed message before the Supervisor sees it, and never needs an
arbitrary command/path/shell field.
"""

from __future__ import annotations

import json
import socket
import time
from typing import TYPE_CHECKING, Any

import pytest

from mdgw.admin.db import AdminStore
from mdgw.admin.lifecycle.control_client import (
    SupervisorControlClient,
    SupervisorControlError,
    SupervisorUnavailableError,
)
from mdgw.admin.lifecycle.store import LifecycleStore
from mdgw.service.supervisor import ChildSpec, Supervisor
from mdgw.service.supervisor_control import (
    CONTROL_ACTION_OPERATION_STATUS,
    CONTROL_ACTION_RELOAD,
    CONTROL_ACTION_START,
    CONTROL_ACTION_STATUS,
    SupervisorControlServer,
    lifecycle_registry,
)

if TYPE_CHECKING:
    from pathlib import Path

HOST = "127.0.0.1"
TOKEN = "test-control-token"  # ruff: ignore[hardcoded-password-string]
NOW = "2026-11-09T12:00:00.000000000Z"
SECOND = 1_000_000_000
BASE_NS = 1_700_000_000 * SECOND


class FakeProcess:
    _next_pid = 7000

    def __init__(self) -> None:
        FakeProcess._next_pid += 1
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


def _store(tmp_path: Path) -> LifecycleStore:
    path = tmp_path / "admin" / "admin.db"
    AdminStore(path).close()
    return LifecycleStore(path)


def _supervisor(tmp_path: Path) -> tuple[Supervisor, LifecycleStore]:
    store = _store(tmp_path)
    supervisor = Supervisor(
        [
            ChildSpec(
                role="api",
                command=["fake"],
                health_url="http://127.0.0.1/api/readyz",
                ready_timeout_s=2.0,
            )
        ],
        spawn=lambda _spec: FakeProcess(),
        probe=lambda _url: True,
        lifecycle=store,
        clock=lambda: BASE_NS,
    )
    return supervisor, store


def _server(
    tmp_path: Path,
    *,
    token: str = TOKEN,
    max_message_bytes: int = 65_536,
) -> tuple[SupervisorControlServer, LifecycleStore]:
    supervisor, store = _supervisor(tmp_path)
    server = SupervisorControlServer(
        supervisor,
        host=HOST,
        port=0,
        token=token,
        max_message_bytes=max_message_bytes,
    )
    server.start()
    return server, store


def _message(
    action: str,
    payload: dict[str, Any] | None = None,
    *,
    correlation_id: str = "req_control",
    target: str = "supervisor",
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "message_id": "msg_control",
        "correlation_id": correlation_id,
        "target": target,
        "action": action,
        "sent_at": NOW,
        "timeout_ms": 5_000,
        "payload": dict(payload or {}),
    }


def _call_raw(
    port: int,
    token: str,
    raw: bytes,
    *,
    timeout: float = 5.0,
) -> dict[str, Any]:
    with socket.create_connection((HOST, port), timeout=timeout) as sock:
        sock.sendall(token.encode("utf-8") + b"\n")
        sock.sendall(raw)
        response = _read_line(sock, 1_000_000)
    return json.loads(response)


def _call(
    port: int,
    action: str,
    payload: dict[str, Any] | None = None,
    *,
    token: str = TOKEN,
) -> dict[str, Any]:
    return _call_raw(
        port,
        token,
        json.dumps(_message(action, payload)).encode("utf-8") + b"\n",
    )


def _read_line(sock: socket.socket, limit: int) -> bytes:
    buffer = b""
    while b"\n" not in buffer:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buffer += chunk
        if len(buffer) > limit:
            break
    return buffer.split(b"\n", 1)[0]


def _start_payload(**overrides: Any) -> dict[str, Any]:
    payload = {
        "instance_id": "api",
        "operation_id": "op_control",
        "request_id": "req_control",
        "reason": "start api",
        "actor": {
            "subject": "subject-superadmin",
            "display_name": "Super Admin",
            "access_role": "SuperAdmin",
        },
    }
    payload.update(overrides)
    return payload


def test_a_valid_action_is_accepted_and_executes_asynchronously(
    tmp_path: Path,
) -> None:
    server, store = _server(tmp_path)
    try:
        response = _call(
            server.bound_port, CONTROL_ACTION_START, _start_payload()
        )

        assert response["ok"] is True
        assert response["error"] is None
        assert response["operation"]["operation_id"] == "op_control"
        assert response["operation"]["status"] == "accepted"
        assert response["correlation_id"] == "req_control"

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            operation = store.get_operation("op_control")
            if operation is not None and operation.status == "succeeded":
                break
            time.sleep(0.01)
        assert store.get_operation("op_control").status == "succeeded"
    finally:
        server.stop()
        store.close()


def test_a_wrong_token_is_rejected_before_validation(tmp_path: Path) -> None:
    server, store = _server(tmp_path)
    try:
        response = _call(
            server.bound_port,
            CONTROL_ACTION_START,
            _start_payload(),
            token="wrong-token",  # ruff: ignore[hardcoded-password-func-arg]
        )
        status = _call(server.bound_port, CONTROL_ACTION_STATUS)

        assert response["ok"] is False
        assert response["error"]["code"] == "unauthorized"
        assert store.get_operation("op_control") is None
        assert status["ok"] is True
    finally:
        server.stop()
        store.close()


@pytest.mark.parametrize(
    ("action", "payload", "error_code"),
    [
        ("shell.exec", {}, "action_not_allowed"),
        ("service.start", {"command": "rm -rf /"}, "invalid_message"),
        (
            "service.start",
            _start_payload(instance_id="database"),
            "invalid_message",
        ),
        (
            "service.start",
            _start_payload(extra="field"),
            "invalid_message",
        ),
    ],
)
def test_non_allowlisted_or_malformed_messages_are_rejected(
    tmp_path: Path,
    action: str,
    payload: dict[str, Any],
    error_code: str,
) -> None:
    server, store = _server(tmp_path)
    try:
        if action == "shell.exec":
            raw = json.dumps(_message(action, payload)).encode("utf-8")
            response = _call_raw(server.bound_port, TOKEN, raw + b"\n")
        else:
            response = _call(server.bound_port, action, payload)
        assert response["ok"] is False
        assert response["error"]["code"] == error_code
        deadline = time.monotonic() + 0.2
        while time.monotonic() < deadline and store.get_operation("op_control"):
            time.sleep(0.01)
        assert store.get_operation("op_control") is None
    finally:
        server.stop()
        store.close()


def test_lifecycle_actions_are_not_allowlisted_for_the_collector_target(
    tmp_path: Path,
) -> None:
    server, store = _server(tmp_path)
    try:
        response = _call_raw(
            server.bound_port,
            TOKEN,
            json.dumps(
                _message(
                    CONTROL_ACTION_START,
                    _start_payload(),
                    target="collector",
                )
            ).encode("utf-8")
            + b"\n",
        )
        assert response["ok"] is False
        assert response["error"]["code"] == "action_not_allowed"
    finally:
        server.stop()
        store.close()


def test_oversized_messages_are_rejected_before_parsing(
    tmp_path: Path,
) -> None:
    server, store = _server(tmp_path, max_message_bytes=256)
    try:
        oversized = (
            json.dumps(_message(CONTROL_ACTION_START, {"pad": "x" * 400}))
            .encode("utf-8")
            + b"\n"
        )
        response = _call_raw(server.bound_port, TOKEN, oversized)

        assert response["ok"] is False
        assert response["error"]["code"] == "message_too_large"
    finally:
        server.stop()
        store.close()


def test_a_repeated_key_returns_the_original_operation_without_a_new_one(
    tmp_path: Path,
) -> None:
    supervisor, store = _supervisor(tmp_path)
    server = SupervisorControlServer(
        supervisor, host=HOST, port=0, token=TOKEN
    )
    server.start()
    try:
        body_hash = "a" * 64
        payload = _start_payload(
            operation_id="op_first",
            idempotency_key="key-repeat",
            idempotency_body_hash=body_hash,
        )
        first = _call(server.bound_port, CONTROL_ACTION_START, payload)
        assert first["ok"] is True
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            operation = store.get_operation("op_first")
            if operation is not None and operation.status == "succeeded":
                break
            time.sleep(0.01)

        repeat = _call(
            server.bound_port,
            CONTROL_ACTION_START,
            {
                **payload,
                "operation_id": "op_second",
            },
        )
        assert repeat["ok"] is True
        assert repeat["operation"]["operation_id"] == "op_first"
        assert store.get_operation("op_second") is None
    finally:
        server.stop()
        store.close()


def test_client_disconnect_after_acceptance_does_not_cancel_the_operation(
    tmp_path: Path,
) -> None:
    server, store = _server(tmp_path)
    try:
        with socket.create_connection(
            (HOST, server.bound_port), timeout=5
        ) as sock:
            sock.sendall(TOKEN.encode("utf-8") + b"\n")
            sock.sendall(
                json.dumps(
                    _message(CONTROL_ACTION_START, _start_payload())
                ).encode("utf-8")
                + b"\n"
            )
            # Drop the connection without reading the accepted response.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            operation = store.get_operation("op_control")
            if operation is not None and operation.status == "succeeded":
                break
            time.sleep(0.01)
        assert store.get_operation("op_control").status == "succeeded"
    finally:
        server.stop()
        store.close()


def test_status_and_operation_status_are_read_only(tmp_path: Path) -> None:
    server, store = _server(tmp_path)
    try:
        status = _call(server.bound_port, CONTROL_ACTION_STATUS)
        assert status["ok"] is True
        instances = status["result"]["instances"]
        assert [item["instance_id"] for item in instances] == ["api"]
        assert instances[0]["observed_state"] == "stopped"
        assert instances[0]["available_actions"] == ["start"]

        missing = _call(
            server.bound_port,
            CONTROL_ACTION_OPERATION_STATUS,
            {"operation_id": "op_missing"},
        )
        assert missing["ok"] is True
        assert missing["result"]["operation"] is None
    finally:
        server.stop()
        store.close()


def test_the_registry_has_no_generic_command_action() -> None:
    allowed = lifecycle_registry().allowed_for("supervisor")
    assert set(allowed) == {
        "ping",
        "service.start",
        "service.stop",
        "service.reload",
        "service.status",
        CONTROL_ACTION_OPERATION_STATUS,
    }
    assert CONTROL_ACTION_RELOAD == "service.reload"


def test_the_server_refuses_a_non_loopback_bind() -> None:
    with pytest.raises(ValueError, match="loopback"):
        SupervisorControlServer(
            object(),  # type: ignore[arg-type]
            host="0.0.0.0",  # ruff: ignore[hardcoded-bind-all-interfaces]
            port=0,
            token=TOKEN,
        )
    with pytest.raises(ValueError, match="token"):
        SupervisorControlServer(
            object(),  # type: ignore[arg-type]
            host=HOST,
            port=0,
            token="",
        )


# -- API-side client --------------------------------------------------------


def test_client_accepts_the_matching_token_and_reads_the_result(
    tmp_path: Path,
) -> None:
    server, store = _server(tmp_path)
    try:
        client = SupervisorControlClient(
            host=HOST, port=server.bound_port, token=TOKEN
        )
        response = client.request(
            CONTROL_ACTION_STATUS, correlation_id="req_status"
        )
        assert response["ok"] is True
        assert response["result"]["instances"][0]["instance_id"] == "api"
    finally:
        server.stop()
        store.close()


def test_client_maps_a_rejected_request_to_a_control_error(
    tmp_path: Path,
) -> None:
    server, store = _server(tmp_path)
    try:
        client = SupervisorControlClient(
            host=HOST,
            port=server.bound_port,
            token="wrong",  # ruff: ignore[hardcoded-password-func-arg]
        )
        with pytest.raises(SupervisorControlError) as raised:
            client.request(CONTROL_ACTION_STATUS)
        assert raised.value.code == "unauthorized"
    finally:
        server.stop()
        store.close()


def test_client_reports_an_unreachable_endpoint() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((HOST, 0))
        port = probe.getsockname()[1]
    client = SupervisorControlClient(host=HOST, port=port, token=TOKEN)
    with pytest.raises(SupervisorUnavailableError):
        client.request(CONTROL_ACTION_STATUS, correlation_id="req_down")
