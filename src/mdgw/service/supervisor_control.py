"""Loopback, token-authenticated control server owned by the Supervisor.

Windows cannot bind ZMQ ``ipc://`` endpoints, so the control transport is a
plain TCP server bound to a loopback address. Every connection sends:

1. one line containing the shared control token (compared in constant time
   and never logged);
2. one line containing a bounded JSON message in the ticket 01 envelope.

The server validates schema, target, action allowlist, payload schema, timeout
and byte size *before* the Supervisor sees anything. Accepted lifecycle
actions are answered immediately with the persisted ``accepted`` Operation and
executed asynchronously, so the API can return 202 even if the caller
disconnects and the API is itself the instance being restarted.

The response is one JSON line:
``{"schema_version", "correlation_id", "ok", "operation", "result", "error"}``.
"""

from __future__ import annotations

import hmac
import ipaddress
import json
import logging
import socketserver
import threading
from typing import TYPE_CHECKING, Any

from mdgw.admin.control import (
    ActionNotAllowedError,
    ControlError,
    ControlMessageTooLargeError,
    ControlValidationError,
    validate_control_message,
)
from mdgw.admin.lifecycle.protocol import (
    ACTION_BY_CONTROL,
    CONTROL_ACTION_OPERATION_STATUS,
    CONTROL_ACTION_PING,
    CONTROL_ACTION_RELOAD,
    CONTROL_ACTION_START,
    CONTROL_ACTION_STATUS,
    CONTROL_ACTION_STOP,
    OperationStatusPayload,
    ServiceActionPayload,
    StatusPayload,
    error_response,
    lifecycle_registry,
    success_response,
)
from mdgw.service.supervisor import LifecycleRejectedError, Supervisor

if TYPE_CHECKING:
    from mdgw.admin.control import ControlEnvelope

logger = logging.getLogger(__name__)

_MAX_AUTH_BYTES = 512
_DEFAULT_REQUEST_TIMEOUT_S = 5.0
_LOOPBACK_HOSTS = frozenset({"localhost"})
_UNKNOWN_CORRELATION = "unknown"


class SupervisorControlServer:
    """Serve the allow-listed Supervisor actions on a loopback TCP port."""

    def __init__(  # ruff: ignore[too-many-arguments]  # explicit transport policy
        self,
        supervisor: Supervisor,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        token: str,
        max_message_bytes: int = 65_536,
        request_timeout_s: float = _DEFAULT_REQUEST_TIMEOUT_S,
    ) -> None:
        _require_loopback(host)
        if not isinstance(token, str) or not token:
            msg = "a non-empty control token is required"
            raise ValueError(msg)
        if max_message_bytes <= 0:
            msg = "max_message_bytes must be positive"
            raise ValueError(msg)
        self._supervisor = supervisor
        self._host = host
        self._port = int(port)
        self._token = token.encode("utf-8")
        self._max_message_bytes = int(max_message_bytes)
        self._request_timeout_s = float(request_timeout_s)
        self._server: socketserver.ThreadingTCPServer | None = None
        self._thread: threading.Thread | None = None
        self._bound_port: int | None = None

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._server is not None:
            return
        server = _ControlTCPServer((self._host, self._port), _ControlHandler)
        server.control = self
        self._server = server
        self._bound_port = int(server.server_address[1])
        thread = threading.Thread(
            target=server.serve_forever,
            name="mdgw-supervisor-control",
            daemon=True,
        )
        self._thread = thread
        thread.start()
        logger.info(
            "supervisor control server started",
            extra={
                "event": "supervisor.control_started",
                "host": self._host,
                "port": self._bound_port,
            },
        )

    def stop(self) -> None:
        server = self._server
        self._server = None
        if server is not None:
            server.shutdown()
            server.server_close()
        thread = self._thread
        self._thread = None
        if thread is not None:
            thread.join(timeout=5)

    @property
    def bound_port(self) -> int:
        if self._bound_port is None:
            msg = "control server is not running"
            raise RuntimeError(msg)
        return self._bound_port

    @property
    def max_message_bytes(self) -> int:
        return self._max_message_bytes

    @property
    def request_timeout_s(self) -> float:
        return self._request_timeout_s

    # -- request handling --------------------------------------------------

    def handle_line_pair(  # ruff: ignore[too-many-return-statements] - explicit rejection matrix
        self, auth_line: bytes, message_line: bytes
    ) -> tuple[dict[str, Any], Any]:
        """Validate one authenticated message and return (response, after)."""
        response = self._authenticate(auth_line)
        if response is not None:
            return response, None
        correlation = _correlation_hint(message_line)
        if not message_line.endswith(b"\n"):
            return (
                error_response(
                    correlation,
                    "message_too_large",
                    "the control message exceeds the configured byte bound",
                ),
                None,
            )
        raw = message_line.strip()
        if len(raw) > self._max_message_bytes:
            return (
                error_response(
                    correlation,
                    "message_too_large",
                    "the control message exceeds the configured byte bound",
                ),
                None,
            )
        try:
            envelope = validate_control_message(
                raw,
                max_message_bytes=self._max_message_bytes,
                registry=lifecycle_registry(),
            )
        except ControlMessageTooLargeError:
            return (
                error_response(
                    correlation,
                    "message_too_large",
                    "the control message exceeds the configured byte bound",
                ),
                None,
            )
        except ActionNotAllowedError as exc:
            return (
                error_response(correlation, "action_not_allowed", str(exc)),
                None,
            )
        except ControlValidationError as exc:
            return (
                error_response(
                    correlation, "invalid_message", str(exc)
                ),
                None,
            )
        except ControlError as exc:  # pragma: no cover - defensive base
            return (
                error_response(correlation, "invalid_message", str(exc)),
                None,
            )
        return self._dispatch(envelope)

    def _authenticate(self, auth_line: bytes) -> dict[str, Any] | None:
        if not auth_line.endswith(b"\n"):
            return error_response(
                _UNKNOWN_CORRELATION,
                "unauthorized",
                "the control token is missing or invalid",
            )
        presented = auth_line.strip()
        if not hmac.compare_digest(presented, self._token):
            logger.warning(
                "supervisor control authentication failed",
                extra={"event": "supervisor.control_unauthorized"},
            )
            return error_response(
                _UNKNOWN_CORRELATION,
                "unauthorized",
                "the control token is missing or invalid",
            )
        return None

    def _dispatch(self, envelope: ControlEnvelope) -> tuple[dict[str, Any], Any]:
        correlation = envelope.correlation_id
        action = envelope.action
        if action == CONTROL_ACTION_PING:
            return success_response(correlation, result={"status": "ok"}), None
        if action == CONTROL_ACTION_STATUS:
            payload = StatusPayload.model_validate(envelope.payload)
            instances = self._supervisor.registry()
            if payload.instance_id is not None:
                instances = [
                    item
                    for item in instances
                    if item.instance_id == payload.instance_id
                ]
            return (
                success_response(
                    correlation,
                    result={
                        "instances": [item.to_wire() for item in instances]
                    },
                ),
                None,
            )
        if action == CONTROL_ACTION_OPERATION_STATUS:
            payload = OperationStatusPayload.model_validate(
                envelope.payload
            )
            record = self._supervisor.operation_status(payload.operation_id)
            return (
                success_response(
                    correlation,
                    result={
                        "operation": (
                            None if record is None else record.to_wire()
                        )
                    },
                ),
                None,
            )

        payload = ServiceActionPayload.model_validate(envelope.payload)
        try:
            record = self._supervisor.accept_operation(
                operation_id=payload.operation_id,
                action=ACTION_BY_CONTROL[action],
                instance_id=payload.instance_id,
                reason=payload.reason,
                actor_subject=payload.actor.subject,
                actor_display_name=payload.actor.display_name,
                actor_access_role=payload.actor.access_role,
                request_id=payload.request_id,
                idempotency_key=payload.idempotency_key,
                idempotency_body_hash=payload.idempotency_body_hash,
                idempotency_expires_at_ns=(
                    payload.idempotency_expires_at_ns
                ),
            )
        except LifecycleRejectedError as exc:
            return (
                error_response(
                    correlation, exc.code, exc.message, details=exc.details
                ),
                None,
            )
        response = success_response(
            correlation, operation=record.to_wire()
        )
        after = _BackgroundExecution(self._supervisor, record)
        return response, after


class _BackgroundExecution:
    """Start the operation only after the accepted response was written."""

    def __init__(self, supervisor: Supervisor, record: Any) -> None:
        self._supervisor = supervisor
        self._record = record

    def __call__(self) -> None:
        thread = threading.Thread(
            target=self._supervisor.execute_operation,
            args=(self._record,),
            name=f"mdgw-lifecycle-{self._record.operation_id}",
            daemon=True,
        )
        thread.start()


class _ControlTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    control: SupervisorControlServer


class _ControlHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        control: SupervisorControlServer = self.server.control  # type: ignore[attr-defined]
        self.connection.settimeout(control.request_timeout_s)
        try:
            auth_line = self.rfile.readline(_MAX_AUTH_BYTES + 1)
            message_line = self.rfile.readline(
                control.max_message_bytes + 1
            )
            response, after = control.handle_line_pair(
                auth_line, message_line
            )
        except OSError:
            return
        try:
            self.wfile.write(_encode(response))
            self.wfile.flush()
        except OSError:
            pass
        finally:
            if after is not None:
                after()


def _encode(response: dict[str, Any]) -> bytes:
    return (
        json.dumps(response, separators=(",", ":"), ensure_ascii=False)
        .encode("utf-8")
        + b"\n"
    )


def _correlation_hint(message_line: bytes) -> str:
    try:
        data = json.loads(message_line.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return _UNKNOWN_CORRELATION
    if not isinstance(data, dict):
        return _UNKNOWN_CORRELATION
    value = data.get("correlation_id")
    return value if isinstance(value, str) and value else _UNKNOWN_CORRELATION


def _require_loopback(host: str) -> None:
    if host in _LOOPBACK_HOSTS:
        return
    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        msg = "the control host must be a loopback address"
        raise ValueError(msg) from exc
    if not address.is_loopback:
        msg = "the control host must be a loopback address"
        raise ValueError(msg)


__all__ = [
    "CONTROL_ACTION_OPERATION_STATUS",
    "CONTROL_ACTION_PING",
    "CONTROL_ACTION_RELOAD",
    "CONTROL_ACTION_START",
    "CONTROL_ACTION_STATUS",
    "CONTROL_ACTION_STOP",
    "SupervisorControlServer",
    "lifecycle_registry",
]
