"""Narrow client for the Supervisor lifecycle control channel.

The API process never spawns, signals or terminates a Service Role itself. It
sends one allow-listed envelope over loopback TCP with the shared token and
waits for the Supervisor's one-line response. Connection loss before an
accepted response means ``control_unavailable`` (nothing was accepted);
connection loss after a 202 means the Operation is ``unknown`` until the
client re-queries it -- never a failure inferred from the disconnect.

No arbitrary command, path, shell or signal name is expressible through this
client: only the action names registered in
``mdgw.admin.lifecycle.protocol.lifecycle_registry`` are buildable.
"""

from __future__ import annotations

import json
import socket
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from mdgw.admin.control import (
    DEFAULT_CONTROL_TIMEOUT_MS,
    DEFAULT_MAX_MESSAGE_BYTES,
    build_control_message,
)
from mdgw.admin.lifecycle.protocol import (
    CONTROL_ACTION_PING,
    CONTROL_ACTION_STATUS,
    lifecycle_registry,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

_DEFAULT_TIMEOUT_S = 5.0
_READ_LIMIT = DEFAULT_MAX_MESSAGE_BYTES * 2
_CODE_INVALID_RESPONSE = "invalid_response"
_CODE_MESSAGE_TOO_LARGE = "message_too_large"
_CODE_SUPERVISOR_ERROR = "supervisor_error"


class SupervisorUnavailableError(RuntimeError):
    """The Supervisor control endpoint could not be reached."""


class SupervisorControlError(RuntimeError):
    """The Supervisor rejected the message with a stable error code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})


class SupervisorControlClient:
    """One-shot request/response client; connections are never pooled."""

    def __init__(  # explicit endpoint policy
        self,
        *,
        host: str,
        port: int,
        token: str,
        timeout_s: float = _DEFAULT_TIMEOUT_S,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    ) -> None:
        self.host = host
        self.port = int(port)
        self._token = token
        self._timeout_s = float(timeout_s)
        self._max_message_bytes = int(max_message_bytes)

    def request(
        self,
        action: str,
        payload: Mapping[str, Any] | None = None,
        *,
        correlation_id: str | None = None,
        timeout_ms: int = DEFAULT_CONTROL_TIMEOUT_MS,
    ) -> dict[str, Any]:
        """Send one validated envelope and return the accepted response."""
        message = build_control_message(
            "supervisor",
            action,
            correlation_id=correlation_id or f"req_{uuid4().hex}",
            payload=payload,
            timeout_ms=timeout_ms,
            registry=lifecycle_registry(),
        )
        response = self._exchange(message)
        if not isinstance(response, dict) or not isinstance(
            response.get("ok"), bool
        ):
            msg = "the supervisor returned a malformed control response"
            raise SupervisorControlError(_CODE_INVALID_RESPONSE, msg)
        if response["ok"]:
            return response
        error = response.get("error")
        if not isinstance(error, dict):
            msg = "the supervisor rejected the request"
            raise SupervisorControlError(_CODE_INVALID_RESPONSE, msg)
        details = error.get("details")
        raise SupervisorControlError(
            str(error.get("code") or _CODE_SUPERVISOR_ERROR),
            str(error.get("message") or "the supervisor rejected the request"),
            details=details if isinstance(details, dict) else {},
        )

    # -- allow-listed helpers ---------------------------------------------

    def ping(self, *, correlation_id: str | None = None) -> dict[str, Any]:
        return self.request(CONTROL_ACTION_PING, correlation_id=correlation_id)

    def status(
        self,
        *,
        instance_id: str | None = None,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        payload = {} if instance_id is None else {"instance_id": instance_id}
        return self.request(
            CONTROL_ACTION_STATUS, payload, correlation_id=correlation_id
        )

    def service_action(  # ruff: ignore[too-many-arguments]  # explicit request fields
        self,
        action: str,
        *,
        instance_id: str,
        operation_id: str,
        request_id: str,
        reason: str,
        actor: Mapping[str, Any],
        idempotency_key: str | None = None,
        idempotency_body_hash: str | None = None,
        idempotency_expires_at_ns: int | None = None,
    ) -> dict[str, Any]:
        """Send Start/Stop/Reload and return the accepted Operation wire."""
        payload: dict[str, Any] = {
            "instance_id": instance_id,
            "operation_id": operation_id,
            "request_id": request_id,
            "reason": reason,
            "actor": dict(actor),
        }
        if idempotency_key is not None:
            payload["idempotency_key"] = idempotency_key
        if idempotency_body_hash is not None:
            payload["idempotency_body_hash"] = idempotency_body_hash
        if idempotency_expires_at_ns is not None:
            payload["idempotency_expires_at_ns"] = idempotency_expires_at_ns
        response = self.request(
            action, payload, correlation_id=request_id
        )
        operation = response.get("operation")
        if not isinstance(operation, dict):
            msg = "the supervisor returned a malformed operation"
            raise SupervisorControlError(_CODE_INVALID_RESPONSE, msg)
        return operation

    # -- transport ---------------------------------------------------------

    def _exchange(self, message: Mapping[str, Any]) -> Any:
        raw = json.dumps(
            dict(message), separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        if len(raw) > self._max_message_bytes:
            msg = "the control message exceeds the configured byte bound"
            raise SupervisorControlError(_CODE_MESSAGE_TOO_LARGE, msg)
        try:
            with socket.create_connection(
                (self.host, self.port), timeout=self._timeout_s
            ) as sock:
                sock.settimeout(self._timeout_s)
                sock.sendall(self._token.encode("utf-8") + b"\n")
                sock.sendall(raw + b"\n")
                line = _read_line(sock, _READ_LIMIT)
        except OSError as exc:
            msg = "the supervisor control endpoint is unreachable"
            raise SupervisorUnavailableError(msg) from exc
        try:
            return json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            msg = "the supervisor returned a malformed control response"
            raise SupervisorControlError(_CODE_INVALID_RESPONSE, msg) from exc


def _read_line(sock: socket.socket, limit: int) -> bytes:
    buffer = b""
    while b"\n" not in buffer:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buffer += chunk
        if len(buffer) > limit:
            break
    line, _, _rest = buffer.partition(b"\n")
    return line


__all__ = [
    "SupervisorControlClient",
    "SupervisorControlError",
    "SupervisorUnavailableError",
]
