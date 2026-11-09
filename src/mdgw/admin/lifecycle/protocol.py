"""Allow-listed wire contract for the Supervisor lifecycle control channel.

The envelope itself is ticket 01's ``mdgw.admin.control`` message contract:
version, correlation id, bounded timeout, allow-listed action and a strict
per-action payload schema. This module registers the lifecycle actions in a
dedicated registry (the default registry stays ``ping`` only) and defines the
small response envelope the Supervisor sends back.

Actions use dots because the shared action grammar is
``^[a-z][a-z0-9_.]{0,63}$``. There is deliberately no command, path, shell,
signal name or arbitrary argv field anywhere in the payload.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from mdgw.admin.control import ControlActionRegistry

CONTROL_ACTION_PING = "ping"
CONTROL_ACTION_START = "service.start"
CONTROL_ACTION_STOP = "service.stop"
CONTROL_ACTION_RELOAD = "service.reload"
CONTROL_ACTION_STATUS = "service.status"
CONTROL_ACTION_OPERATION_STATUS = "service.operation_status"

ACTION_BY_CONTROL: dict[str, str] = {
    CONTROL_ACTION_START: "start",
    CONTROL_ACTION_STOP: "stop",
    CONTROL_ACTION_RELOAD: "reload",
}

_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
_KEY_PATTERN = r"^[A-Za-z0-9._:-]{1,256}$"
_HASH_PATTERN = r"^[a-f0-9]{64}$"
_ROLES = Literal["SuperAdmin", "Operator", "Auditor"]
_INSTANCES = Literal["collector", "api", "replay"]
_MAX_REASON = 1_000


class _EmptyPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ActorPayload(BaseModel):
    """Identity snapshot taken server-side by the API before dispatch."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    subject: str = Field(min_length=1, max_length=128)
    display_name: str = Field(min_length=1, max_length=256)
    access_role: _ROLES | None = None


class ServiceActionPayload(BaseModel):
    """Start/Stop/Reload request; no free-form command fields."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    instance_id: _INSTANCES
    operation_id: str = Field(min_length=1, max_length=128, pattern=_ID_PATTERN)
    request_id: str = Field(min_length=1, max_length=128, pattern=_ID_PATTERN)
    reason: str = Field(min_length=1, max_length=_MAX_REASON)
    actor: ActorPayload
    idempotency_key: str | None = Field(
        default=None, pattern=_KEY_PATTERN
    )
    idempotency_body_hash: str | None = Field(
        default=None, pattern=_HASH_PATTERN
    )
    idempotency_expires_at_ns: int | None = Field(default=None, ge=0)


class StatusPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    instance_id: _INSTANCES | None = None


class OperationStatusPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(min_length=1, max_length=128, pattern=_ID_PATTERN)


def lifecycle_registry() -> ControlActionRegistry:
    """Build the Supervisor lifecycle allowlist (never the default registry)."""
    registry = ControlActionRegistry()
    registry.register(
        CONTROL_ACTION_PING, targets=("supervisor",), payload=_EmptyPayload
    )
    for action in (
        CONTROL_ACTION_START,
        CONTROL_ACTION_STOP,
        CONTROL_ACTION_RELOAD,
    ):
        registry.register(
            action,
            targets=("supervisor",),
            payload=ServiceActionPayload,
        )
    registry.register(
        CONTROL_ACTION_STATUS, targets=("supervisor",), payload=StatusPayload
    )
    registry.register(
        CONTROL_ACTION_OPERATION_STATUS,
        targets=("supervisor",),
        payload=OperationStatusPayload,
    )
    return registry


def success_response(
    correlation_id: str,
    *,
    operation: dict[str, Any] | None = None,
    result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One accepted response; exactly one of ``operation``/``result`` is set."""
    return {
        "schema_version": 1,
        "correlation_id": correlation_id,
        "ok": True,
        "operation": operation,
        "result": result,
        "error": None,
    }


def error_response(
    correlation_id: str,
    code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One rejected response with a stable code and object details."""
    return {
        "schema_version": 1,
        "correlation_id": correlation_id,
        "ok": False,
        "operation": None,
        "result": None,
        "error": {
            "code": code,
            "message": message,
            "details": dict(details or {}),
        },
    }


__all__ = [
    "ACTION_BY_CONTROL",
    "CONTROL_ACTION_OPERATION_STATUS",
    "CONTROL_ACTION_PING",
    "CONTROL_ACTION_RELOAD",
    "CONTROL_ACTION_START",
    "CONTROL_ACTION_STATUS",
    "CONTROL_ACTION_STOP",
    "ActorPayload",
    "OperationStatusPayload",
    "ServiceActionPayload",
    "StatusPayload",
    "error_response",
    "lifecycle_registry",
    "success_response",
]
