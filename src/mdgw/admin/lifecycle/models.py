"""Service Instance and Operation value objects for lifecycle control.

The wire shapes are the Operator Console contract target section 5.1/5.3:
`ServiceInstance` carries the observed registry state plus the actions the
Supervisor allows right now, and `Operation` carries the requested identity
snapshot, every timestamp and, once terminal, either a result or a stable
error code. `to_wire` is the only place nanoseconds become RFC3339 strings.

These models are shared by the API process (read/query routes) and the
Supervisor process (the single lifecycle executor), so neither side invents
an alternative shape.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mdgw.core.time import ns_to_iso

SERVICE_ROLES: tuple[str, ...] = ("collector", "api", "replay")
INSTANCE_ORDER = {"collector": 0, "api": 1, "replay": 2}

DESIRED_RUNNING = "running"
DESIRED_STOPPED = "stopped"
DESIRED_STATES: tuple[str, ...] = (DESIRED_RUNNING, DESIRED_STOPPED)

OBSERVED_RUNNING = "running"
OBSERVED_STARTING = "starting"
OBSERVED_STOPPING = "stopping"
OBSERVED_RELOADING = "reloading"
OBSERVED_STOPPED = "stopped"
OBSERVED_UNKNOWN = "unknown"
OBSERVED_STATES: tuple[str, ...] = (
    OBSERVED_RUNNING,
    OBSERVED_STARTING,
    OBSERVED_STOPPING,
    OBSERVED_RELOADING,
    OBSERVED_STOPPED,
    OBSERVED_UNKNOWN,
)

STATUS_ACCEPTED = "accepted"
STATUS_RUNNING = "running"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_UNKNOWN = "unknown"
OPERATION_STATUSES: tuple[str, ...] = (
    STATUS_ACCEPTED,
    STATUS_RUNNING,
    STATUS_SUCCEEDED,
    STATUS_FAILED,
    STATUS_UNKNOWN,
)

KIND_START = "service_instance.start"
KIND_STOP = "service_instance.stop"
KIND_RELOAD = "service_instance.reload"
OPERATION_KINDS: tuple[str, ...] = (KIND_START, KIND_STOP, KIND_RELOAD)
KIND_ACTIONS = {
    KIND_START: "start",
    KIND_STOP: "stop",
    KIND_RELOAD: "reload",
}
ACTION_KINDS = {action: kind for kind, action in KIND_ACTIONS.items()}

#: Availability is derived from the observed state, never from desired intent.
_RUNNING_ACTIONS = ["stop", "reload"]
_STOPPED_ACTIONS = ["start"]
_HEALTHY = "healthy"
_DEGRADED = "degraded"
_UNHEALTHY = "unhealthy"
_STOPPED_HEALTH = "stopped"


@dataclass(frozen=True)
class InstanceSnapshot:
    """One row of the Supervisor-owned Service Instance registry."""

    instance_id: str
    service_role: str
    display_name: str
    desired_state: str
    observed_state: str
    ready: bool
    ready_reason: str | None
    pid: int | None
    started_at_ns: int | None
    last_observed_at_ns: int | None
    last_error: dict[str, Any] | None
    updated_at_ns: int

    @property
    def health(self) -> str:
        if self.last_error is not None and self.observed_state in {
            OBSERVED_STARTING,
            OBSERVED_UNKNOWN,
        }:
            return _UNHEALTHY
        if self.ready and self.observed_state == OBSERVED_RUNNING:
            return _HEALTHY
        if self.observed_state == OBSERVED_STOPPED:
            return _STOPPED_HEALTH
        return _DEGRADED

    @property
    def available_actions(self) -> list[str]:
        if self.observed_state == OBSERVED_RUNNING:
            return list(_RUNNING_ACTIONS)
        if self.observed_state == OBSERVED_STOPPED:
            return list(_STOPPED_ACTIONS)
        return []

    def to_wire(self) -> dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "service_role": self.service_role,
            "display_name": self.display_name,
            "desired_state": self.desired_state,
            "observed_state": self.observed_state,
            "health": self.health,
            "ready": self.ready,
            "ready_reason": self.ready_reason,
            "pid": self.pid,
            "started_at": _iso_or_none(self.started_at_ns),
            "last_observed_at": _iso_or_none(self.last_observed_at_ns),
            "last_error": self.last_error,
            "available_actions": self.available_actions,
        }


@dataclass(frozen=True)
class NewOperation:
    """The accepted request the Supervisor persists before executing."""

    operation_id: str
    kind: str
    instance_id: str
    service_role: str
    actor_subject: str
    actor_display_name: str
    actor_access_role: str | None
    reason: str
    request_id: str
    requested_at_ns: int
    idempotency_key: str | None = None
    idempotency_body_hash: str | None = None
    idempotency_expires_at_ns: int | None = None


@dataclass(frozen=True)
class OperationRecord:
    """One persisted operation, including every transition timestamp."""

    sequence: int
    operation_id: str
    kind: str
    instance_id: str
    service_role: str
    actor_subject: str
    actor_display_name: str
    actor_access_role: str | None
    reason: str
    status: str
    requested_at_ns: int
    started_at_ns: int | None
    finished_at_ns: int | None
    result: dict[str, Any] | None
    error: dict[str, Any] | None
    request_id: str
    idempotency_key: str | None
    idempotency_body_hash: str | None
    idempotency_expires_at_ns: int | None
    updated_at_ns: int

    @property
    def terminal(self) -> bool:
        return self.status in {
            STATUS_SUCCEEDED,
            STATUS_FAILED,
            STATUS_UNKNOWN,
        }

    def to_wire(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "kind": self.kind,
            "target": {
                "instance_id": self.instance_id,
                "service_role": self.service_role,
            },
            "requested_by": {
                "subject": self.actor_subject,
                "display_name": self.actor_display_name,
                "access_role": self.actor_access_role,
            },
            "reason": self.reason,
            "status": self.status,
            "requested_at": ns_to_iso(self.requested_at_ns),
            "started_at": _iso_or_none(self.started_at_ns),
            "finished_at": _iso_or_none(self.finished_at_ns),
            "result": self.result,
            "error": self.error,
            "request_id": self.request_id,
        }


@dataclass(frozen=True)
class OperationQuery:
    """Normalized filters for one keyset page of operations."""

    instance_id: str | None = None
    kind: str | None = None
    status: str | None = None
    from_ns: int | None = None
    to_ns: int | None = None
    before_sequence: int | None = None
    limit: int = 50


def _iso_or_none(value: int | None) -> str | None:
    return None if value is None else ns_to_iso(value)


__all__ = [
    "ACTION_KINDS",
    "DESIRED_RUNNING",
    "DESIRED_STATES",
    "DESIRED_STOPPED",
    "INSTANCE_ORDER",
    "KIND_ACTIONS",
    "KIND_RELOAD",
    "KIND_START",
    "KIND_STOP",
    "OBSERVED_RELOADING",
    "OBSERVED_RUNNING",
    "OBSERVED_STARTING",
    "OBSERVED_STATES",
    "OBSERVED_STOPPED",
    "OBSERVED_STOPPING",
    "OBSERVED_UNKNOWN",
    "OPERATION_KINDS",
    "OPERATION_STATUSES",
    "SERVICE_ROLES",
    "STATUS_ACCEPTED",
    "STATUS_FAILED",
    "STATUS_RUNNING",
    "STATUS_SUCCEEDED",
    "STATUS_UNKNOWN",
    "InstanceSnapshot",
    "NewOperation",
    "OperationQuery",
    "OperationRecord",
]
