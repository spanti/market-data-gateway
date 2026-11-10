"""Operator Audit Event value objects and wire rendering.

The event shape is the contract target section 8.2, field for field. Dataclasses
keep the writer, store and query routes using one representation; ``to_wire``
is the only place that turns UTC nanoseconds into the RFC3339 ``Z`` string the
Operator Console parses.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from mdgw.core.time import ns_to_iso

RESULT_REQUESTED = "requested"
RESULT_SUCCEEDED = "succeeded"
RESULT_FAILED = "failed"
RESULT_DENIED = "denied"

#: The four terminal/initial result values (contract section 8.2).
AUDIT_RESULTS: tuple[str, ...] = (
    RESULT_REQUESTED,
    RESULT_SUCCEEDED,
    RESULT_FAILED,
    RESULT_DENIED,
)

HASH_ALGORITHM = "sha256"


@dataclass(frozen=True)
class AuditActor:
    """The operator identity snapshot taken at action time."""

    subject: str
    display_name: str
    access_role: str | None = None


@dataclass(frozen=True)
class AuditTarget:
    """The affected resource; ``display_name`` is optional metadata."""

    type: str
    id: str
    display_name: str | None = None


@dataclass(frozen=True)
class AuditDiff:
    """Redacted before/after state plus the JSON Pointer paths replaced.

    Creation uses ``before=None``; deletion uses ``after=None``. The values
    stored here are already redacted; the writer performs redaction before the
    event is appended.
    """

    before: Any = None
    after: Any = None
    redacted_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class HashChain:
    """Tamper-detection metadata for one event (not a WORM claim)."""

    algorithm: str
    previous_event_hash: str | None
    event_hash: str


@dataclass(frozen=True)
class OperatorAuditEvent:
    """One append-only Operator Audit Event."""

    event_id: str
    sequence: int
    occurred_at_ns: int
    actor: AuditActor
    action: str
    target: AuditTarget
    result: str
    failure_reason: str | None
    reason: str | None
    request_id: str
    operation_id: str | None
    job_id: str | None
    instance_id: str | None
    diff: AuditDiff = field(default_factory=AuditDiff)
    hash_chain: HashChain = field(
        default_factory=lambda: HashChain(HASH_ALGORITHM, None, "")
    )

    @property
    def occurred_at(self) -> str:
        """UTC RFC3339 with nine fractional digits and a ``Z`` suffix."""
        return ns_to_iso(self.occurred_at_ns)

    def to_wire(self) -> dict[str, Any]:
        """Render the exact JSON object returned by the query endpoints."""
        return {
            "event_id": self.event_id,
            "sequence": self.sequence,
            "occurred_at": self.occurred_at,
            "actor": {
                "subject": self.actor.subject,
                "display_name": self.actor.display_name,
                "access_role": self.actor.access_role,
            },
            "action": self.action,
            "target": {
                "type": self.target.type,
                "id": self.target.id,
                "display_name": self.target.display_name,
            },
            "result": self.result,
            "failure_reason": self.failure_reason,
            "reason": self.reason,
            "request_id": self.request_id,
            "operation_id": self.operation_id,
            "job_id": self.job_id,
            "instance_id": self.instance_id,
            "diff": {
                "before": self.diff.before,
                "after": self.diff.after,
                "redacted_paths": list(self.diff.redacted_paths),
            },
            "hash_chain": {
                "algorithm": self.hash_chain.algorithm,
                "previous_event_hash": self.hash_chain.previous_event_hash,
                "event_hash": self.hash_chain.event_hash,
            },
        }


@dataclass(frozen=True)
class ChainVerification:
    """Result of walking the whole append-only hash chain."""

    ok: bool
    checked: int
    first_broken_sequence: int | None = None
    reason: str | None = None


@dataclass(frozen=True)
class AuditQuery:
    """Normalized filters for one keyset page of audit events."""

    from_ns: int | None = None
    to_ns: int | None = None
    actor_subject: str | None = None
    access_role: str | None = None
    action: str | None = None
    target_type: str | None = None
    target_id: str | None = None
    result: str | None = None
    request_id: str | None = None
    operation_id: str | None = None
    job_id: str | None = None
    instance_id: str | None = None
    order: str = "desc"
    after_sequence: int | None = None
    limit: int = 50


__all__ = [
    "AUDIT_RESULTS",
    "HASH_ALGORITHM",
    "RESULT_DENIED",
    "RESULT_FAILED",
    "RESULT_REQUESTED",
    "RESULT_SUCCEEDED",
    "AuditActor",
    "AuditDiff",
    "AuditQuery",
    "AuditTarget",
    "ChainVerification",
    "HashChain",
    "OperatorAuditEvent",
]
