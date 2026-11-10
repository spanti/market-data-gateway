"""Operator Audit Event substrate.

The public API later tickets compose:

* ``AuditWriter.begin(...) -> AuditHandle`` then ``AuditHandle.succeeded()`` /
  ``AuditHandle.failed(...)`` for high-risk management writes (requested is
  appended before execution; an unavailable store raises
  ``AuditUnavailableError`` and the action must not run);
* ``AuditWriter.append(...)`` for one complete synchronous event;
* ``AuditWriter.record_security_event(...)`` installed on
  ``AdminAuth.security_event_sink`` for authentication/authorization failures;
* ``AuditStore`` for the read-only query routes and tamper verification.
"""

from __future__ import annotations

from mdgw.admin.audit.canonical import (
    CANONICAL_ALGORITHM,
    REDACTED,
    canonical_event_payload,
    compute_event_hash,
    redact,
)
from mdgw.admin.audit.models import (
    AUDIT_RESULTS,
    RESULT_DENIED,
    RESULT_FAILED,
    RESULT_REQUESTED,
    RESULT_SUCCEEDED,
    AuditActor,
    AuditDiff,
    AuditQuery,
    AuditTarget,
    ChainVerification,
    HashChain,
    OperatorAuditEvent,
)
from mdgw.admin.audit.store import MIN_AUDIT_SCHEMA_VERSION, AuditStore
from mdgw.admin.audit.writer import (
    AuditHandle,
    AuditWriter,
    actor_from_session,
)

__all__ = [
    "AUDIT_RESULTS",
    "CANONICAL_ALGORITHM",
    "MIN_AUDIT_SCHEMA_VERSION",
    "REDACTED",
    "RESULT_DENIED",
    "RESULT_FAILED",
    "RESULT_REQUESTED",
    "RESULT_SUCCEEDED",
    "AuditActor",
    "AuditDiff",
    "AuditHandle",
    "AuditQuery",
    "AuditStore",
    "AuditTarget",
    "AuditWriter",
    "ChainVerification",
    "HashChain",
    "OperatorAuditEvent",
    "actor_from_session",
    "canonical_event_payload",
    "compute_event_hash",
    "redact",
]
