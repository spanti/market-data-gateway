"""Terminal Operator Audit Event sink for lifecycle operations.

The API appends ``requested`` before dispatch (fail-closed). The Supervisor
process is the lifecycle executor and keeps running even when the API stops
itself, so it owns the terminal event: same actor snapshot, action, target,
request id, reason, operation id and instance id as the requested row.

A terminal append failure is logged by the Supervisor and never changes the
Operation outcome the executor already persisted.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mdgw.admin.audit.models import AuditActor, AuditTarget

if TYPE_CHECKING:
    from collections.abc import Callable

    from mdgw.admin.audit.writer import AuditWriter
    from mdgw.admin.lifecycle.models import OperationRecord


def terminal_audit_sink(
    writer: AuditWriter,
) -> Callable[[OperationRecord], None]:
    """Build the callback the Supervisor invokes after a terminal transition."""

    def record(operation: OperationRecord) -> None:
        succeeded = operation.status == "succeeded"
        error = operation.error or {}
        writer.append(
            actor=AuditActor(
                subject=operation.actor_subject,
                display_name=operation.actor_display_name,
                access_role=operation.actor_access_role,
            ),
            action=operation.kind,
            target=AuditTarget(
                type="service_instance",
                id=operation.instance_id,
                display_name=operation.instance_id,
            ),
            result="succeeded" if succeeded else "failed",
            request_id=operation.request_id,
            reason=operation.reason,
            failure_reason=(
                None
                if succeeded
                else str(error.get("code") or "supervisor_error")
            ),
            operation_id=operation.operation_id,
            instance_id=operation.instance_id,
            after={
                "status": operation.status,
                "result": operation.result,
                "error": operation.error,
            },
            occurred_at_ns=operation.finished_at_ns,
        )

    return record


__all__ = ["terminal_audit_sink"]
