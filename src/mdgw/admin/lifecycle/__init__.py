"""Service Instance lifecycle adapter (Supervisor) for the Admin API.

Ticket 05 adds the Supervisor as the single controlled lifecycle adapter for
``collector``/``api``/``replay``: the persisted registry and operation history
live in the admin SQLite database, the lifecycle execution lives in the
``mdgw up`` Supervisor process, and the API reaches it only through the
allow-listed loopback control channel.
"""

from __future__ import annotations

from mdgw.admin.lifecycle.models import (
    InstanceSnapshot,
    NewOperation,
    OperationQuery,
    OperationRecord,
)
from mdgw.admin.lifecycle.store import (
    IdempotencyKeyReuseError,
    LifecycleStore,
    LifecycleStoreError,
)

__all__ = [
    "IdempotencyKeyReuseError",
    "InstanceSnapshot",
    "LifecycleStore",
    "LifecycleStoreError",
    "NewOperation",
    "OperationQuery",
    "OperationRecord",
]
