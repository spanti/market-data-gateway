"""Admin-side lifecycle dependencies owned by one ``AdminContext``.

The API process never executes lifecycle actions directly. This module wires
the persisted registry/operation store and the narrow Supervisor control
client, and reports the adapter's truthful tri-state for capability
discovery:

* ``unsupported`` - the deployment disabled the supervisor adapter;
* ``unavailable`` - enabled but no loopback endpoint/token is configured (or
  the Supervisor is not reachable for an action);
* ``available`` - a control endpoint and token are configured, so actions may
  be dispatched. A per-action connection failure is still reported as
  ``503 control_unavailable`` by the route.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from mdgw.admin.capabilities import AdapterStatus
from mdgw.admin.lifecycle.control_client import SupervisorControlClient
from mdgw.admin.lifecycle.store import LifecycleStore

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mdgw.config import AdminConfig

_UNAVAILABLE_NOT_CONFIGURED = (
    "supervisor control endpoint is not configured"
)
_UNAVAILABLE_CREDENTIAL = "supervisor control token is not configured"


class AdminLifecycle:
    """Lazy lifecycle store/client pair for one API process."""

    def __init__(
        self,
        *,
        config: AdminConfig,
        db_path: Path,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        self._config = config
        self._db_path = Path(db_path)
        self._environ = environ
        self._store: LifecycleStore | None = None
        self._lock = threading.Lock()

    @property
    def store(self) -> LifecycleStore:
        existing = self._store
        if existing is not None:
            return existing
        with self._lock:
            if self._store is None:
                self._store = LifecycleStore(self._db_path)
            return self._store

    @property
    def token(self) -> str | None:
        environ = os.environ if self._environ is None else self._environ
        return environ.get(self._config.control.token_env) or None

    def control_client(self) -> SupervisorControlClient | None:
        """Return a client when the adapter is configured, else ``None``."""
        if not self._config.adapters.supervisor:
            return None
        control = self._config.control
        token = self.token
        if control.supervisor_port <= 0 or not token:
            return None
        return SupervisorControlClient(
            host=control.supervisor_host,
            port=control.supervisor_port,
            token=token,
            timeout_s=control.timeout_s,
            max_message_bytes=control.max_message_bytes,
        )

    def adapter_status(self) -> AdapterStatus:
        """Truthful adapter state for ``GET /capabilities``."""
        if not self._config.adapters.supervisor:
            return AdapterStatus(
                "unsupported", "disabled by configuration"
            )
        control = self._config.control
        if control.supervisor_port <= 0:
            return AdapterStatus("unavailable", _UNAVAILABLE_NOT_CONFIGURED)
        if not self.token:
            return AdapterStatus("unavailable", _UNAVAILABLE_CREDENTIAL)
        return AdapterStatus("available")

    def close(self) -> None:
        store = self._store
        self._store = None
        if store is not None:
            store.close()


__all__ = ["AdminLifecycle"]
