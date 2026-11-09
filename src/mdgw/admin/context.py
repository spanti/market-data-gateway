"""Stable runtime context for the Admin API.

Ticket 01 creates one ``AdminContext`` per API process and passes it to
``create_app`` as the single keyword-only ``admin`` argument. Later tickets
hang their dependencies off this object (authenticator, audit writer,
collector/supervisor control clients, job registries) instead of editing the
app factory again.

The context owns the management SQLite lifecycle: opening it claims the
single-API-instance lease, and closing it releases the lease before closing
the connection. ``create_app`` registers ``close`` as the ASGI shutdown hook.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from mdgw.admin.capabilities import AdapterStatus
from mdgw.admin.db import AdminStore
from mdgw.admin.proxy import ClientIpResolver
from mdgw.config import ensure_admin_store_is_independent

if TYPE_CHECKING:
    from collections.abc import Callable

    from mdgw.config import AdminConfig
    from mdgw.metrics import MetricsRegistry
    from mdgw.reference import InstrumentStore
    from mdgw.store.parquet_dataset import ParquetBarStore

logger = logging.getLogger(__name__)


class AdminContext:
    """Dependencies and lifecycle owned by one Admin API instance."""

    def __init__(  # ruff: ignore[too-many-arguments]  # explicit dependency set
        self,
        *,
        config: AdminConfig,
        storage_root: Path,
        store: ParquetBarStore | None = None,
        instrument_store: InstrumentStore | None = None,
        metrics: MetricsRegistry | None = None,
        instance_id: str | None = None,
    ) -> None:
        self.config = config
        self.storage_root = Path(storage_root)
        self.store = store
        self.instrument_store = instrument_store
        self.metrics = metrics
        self.instance_id = (
            f"api-{uuid4().hex}" if instance_id is None else instance_id
        )
        self.db: AdminStore | None = None
        self._client_ip_resolver: ClientIpResolver | None = None
        self._cleanup_callbacks: list[Callable[[], None]] = []

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    @property
    def db_path(self) -> Path:
        return self.config.resolved_db_path(self.storage_root)

    @property
    def client_ip_resolver(self) -> ClientIpResolver:
        """Trusted-proxy resolver used by the admin request context."""
        if self._client_ip_resolver is None:
            proxy = self.config.proxy
            self._client_ip_resolver = ClientIpResolver(
                proxy.trusted_proxies,
                forwarded_for_header=proxy.forwarded_for_header,
                max_forwarded_hops=proxy.max_forwarded_hops,
            )
        return self._client_ip_resolver

    def adapter_statuses(self) -> dict[str, AdapterStatus]:
        """Runtime adapter statuses for capability discovery.

        Later tickets add their adapter entries here (or in their feature
        module) as they are implemented.
        """
        return {
            "admin_store": AdapterStatus(
                "available" if self.db is not None else "unavailable",
                None
                if self.db is not None
                else "admin store is not open",
            )
        }

    def register_cleanup(self, callback: Callable[[], None]) -> None:
        """Register a callback run before the admin DB closes."""
        self._cleanup_callbacks.append(callback)

    def ensure_open(self) -> None:
        """Open the management store and claim the single-instance lease."""
        if not self.enabled or self.db is not None:
            return
        # Re-check at runtime: a ``data_root`` override can move the Bars
        # Dataset without changing the static config validation.
        ensure_admin_store_is_independent(
            self.db_path,
            self.storage_root / "bars",
            self.storage_root / "quarantine",
        )
        database = AdminStore(
            self.db_path, lease_ttl_s=self.config.instance_lease_s
        )
        try:
            database.claim_instance(self.instance_id)
        except BaseException:
            database.close()
            raise
        self.db = database

    def close(self) -> None:
        """Release the lease, run registered cleanups, and close the store.

        Idempotent: the ASGI shutdown hook and ``ApiService.stop`` may both
        call it. A later ``ensure_open`` (service restart) reopens the store.
        """
        for callback in reversed(self._cleanup_callbacks):
            try:
                callback()
            except Exception:  # cleanup must not block shutdown
                logger.exception(
                    "admin context cleanup failed",
                    extra={"event": "admin.context.cleanup_failed"},
                )
        database = self.db
        self.db = None
        if database is not None:
            try:
                database.release_instance(self.instance_id)
            except Exception:  # release is best effort
                logger.exception(
                    "admin store lease release failed",
                    extra={"event": "admin.store.lease_release_failed"},
                )
            finally:
                database.close()


__all__ = ["AdminContext"]
