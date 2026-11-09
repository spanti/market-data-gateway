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
import threading
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from mdgw.admin.auth.oidc import OidcClient
from mdgw.admin.auth.service import AdminAuth
from mdgw.admin.auth.store import AuthStore
from mdgw.admin.capabilities import AdapterStatus
from mdgw.admin.db import AdminStore
from mdgw.admin.proxy import ClientIpResolver
from mdgw.config import ensure_admin_store_is_independent

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from mdgw.admin.auth.oidc import OidcTransport
    from mdgw.admin.auth.store import SessionRecord
    from mdgw.api.ws_hub import WebSocketHub
    from mdgw.config import AdminConfig
    from mdgw.metrics import MetricsRegistry
    from mdgw.reference import InstrumentStore
    from mdgw.store.parquet_dataset import ParquetBarStore

logger = logging.getLogger(__name__)

#: Adapters/capabilities this build implements. Later tickets extend the
#: registration through ``register_capabilities``.
_BUILT_ADAPTERS = frozenset({"admin_store", "oidc"})
_BUILT_CAPABILITIES = frozenset({"oidc_session", "ws_admin_session"})
_WS_CAPABILITY_DETAILS = {
    "ws_admin_session": {
        "path": "/api/v1/admin/ws",
        "ops": ["subscribe", "unsubscribe", "ping"],
    }
}


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
        oidc_transport: OidcTransport | None = None,
        clock: Callable[[], int] | None = None,
        environ: Mapping[str, str] | None = None,
        ws_hub: WebSocketHub | None = None,
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
        #: Hub attached by the API service before the app is built; the Admin
        #: WebSocket reuses its bounded queue/lag_notice fan-out.
        self.ws_hub = ws_hub
        #: Narrow hook for ticket 04 flow control: a callable may deny a new
        #: Admin WS session (close 4429). ``None`` means no quota.
        self.ws_quota_checker: Callable[[SessionRecord], bool] | None = None
        self._auth: AdminAuth | None = None
        self._auth_lock = threading.Lock()
        self._oidc_transport = oidc_transport
        self._clock = clock
        self._environ = environ
        self._client_ip_resolver: ClientIpResolver | None = None
        self._cleanup_callbacks: list[Callable[[], None]] = []
        self._built_adapters: set[str] = set(_BUILT_ADAPTERS)
        self._built_capabilities: set[str] = set(_BUILT_CAPABILITIES)
        self._capability_details: dict[str, dict[str, object]] = {
            name: dict(detail)
            for name, detail in _WS_CAPABILITY_DETAILS.items()
        }

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    @property
    def auth(self) -> AdminAuth:
        """Authentication facade; opening it opens the management store."""
        existing = self._auth
        if existing is not None:
            return existing
        with self._auth_lock:
            if self._auth is None:
                if not self.enabled:
                    msg = "Admin API is disabled"
                    raise RuntimeError(msg)
                self.ensure_open()
                oidc = (
                    OidcClient(
                        self.config.oidc,
                        transport=self._oidc_transport,
                        clock=self._clock,
                        environ=self._environ,
                    )
                    if self.config.adapters.oidc
                    and self.config.oidc.is_configured
                    else None
                )
                self._auth = AdminAuth(
                    self.config,
                    AuthStore(self.db_path),
                    oidc=oidc,
                    clock=self._clock,
                    environ=self._environ,
                )
            return self._auth

    # -- capability registration ------------------------------------------

    def register_capabilities(
        self,
        *,
        adapters: Iterable[str] = (),
        capabilities: Iterable[str] = (),
        details: Mapping[str, Mapping[str, object]] | None = None,
    ) -> None:
        """Add adapters/capabilities implemented by a later ticket."""
        self._built_adapters.update(adapters)
        self._built_capabilities.update(capabilities)
        if details:
            for name, detail in details.items():
                self._capability_details[name] = dict(detail)

    def built_adapters(self) -> frozenset[str]:
        return frozenset(self._built_adapters)

    def built_capabilities(self) -> frozenset[str]:
        return frozenset(self._built_capabilities)

    def capability_details(self) -> dict[str, dict[str, object]]:
        return {
            name: dict(detail)
            for name, detail in self._capability_details.items()
        }

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
        statuses = {
            "admin_store": AdapterStatus(
                "available" if self.db is not None else "unavailable",
                None
                if self.db is not None
                else "admin store is not open",
            )
        }
        if not self.config.adapters.oidc:
            statuses["oidc"] = AdapterStatus(
                "unsupported", "disabled by configuration"
            )
        elif not self.config.oidc.is_configured:
            statuses["oidc"] = AdapterStatus(
                "unsupported", "OIDC client is not configured"
            )
        else:
            statuses["oidc"] = AdapterStatus("available")
        return statuses

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
        auth = self._auth
        self._auth = None
        if auth is not None:
            try:
                auth.close()
            except Exception:  # cleanup must not block shutdown
                logger.exception(
                    "admin auth store close failed",
                    extra={"event": "admin.auth.close_failed"},
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
