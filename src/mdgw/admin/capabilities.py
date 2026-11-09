"""Capability discovery document and truthful availability states.

``GET /api/v1/admin/capabilities`` is the only way the Operator Console
decides which runtime mode it is in. The document therefore has fixed
top-level keys and stable enums:

* an adapter is ``available``, ``unavailable`` (configured but failing) or
  ``unsupported`` (this build does not provide it);
* a capability is ``enabled``, ``adapter_unavailable`` (implemented but a
  required adapter is failing) or ``unsupported`` (this build does not
  implement it, or the deployment disabled it).

Ticket 01 ships only the admin store. Later tickets pass their built adapter/
capability sets and adapter statuses into ``build_capabilities_document`` as
they land, without changing the wire shape.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mdgw.core.time import datetime_to_ns, ns_to_iso, utc_now

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from mdgw.config import AdminConfig

ADMIN_API_VERSION = "1.0.0"
ADMIN_CONTRACT_VERSION = 1
SUPPORTED_ADMIN_CONTRACTS: tuple[int, ...] = (1,)

#: Contract adapter order and kind (Operator Console contract section 1.2).
CONTRACT_ADAPTERS: tuple[tuple[str, str], ...] = (
    ("oidc", "identity"),
    ("admin_store", "storage"),
    ("audit_store", "audit"),
    ("supervisor", "service_lifecycle"),
    ("collector_control", "feed_subscription"),
    ("export_executor", "export"),
    ("prometheus", "monitoring"),
)

#: Capability keys the frontend gates on; every key must be present.
REQUIRED_CAPABILITY_KEYS: tuple[str, ...] = (
    "oidc_session",
    "feed_subscription",
    "feed_flow_control",
    "service_lifecycle",
    "export_job",
    "backfill_job",
    "persistent_backfill_gaps",
    "operator_audit",
    "monitoring_proxy",
    "ws_admin_session",
    "user_management",
)

#: Adapters this ticket actually implements. Later tickets add their own.
TICKET_01_BUILT_ADAPTERS = frozenset({"admin_store"})
TICKET_01_BUILT_CAPABILITIES: frozenset[str] = frozenset()

#: Capabilities that stay unsupported even if adapters exist.
PERMANENTLY_UNSUPPORTED_CAPABILITIES = frozenset({"user_management"})

#: Adapters each capability depends on, for truthful tri-state reporting.
CAPABILITY_ADAPTERS: dict[str, tuple[str, ...]] = {
    "oidc_session": ("oidc",),
    "feed_subscription": ("collector_control",),
    "feed_flow_control": ("collector_control",),
    "service_lifecycle": ("supervisor",),
    "export_job": ("export_executor",),
    "backfill_job": ("collector_control",),
    "persistent_backfill_gaps": ("collector_control",),
    "operator_audit": ("audit_store",),
    "monitoring_proxy": ("prometheus",),
    "ws_admin_session": ("oidc",),
    "user_management": (),
}

_ADAPTER_FLAGS: dict[str, str] = {
    "oidc": "oidc",
    "audit_store": "audit_store",
    "supervisor": "supervisor",
    "collector_control": "collector_control",
    "export_executor": "export_executor",
    "prometheus": "prometheus",
}

_ADAPTER_LABELS: dict[str, str] = {
    "oidc": "OIDC sessions are not implemented in this build",
    "admin_store": "Admin storage is not implemented in this build",
    "audit_store": "Operator Audit Events are not implemented in this build",
    "supervisor": "Service lifecycle control is not implemented in this build",
    "collector_control": (
        "Feed subscription control is not implemented in this build"
    ),
    "export_executor": "Export Jobs are not implemented in this build",
    "prometheus": "Monitoring proxy is not implemented in this build",
}

AUTH_LOGIN_PATH = "/api/v1/admin/auth/login"
AUTH_CALLBACK_PATH = "/api/v1/admin/auth/callback"
AUTH_LOGOUT_PATH = "/api/v1/admin/auth/logout"
AUTH_SESSION_PATH = "/api/v1/admin/auth/session"
AUTH_REFRESH_PATH = "/api/v1/admin/auth/refresh"
CSRF_HEADER_NAME = "X-CSRF-Token"
ADMIN_ROLES: tuple[str, ...] = ("SuperAdmin", "Operator", "Auditor")

_ADAPTER_STATES = frozenset({"available", "unavailable", "unsupported"})
_CAPABILITY_STATES = frozenset(
    {"enabled", "unsupported", "adapter_unavailable"}
)


@dataclass(frozen=True)
class AdapterStatus:
    """Runtime or deployment state for one adapter."""

    state: str
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.state not in _ADAPTER_STATES:
            msg = f"invalid adapter state: {self.state!r}"
            raise ValueError(msg)


def build_capabilities_document(  # ruff: ignore[too-many-arguments]  # explicit document inputs
    *,
    config: AdminConfig,
    adapter_states: Mapping[str, AdapterStatus] | None = None,
    built_adapters: Iterable[str] | None = None,
    built_capabilities: Iterable[str] | None = None,
    capability_details: Mapping[str, Mapping[str, Any]] | None = None,
    server_time: str | None = None,
) -> dict[str, Any]:
    """Build the capability document for one admin deployment."""
    adapters = _resolve_adapters(
        config,
        adapter_states=adapter_states or {},
        built_adapters=(
            TICKET_01_BUILT_ADAPTERS
            if built_adapters is None
            else frozenset(built_adapters)
        ),
    )
    capabilities = _resolve_capabilities(
        adapters,
        built_capabilities=(
            TICKET_01_BUILT_CAPABILITIES
            if built_capabilities is None
            else frozenset(built_capabilities)
        ),
        details=capability_details or {},
    )
    return {
        "schema_version": 1,
        "server_time": (
            ns_to_iso(datetime_to_ns(utc_now()))
            if server_time is None
            else server_time
        ),
        "version": {
            "admin_api": ADMIN_API_VERSION,
            "contract": ADMIN_CONTRACT_VERSION,
            "supported_contracts": list(SUPPORTED_ADMIN_CONTRACTS),
        },
        "auth": _auth_document(config, adapters["oidc"]),
        "adapters": [
            {
                "name": name,
                "kind": kind,
                "state": adapters[name].state,
                "reason": adapters[name].reason,
            }
            for name, kind in CONTRACT_ADAPTERS
        ],
        "capabilities": capabilities,
        "limits": {
            "page_default": config.limits.page_default,
            "page_max": config.limits.page_max,
            "reason_max_length": config.limits.reason_max_length,
            "operation_retention_s": config.limits.operation_retention_s,
            "backfill_max_range_days": config.limits.backfill_max_range_days,
        },
    }


def _resolve_adapters(
    config: AdminConfig,
    *,
    adapter_states: Mapping[str, AdapterStatus],
    built_adapters: frozenset[str],
) -> dict[str, AdapterStatus]:
    resolved: dict[str, AdapterStatus] = {}
    for name, _kind in CONTRACT_ADAPTERS:
        if name not in built_adapters:
            resolved[name] = AdapterStatus(
                "unsupported", _ADAPTER_LABELS.get(name)
            )
            continue
        if not _adapter_enabled(config, name):
            resolved[name] = AdapterStatus(
                "unsupported", "disabled by configuration"
            )
            continue
        resolved[name] = adapter_states.get(
            name, AdapterStatus("available")
        )
    return resolved


def _adapter_enabled(config: AdminConfig, name: str) -> bool:
    flag = _ADAPTER_FLAGS.get(name)
    if flag is None:
        return True
    return bool(getattr(config.adapters, flag))


def _resolve_capabilities(
    adapters: Mapping[str, AdapterStatus],
    *,
    built_capabilities: frozenset[str],
    details: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    resolved: dict[str, dict[str, Any]] = {}
    for name in REQUIRED_CAPABILITY_KEYS:
        if (
            name in PERMANENTLY_UNSUPPORTED_CAPABILITIES
            or name not in built_capabilities
        ):
            resolved[name] = {"state": "unsupported"}
            continue
        required = CAPABILITY_ADAPTERS.get(name, ())
        unavailable = next(
            (
                adapters[adapter]
                for adapter in required
                if adapters[adapter].state == "unavailable"
            ),
            None,
        )
        if unavailable is not None:
            resolved[name] = {
                "state": "adapter_unavailable",
                "reason": unavailable.reason,
            }
            continue
        if any(
            adapters[adapter].state == "unsupported"
            for adapter in required
        ):
            resolved[name] = {"state": "unsupported"}
            continue
        entry: dict[str, Any] = {"state": "enabled"}
        entry.update(dict(details.get(name, {})))
        resolved[name] = entry
    return resolved


def _auth_document(
    config: AdminConfig, oidc: AdapterStatus
) -> dict[str, Any]:
    return {
        "required": True,
        "mode": "oidc",
        "state": oidc.state,
        "login_path": AUTH_LOGIN_PATH,
        "callback_path": AUTH_CALLBACK_PATH,
        "logout_path": AUTH_LOGOUT_PATH,
        "session_path": AUTH_SESSION_PATH,
        "refresh_path": AUTH_REFRESH_PATH,
        "csrf": {
            "mode": "session_token",
            "header_name": CSRF_HEADER_NAME,
        },
        "idle_timeout_s": config.sessions.idle_timeout_s,
        "absolute_timeout_s": config.sessions.absolute_timeout_s,
        "expiry_warning_s": config.sessions.expiry_warning_s,
        "roles": list(ADMIN_ROLES),
    }


__all__ = [
    "ADMIN_API_VERSION",
    "ADMIN_CONTRACT_VERSION",
    "ADMIN_ROLES",
    "AUTH_CALLBACK_PATH",
    "AUTH_LOGIN_PATH",
    "AUTH_LOGOUT_PATH",
    "AUTH_REFRESH_PATH",
    "AUTH_SESSION_PATH",
    "CAPABILITY_ADAPTERS",
    "CONTRACT_ADAPTERS",
    "CSRF_HEADER_NAME",
    "PERMANENTLY_UNSUPPORTED_CAPABILITIES",
    "REQUIRED_CAPABILITY_KEYS",
    "SUPPORTED_ADMIN_CONTRACTS",
    "TICKET_01_BUILT_ADAPTERS",
    "TICKET_01_BUILT_CAPABILITIES",
    "AdapterStatus",
    "build_capabilities_document",
]
