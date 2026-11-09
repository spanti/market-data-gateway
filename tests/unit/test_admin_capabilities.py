# ruff: file-ignore[implicit-namespace-package]
"""Capability document shape and the three availability states.

The document is the only way the Operator Console decides whether it is in
Current Contract, Admin API, or capability-unknown mode, so every field is
part of the wire contract and every capability distinguishes enabled,
unsupported and adapter-unavailable truthfully.
"""

from __future__ import annotations

from mdgw.admin.capabilities import (
    ADMIN_API_VERSION,
    ADMIN_CONTRACT_VERSION,
    AdapterStatus,
    build_capabilities_document,
)
from mdgw.config import AdminConfig

SERVER_TIME = "2026-11-09T12:00:00.000000000Z"
REQUIRED_CAPABILITIES = {
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
}
ADAPTERS = [
    ("oidc", "identity"),
    ("admin_store", "storage"),
    ("audit_store", "audit"),
    ("supervisor", "service_lifecycle"),
    ("collector_control", "feed_subscription"),
    ("export_executor", "export"),
    ("prometheus", "monitoring"),
]
ADAPTER_STATES = {"available", "unavailable", "unsupported"}
CAPABILITY_STATES = {"enabled", "unsupported", "adapter_unavailable"}


def _document(**kwargs) -> dict:
    return build_capabilities_document(
        config=AdminConfig(), server_time=SERVER_TIME, **kwargs
    )


def test_document_has_exactly_the_contract_top_level_keys() -> None:
    document = _document()

    assert set(document) == {
        "schema_version",
        "server_time",
        "version",
        "auth",
        "adapters",
        "capabilities",
        "limits",
    }
    assert document["schema_version"] == 1
    assert document["server_time"] == SERVER_TIME


def test_version_declares_the_admin_api_and_contract() -> None:
    version = _document()["version"]

    assert version == {
        "admin_api": ADMIN_API_VERSION,
        "contract": ADMIN_CONTRACT_VERSION,
        "supported_contracts": [1],
    }
    assert ADMIN_API_VERSION


def test_auth_declares_the_contract_fields_and_timeouts() -> None:
    auth = _document()["auth"]

    assert auth == {
        "required": True,
        "mode": "oidc",
        "state": "unsupported",
        "login_path": "/api/v1/admin/auth/login",
        "callback_path": "/api/v1/admin/auth/callback",
        "logout_path": "/api/v1/admin/auth/logout",
        "session_path": "/api/v1/admin/auth/session",
        "refresh_path": "/api/v1/admin/auth/refresh",
        "csrf": {
            "mode": "session_token",
            "header_name": "X-CSRF-Token",
        },
        "idle_timeout_s": 1800,
        "absolute_timeout_s": 28800,
        "expiry_warning_s": 300,
        "roles": ["SuperAdmin", "Operator", "Auditor"],
    }


def test_adapters_are_ordered_and_distinguish_available_and_unsupported() -> None:
    adapters = _document()["adapters"]

    assert [(entry["name"], entry["kind"]) for entry in adapters] == ADAPTERS
    for entry in adapters:
        assert set(entry) == {"name", "kind", "state", "reason"}
        assert entry["state"] in ADAPTER_STATES

    states = {entry["name"]: entry["state"] for entry in adapters}
    assert states["admin_store"] == "available"
    assert states["oidc"] == "unsupported"
    assert adapters[0]["reason"]


def test_every_required_capability_is_present_and_fails_closed() -> None:
    capabilities = _document()["capabilities"]

    assert set(capabilities) == REQUIRED_CAPABILITIES
    for entry in capabilities.values():
        assert set(entry) == {"state"}
        assert entry["state"] in CAPABILITY_STATES
    assert capabilities["user_management"]["state"] == "unsupported"
    assert capabilities["oidc_session"]["state"] == "unsupported"


def test_limits_come_from_configuration() -> None:
    limits = _document()["limits"]

    assert limits == {
        "page_default": 50,
        "page_max": 200,
        "reason_max_length": 1000,
        "operation_retention_s": 604800,
        "backfill_max_range_days": 366,
    }


def test_a_built_capability_is_enabled_when_its_adapter_is_available() -> None:
    document = build_capabilities_document(
        config=AdminConfig.model_validate({"adapters": {"oidc": True}}),
        built_adapters={"admin_store", "oidc"},
        built_capabilities={"oidc_session"},
        adapter_states={"oidc": AdapterStatus("available")},
        server_time=SERVER_TIME,
    )

    assert document["capabilities"]["oidc_session"] == {"state": "enabled"}
    assert document["auth"]["state"] == "available"
    assert document["adapters"][0]["state"] == "available"


def test_an_implemented_capability_reports_adapter_unavailable() -> None:
    document = build_capabilities_document(
        config=AdminConfig.model_validate(
            {"adapters": {"collector_control": True}}
        ),
        built_adapters={"admin_store", "collector_control"},
        built_capabilities={"feed_subscription"},
        adapter_states={
            "collector_control": AdapterStatus(
                "unavailable", "Collector control socket is unreachable"
            )
        },
        server_time=SERVER_TIME,
    )

    entry = document["capabilities"]["feed_subscription"]
    assert entry["state"] == "adapter_unavailable"
    assert entry["reason"] == "Collector control socket is unreachable"


def test_a_capability_whose_adapter_is_not_built_is_unsupported() -> None:
    document = _document(
        built_adapters={"admin_store"},
        built_capabilities={"feed_subscription"},
    )

    assert document["capabilities"]["feed_subscription"] == {
        "state": "unsupported"
    }


def test_an_adapter_disabled_by_configuration_is_unsupported() -> None:
    document = build_capabilities_document(
        config=AdminConfig.model_validate({"adapters": {"oidc": False}}),
        built_adapters={"admin_store", "oidc"},
        built_capabilities={"oidc_session"},
        adapter_states={"oidc": AdapterStatus("available")},
        server_time=SERVER_TIME,
    )

    assert document["adapters"][0]["state"] == "unsupported"
    assert document["adapters"][0]["reason"] == "disabled by configuration"
    assert document["capabilities"]["oidc_session"] == {
        "state": "unsupported"
    }


def test_enabled_capabilities_can_carry_documented_details() -> None:
    document = build_capabilities_document(
        config=AdminConfig.model_validate(
            {"adapters": {"export_executor": True}}
        ),
        built_adapters={"admin_store", "export_executor"},
        built_capabilities={"export_job"},
        capability_details={
            "export_job": {
                "formats": ["csv"],
                "min_days": 31,
                "max_days": 366,
                "retention_s": 86400,
                "max_running_per_user": 1,
            }
        },
        server_time=SERVER_TIME,
    )

    entry = document["capabilities"]["export_job"]
    assert entry["state"] == "enabled"
    assert entry["formats"] == ["csv"]
    assert entry["max_days"] == 366
