# ruff: file-ignore[implicit-namespace-package]
"""Admin configuration defaults and validation (ticket 01).

The admin section must stay disabled by default so every existing deployment
and test keeps the Current Contract surface, and it must never point the
management SQLite database into the Bars or Quarantine Datasets.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from mdgw.config import AdminConfig, GatewayConfig, load_config

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_admin_defaults_are_disabled_and_backward_compatible() -> None:
    config = GatewayConfig()

    assert config.admin.enabled is False
    assert config.admin.db_path is None
    assert config.admin.instance_lease_s == pytest.approx(60.0)
    assert config.admin.proxy.trusted_proxies == ()
    assert config.admin.proxy.public_origin is None
    assert config.admin.proxy.forwarded_for_header == "X-Forwarded-For"
    assert config.admin.proxy.max_forwarded_hops == 16


def test_repository_config_keeps_admin_disabled() -> None:
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")

    assert config.admin.enabled is False


def test_admin_placeholders_have_contract_defaults() -> None:
    admin = AdminConfig()

    assert admin.adapters.oidc is False
    assert admin.adapters.audit_store is False
    assert admin.adapters.supervisor is False
    assert admin.adapters.collector_control is False
    assert admin.adapters.export_executor is False
    assert admin.adapters.prometheus is False

    assert admin.sessions.idle_timeout_s == 1800
    assert admin.sessions.absolute_timeout_s == 28800
    assert admin.sessions.expiry_warning_s == 300

    assert admin.export.min_days == 31
    assert admin.export.max_days == 366
    assert admin.export.retention_s == 86400
    assert admin.export.max_running_per_user == 1

    assert admin.control.timeout_s == pytest.approx(5.0)
    assert admin.control.max_message_bytes == 65536

    assert admin.limits.page_default == 50
    assert admin.limits.page_max == 200
    assert admin.limits.reason_max_length == 1000
    assert admin.limits.operation_retention_s == 604800
    assert admin.limits.backfill_max_range_days == 366


def test_admin_resolves_database_under_the_storage_root() -> None:
    admin = AdminConfig()

    assert admin.resolved_db_path(Path("data")) == Path(
        "data/admin/admin.db"
    )

    explicit = AdminConfig(db_path=Path("elsewhere/admin.db"))
    assert explicit.resolved_db_path(Path("data")) == Path(
        "elsewhere/admin.db"
    )


def test_admin_accepts_typed_settings() -> None:
    config = GatewayConfig.model_validate(
        {
            "admin": {
                "enabled": True,
                "db_path": "runtime/admin.db",
                "instance_lease_s": 30,
                "proxy": {
                    "public_origin": "https://console.example.com",
                    "trusted_proxies": ["127.0.0.1", "10.0.0.0/8", "::1"],
                },
                "adapters": {"oidc": True, "audit_store": True},
                "oidc": {
                    "issuer": "https://idp.example.com",
                    "client_id": "mdgw-console",
                    "client_secret_env": "MDGW_OIDC_CLIENT_SECRET",
                    "roles_claim": "groups",
                },
                "sessions": {
                    "idle_timeout_s": 600,
                    "absolute_timeout_s": 3600,
                    "expiry_warning_s": 120,
                },
                "export": {
                    "root": "exports",
                    "retention_s": 7200,
                    "min_days": 31,
                    "max_days": 90,
                    "max_running_per_user": 2,
                    "global_concurrency": 4,
                    "storage_quota_bytes": 10_000,
                },
                "control": {
                    "socket_path": "runtime/control.sock",
                    "timeout_s": 2.5,
                    "max_message_bytes": 4096,
                },
                "limits": {
                    "page_default": 20,
                    "page_max": 100,
                    "reason_max_length": 500,
                    "operation_retention_s": 3600,
                    "backfill_max_range_days": 120,
                },
            }
        }
    )

    admin = config.admin
    assert admin.enabled is True
    assert admin.db_path == Path("runtime/admin.db")
    assert admin.instance_lease_s == 30
    assert admin.proxy.trusted_proxies == (
        "127.0.0.1",
        "10.0.0.0/8",
        "::1",
    )
    assert admin.oidc.issuer == "https://idp.example.com"
    assert admin.oidc.client_id == "mdgw-console"
    assert admin.export.max_days == 90
    assert admin.control.timeout_s == pytest.approx(2.5)
    assert admin.limits.page_max == 100


@pytest.mark.parametrize(
    "value",
    ["not-an-ip", "10.0.0.0/33", "", "example.com"],
)
def test_admin_rejects_invalid_trusted_proxies(value: str) -> None:
    with pytest.raises(ValidationError, match="trusted_proxies"):
        GatewayConfig.model_validate(
            {"admin": {"proxy": {"trusted_proxies": [value]}}}
        )


@pytest.mark.parametrize(
    "value",
    [
        "console.example.com",
        "/relative",
        "https://console.example.com/console",
        "https://console.example.com/?x=1",
        "ftp://console.example.com",
    ],
)
def test_admin_rejects_invalid_public_origin(value: str) -> None:
    with pytest.raises(ValidationError, match="public_origin"):
        GatewayConfig.model_validate(
            {"admin": {"proxy": {"public_origin": value}}}
        )


@pytest.mark.parametrize("value", ["https://console.example.com", "http://127.0.0.1:8080"])
def test_admin_accepts_valid_public_origin(value: str) -> None:
    config = GatewayConfig.model_validate(
        {"admin": {"proxy": {"public_origin": value}}}
    )
    assert config.admin.proxy.public_origin == value


def test_admin_rejects_unordered_session_timeouts() -> None:
    with pytest.raises(ValidationError, match="expiry_warning_s"):
        GatewayConfig.model_validate(
            {
                "admin": {
                    "sessions": {
                        "idle_timeout_s": 600,
                        "absolute_timeout_s": 3600,
                        "expiry_warning_s": 600,
                    }
                }
            }
        )

    with pytest.raises(ValidationError, match="absolute_timeout_s"):
        GatewayConfig.model_validate(
            {
                "admin": {
                    "sessions": {
                        "idle_timeout_s": 4000,
                        "absolute_timeout_s": 3600,
                    }
                }
            }
        )


def test_admin_rejects_an_export_range_where_min_exceeds_max() -> None:
    with pytest.raises(ValidationError, match="export"):
        GatewayConfig.model_validate(
            {"admin": {"export": {"min_days": 100, "max_days": 90}}}
        )


def test_admin_rejects_page_default_above_page_max() -> None:
    with pytest.raises(ValidationError, match="page_default"):
        GatewayConfig.model_validate(
            {"admin": {"limits": {"page_default": 500, "page_max": 200}}}
        )


def test_admin_database_must_not_live_inside_the_bars_dataset(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValidationError, match="Bars Dataset"):
        GatewayConfig.model_validate(
            {
                "storage": {"root": str(tmp_path)},
                "admin": {"db_path": str(tmp_path / "bars" / "admin.db")},
            }
        )


def test_admin_database_must_not_live_inside_quarantine(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="Quarantine Dataset"):
        GatewayConfig.model_validate(
            {
                "storage": {"root": str(tmp_path)},
                "admin": {"db_path": str(tmp_path / "quarantine" / "admin.db")},
            }
        )


def test_admin_database_beside_the_datasets_is_allowed(tmp_path: Path) -> None:
    config = GatewayConfig.model_validate(
        {
            "storage": {"root": str(tmp_path)},
            "admin": {"db_path": str(tmp_path / "admin" / "admin.db")},
        }
    )
    assert config.admin.db_path == tmp_path / "admin" / "admin.db"
