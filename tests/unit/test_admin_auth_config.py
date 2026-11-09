# ruff: file-ignore[implicit-namespace-package]
"""OIDC/session configuration for ticket 02.

The defaults must stay inert (OIDC off, role mapping empty, short transaction
TTL) and the configuration must reject an ambiguous role mapping and unsafe
redirect paths before the service starts.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from mdgw.config import AdminConfig, AdminOidcConfig, GatewayConfig

ROLES = {"SuperAdmin", "Operator", "Auditor"}


def test_oidc_defaults_stay_fail_closed() -> None:
    oidc = AdminConfig().oidc

    assert oidc.issuer is None
    assert oidc.client_id is None
    assert oidc.client_secret_env is None
    assert oidc.role_mapping == {}
    assert oidc.roles_claim == "groups"
    assert oidc.redirect_uri is None
    assert oidc.default_return_to == "/"
    assert oidc.error_redirect_path == "/login"
    assert oidc.transaction_ttl_s == 300
    assert oidc.transaction_ttl_s <= 600
    assert oidc.discovery_ttl_s > 0
    assert oidc.jwks_ttl_s > 0
    assert oidc.http_timeout_s > 0
    assert oidc.clock_skew_s >= 0
    assert oidc.is_configured is False


def test_repository_config_keeps_oidc_inert() -> None:
    config = GatewayConfig()

    assert config.admin.enabled is False
    assert config.admin.oidc.is_configured is False


def test_oidc_is_configured_only_with_issuer_client_and_secret_env() -> None:
    partial = AdminOidcConfig(
        issuer="https://idp.example.com", client_id="mdgw-console"
    )
    assert partial.is_configured is False

    complete = AdminOidcConfig(
        issuer="https://idp.example.com",
        client_id="mdgw-console",
        client_secret_env="MDGW_OIDC_CLIENT_SECRET",  # ruff: ignore[hardcoded-password-func-arg]  # fake env name
    )
    assert complete.is_configured is True


def test_role_mapping_is_ordered_and_explicit() -> None:
    oidc = AdminOidcConfig(
        role_mapping={
            "mdgw-admins": "SuperAdmin",
            "mdgw-operators": "Operator",
            "mdgw-auditors": "Auditor",
        }
    )

    assert list(oidc.role_mapping) == [
        "mdgw-admins",
        "mdgw-operators",
        "mdgw-auditors",
    ]
    assert set(oidc.role_mapping.values()) == ROLES


def test_role_mapping_accepts_list_of_pairs() -> None:
    oidc = AdminOidcConfig(
        role_mapping=[["mdgw-admins", "SuperAdmin"], ["mdgw-ops", "Operator"]]
    )

    assert oidc.role_mapping == {
        "mdgw-admins": "SuperAdmin",
        "mdgw-ops": "Operator",
    }


@pytest.mark.parametrize(
    "role_mapping",
    [
        [["mdgw-admins", "SuperAdmin"], ["mdgw-admins", "Operator"]],
        {"": "SuperAdmin"},
        {"mdgw-admins": "Root"},
        {"mdgw-admins": ""},
    ],
)
def test_role_mapping_rejects_ambiguous_or_invalid_entries(
    role_mapping,
) -> None:
    with pytest.raises(ValidationError, match="role_mapping"):
        AdminOidcConfig(role_mapping=role_mapping)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("default_return_to", "https://evil.example/"),
        ("default_return_to", "//evil.example/"),
        ("default_return_to", "relative"),
        ("default_return_to", "/monitoring\u0000"),
        ("error_redirect_path", "https://evil.example/login"),
        ("error_redirect_path", "//evil.example/login"),
        ("error_redirect_path", "/login\n"),
        ("error_redirect_path", ""),
    ],
)
def test_redirect_paths_must_be_relative_same_origin(
    field: str, value: str
) -> None:
    with pytest.raises(ValidationError, match=field):
        AdminOidcConfig(**{field: value})


@pytest.mark.parametrize(
    "value", ["not-a-url", "ftp://idp.example.com", "https://"]
)
def test_redirect_uri_must_be_an_absolute_http_url(value: str) -> None:
    with pytest.raises(ValidationError, match="redirect_uri"):
        AdminOidcConfig(redirect_uri=value)


def test_transaction_ttl_is_bounded_to_ten_minutes() -> None:
    assert AdminOidcConfig(transaction_ttl_s=600).transaction_ttl_s == 600
    with pytest.raises(ValidationError, match="transaction_ttl_s"):
        AdminOidcConfig(transaction_ttl_s=601)
    with pytest.raises(ValidationError, match="transaction_ttl_s"):
        AdminOidcConfig(transaction_ttl_s=0)


def test_gateway_config_accepts_the_oidc_section() -> None:
    config = GatewayConfig.model_validate(
        {
            "admin": {
                "enabled": True,
                "adapters": {"oidc": True},
                "oidc": {
                    "issuer": "https://idp.example.com",
                    "client_id": "mdgw-console",
                    "client_secret_env": "MDGW_OIDC_CLIENT_SECRET",
                    "roles_claim": "groups",
                    "role_mapping": {
                        "mdgw-admins": "SuperAdmin",
                        "mdgw-operators": "Operator",
                    },
                    "redirect_uri": (
                        "https://console.example.com/api/v1/admin/auth/callback"
                    ),
                },
            }
        }
    )

    assert config.admin.oidc.role_mapping["mdgw-operators"] == "Operator"
    assert config.admin.oidc.is_configured is True
