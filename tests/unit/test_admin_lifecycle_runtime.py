# ruff: file-ignore[implicit-namespace-package]
"""Admin-side lifecycle dependency wiring and truthful adapter states."""

from __future__ import annotations

from typing import TYPE_CHECKING

from mdgw.admin.lifecycle.runtime import AdminLifecycle
from mdgw.config import (
    AdminAdaptersConfig,
    AdminConfig,
    AdminControlConfig,
)

if TYPE_CHECKING:
    from pathlib import Path

TOKEN_ENV = "MDGW_TEST_RUNTIME_TOKEN"  # ruff: ignore[hardcoded-password-string]


def _config(**updates) -> AdminConfig:
    base = AdminConfig(
        enabled=True,
        adapters=AdminAdaptersConfig(supervisor=True),
        control=AdminControlConfig(port=0, token_env=TOKEN_ENV),
    )
    return base.model_copy(update=updates) if updates else base


def _runtime(
    tmp_path: Path,
    config: AdminConfig,
    environ: dict[str, str] | None = None,
) -> AdminLifecycle:
    return AdminLifecycle(
        config=config,
        db_path=tmp_path / "admin" / "admin.db",
        environ=environ or {},
    )


def test_the_adapter_reports_unsupported_when_disabled(tmp_path: Path) -> None:
    config = _config(
        adapters=AdminAdaptersConfig(supervisor=False),
        control=AdminControlConfig(port=12345, token_env=TOKEN_ENV),
    )
    runtime = _runtime(tmp_path, config, {TOKEN_ENV: "secret"})

    assert runtime.adapter_status().state == "unsupported"
    assert runtime.control_client() is None


def test_the_adapter_reports_unavailable_without_endpoint_or_token(
    tmp_path: Path,
) -> None:
    no_port = _runtime(tmp_path, _config(), {TOKEN_ENV: "secret"})
    assert no_port.adapter_status().state == "unavailable"
    assert no_port.adapter_status().reason == (
        "supervisor control endpoint is not configured"
    )

    no_token = _runtime(
        tmp_path,
        _config(control=AdminControlConfig(port=12345, token_env=TOKEN_ENV)),
        {},
    )
    assert no_token.adapter_status().state == "unavailable"
    assert no_token.adapter_status().reason == (
        "supervisor control token is not configured"
    )


def test_the_adapter_is_available_and_builds_a_client_when_configured(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        _config(control=AdminControlConfig(port=12345, token_env=TOKEN_ENV)),
        {TOKEN_ENV: "secret"},
    )

    assert runtime.adapter_status().state == "available"
    client = runtime.control_client()
    assert client is not None
    assert client.port == 12345
    assert client.host == "127.0.0.1"


def test_the_store_is_lazy_and_closes_cleanly(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path, _config(), {})

    store = runtime.store
    assert store.is_open
    assert store.list_instances() == []
    runtime.close()
    assert not store.is_open
