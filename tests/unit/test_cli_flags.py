"""CLI flags that control the collector -> api startup handshake."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pytest
from typer.testing import CliRunner

from mdgw import cli
from mdgw.cli import _build_up_specs
from mdgw.config import GatewayConfig

RUNNER = CliRunner()


@pytest.fixture
def run_service_spy(monkeypatch):
    captured: dict[str, object] = {}

    def fake_run_service(
        service: str,
        *,
        config_path: Optional[Path],
        root: Optional[Path],
        log_level: Optional[str],
        wait_for_api: Optional[bool],
    ) -> int:
        captured.update(
            service=service,
            config_path=config_path,
            root=root,
            log_level=log_level,
            wait_for_api=wait_for_api,
        )
        return 0

    monkeypatch.setattr(cli, "_run_service", fake_run_service)
    return captured


def test_run_accepts_wait_for_api_flag(run_service_spy) -> None:
    result = RUNNER.invoke(
        cli.app, ["run", "--service", "collector", "--wait-for-api"]
    )

    assert result.exit_code == 0, result.output
    assert run_service_spy["wait_for_api"] is True


def test_run_accepts_no_wait_for_api_flag(run_service_spy) -> None:
    result = RUNNER.invoke(
        cli.app, ["run", "--service", "collector", "--no-wait-for-api"]
    )

    assert result.exit_code == 0, result.output
    assert run_service_spy["wait_for_api"] is False


def test_run_wait_for_api_defaults_to_config(run_service_spy) -> None:
    result = RUNNER.invoke(cli.app, ["run", "--service", "collector"])

    assert result.exit_code == 0, result.output
    assert run_service_spy["wait_for_api"] is None


def test_common_options_are_accepted_before_the_subcommand(
    run_service_spy,
) -> None:
    result = RUNNER.invoke(
        cli.app,
        [
            "--config",
            "gateway.toml",
            "--root",
            "data",
            "run",
            "--service",
            "api",
        ],
    )

    assert result.exit_code == 0, result.output
    assert run_service_spy["config_path"] == Path("gateway.toml")
    assert run_service_spy["root"] == Path("data")


def test_common_options_are_accepted_after_the_subcommand(
    run_service_spy,
) -> None:
    result = RUNNER.invoke(
        cli.app,
        [
            "run",
            "--service",
            "api",
            "--config",
            "gateway.toml",
            "--root",
            "data",
        ],
    )

    assert result.exit_code == 0, result.output
    assert run_service_spy["config_path"] == Path("gateway.toml")
    assert run_service_spy["root"] == Path("data")


def test_run_rejects_an_unknown_service_role() -> None:
    result = RUNNER.invoke(cli.app, ["run", "--service", "scheduler"])

    assert result.exit_code == 2
    assert "scheduler" in result.output


def test_up_collector_child_enables_wait_for_api() -> None:
    specs = _build_up_specs(
        GatewayConfig(),
        None,
        Path("data"),
        "INFO",
    )
    collector = next(spec for spec in specs if spec.role == "collector")
    assert "--wait-for-api" in collector.command
