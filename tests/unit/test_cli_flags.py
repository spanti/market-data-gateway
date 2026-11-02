"""CLI flags that control the collector -> api startup handshake."""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from mdgw.cli import _build_up_specs, build_parser
from mdgw.config import GatewayConfig


def test_run_accepts_wait_for_api_flag() -> None:
    args = build_parser().parse_args(
        ["run", "--service", "collector", "--wait-for-api"]
    )
    assert args.wait_for_api is True


def test_run_accepts_no_wait_for_api_flag() -> None:
    args = build_parser().parse_args(
        ["run", "--service", "collector", "--no-wait-for-api"]
    )
    assert args.wait_for_api is False


def test_run_wait_for_api_defaults_to_config() -> None:
    args = build_parser().parse_args(["run", "--service", "collector"])
    assert args.wait_for_api is None


def test_up_collector_child_enables_wait_for_api() -> None:
    specs = _build_up_specs(
        Namespace(config=None, root=None, log_level=None),
        GatewayConfig(),
        Path("data"),
        "INFO",
    )
    collector = next(spec for spec in specs if spec.role == "collector")
    assert "--wait-for-api" in collector.command
