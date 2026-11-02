"""Minimal operator CLI: `mdgw compact` and `mdgw gc`.

`run --service` / `up` belong to a later ticket; this module deliberately uses
only `argparse`.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from mdgw.config import GatewayConfig, load_config
from mdgw.store.maintenance import parse_duration
from mdgw.store.parquet_dataset import ParquetBarStore

DEFAULT_CONFIG_PATH = Path("config/gateway.toml")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mdgw",
        description="Market Data Gateway operator commands",
    )
    _add_common_options(parser)

    subcommands = parser.add_subparsers(dest="command", required=True)

    compact = subcommands.add_parser(
        "compact", help="merge small Data Files per partition"
    )
    _add_common_options(compact, suppress_defaults=True)
    compact.add_argument(
        "--older-than",
        required=True,
        metavar="DURATION",
        help="only compact Data Files older than this (for example 24h or 7d)",
    )

    gc = subcommands.add_parser(
        "gc", help="delete whole month partitions past the Retention window"
    )
    _add_common_options(gc, suppress_defaults=True)
    mode = gc.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be deleted (default)",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="actually delete Data Files and switch the Manifest",
    )

    return parser


def _add_common_options(
    parser: argparse.ArgumentParser, *, suppress_defaults: bool = False
) -> None:
    default = argparse.SUPPRESS if suppress_defaults else None
    parser.add_argument(
        "--config",
        type=Path,
        default=default,
        help="path to gateway.toml",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=default,
        help="override the Dataset parent directory",
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = _load_config(args)
    store = _build_store(args, config)

    if args.command == "compact":
        try:
            older_than = parse_duration(args.older_than)
        except ValueError as exc:
            parser.error(str(exc))
        report = store.compact(older_than=older_than)
        print(
            "compact: merged "
            f"{len(report.merged_partitions)} partition(s), wrote "
            f"{len(report.new_files)} file(s), removed "
            f"{len(report.removed_files)} file(s), skipped "
            f"{len(report.skipped_locked)} locked partition(s)"
        )
        for failure in report.failures:
            print(f"error: {failure}", file=sys.stderr)
        return 1 if report.failures else 0

    apply = bool(getattr(args, "apply", False))
    if not config.retention.enabled:
        print("gc: retention is disabled; nothing to do")
        return 0

    report = store.gc(
        bars_days=config.retention.bars_days,
        apply=apply,
    )
    mode = "apply" if apply else "dry-run"
    print(
        f"gc ({mode}): {len(report.removed_partitions)} month partition(s), "
        f"{len(report.removed_files)} file(s), skipped "
        f"{len(report.skipped_locked)} locked and "
        f"{len(report.skipped_current_month)} current month"
    )
    for failure in report.failures:
        print(f"error: {failure}", file=sys.stderr)
    return 1 if report.failures else 0


def _load_config(args: argparse.Namespace) -> GatewayConfig:
    config_path = getattr(args, "config", None)
    if config_path is None and DEFAULT_CONFIG_PATH.exists():
        config_path = DEFAULT_CONFIG_PATH
    if config_path is None:
        return GatewayConfig()
    try:
        return load_config(config_path)
    except (OSError, ValueError) as exc:
        raise SystemExit(f"mdgw: cannot load config {config_path}: {exc}") from exc


def _build_store(
    args: argparse.Namespace, config: GatewayConfig
) -> ParquetBarStore:
    root = getattr(args, "root", None) or config.storage.root
    return ParquetBarStore(
        root,
        compression=config.storage.compression,
        row_group_size=config.storage.row_group_size,
        format_version=config.storage.format_version,
        schema_version=config.storage.schema_version,
    )
