"""Minimal `mdgw` CLI: `compact` and `gc` (run/up belong to a later ticket)."""

from __future__ import annotations

import importlib.metadata
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mdgw.core.bar import Bar
from mdgw.store.parquet_dataset import ParquetBarStore

UTC = timezone.utc
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC = PROJECT_ROOT / "src"


def _bar(open_ts: datetime, *, instrument_id: str = "binance:BTC/USDT") -> Bar:
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=instrument_id,
        interval="1m",
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=None,
        aggregation_source="external",
        open=Decimal("1"),
        high=Decimal("2"),
        low=Decimal("0.5"),
        close=Decimal("1.5"),
        volume=Decimal("3"),
        trade_count=None,
        revision=0,
    )


def _active_count(dataset_root: Path) -> int:
    manifest = json.loads((dataset_root / "_manifest.json").read_text(encoding="utf-8"))
    return sum(1 for entry in manifest["files"] if entry["active"])


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    # Run this checkout's `src/`, not whatever `mdgw` is installed in the
    # environment (a worktree checkout must test its own code).
    env = os.environ.copy()
    previous = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        str(SRC) if not previous else str(SRC) + os.pathsep + previous
    )
    return subprocess.run(
        [sys.executable, "-m", "mdgw", *args],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=str(PROJECT_ROOT),
        env=env,
    )


def test_console_script_is_declared() -> None:
    scripts = {
        entry.name
        for entry in importlib.metadata.entry_points(group="console_scripts")
    }
    assert "mdgw" in scripts


def test_console_script_invokes_the_cli() -> None:
    executable = shutil.which("mdgw")
    if executable is None:  # pragma: no cover - depends on the active environment
        import pytest

        pytest.skip("mdgw console script is not on PATH")
    result = subprocess.run(
        [executable, "--help"], capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0
    assert "compact" in result.stdout


def test_cli_compact_merges_files(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(datetime(2024, 1, 1, 0, 0, tzinfo=UTC))], asset_class="crypto")
    store.write_bars([_bar(datetime(2024, 1, 1, 0, 1, tzinfo=UTC))], asset_class="crypto")

    result = _run_cli(
        "--root", str(tmp_path), "compact", "--older-than", "0s"
    )

    assert result.returncode == 0, result.stderr
    assert "merged" in result.stdout.lower()
    assert _active_count(store.dataset_root) == 1


def test_cli_gc_defaults_to_dry_run_and_apply_deletes(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [_bar(datetime(2024, 1, 15, tzinfo=UTC))], asset_class="crypto"
    )
    store.write_bars(
        [_bar(datetime.now(tz=UTC), instrument_id="binance:SOL/USDT")],
        asset_class="crypto",
    )
    config_path = tmp_path / "gateway.toml"
    config_path.write_text(
        "\n".join(
            [
                "[storage]",
                f'root = "{tmp_path.as_posix()}"',
                "[retention]",
                "enabled = true",
                "bars_days = 90",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    dry = _run_cli("--config", str(config_path), "gc")
    assert dry.returncode == 0, dry.stderr
    assert "dry" in dry.stdout.lower()
    assert _active_count(store.dataset_root) == 2

    applied = _run_cli("--config", str(config_path), "gc", "--apply")
    assert applied.returncode == 0, applied.stderr
    assert _active_count(store.dataset_root) == 1
    assert store.read_bars(interval="1m").num_rows == 1


def test_cli_gc_apply_is_a_no_op_when_retention_is_disabled(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [_bar(datetime(2024, 1, 15, tzinfo=UTC))], asset_class="crypto"
    )
    config_path = tmp_path / "gateway.toml"
    config_path.write_text(
        "\n".join(
            [
                "[storage]",
                f'root = "{tmp_path.as_posix()}"',
                "[retention]",
                "enabled = false",
                "bars_days = 90",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = _run_cli("--config", str(config_path), "gc", "--apply")

    assert result.returncode == 0, result.stderr
    assert _active_count(store.dataset_root) == 1
