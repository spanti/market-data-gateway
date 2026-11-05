"""Smoke tests for the consumer examples, benchmarks and demo config.

The subprocess tests run the scripts exactly as a new user would:
``sys.executable <script> ...`` with this checkout's ``src/`` on
``PYTHONPATH`` and the repository root as the working directory.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from mdgw.config import load_config
from mdgw.core.bar import Bar
from mdgw.store.parquet_dataset import ParquetBarStore

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = REPO_ROOT / "examples"
BENCHMARKS = REPO_ROOT / "benchmarks"
DEMO_CONFIG = REPO_ROOT / "config" / "gateway.demo.toml"

UTC = timezone.utc
INSTRUMENT_ID = "binance:BTC/USDT"
OPEN_TS = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)


def _subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    source = str(REPO_ROOT / "src")
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = source + (os.pathsep + existing if existing else "")
    env.pop("PYTHONHOME", None)
    return env


def _run(*args: object, timeout: float = 60.0) -> subprocess.CompletedProcess:
    command = [sys.executable, *(str(arg) for arg in args)]
    return subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=_subprocess_env(),
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _seed_store(root: Path) -> list[Bar]:
    bars = [
        Bar(
            schema_version=1,
            data_type="bar",
            instrument_id=INSTRUMENT_ID,
            interval="1m",
            open_ts=OPEN_TS + timedelta(minutes=index),
            close_ts=OPEN_TS + timedelta(minutes=index + 1),
            event_ts=OPEN_TS + timedelta(minutes=index + 1),
            ingest_ts=OPEN_TS + timedelta(minutes=index + 1, milliseconds=100),
            feed="fake_spot",
            seq=index,
            aggregation_source="external",
            open=price,
            high=price + Decimal("0.5"),
            low=price - Decimal("0.5"),
            close=price + Decimal("0.25"),
            volume=Decimal("1.5"),
            trade_count=3,
            revision=0,
        )
        for index, price in enumerate((Decimal("100.5"), Decimal("101.25")))
    ]
    store = ParquetBarStore(root)
    store.write_bars(bars, asset_class="crypto")
    return bars


def test_parquet_history_example_keeps_only_the_max_revision(
    tmp_path: Path,
) -> None:
    bars = _seed_store(tmp_path)
    corrected = replace(
        bars[0],
        high=Decimal("999.0"),
        close=Decimal("999.0"),
        revision=1,
    )
    ParquetBarStore(tmp_path).write_bars([corrected], asset_class="crypto")

    result = _run(
        EXAMPLES / "parquet_history.py",
        "--data-root",
        tmp_path,
        "--instrument",
        INSTRUMENT_ID,
        "--interval",
        "1m",
        "--limit",
        "10",
    )

    assert result.returncode == 0, result.stderr
    # Two identities exist, with three raw rows after the correction.
    assert "Rows: 2 matching" in result.stdout
    assert "'revision': 1" in result.stdout
    assert "999.0" in result.stdout


def test_demo_config_loads_and_declares_fake_feed() -> None:
    config = load_config(DEMO_CONFIG)

    assert config.server.host == "127.0.0.1"
    assert config.server.http_port == 8080
    assert config.bus.transport == "tcp"
    assert config.bus.collector_pub_bind == "tcp://127.0.0.1:5555"
    assert config.bus.collector_pub_connect == "tcp://127.0.0.1:5555"
    assert config.bus.api_pub_bind == "tcp://127.0.0.1:5556"
    assert config.bus.api_pub_connect == "tcp://127.0.0.1:5556"
    assert config.service.collector_health_port == 8081
    assert config.service.replay_health_port == 8082
    assert config.service.collector_required is True
    assert config.service.wait_for_api_ready is True
    assert config.service.startup_delay_s == pytest.approx(0.2)
    assert config.replay.control_port == 5557
    assert config.replay.publish_port == 5558
    assert config.storage.root == Path("./data-demo")
    assert config.instruments.path == Path("config/instruments")

    (feed,) = config.feeds
    assert feed.name == "fake_spot"
    assert feed.kind == "fake"
    assert feed.venue == "binance"
    assert feed.symbols == ("BTC/USDT", "ETH/USDT")
    assert feed.fake_bars == 20
    assert feed.fake_price == "100"


def test_parquet_history_example_reads_seeded_dataset(tmp_path: Path) -> None:
    _seed_store(tmp_path)

    result = _run(
        EXAMPLES / "parquet_history.py",
        "--data-root",
        tmp_path,
        "--instrument",
        INSTRUMENT_ID,
        "--interval",
        "1m",
        "--limit",
        "10",
    )

    assert result.returncode == 0, result.stderr
    assert INSTRUMENT_ID in result.stdout
    assert "100.5" in result.stdout
    assert "101.25" in result.stdout
    assert "mdgw package was not imported" in result.stdout
    assert "timestamp[ns, tz=UTC]" in result.stdout
    assert "decimal128(38, 18)" in result.stdout
    assert "pyarrow" in result.stdout


@pytest.mark.parametrize(
    "script",
    [
        EXAMPLES / "parquet_history.py",
        EXAMPLES / "rest_history.py",
        EXAMPLES / "ws_live.py",
        EXAMPLES / "replay.py",
    ],
    ids=lambda path: path.name,
)
def test_examples_help_exits_zero(script: Path) -> None:
    result = _run(script, "--help")

    assert result.returncode == 0, result.stderr
    assert "usage" in result.stdout.lower()


def test_nautilus_macd_example_help_exits_zero() -> None:
    # NautilusTrader is an optional consumer extra; skip where it is absent.
    pytest.importorskip("nautilus_trader")
    result = _run(EXAMPLES / "nautilus_macd_backtest.py", "--help")

    assert result.returncode == 0, result.stderr
    assert "usage" in result.stdout.lower()


@pytest.mark.parametrize(
    "script",
    [
        BENCHMARKS / "bench_ingest.py",
        BENCHMARKS / "bench_query.py",
        BENCHMARKS / "bench_replay.py",
        BENCHMARKS / "bench_baseline.py",
    ],
    ids=lambda path: path.name,
)
def test_benchmarks_help_exits_zero(script: Path) -> None:
    result = _run(script, "--help")

    assert result.returncode == 0, result.stderr
    assert "usage" in result.stdout.lower()
