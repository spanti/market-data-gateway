"""Quarantine Retention/GC through the operator CLI, isolated from Bars.

The Quarantine Dataset has its own Manifest and never shares state with the
Bars Dataset, Instrument Snapshot or the data High-Water Mark. GC is explicit,
dry-run by default, and only removes complete UTC months past Retention.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import typer

from mdgw.cli import main
from mdgw.core.bar import Bar
from mdgw.core.time import datetime_to_ns, utc_now
from mdgw.quarantine import (
    REASON_FLOAT_VALUE,
    SOURCE_STAGE_LIVE_NORMALIZE,
    QuarantineRecord,
    QuarantineStore,
)
from mdgw.state.high_water_mark import HighWaterMarkStore
from mdgw.store.parquet_dataset import ParquetBarStore

INSTRUMENT = "binance:BTC/USDT"


def _record(recorded_ts: datetime) -> QuarantineRecord:
    return QuarantineRecord(
        source_stage=SOURCE_STAGE_LIVE_NORMALIZE,
        reason_code=REASON_FLOAT_VALUE,
        rule_version="1",
        source_location="live:batch:2024-01-05T12:00:00.000000000Z:row=0",
        details={"field": "open"},
        recorded_ts=recorded_ts,
    )


def _write_config(
    tmp_path: Path,
    root: Path,
    *,
    quarantine_root: Path | None = None,
    retention_days: int = 30,
) -> Path:
    config_path = tmp_path / "gateway.toml"
    lines = [
        "[storage]",
        f'root = "{root.as_posix()}"',
        "[storage.quarantine]",
    ]
    if quarantine_root is not None:
        lines.append(f'root = "{quarantine_root.as_posix()}"')
    lines.append(f"retention_days = {retention_days}")
    config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return config_path


def _bar(open_ts: datetime) -> Bar:
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=INSTRUMENT,
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


def test_cli_quarantine_gc_defaults_to_dry_run_and_apply_deletes(
    tmp_path: Path, capsys
) -> None:
    root = tmp_path / "data"
    quarantine = QuarantineStore(root / "quarantine")
    quarantine.write_records(
        [_record(datetime(2020, 1, 5, tzinfo=UTC)), _record(utc_now())]
    )
    old_file = next((root / "quarantine" / "year=2020" / "month=01").glob("*.parquet"))
    config_path = _write_config(tmp_path, root)

    dry = main(["--config", str(config_path), "quarantine-gc"])
    assert dry == 0
    output = capsys.readouterr().out
    assert "dry-run" in output
    assert "2020-01" in output
    assert old_file.exists()
    assert quarantine.stats()["active_file_count"] == 2

    applied = main(
        ["--config", str(config_path), "quarantine-gc", "--apply"]
    )
    assert applied == 0
    output = capsys.readouterr().out
    assert "apply" in output
    assert not old_file.exists()
    assert not (root / "quarantine" / "year=2020").exists()
    assert quarantine.stats()["active_file_count"] == 1


def test_cli_quarantine_gc_zero_retention_keeps_everything(
    tmp_path: Path, capsys
) -> None:
    root = tmp_path / "data"
    quarantine = QuarantineStore(root / "quarantine")
    quarantine.write_records([_record(datetime(2020, 1, 5, tzinfo=UTC))])
    config_path = _write_config(tmp_path, root)

    result = main(
        [
            "--config",
            str(config_path),
            "quarantine-gc",
            "--apply",
            "--retention-days",
            "0",
        ]
    )

    assert result == 0
    assert "forever" in capsys.readouterr().out
    assert quarantine.stats()["active_file_count"] == 1
    assert quarantine.read_records().num_rows == 1


def test_cli_quarantine_gc_default_root_follows_the_root_override(
    tmp_path: Path, capsys
) -> None:
    configured_root = tmp_path / "configured"
    override_root = tmp_path / "override"
    quarantine = QuarantineStore(override_root / "quarantine")
    quarantine.write_records(
        [_record(datetime(2020, 1, 5, tzinfo=UTC)), _record(utc_now())]
    )
    config_path = _write_config(tmp_path, configured_root)

    result = main(
        [
            "--config",
            str(config_path),
            "--root",
            str(override_root),
            "quarantine-gc",
            "--apply",
        ]
    )

    assert result == 0
    assert "2020-01" in capsys.readouterr().out
    assert quarantine.stats()["active_file_count"] == 1
    # The configured root is not touched, and Quarantine-only composition
    # creates no Bars Dataset state or DuckDB temp directory.
    assert not configured_root.exists()
    assert not (override_root / "bars").exists()
    assert not (override_root / ".duckdb_tmp").exists()


def test_cli_quarantine_gc_explicit_root_does_not_follow_the_root_override(
    tmp_path: Path, capsys
) -> None:
    configured_root = tmp_path / "configured"
    override_root = tmp_path / "override"
    explicit_root = tmp_path / "audit" / "quarantine"
    quarantine = QuarantineStore(explicit_root)
    quarantine.write_records(
        [_record(datetime(2020, 1, 5, tzinfo=UTC)), _record(utc_now())]
    )
    config_path = _write_config(
        tmp_path, configured_root, quarantine_root=explicit_root
    )

    result = main(
        [
            "--config",
            str(config_path),
            "--root",
            str(override_root),
            "quarantine-gc",
            "--apply",
        ]
    )

    assert result == 0
    assert "2020-01" in capsys.readouterr().out
    assert quarantine.stats()["active_file_count"] == 1
    assert not configured_root.exists()
    assert not override_root.exists()


def test_cli_quarantine_gc_rejects_a_layout_nested_under_the_root_override(
    tmp_path: Path,
) -> None:
    configured_root = tmp_path / "configured"
    override_root = tmp_path / "override"
    # Valid against the configured root, nested only once the override wins.
    nested_quarantine = override_root / "bars" / "quarantine"
    config_path = _write_config(
        tmp_path, configured_root, quarantine_root=nested_quarantine
    )

    with pytest.raises(ValueError, match="must not be inside the Bars Dataset"):
        main(
            [
                "--config",
                str(config_path),
                "--root",
                str(override_root),
                "quarantine-gc",
            ]
        )

    assert not configured_root.exists()
    assert not override_root.exists()


def test_cli_quarantine_gc_validates_retention_before_composing_storage(
    tmp_path: Path,
) -> None:
    configured_root = tmp_path / "configured"
    override_root = tmp_path / "override"
    # This layout only becomes invalid once the override is applied; the
    # invalid --retention-days must still win before composition runs.
    nested_quarantine = override_root / "bars" / "quarantine"
    config_path = _write_config(
        tmp_path, configured_root, quarantine_root=nested_quarantine
    )

    # A parameter error (not the composition ValueError the nested layout
    # would produce) proves validation ran first.
    with pytest.raises(typer.BadParameter):
        main(
            [
                "--config",
                str(config_path),
                "--root",
                str(override_root),
                "quarantine-gc",
                "--retention-days",
                "-1",
            ]
        )

    assert not configured_root.exists()
    assert not override_root.exists()


def test_quarantine_gc_does_not_touch_bars_manifest_snapshot_or_hwm(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    bars = ParquetBarStore(root)
    bars.write_bars(
        [_bar(datetime(2020, 1, 15, tzinfo=UTC))], asset_class="crypto"
    )
    manifest_before = (bars.dataset_root / "_manifest.json").read_bytes()
    descriptor_before = (bars.dataset_root / "_dataset.json").read_bytes()

    snapshot_dir = root / "instruments"
    snapshot_dir.mkdir(parents=True)
    snapshot = snapshot_dir / "snapshot.json"
    snapshot.write_text('{"instruments": []}\n', encoding="utf-8")

    hwm = HighWaterMarkStore(root / "state.db")
    hwm_ts = datetime_to_ns(datetime(2020, 1, 15, tzinfo=UTC))
    try:
        hwm.set(INSTRUMENT, "1m", hwm_ts)
        before_hwm = dict(hwm.all())

        quarantine = QuarantineStore(root / "quarantine")
        quarantine.write_records(
            [_record(datetime(2020, 1, 5, tzinfo=UTC)), _record(utc_now())]
        )
        report = quarantine.gc(30, apply=True, now=utc_now())
        assert report.removed_partitions == ("2020-01",)

        assert (bars.dataset_root / "_manifest.json").read_bytes() == manifest_before
        assert (bars.dataset_root / "_dataset.json").read_bytes() == descriptor_before
        assert snapshot.read_text(encoding="utf-8") == '{"instruments": []}\n'
        assert dict(hwm.all()) == before_hwm
        assert bars.read_bars(
            instrument_id=INSTRUMENT, interval="1m"
        ).num_rows == 1

        manifest = json.loads(manifest_before)
        assert manifest["files"]
        assert all("quarantine" not in entry["path"] for entry in manifest["files"])
    finally:
        hwm.close()
