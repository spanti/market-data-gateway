"""Replay Service Role start log exposes the resolved endpoints."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from mdgw.config import GatewayConfig
from mdgw.logging import JsonFormatter
from mdgw.reference import (
    export_instrument_snapshot,
    load_instrument_reference,
)
from mdgw.service.replay import ReplayServiceRole

INSTRUMENT_YAML = """\
- instrument_id: "binance:BTC/USDT"
  venue: binance
  symbol: "BTC/USDT"
  asset_class: crypto
  instrument_type: spot
  base: BTC
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.00001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
- instrument_id: "binance:ETH/USDT"
  venue: binance
  symbol: "ETH/USDT"
  asset_class: crypto
  instrument_type: spot
  base: ETH
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.0001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
"""


def _export_snapshot(root: Path) -> Path:
    reference_dir = root / "reference"
    reference_dir.mkdir()
    (reference_dir / "binance.yaml").write_text(
        INSTRUMENT_YAML, encoding="utf-8"
    )
    reference = load_instrument_reference(reference_dir)
    return export_instrument_snapshot(reference, root / "instruments").path


def _config(tmp_path: Path) -> GatewayConfig:
    return GatewayConfig.model_validate(
        {
            "storage": {"root": str(tmp_path)},
            "replay": {"control_port": 0, "publish_port": 0},
            "service": {"replay_health_port": 0},
        }
    )


def test_replay_started_log_contains_resolved_endpoints(
    tmp_path: Path, caplog
) -> None:
    role = ReplayServiceRole(_config(tmp_path))
    try:
        with caplog.at_level(logging.INFO, logger="mdgw.service.replay"):
            role.start()

        record = next(
            record
            for record in caplog.records
            if getattr(record, "event", None) == "replay.started"
        )
        assert record.control_endpoint.startswith("tcp://")
        assert record.publish_endpoint.startswith("tcp://")
        assert record.control_endpoint == role.control_endpoint
        assert record.publish_endpoint == role.publish_endpoint

        payload = json.loads(JsonFormatter(service="replay").format(record))
        assert payload["control_endpoint"] == role.control_endpoint
        assert payload["publish_endpoint"] == role.publish_endpoint
    finally:
        role.stop()


def test_replay_role_loads_the_instrument_snapshot(
    tmp_path: Path, caplog
) -> None:
    snapshot_path = _export_snapshot(tmp_path)
    role = ReplayServiceRole(_config(tmp_path))
    try:
        with caplog.at_level(logging.INFO, logger="mdgw.service.replay"):
            role.start()

        assert role.instrument_store is not None
        assert len(role.instrument_store) == 2
        assert role.service is not None

        status = role.status_payload()["instrument_snapshot"]
        assert status == {
            "status": "loaded",
            "loaded": True,
            "snapshot": str(snapshot_path),
            "count": 2,
        }
        record = next(
            record
            for record in caplog.records
            if getattr(record, "event", None)
            == "replay.instrument_snapshot_loaded"
        )
        assert record.snapshot == str(snapshot_path)
        assert record.instruments == 2
    finally:
        role.stop()


def test_replay_role_reports_a_missing_instrument_snapshot(
    tmp_path: Path, caplog
) -> None:
    role = ReplayServiceRole(_config(tmp_path))
    try:
        with caplog.at_level(logging.WARNING, logger="mdgw.service.replay"):
            role.start()

        assert role.instrument_store is None
        assert role.status_payload()["instrument_snapshot"] == {
            "status": "missing",
            "loaded": False,
            "snapshot": None,
            "count": 0,
        }
        record = next(
            record
            for record in caplog.records
            if getattr(record, "event", None)
            == "replay.instrument_snapshot_missing"
        )
        assert record.directory == str(tmp_path / "instruments")
    finally:
        role.stop()
