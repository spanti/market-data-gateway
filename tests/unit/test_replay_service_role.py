"""Replay Service Role start log exposes the resolved endpoints."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from mdgw.config import GatewayConfig
from mdgw.logging import JsonFormatter
from mdgw.service.replay import ReplayServiceRole


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
