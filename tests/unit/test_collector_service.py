"""CollectorService startup handshake (no 30-second waits)."""

from __future__ import annotations

import logging
import time
from pathlib import Path

from mdgw.config import GatewayConfig
from mdgw.ingest.fake import FakeFeedHandler
from mdgw.service.collector import CollectorService


def _config(*, wait_for_api_ready: bool, timeout_s: float = 0.05) -> GatewayConfig:
    return GatewayConfig.model_validate(
        {
            "service": {
                "wait_for_api_ready": wait_for_api_ready,
                "api_ready_timeout_s": timeout_s,
                "startup_delay_s": 0.0,
            }
        }
    )


def _service(tmp_path: Path, config: GatewayConfig, **kwargs) -> CollectorService:
    feed = FakeFeedHandler(instrument_id="binance:BTC/USDT", bars=())
    return CollectorService(
        config,
        data_root=tmp_path,
        feed_handler=feed,
        **kwargs,
    )


def test_wait_for_api_timeout_is_bounded_and_logged(
    tmp_path: Path, caplog
) -> None:
    service = _service(tmp_path, _config(wait_for_api_ready=True))

    with caplog.at_level(
        logging.WARNING, logger="mdgw.service.collector"
    ):
        started = time.monotonic()
        assert service._wait_for_api_ready() is False
        elapsed = time.monotonic() - started

    assert elapsed >= 0.04
    assert elapsed < 5.0
    assert any(
        getattr(record, "event", None) == "collector.api_ready_timeout"
        for record in caplog.records
    )


def test_wait_for_api_returns_once_the_event_is_set(tmp_path: Path) -> None:
    service = _service(tmp_path, _config(wait_for_api_ready=True))

    service._api_ready.set()

    assert service._wait_for_api_ready() is True


def test_wait_for_api_defaults_to_config_and_can_be_overridden(
    tmp_path: Path,
) -> None:
    from_config = _service(tmp_path, _config(wait_for_api_ready=True))
    assert from_config._wait_for_api is True

    overridden = _service(
        tmp_path, _config(wait_for_api_ready=True), wait_for_api=False
    )
    assert overridden._wait_for_api is False
    started = time.monotonic()
    assert overridden._wait_for_api_ready() is True
    assert time.monotonic() - started < 1.0
