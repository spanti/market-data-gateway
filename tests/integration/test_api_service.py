# ruff: file-ignore[implicit-namespace-package]
"""In-process integration test for the ApiService HTTP lifecycle.

The api can serve historical REST without a live collector: `collector_required`
defaults to false, so `/readyz` is 200 while `collector_ready` is false. This
exercises the api's public start/stop/base_url seam directly instead of spawning
a subprocess.
"""

from __future__ import annotations

import json
import socket
import threading
import urllib.request
from pathlib import Path

from mdgw.config import load_config
from mdgw.service.api import ApiService

PROJECT_ROOT = Path(__file__).resolve().parents[2]
HTTP_THREAD_NAME = "mdgw-api-http"
WAIT_TIMEOUT_S = 15.0
HOST = "127.0.0.1"


def _config():
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(
        update={"http_port": 0, "host": HOST}
    )
    bus = config.bus.model_copy(
        update={"api_pub_bind": "tcp://127.0.0.1:0"}
    )
    return config.model_copy(update={"server": server, "bus": bus})


def _get_json(url: str) -> tuple[int, dict]:
    with urllib.request.urlopen(  # ruff: ignore[suspicious-url-open-usage]
        url, timeout=WAIT_TIMEOUT_S
    ) as response:
        return response.status, json.loads(response.read())


def _threads_named(name: str) -> list[threading.Thread]:
    return [thread for thread in threading.enumerate() if thread.name == name]


def test_api_service_serves_operational_endpoints_without_a_collector(
    tmp_path: Path,
) -> None:
    service = ApiService(_config(), data_root=tmp_path)
    try:
        service.start()
        port = service.http_port
        base_url = service.base_url

        assert port > 0
        assert base_url == f"http://{HOST}:{port}"

        health_status, health = _get_json(f"{base_url}/healthz")
        assert health_status == 200
        assert health["status"] == "ok"
        assert health["service"] == "api"

        ready_status, ready = _get_json(f"{base_url}/readyz")
        assert ready_status == 200
        assert ready["ready"] is True
        assert ready["collector_ready"] is False

        dataset_status, dataset = _get_json(f"{base_url}/api/v1/status")
        assert dataset_status == 200
        assert dataset["dataset"] == "bars"

        status_code, status = _get_json(f"{base_url}/status")
        assert status_code == 200
        assert status["service"] == "api"
        assert status["collector"]["ready"] is False
    finally:
        service.stop()

    assert _threads_named(HTTP_THREAD_NAME) == []
    # No lingering listener: the port is free for immediate reuse.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind((HOST, port))
