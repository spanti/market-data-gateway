# ruff: file-ignore[implicit-namespace-package,hardcoded-password-string]
"""Collector and Supervisor control servers use independent endpoints (S3).

Both adapters previously read the same ``admin.control.host/port`` pair, so a
deployment enabling both could not bind them at once. The configuration now
has a separate endpoint per server; this test starts both servers from the
configured values and verifies each client reaches its own service.
"""

from __future__ import annotations

import socket
from typing import TYPE_CHECKING

from mdgw.admin.feed.client import CollectorControlClient
from mdgw.admin.lifecycle.runtime import AdminLifecycle
from mdgw.config import (
    AdminAdaptersConfig,
    AdminConfig,
    AdminControlConfig,
)
from mdgw.service.control_plane import (
    ACTION_PING,
    ControlPlaneClient,
    ControlPlaneServer,
    collector_control_registry,
)
from mdgw.service.supervisor import ChildSpec, Supervisor
from mdgw.service.supervisor_control import SupervisorControlServer

if TYPE_CHECKING:
    from pathlib import Path

HOST = "127.0.0.1"
TOKEN_ENV = "MDGW_TEST_SPLIT_CONTROL_TOKEN"
TOKEN = "split-control-token"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((HOST, 0))
        return int(probe.getsockname()[1])


def _client_config(
    collector_port: int, supervisor_port: int
) -> AdminControlConfig:
    return AdminControlConfig(
        collector_host=HOST,
        collector_port=collector_port,
        supervisor_host=HOST,
        supervisor_port=supervisor_port,
        token_env=TOKEN_ENV,
        timeout_s=2.0,
    )


def _lifecycle(
    tmp_path: Path, control: AdminControlConfig
) -> AdminLifecycle:
    return AdminLifecycle(
        config=AdminConfig(
            enabled=True,
            adapters=AdminAdaptersConfig(supervisor=True),
            control=control,
        ),
        db_path=tmp_path / "admin" / "admin.db",
        environ={TOKEN_ENV: TOKEN},
    )


def test_both_control_servers_bind_simultaneously_on_distinct_endpoints(
    tmp_path: Path,
) -> None:
    collector_port = _free_port()
    supervisor_port = _free_port()
    config = _client_config(collector_port, supervisor_port)

    collector = ControlPlaneServer(
        host=config.collector_host,
        port=config.collector_port,
        token=TOKEN,
        registry=collector_control_registry(),
    )
    collector.register(ACTION_PING, lambda _payload: {"pong": True})
    supervisor = Supervisor([ChildSpec(role="api", command=["fake"])])
    supervisor_server = SupervisorControlServer(
        supervisor,
        host=config.supervisor_host,
        port=config.supervisor_port,
        token=TOKEN,
    )
    collector.start()
    supervisor_server.start()
    try:
        assert collector.port == collector_port
        assert supervisor_server.bound_port == supervisor_port
        collector_result = ControlPlaneClient(
            host=config.collector_host,
            port=config.collector_port,
            token=TOKEN,
            registry=collector_control_registry(),
            timeout_s=2.0,
        ).call(ACTION_PING, {}, timeout_ms=2000)
        assert collector_result == {"pong": True}

        lifecycle = _lifecycle(tmp_path, config)
        client = lifecycle.control_client()
        assert client is not None
        assert client.port == supervisor_port
        assert client.ping()["result"] == {"status": "ok"}
    finally:
        supervisor_server.stop()
        collector.stop()


def test_clients_read_their_own_configured_endpoint(tmp_path: Path) -> None:
    config = _client_config(1111, 2222)
    environ = {TOKEN_ENV: TOKEN}

    collector_client = CollectorControlClient(config, environ=environ)
    lifecycle = _lifecycle(tmp_path, config)
    try:
        assert collector_client.configured is True
        supervisor_client = lifecycle.control_client()
        assert supervisor_client is not None
        assert supervisor_client.port == 2222
    finally:
        lifecycle.close()

    # Disabling only one endpoint leaves the other configured.
    only_supervisor = AdminControlConfig(
        collector_port=0,
        supervisor_port=2222,
        token_env=TOKEN_ENV,
    )
    assert not CollectorControlClient(
        only_supervisor, environ=environ
    ).configured
    supervisor_lifecycle = _lifecycle(tmp_path, only_supervisor)
    try:
        assert supervisor_lifecycle.control_client() is not None
    finally:
        supervisor_lifecycle.close()

    only_collector = AdminControlConfig(
        collector_port=1111,
        supervisor_port=0,
        token_env=TOKEN_ENV,
    )
    assert CollectorControlClient(only_collector, environ=environ).configured
    collector_lifecycle = _lifecycle(tmp_path, only_collector)
    try:
        assert collector_lifecycle.control_client() is None
    finally:
        collector_lifecycle.close()
