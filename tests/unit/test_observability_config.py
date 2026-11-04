# ruff: file-ignore[implicit-namespace-package]
"""Observability deployment configuration is parseable and wired correctly.

These tests treat the shipped YAML/JSON as the public interface: no Docker
daemon is required, and the assertions only cover what an operator relies on
(scrape targets, datasource URL, dashboard metrics, optional ports).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]

PROMETHEUS_CONFIG = (
    REPO_ROOT / "observability" / "prometheus" / "prometheus.yml"
)
GRAFANA_DATASOURCE = (
    REPO_ROOT
    / "observability"
    / "grafana"
    / "provisioning"
    / "datasources"
    / "datasource.yml"
)
GRAFANA_DASHBOARDS = (
    REPO_ROOT
    / "observability"
    / "grafana"
    / "provisioning"
    / "dashboards"
    / "dashboards.yml"
)
GRAFANA_DASHBOARD = (
    REPO_ROOT / "observability" / "grafana" / "dashboards" / "mdgw.json"
)
OVERRIDE_COMPOSE = REPO_ROOT / "docker-compose.observability.yml"
BASE_COMPOSE = REPO_ROOT / "docker-compose.yml"


def _load_yaml(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def test_prometheus_scrapes_all_three_service_roles_every_15_seconds() -> None:
    config = _load_yaml(PROMETHEUS_CONFIG)

    assert config["global"]["scrape_interval"] == "15s"

    jobs = {job["job_name"]: job for job in config["scrape_configs"]}
    expected_targets = {
        "collector": "collector:8081",
        "api": "api:8080",
        "replay": "replay:8082",
    }

    assert set(jobs) == set(expected_targets)
    for role, target in expected_targets.items():
        job = jobs[role]
        assert job["metrics_path"] == "/metrics"
        assert job["static_configs"][0]["targets"] == [target]
        assert job["labels"]["role"] == role


def test_grafana_datasource_points_at_the_prometheus_service() -> None:
    config = _load_yaml(GRAFANA_DATASOURCE)

    datasources = config["datasources"]
    assert len(datasources) == 1
    datasource = datasources[0]
    assert datasource["type"] == "prometheus"
    assert datasource["url"] == "http://prometheus:9090"
    assert datasource["isDefault"] is True


def test_grafana_dashboard_provider_reads_the_dashboards_directory() -> None:
    config = _load_yaml(GRAFANA_DASHBOARDS)

    providers = config["providers"]
    assert len(providers) == 1
    provider = providers[0]
    assert provider["type"] == "file"
    assert provider["options"]["path"] == "/var/lib/grafana/dashboards"


def test_dashboard_is_valid_grafana_json_and_references_required_metrics() -> None:
    dashboard = json.loads(GRAFANA_DASHBOARD.read_text(encoding="utf-8"))

    assert dashboard["title"]
    assert isinstance(dashboard["schemaVersion"], int)
    panels = dashboard["panels"]
    assert panels

    expressions = "\n".join(
        target["expr"]
        for panel in panels
        for target in panel.get("targets", [])
    )
    assert "rate(mdgw_api_http_requests_total[5m])" in expressions
    assert "histogram_quantile(" in expressions
    assert "mdgw_api_http_request_duration_seconds_bucket" in expressions
    assert "rate(mdgw_api_rate_limited_total[5m])" in expressions
    assert "mdgw_up" in expressions
    assert "mdgw_ready" in expressions


def test_observability_override_exposes_prometheus_and_grafana_ports() -> None:
    override = _load_yaml(OVERRIDE_COMPOSE)
    services = override["services"]

    assert "9090:9090" in [str(port) for port in services["prometheus"]["ports"]]
    assert "3000:3000" in [str(port) for port in services["grafana"]["ports"]]


def test_override_mounts_observability_configuration() -> None:
    services = _load_yaml(OVERRIDE_COMPOSE)["services"]

    prometheus = services["prometheus"]
    assert any(
        "observability/prometheus/prometheus.yml" in volume
        and "/etc/prometheus/prometheus.yml" in volume
        for volume in prometheus["volumes"]
    )
    assert any(
        "/etc/prometheus/prometheus.yml" in argument
        for argument in prometheus["command"]
    )

    grafana = services["grafana"]
    assert any(
        "observability/grafana/provisioning" in volume
        and "/etc/grafana/provisioning" in volume
        for volume in grafana["volumes"]
    )
    assert any(
        "observability/grafana/dashboards" in volume
        and "/var/lib/grafana/dashboards" in volume
        for volume in grafana["volumes"]
    )
    assert "GRAFANA_ADMIN_PASSWORD" in grafana["environment"][
        "GF_SECURITY_ADMIN_PASSWORD"
    ]


def test_base_compose_does_not_start_prometheus_or_grafana() -> None:
    base = _load_yaml(BASE_COMPOSE)

    assert "prometheus" not in base["services"]
    assert "grafana" not in base["services"]

    text = BASE_COMPOSE.read_text(encoding="utf-8").lower()
    assert "prometheus" not in text
    assert "grafana" not in text
