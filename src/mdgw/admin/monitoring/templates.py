"""Fixed monitoring templates and their server-side PromQL.

Only these queries can ever reach Prometheus. ``service_role`` is an enum that
maps to a fixed ``role="..."`` selector from the deployment scrape labels; no
part of a request is interpolated as PromQL syntax or as a label selector.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mdgw.config import AdminMonitoringConfig

#: Enumerated Service Roles accepted by every template.
SERVICE_ROLES: tuple[str, ...] = ("api", "collector", "replay")

#: The deployment's Prometheus scrape configuration labels each role with
#: ``role="api|collector|replay"`` (observability/prometheus/prometheus.yml).
SERVICE_ROLE_LABEL = "role"

_QPS_RATE_WINDOW = "1m"
_LATENCY_RATE_WINDOW = "5m"

_MAX_SERIES = 50


@dataclass(frozen=True, slots=True)
class MonitoringTemplate:
    """One named, server-owned monitoring query."""

    name: str
    description: str
    unit: str
    quantile: float | None = None
    peak: bool = False
    max_range_s: int = 86_400
    max_points: int = 1_000
    min_step_s: int = 15

    @staticmethod
    def params() -> list[dict[str, Any]]:
        return [
            {
                "name": "service_role",
                "type": "enum",
                "values": list(SERVICE_ROLES),
                "required": True,
            }
        ]

    def promql(self, service_role: str, *, range_s: int, step_s: int) -> str:
        """Return the one fixed query for this template and Service Role."""
        if service_role not in SERVICE_ROLES:  # defensive: callers validate
            msg = f"unknown service_role: {service_role!r}"
            raise ValueError(msg)
        selector = f'{SERVICE_ROLE_LABEL}="{service_role}"'
        if self.quantile is not None:
            return (
                f"histogram_quantile({self.quantile:.2f}, sum by (le) "
                "(rate(mdgw_api_http_request_duration_seconds_bucket"
                f"{{{selector}}}[{_LATENCY_RATE_WINDOW}])))"
            )
        rate = (
            "sum by (path) (rate(mdgw_api_http_requests_total"
            f"{{{selector}}}[{_QPS_RATE_WINDOW}]))"
        )
        if not self.peak:
            return rate
        return (
            f"max_over_time(({rate})"
            f"[{int(range_s)}s:{int(step_s)}s])"
        )


TEMPLATES: tuple[MonitoringTemplate, ...] = (
    MonitoringTemplate(
        name="http_qps",
        description=(
            "HTTP requests per second by route template (1m rate), from the "
            "api Service Role's Prometheus scrape"
        ),
        unit="requests_per_second",
    ),
    MonitoringTemplate(
        name="http_peak_qps",
        description=(
            "Peak sampled HTTP request rate over the selected range "
            "(1m rate); it is a sampled peak, not an instantaneous maximum"
        ),
        unit="requests_per_second",
        peak=True,
    ),
    MonitoringTemplate(
        name="http_latency_p50",
        description=(
            "HTTP request duration p50; includes the complete response "
            "stream, not only time to first byte"
        ),
        unit="seconds",
        quantile=0.50,
    ),
    MonitoringTemplate(
        name="http_latency_p95",
        description=(
            "HTTP request duration p95; includes the complete response "
            "stream, not only time to first byte"
        ),
        unit="seconds",
        quantile=0.95,
    ),
    MonitoringTemplate(
        name="http_latency_p99",
        description=(
            "HTTP request duration p99; includes the complete response "
            "stream, not only time to first byte"
        ),
        unit="seconds",
        quantile=0.99,
    ),
)

TEMPLATES_BY_NAME: dict[str, MonitoringTemplate] = {
    template.name: template for template in TEMPLATES
}

TEMPLATE_NAMES: tuple[str, ...] = tuple(template.name for template in TEMPLATES)

#: Upper bound on returned series; extra series are dropped with a warning.
MAX_SERIES = _MAX_SERIES


def effective_limits(
    template: MonitoringTemplate, config: AdminMonitoringConfig
) -> tuple[int, int, int]:
    """Return the advertised ``(max_range_s, max_points, min_step_s)``.

    Template values describe the query; configuration values are deployment
    caps. The advertised limits are the stricter of the two so a client never
    plans a request the server will reject.
    """
    return (
        min(template.max_range_s, config.max_range_s),
        min(template.max_points, config.max_points),
        max(template.min_step_s, config.min_step_s),
    )


def render_templates(config: AdminMonitoringConfig) -> dict[str, Any]:
    """The ``GET /monitoring/templates`` payload."""
    items: list[dict[str, Any]] = []
    for template in TEMPLATES:
        max_range_s, max_points, min_step_s = effective_limits(template, config)
        items.append(
            {
                "name": template.name,
                "description": template.description,
                "unit": template.unit,
                "params": template.params(),
                "max_range_s": max_range_s,
                "max_points": max_points,
                "min_step": f"{min_step_s}s",
            }
        )
    return {"items": items}


__all__ = [
    "MAX_SERIES",
    "SERVICE_ROLES",
    "SERVICE_ROLE_LABEL",
    "TEMPLATES",
    "TEMPLATES_BY_NAME",
    "TEMPLATE_NAMES",
    "MonitoringTemplate",
    "effective_limits",
    "render_templates",
]
