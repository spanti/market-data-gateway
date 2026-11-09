"""Admin monitoring proxy routes (authenticated, fixed templates only).

``GET /api/v1/admin/monitoring/templates`` advertises the server-owned query
templates and their effective limits. ``GET /api/v1/admin/monitoring/query``
accepts ``template``, ``from``, ``to``, ``step`` and only the parameters that
template declares. Any other query parameter (``query``, ``expr``, ``promql``,
label selectors, ...) is rejected before Prometheus is contacted.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request
from starlette.responses import JSONResponse

from mdgw.admin.errors import ControlUnavailableError, InvalidParameterError
from mdgw.admin.monitoring.prometheus import (
    PrometheusClient,
    PrometheusSeries,
    PrometheusUnavailableError,
)
from mdgw.admin.monitoring.templates import (
    MAX_SERIES,
    SERVICE_ROLES,
    TEMPLATE_NAMES,
    TEMPLATES_BY_NAME,
    MonitoringTemplate,
    effective_limits,
    render_templates,
)
from mdgw.core.time import datetime_to_ns, ns_to_iso, to_utc

if TYPE_CHECKING:
    from mdgw.admin.context import AdminContext
    from mdgw.config import AdminMonitoringConfig

TEMPLATES_PATH = "/api/v1/admin/monitoring/templates"
QUERY_PATH = "/api/v1/admin/monitoring/query"

_STEP_PATTERN = re.compile(r"^(\d+)([smh]?)$")
_STEP_SECONDS = {"": 1, "s": 1, "m": 60, "h": 3_600}
_BASE_PARAMS = frozenset({"template", "from", "to", "step"})
_SOURCE_PROMETHEUS = "prometheus"


def create_monitoring_router(context: AdminContext) -> APIRouter:
    """Build the monitoring router and announce its capabilities."""
    context.register_capabilities(
        adapters=("prometheus",),
        capabilities=("monitoring_proxy",),
        details={
            "monitoring_proxy": {
                "templates_path": TEMPLATES_PATH,
                "arbitrary_promql": False,
            }
        },
    )
    config = context.config.monitoring
    adapter_enabled = context.config.adapters.prometheus
    router = APIRouter()

    @router.get("/monitoring/templates")
    def monitoring_templates() -> JSONResponse:
        return JSONResponse(render_templates(config))

    @router.get("/monitoring/query")
    def monitoring_query(request: Request) -> JSONResponse:
        return _run_query(request, config, adapter_enabled=adapter_enabled)

    return router


def _run_query(
    request: Request,
    config: AdminMonitoringConfig,
    *,
    adapter_enabled: bool,
) -> JSONResponse:
    params = request.query_params
    template = _resolve_template(params)
    _reject_unknown_params(params, template)
    service_role = _service_role(params, template)
    from_ns, to_ns, step_s = _requested_range(params)
    max_points = _validate_limits(
        template, config, from_ns=from_ns, to_ns=to_ns, step_s=step_s
    )

    query = template.promql(
        service_role,
        range_s=int((to_ns - from_ns) / 1_000_000_000),
        step_s=step_s,
    )
    series = _query_prometheus(
        config,
        query,
        from_ns,
        to_ns,
        step_s,
        adapter_enabled=adapter_enabled,
    )
    rendered, truncated, warnings = _render_series(series, max_points)
    return JSONResponse(
        {
            "template": template.name,
            "unit": template.unit,
            "source": _SOURCE_PROMETHEUS,
            "from": ns_to_iso(from_ns),
            "to": ns_to_iso(to_ns),
            "step": f"{step_s}s",
            "series": rendered,
            "truncated": truncated,
            "warnings": warnings,
        }
    )


def _resolve_template(params: Any) -> MonitoringTemplate:
    name = params.get("template")
    if not name:
        msg = "the template parameter is required"
        raise InvalidParameterError(
            msg, details={"field": "template", "reason": "missing"}
        )
    template = TEMPLATES_BY_NAME.get(name)
    if template is None:
        msg = "unknown monitoring template"
        raise InvalidParameterError(
            msg,
            details={
                "field": "template",
                "reason": "unknown_template",
                "allowed": list(TEMPLATE_NAMES),
            },
        )
    return template


def _reject_unknown_params(params: Any, template: MonitoringTemplate) -> None:
    allowed = _BASE_PARAMS | {param["name"] for param in template.params()}
    for name in params:
        if name not in allowed:
            msg = "unknown query parameter"
            raise InvalidParameterError(
                msg,
                details={
                    "field": name,
                    "reason": "unknown_parameter",
                    "allowed": sorted(allowed),
                },
            )


def _service_role(params: Any, template: MonitoringTemplate) -> str:
    for param in template.params():
        name = param["name"]
        if not params.get(name):
            msg = f"the {name} parameter is required"
            raise InvalidParameterError(
                msg, details={"field": name, "reason": "missing"}
            )
    service_role = params.get("service_role")
    if service_role not in SERVICE_ROLES:
        msg = "unsupported service_role"
        raise InvalidParameterError(
            msg,
            details={
                "field": "service_role",
                "reason": "invalid_value",
                "allowed": list(SERVICE_ROLES),
            },
        )
    return service_role


def _requested_range(params: Any) -> tuple[int, int, int]:
    from_dt = _parse_timestamp(params.get("from"), "from")
    to_dt = _parse_timestamp(params.get("to"), "to")
    step_s = _parse_step(params.get("step"))
    from_ns = datetime_to_ns(from_dt)
    to_ns = datetime_to_ns(to_dt)
    if to_ns <= from_ns:
        msg = "from must be earlier than to"
        raise InvalidParameterError(
            msg, details={"field": "range", "reason": "from_before_to"}
        )
    return from_ns, to_ns, step_s


def _validate_limits(
    template: MonitoringTemplate,
    config: AdminMonitoringConfig,
    *,
    from_ns: int,
    to_ns: int,
    step_s: int,
) -> int:
    max_range_s, max_points, min_step_s = effective_limits(template, config)
    if (to_ns - from_ns) / 1_000_000_000 > max_range_s:
        msg = "the requested range exceeds the template limit"
        raise InvalidParameterError(
            msg,
            details={
                "field": "range",
                "constraint": "max_range_s",
                "max_range_s": max_range_s,
            },
        )
    if step_s < min_step_s:
        msg = "the requested step is finer than the template minimum"
        raise InvalidParameterError(
            msg,
            details={
                "field": "step",
                "constraint": "min_step_s",
                "min_step_s": min_step_s,
            },
        )
    step_ns = step_s * 1_000_000_000
    if (to_ns - from_ns) // step_ns + 1 > max_points:
        msg = "the requested range and step exceed the point limit"
        raise InvalidParameterError(
            msg,
            details={
                "field": "range",
                "constraint": "max_points",
                "max_points": max_points,
            },
        )
    return max_points


def _query_prometheus(  # ruff: ignore[too-many-arguments] - query fields
    config: AdminMonitoringConfig,
    query: str,
    from_ns: int,
    to_ns: int,
    step_s: int,
    *,
    adapter_enabled: bool,
) -> list[PrometheusSeries]:
    if not adapter_enabled or not config.prometheus_url:
        msg = "the Prometheus adapter is not configured"
        raise ControlUnavailableError(
            msg, details={"source": _SOURCE_PROMETHEUS, "retryable": False}
        )
    client = PrometheusClient(
        config.prometheus_url, timeout_s=config.timeout_s
    )
    try:
        return client.query_range(
            query,
            start_s=from_ns / 1_000_000_000,
            end_s=to_ns / 1_000_000_000,
            step_s=step_s,
        )
    except PrometheusUnavailableError as exc:
        msg = "the Prometheus query failed"
        raise ControlUnavailableError(
            msg, details={"source": _SOURCE_PROMETHEUS, "retryable": True}
        ) from exc


def _render_series(
    series: list[PrometheusSeries], max_points: int
) -> tuple[list[dict[str, Any]], bool, list[str]]:
    truncated = False
    warnings: list[str] = []
    if len(series) > MAX_SERIES:
        series = series[:MAX_SERIES]
        truncated = True
        warnings.append(f"series truncated to {MAX_SERIES}")
    rendered: list[dict[str, Any]] = []
    points_trimmed = False
    for item in series:
        points = item.points
        if len(points) > max_points:
            points = points[:max_points]
            points_trimmed = True
        rendered.append(
            {
                "labels": dict(item.labels),
                "points": [
                    {
                        "ts": ns_to_iso(round(point.ts_s * 1_000_000_000)),
                        "value": point.value,
                    }
                    for point in points
                ],
            }
        )
    if points_trimmed:
        truncated = True
        warnings.append(f"points truncated to {max_points}")
    return rendered, truncated, warnings


def _parse_timestamp(value: str | None, field: str) -> datetime:
    if not value:
        msg = f"the {field} parameter is required"
        raise InvalidParameterError(
            msg, details={"field": field, "reason": "missing"}
        )
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        msg = f"the {field} parameter is not an ISO-8601 timestamp"
        raise InvalidParameterError(
            msg,
            details={"field": field, "reason": "invalid_timestamp"},
        ) from exc
    if parsed.tzinfo is None:
        msg = f"the {field} parameter must include a timezone"
        raise InvalidParameterError(
            msg,
            details={"field": field, "reason": "timezone_required"},
        )
    return to_utc(parsed)


def _parse_step(value: str | None) -> int:
    if not value:
        msg = "the step parameter is required"
        raise InvalidParameterError(
            msg, details={"field": "step", "reason": "missing"}
        )
    match = _STEP_PATTERN.fullmatch(value.strip())
    if match is None:
        msg = "the step parameter must be a duration such as 15s, 1m or 1h"
        raise InvalidParameterError(
            msg, details={"field": "step", "reason": "invalid_duration"}
        )
    seconds = int(match.group(1)) * _STEP_SECONDS[match.group(2)]
    if seconds <= 0:
        msg = "the step parameter must be positive"
        raise InvalidParameterError(
            msg, details={"field": "step", "reason": "invalid_duration"}
        )
    return seconds


__all__ = ["QUERY_PATH", "TEMPLATES_PATH", "create_monitoring_router"]
