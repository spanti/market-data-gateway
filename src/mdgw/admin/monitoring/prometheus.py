"""Minimal stdlib Prometheus ``/api/v1/query_range`` client.

Only the fixed template queries built in :mod:`mdgw.admin.monitoring.templates`
reach this client. Any transport, HTTP or response-shape failure is surfaced as
:class:`PrometheusUnavailableError`, which the Admin route maps to
``503 control_unavailable`` with ``details.source="prometheus"``.
"""

from __future__ import annotations

import json
import math
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

#: A Prometheus matrix/vector point: timestamp seconds and a nullable value.
_QUERY_RANGE_PATH = "/api/v1/query_range"
#: Bound the response body so a broken/hostile upstream cannot exhaust memory.
_MAX_RESPONSE_BYTES = 4 * 1024 * 1024


class PrometheusUnavailableError(Exception):
    """Prometheus is unreachable or returned an unusable response."""


@dataclass(frozen=True, slots=True)
class PrometheusPoint:
    ts_s: float
    value: float | None


@dataclass(frozen=True, slots=True)
class PrometheusSeries:
    labels: dict[str, str]
    points: list[PrometheusPoint]


class PrometheusClient:
    """Bounded, synchronous Prometheus range-query client."""

    def __init__(self, base_url: str, *, timeout_s: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = float(timeout_s)

    def query_range(
        self,
        query: str,
        *,
        start_s: float,
        end_s: float,
        step_s: int,
    ) -> list[PrometheusSeries]:
        parameters = urlencode(
            {
                "query": query,
                "start": f"{start_s:.3f}",
                "end": f"{end_s:.3f}",
                "step": f"{int(step_s)}s",
            }
        )
        url = f"{self.base_url}{_QUERY_RANGE_PATH}?{parameters}"
        try:
            with urllib.request.urlopen(  # ruff: ignore[suspicious-url-open-usage]
                url, timeout=self.timeout_s
            ) as response:
                body = response.read(_MAX_RESPONSE_BYTES + 1)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            msg = "Prometheus is unreachable"
            raise PrometheusUnavailableError(msg) from exc
        if len(body) > _MAX_RESPONSE_BYTES:
            msg = "Prometheus response is too large"
            raise PrometheusUnavailableError(msg)
        try:
            payload = json.loads(body)
        except (ValueError, TypeError) as exc:
            msg = "Prometheus returned an invalid response"
            raise PrometheusUnavailableError(msg) from exc
        return _parse_matrix(payload)


def _parse_matrix(payload: Any) -> list[PrometheusSeries]:
    if not isinstance(payload, dict) or payload.get("status") != "success":
        msg = "Prometheus did not report success"
        raise PrometheusUnavailableError(msg)
    data = payload.get("data")
    if not isinstance(data, dict):
        msg = "Prometheus response is missing data"
        raise PrometheusUnavailableError(msg)
    result_type = data.get("resultType")
    if result_type not in {"matrix", "vector"}:
        msg = "Prometheus result is not a range series"
        raise PrometheusUnavailableError(msg)
    result = data.get("result")
    if not isinstance(result, list):
        msg = "Prometheus response is missing a result list"
        raise PrometheusUnavailableError(msg)

    series: list[PrometheusSeries] = []
    for entry in result:
        if not isinstance(entry, dict):
            msg = "Prometheus series entry is malformed"
            raise PrometheusUnavailableError(msg)
        raw_labels = entry.get("metric", {})
        if not isinstance(raw_labels, dict):
            msg = "Prometheus series labels are malformed"
            raise PrometheusUnavailableError(msg)
        labels = {str(key): str(value) for key, value in raw_labels.items()}
        raw_points = (
            entry.get("values")
            if result_type == "matrix"
            else [entry.get("value")]
        )
        if not isinstance(raw_points, list):
            msg = "Prometheus series points are malformed"
            raise PrometheusUnavailableError(msg)
        points: list[PrometheusPoint] = []
        for raw_point in raw_points:
            if not isinstance(raw_point, (list, tuple)) or len(raw_point) != 2:
                msg = "Prometheus sample is malformed"
                raise PrometheusUnavailableError(msg)
            try:
                ts_s = float(raw_point[0])
                value = _sample_value(raw_point[1])
            except (TypeError, ValueError) as exc:
                msg = "Prometheus sample is malformed"
                raise PrometheusUnavailableError(msg) from exc
            points.append(PrometheusPoint(ts_s=ts_s, value=value))
        series.append(PrometheusSeries(labels=labels, points=points))

    # Deterministic order keeps truncation stable across scrapes.
    series.sort(key=lambda item: sorted(item.labels.items()))
    return series


def _sample_value(raw: Any) -> float | None:
    value = float(raw)
    return value if math.isfinite(value) else None


__all__ = [
    "PrometheusClient",
    "PrometheusPoint",
    "PrometheusSeries",
    "PrometheusUnavailableError",
]
