# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[too-many-arguments]
"""Admin monitoring proxy: templates, fixed PromQL, limits and errors.

A local stdlib ``http.server`` plays Prometheus. Every request is recorded so
the tests can prove the server never forwards arbitrary PromQL or label
selectors, only the fixed per-template query for a validated ``service_role``.
"""

from __future__ import annotations

import http.client
import json
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, quote, urlsplit

import pytest

from mdgw.admin.context import AdminContext
from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.config import (
    AdminAdaptersConfig,
    AdminConfig,
    AdminMonitoringConfig,
)
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-admin-monitoring"
ROLES = ("SuperAdmin", "Operator", "Auditor")
TEMPLATES = (
    "http_qps",
    "http_peak_qps",
    "http_latency_p50",
    "http_latency_p95",
    "http_latency_p99",
)
TEMPLATES_PATH = "/api/v1/admin/monitoring/templates"
QUERY_PATH = "/api/v1/admin/monitoring/query"
FROM = "2026-11-09T10:00:00Z"
TO = "2026-11-09T10:15:00Z"


# -- HTTP client ------------------------------------------------------------


@dataclass
class Result:
    status: int
    headers: list[tuple[str, str]]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body)


class Client:
    def __init__(self, server: AsgiServer) -> None:
        self.server = server

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> Result:
        connection = http.client.HTTPConnection(
            HOST, self.server.bound_port, timeout=15
        )
        try:
            connection.request(method, path, headers=dict(headers or {}))
            response = connection.getresponse()
            payload = response.read()
            return Result(
                int(response.status), list(response.getheaders()), payload
            )
        finally:
            connection.close()


class Credential:
    def __init__(self, context: AdminContext, role: str) -> None:
        issued = context.auth.sessions.create(
            subject=f"subject-{role.lower()}",
            display_name=f"{role} User",
            email=None,
            access_role=role,
        )
        self.token = issued.token

    def headers(self) -> dict[str, str]:
        return {"Cookie": f"mdgw_session={self.token}"}


# -- fake Prometheus --------------------------------------------------------


def _matrix(points: int = 2) -> dict[str, Any]:
    values = [
        [1_700_000_000 + index * 15, str(1.5 + index)] for index in range(points)
    ]
    return {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {"metric": {"path": "/healthz"}, "values": values},
                {
                    "metric": {"path": "/api/v1/bars"},
                    "values": [[1_700_000_000, "NaN"], [1_700_000_015, "2"]],
                },
            ],
        },
    }


class FakePrometheus:
    """Local Prometheus API double with request recording."""

    def __init__(
        self,
        *,
        payload: dict[str, Any] | None = None,
        status: int = 200,
        raw_body: bytes | None = None,
        delay_s: float = 0.0,
    ) -> None:
        self.payload = payload if payload is not None else _matrix()
        self.status = status
        self.raw_body = raw_body
        self.delay_s = delay_s
        self.requests: list[dict[str, list[str]]] = []
        self.paths: list[str] = []

        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(  # ruff: ignore[no-self-use]  # http.server callback
                self, *_args: object
            ) -> None:
                return

            def do_GET(self) -> None:
                parsed = urlsplit(self.path)
                fake.paths.append(parsed.path)
                if parsed.path != "/api/v1/query_range":
                    self._send(404, b'{"status":"error"}')
                    return
                fake.requests.append(parse_qs(parsed.query))
                if fake.delay_s:
                    time.sleep(fake.delay_s)
                if fake.raw_body is not None:
                    self._send(fake.status, fake.raw_body)
                    return
                body = json.dumps(fake.payload).encode()
                self._send(fake.status, body)

            def _send(self, status: int, body: bytes) -> None:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._server = ThreadingHTTPServer((HOST, 0), Handler)
        self.port = self._server.server_address[1]
        self.url = f"http://{HOST}:{self.port}"
        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True
        )
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


@contextmanager
def _running_admin(
    tmp_path: Path,
    *,
    admin_config: AdminConfig,
) -> Generator[tuple[AsgiServer, Client, AdminContext]]:
    store = ParquetBarStore(tmp_path / "data")
    context = AdminContext(
        config=admin_config, storage_root=tmp_path, store=store
    )
    app = create_app(store, admin=context)
    server = AsgiServer()
    try:
        server.start(app, host=HOST, port=0, thread_name=THREAD_NAME)
        yield server, Client(server), context
    finally:
        server.stop()
        store.close()


def _admin_config(
    tmp_path: Path,
    *,
    prometheus_url: str | None,
    prometheus: bool = True,
    max_range_s: int = 86400,
    max_points: int = 1000,
    min_step_s: int = 15,
) -> AdminConfig:
    return AdminConfig(
        enabled=True,
        db_path=tmp_path / "admin" / "admin.db",
        adapters=AdminAdaptersConfig(oidc=False, prometheus=prometheus),
        monitoring=AdminMonitoringConfig(
            prometheus_url=prometheus_url,
            timeout_s=2.0,
            max_range_s=max_range_s,
            max_points=max_points,
            min_step_s=min_step_s,
        ),
    )


@pytest.fixture
def prometheus() -> Generator[FakePrometheus]:
    fake = FakePrometheus()
    try:
        yield fake
    finally:
        fake.close()


def _running_with(
    tmp_path: Path, fake: FakePrometheus, **config_kwargs
) -> Any:
    return _running_admin(
        tmp_path,
        admin_config=_admin_config(
            tmp_path, prometheus_url=fake.url, **config_kwargs
        ),
    )


def _get(
    client: Client, path: str, credential: Credential | None = None
) -> Result:
    return client.request(
        "GET", path, headers=credential.headers() if credential else None
    )


def _query(
    *,
    template: str = "http_qps",
    service_role: str = "api",
    from_value: str = FROM,
    to_value: str = TO,
    step: str = "15s",
    extra: dict[str, str] | None = None,
) -> str:
    params = {
        "template": template,
        "service_role": service_role,
        "from": from_value,
        "to": to_value,
        "step": step,
    }
    if extra:
        params.update(extra)
    query = "&".join(
        f"{quote(name, safe='')}={quote(value, safe='')}"
        for name, value in params.items()
    )
    return f"{QUERY_PATH}?{query}"


# -- templates --------------------------------------------------------------


def test_templates_require_auth_and_expose_the_fixed_contract(
    tmp_path: Path, prometheus: FakePrometheus
) -> None:
    with _running_with(tmp_path, prometheus) as (_server, client, context):
        anonymous = _get(client, TEMPLATES_PATH)
        assert anonymous.status == 401
        assert anonymous.json()["error"]["code"] == "unauthenticated"

        for role in ROLES:
            credential = Credential(context, role)
            response = _get(client, TEMPLATES_PATH, credential)
            assert response.status == 200, response.body
            items = response.json()["items"]
            assert [item["name"] for item in items] == list(TEMPLATES)
            for item in items:
                assert item["unit"] in {"requests_per_second", "seconds"}
                assert isinstance(item["description"], str)
                assert item["max_range_s"] == 86400
                assert item["max_points"] == 1000
                assert item["min_step"] == "15s"
                assert item["params"] == [
                    {
                        "name": "service_role",
                        "type": "enum",
                        "values": ["api", "collector", "replay"],
                        "required": True,
                    }
                ]

        anonymous_query = _get(client, _query(), None)
        assert anonymous_query.status == 401


# -- fixed queries ----------------------------------------------------------


@pytest.mark.parametrize("template", TEMPLATES)
def test_each_template_sends_only_its_fixed_query(
    tmp_path: Path, prometheus: FakePrometheus, template: str
) -> None:
    with _running_with(tmp_path, prometheus) as (_server, client, context):
        credential = Credential(context, "Operator")
        response = _get(
            client, _query(template=template), credential
        )

    assert response.status == 200, response.body
    body = response.json()
    assert body["template"] == template
    assert body["source"] == "prometheus"
    assert body["from"] == "2026-11-09T10:00:00.000000000Z"
    assert body["to"] == "2026-11-09T10:15:00.000000000Z"
    assert body["step"] == "15s"
    assert body["truncated"] is False
    assert body["warnings"] == []
    assert body["unit"] == (
        "seconds"
        if template.startswith("http_latency")
        else "requests_per_second"
    )
    series = {item["labels"]["path"]: item for item in body["series"]}
    assert series["/healthz"]["points"][0] == {
        "ts": "2023-11-14T22:13:20.000000000Z",
        "value": 1.5,
    }
    # A NaN sample is a gap, never a fabricated zero.
    assert series["/api/v1/bars"]["points"][0]["value"] is None

    request = prometheus.requests[0]
    query = request["query"][0]
    assert 'role="api"' in query
    assert "mdgw_api_http_" in query
    if template == "http_qps":
        assert query == (
            "sum by (path) (rate(mdgw_api_http_requests_total"
            '{role="api"}[1m]))'
        )
    elif template == "http_latency_p50":
        assert query == (
            "histogram_quantile(0.50, sum by (le) "
            "(rate(mdgw_api_http_request_duration_seconds_bucket"
            '{role="api"}[5m])))'
        )
    assert request["step"] == ["15s"]
    assert int(float(request["start"][0])) == 1_794_218_400
    assert int(float(request["end"][0])) == 1_794_219_300
    # The role is a validated enum mapped to a fixed selector, never
    # interpolated user text.
    assert "selector" not in query
    assert "on(" not in query


@pytest.mark.parametrize("role", ROLES)
def test_every_access_role_may_query_templates(
    tmp_path: Path, prometheus: FakePrometheus, role: str
) -> None:
    with _running_with(tmp_path, prometheus) as (_server, client, context):
        credential = Credential(context, role)
        response = _get(client, _query(), credential)

    assert response.status == 200, response.body


def test_prometheus_timeout_is_control_unavailable(tmp_path: Path) -> None:
    fake = FakePrometheus(delay_s=1.0)
    try:
        config = _admin_config(tmp_path, prometheus_url=fake.url)
        config = config.model_copy(
            update={
                "monitoring": config.monitoring.model_copy(
                    update={"timeout_s": 0.2}
                )
            }
        )
        with _running_admin(tmp_path, admin_config=config) as (
            _server,
            client,
            context,
        ):
            credential = Credential(context, "Operator")
            response = _get(client, _query(), credential)
    finally:
        fake.close()

    assert response.status == 503
    assert response.json()["error"]["code"] == "control_unavailable"
    assert response.json()["error"]["details"]["source"] == "prometheus"


def test_service_role_is_an_enumerated_fixed_selector(
    tmp_path: Path, prometheus: FakePrometheus
) -> None:
    with _running_with(tmp_path, prometheus) as (_server, client, context):
        credential = Credential(context, "Auditor")
        response = _get(
            client, _query(service_role="collector"), credential
        )

    assert response.status == 200, response.body
    assert 'role="collector"' in prometheus.requests[0]["query"][0]


# -- validation / no arbitrary PromQL ---------------------------------------


def test_unknown_template_or_parameter_is_rejected_before_prometheus(
    tmp_path: Path, prometheus: FakePrometheus
) -> None:
    with _running_with(tmp_path, prometheus) as (_server, client, context):
        credential = Credential(context, "Operator")

        unknown_template = _get(
            client, _query(template="not_a_template"), credential
        )
        assert unknown_template.status == 400
        envelope = unknown_template.json()["error"]
        assert envelope["code"] == "invalid_parameter"
        assert envelope["details"]["reason"] == "unknown_template"
        assert envelope["details"]["allowed"] == list(TEMPLATES)

        bad_role = _get(
            client, _query(service_role="SuperAdmin"), credential
        )
        assert bad_role.status == 400
        assert bad_role.json()["error"]["details"]["field"] == "service_role"

        missing_role = _get(
            client, QUERY_PATH + f"?template=http_qps&from={FROM}&to={TO}&step=15s",
            credential,
        )
        assert missing_role.status == 400
        assert missing_role.json()["error"]["details"]["field"] == "service_role"

        extra_param = _get(
            client, _query(extra={"label": "path"}), credential
        )
        assert extra_param.status == 400
        assert extra_param.json()["error"]["details"]["field"] == "label"

        assert prometheus.requests == []


@pytest.mark.parametrize(
    "extra",
    [
        {"query": "up"},
        {"expr": "up"},
        {"promql": "up"},
        {"query": "mdgw_api_http_requests_total", "label": "path"},
    ],
)
def test_arbitrary_promql_and_label_selectors_are_never_accepted(
    tmp_path: Path,
    prometheus: FakePrometheus,
    extra: dict[str, str],
) -> None:
    with _running_with(tmp_path, prometheus) as (_server, client, context):
        credential = Credential(context, "SuperAdmin")
        response = _get(client, _query(extra=extra), credential)

    assert response.status == 400
    assert response.json()["error"]["code"] == "invalid_parameter"
    assert prometheus.requests == []


def test_range_step_and_point_limits_are_enforced(
    tmp_path: Path, prometheus: FakePrometheus
) -> None:
    with _running_with(
        tmp_path,
        prometheus,
        max_range_s=3600,
        max_points=240,
        min_step_s=15,
    ) as (_server, client, context):
        credential = Credential(context, "Operator")

        # Advertised limits are the effective, stricter config caps.
        templates = _get(client, TEMPLATES_PATH, credential).json()["items"]
        assert all(item["max_range_s"] == 3600 for item in templates)
        assert all(item["max_points"] == 240 for item in templates)
        assert all(item["min_step"] == "15s" for item in templates)

        reversed_range = _get(
            client,
            _query(from_value=TO, to_value=FROM),
            credential,
        )
        assert reversed_range.status == 400
        assert reversed_range.json()["error"]["details"]["field"] == "range"

        too_wide = _get(
            client,
            _query(from_value="2026-11-09T08:00:00Z", to_value=TO),
            credential,
        )
        assert too_wide.status == 400
        assert too_wide.json()["error"]["details"]["field"] == "range"
        assert too_wide.json()["error"]["details"]["constraint"] == (
            "max_range_s"
        )

        too_fine = _get(client, _query(step="5s"), credential)
        assert too_fine.status == 400
        assert too_fine.json()["error"]["details"]["field"] == "step"
        assert too_fine.json()["error"]["details"]["constraint"] == (
            "min_step_s"
        )

        malformed_step = _get(client, _query(step="fast"), credential)
        assert malformed_step.status == 400
        assert malformed_step.json()["error"]["details"]["field"] == "step"

        too_many_points = _get(
            client,
            _query(
                from_value="2026-11-09T10:00:00Z",
                to_value="2026-11-09T11:00:00Z",
                step="15s",
            ),
            credential,
        )
        assert too_many_points.status == 400
        assert too_many_points.json()["error"]["details"]["constraint"] == (
            "max_points"
        )

        naive = _get(
            client,
            _query(from_value="2026-11-09T10:00:00", to_value=TO),
            credential,
        )
        assert naive.status == 400
        assert naive.json()["error"]["details"]["field"] == "from"

        assert prometheus.requests == []


def test_template_min_step_follows_the_stricter_configuration(
    tmp_path: Path, prometheus: FakePrometheus
) -> None:
    with _running_with(tmp_path, prometheus, min_step_s=30) as (
        _server,
        client,
        context,
    ):
        credential = Credential(context, "Operator")
        templates = _get(client, TEMPLATES_PATH, credential).json()["items"]
        assert all(item["min_step"] == "30s" for item in templates)

        rejected = _get(client, _query(step="15s"), credential)
        accepted = _get(client, _query(step="30s"), credential)

    assert rejected.status == 400
    assert rejected.json()["error"]["details"]["constraint"] == "min_step_s"
    assert accepted.status == 200
    assert prometheus.requests[0]["step"] == ["30s"]


# -- failure mapping --------------------------------------------------------


def test_disabled_prometheus_adapter_refuses_to_query(
    tmp_path: Path, prometheus: FakePrometheus
) -> None:
    with _running_admin(
        tmp_path,
        admin_config=_admin_config(
            tmp_path,
            prometheus_url=prometheus.url,
            prometheus=False,
        ),
    ) as (_server, client, context):
        credential = Credential(context, "Operator")
        response = _get(client, _query(), credential)

    assert response.status == 503
    envelope = response.json()["error"]
    assert envelope["code"] == "control_unavailable"
    assert envelope["details"]["source"] == "prometheus"
    assert envelope["details"]["retryable"] is False
    assert prometheus.requests == []


def test_unreachable_prometheus_is_control_unavailable(tmp_path: Path) -> None:
    fake = FakePrometheus()
    dead_url = fake.url
    fake.close()

    with _running_admin(
        tmp_path,
        admin_config=_admin_config(tmp_path, prometheus_url=dead_url),
    ) as (_server, client, context):
        credential = Credential(context, "Operator")
        response = _get(client, _query(), credential)

    assert response.status == 503
    envelope = response.json()["error"]
    assert envelope["code"] == "control_unavailable"
    assert envelope["details"]["source"] == "prometheus"
    assert envelope["details"]["retryable"] is True


@pytest.mark.parametrize(
    "kwargs",
    [
        {"status": 500},
        {"raw_body": b"not json"},
        {"payload": {"status": "error", "error": "bad_data"}},
        {"payload": {"status": "success", "data": {"resultType": "xx"}}},
    ],
)
def test_invalid_prometheus_responses_are_control_unavailable(
    tmp_path: Path, kwargs: dict[str, Any]
) -> None:
    fake = FakePrometheus(**kwargs)
    try:
        with _running_admin(
            tmp_path,
            admin_config=_admin_config(tmp_path, prometheus_url=fake.url),
        ) as (_server, client, context):
            credential = Credential(context, "Auditor")
            response = _get(client, _query(), credential)
    finally:
        fake.close()

    assert response.status == 503
    envelope = response.json()["error"]
    assert envelope["code"] == "control_unavailable"
    assert envelope["details"]["source"] == "prometheus"


# -- empty / truncated ------------------------------------------------------


def test_empty_series_is_a_successful_empty_result(
    tmp_path: Path,
) -> None:
    fake = FakePrometheus(
        payload={
            "status": "success",
            "data": {"resultType": "matrix", "result": []},
        }
    )
    try:
        with _running_admin(
            tmp_path,
            admin_config=_admin_config(tmp_path, prometheus_url=fake.url),
        ) as (_server, client, context):
            credential = Credential(context, "Operator")
            response = _get(client, _query(), credential)
    finally:
        fake.close()

    assert response.status == 200
    body = response.json()
    assert body["series"] == []
    assert body["truncated"] is False
    assert body["warnings"] == []


def test_series_and_points_beyond_the_cap_are_truncated_with_warnings(
    tmp_path: Path,
) -> None:
    many_series = {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {"path": f"/path-{index}"},
                    "values": [[1_700_000_000, "1"]],
                }
                for index in range(60)
            ],
        },
    }
    fake = FakePrometheus(payload=many_series)
    try:
        with _running_admin(
            tmp_path,
            admin_config=_admin_config(
                tmp_path,
                prometheus_url=fake.url,
                max_points=240,
            ),
        ) as (_server, client, context):
            credential = Credential(context, "Operator")
            response = _get(client, _query(), credential)
    finally:
        fake.close()

    assert response.status == 200
    body = response.json()
    assert len(body["series"]) == 50
    assert body["truncated"] is True
    assert body["warnings"]


def test_prometheus_points_beyond_max_points_are_trimmed(
    tmp_path: Path,
) -> None:
    fake = FakePrometheus(payload=_matrix(points=300))
    try:
        with _running_admin(
            tmp_path,
            admin_config=_admin_config(
                tmp_path, prometheus_url=fake.url, max_points=240
            ),
        ) as (_server, client, context):
            credential = Credential(context, "Operator")
            response = _get(client, _query(), credential)
    finally:
        fake.close()

    assert response.status == 200
    body = response.json()
    series = {item["labels"]["path"]: item for item in body["series"]}
    assert len(series["/healthz"]["points"]) == 240
    assert body["truncated"] is True
    assert body["warnings"]


# -- capabilities -----------------------------------------------------------


def test_capabilities_report_monitoring_proxy_truthfully(tmp_path: Path) -> None:
    fake = FakePrometheus()
    try:
        with _running_admin(
            tmp_path,
            admin_config=_admin_config(tmp_path, prometheus_url=fake.url),
        ) as (_server, client, _context):
            available = _get(client, "/api/v1/admin/capabilities").json()
    finally:
        fake.close()

    adapters = {entry["name"]: entry for entry in available["adapters"]}
    assert adapters["prometheus"]["state"] == "available"
    monitoring = available["capabilities"]["monitoring_proxy"]
    assert monitoring == {
        "state": "enabled",
        "templates_path": TEMPLATES_PATH,
        "arbitrary_promql": False,
    }

    second = tmp_path / "second"
    second.mkdir()
    with _running_admin(
        second,
        admin_config=_admin_config(second, prometheus_url=None),
    ) as (_server, client, _context):
        unavailable = _get(client, "/api/v1/admin/capabilities").json()
    adapters = {entry["name"]: entry for entry in unavailable["adapters"]}
    assert adapters["prometheus"]["state"] == "unavailable"
    assert unavailable["capabilities"]["monitoring_proxy"]["state"] == (
        "adapter_unavailable"
    )

    third = tmp_path / "third"
    third.mkdir()
    with _running_admin(
        third,
        admin_config=_admin_config(
            third, prometheus_url="http://127.0.0.1:9", prometheus=False
        ),
    ) as (_server, client, _context):
        unsupported = _get(client, "/api/v1/admin/capabilities").json()
    adapters = {entry["name"]: entry for entry in unsupported["adapters"]}
    assert adapters["prometheus"]["state"] == "unsupported"
    assert unsupported["capabilities"]["monitoring_proxy"] == {
        "state": "unsupported"
    }
