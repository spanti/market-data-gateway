# ruff: file-ignore[implicit-namespace-package]
"""Server-side authorization matrix for every mounted Admin route.

The route-matrix test enumerates the real FastAPI routes and proves that only
the explicit public set is reachable anonymously. A temporary feature router
then exercises the reusable role, CSRF and ownership dependencies that later
tickets compose, plus the "owner or SuperAdmin" policy.
"""

from __future__ import annotations

import http.client
import json
import re
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest
from fastapi import APIRouter, Depends
from starlette.routing import WebSocketRoute

from mdgw.admin.app import (
    FEATURE_ROUTER_FACTORIES,
    PUBLIC_ADMIN_PATHS,
    register_feature,
)
from mdgw.admin.auth.dependencies import (
    owner_or_superadmin,
    require_csrf,
    require_ownership,
    require_role,
    require_session,
    require_write,
)
from mdgw.admin.auth.store import SessionRecord
from mdgw.admin.context import AdminContext
from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.config import AdminAdaptersConfig, AdminConfig
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-admin-authorization"
ROLES = ("SuperAdmin", "Operator", "Auditor")


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

    @property
    def origin(self) -> str:
        return f"http://{HOST}:{self.server.bound_port}"

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> Result:
        connection = http.client.HTTPConnection(
            HOST, self.server.bound_port, timeout=15
        )
        try:
            connection.request(
                method, path, body=body, headers=dict(headers or {})
            )
            response = connection.getresponse()
            payload = response.read()
            result = Result(
                int(response.status), list(response.getheaders()), payload
            )
        finally:
            connection.close()
        return result


@contextmanager
def _running_admin(
    tmp_path: Path,
) -> Generator[tuple[AsgiServer, Client, AdminContext, Any]]:
    config = AdminConfig(
        enabled=True,
        db_path=tmp_path / "admin" / "admin.db",
        adapters=AdminAdaptersConfig(oidc=False),
    )
    store = ParquetBarStore(tmp_path / "data")
    context = AdminContext(config=config, storage_root=tmp_path, store=store)
    app = create_app(store, admin=context)
    server = AsgiServer()
    try:
        server.start(
            app, host=HOST, port=0, thread_name=THREAD_NAME
        )
        yield server, Client(server), context, app
    finally:
        server.stop()
        store.close()


class Credential:
    def __init__(
        self, context: AdminContext, role: str, *, subject: str | None = None
    ) -> None:
        issued = context.auth.sessions.create(
            subject=subject or f"subject-{role.lower()}",
            display_name=f"{role} User",
            email=None,
            access_role=role,
        )
        self.token = issued.token
        self.csrf = issued.csrf_token
        self.subject = issued.record.subject
        self.role = role

    def headers(
        self,
        *,
        csrf: str | None = None,
        origin: str | bool = True,
        client: Client | None = None,
    ) -> dict[str, str]:
        headers = {"Cookie": f"mdgw_session={self.token}"}
        if csrf is not None:
            headers["X-CSRF-Token"] = csrf
        if origin is True:
            assert client is not None
            headers["Origin"] = client.origin
        elif isinstance(origin, str):
            headers["Origin"] = origin
        return headers


def _leaf_routes(routes: Any, prefix: str = "") -> Any:
    """Unwrap FastAPI ``_IncludedRouter`` wrappers and accumulate prefixes."""
    for route in routes:
        original = getattr(route, "original_router", None)
        if original is not None and hasattr(original, "routes"):
            yield from _leaf_routes(
                original.routes, prefix + str(getattr(original, "prefix", ""))
            )
        else:
            yield prefix, route


def _admin_http_routes(app: Any) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for prefix, route in _leaf_routes(app.routes):
        path = getattr(route, "path", None)
        if not isinstance(path, str):
            continue
        full_path = prefix + path
        if not full_path.startswith("/api/v1/admin"):
            continue
        if isinstance(route, WebSocketRoute):
            continue
        methods = getattr(route, "methods", None)
        if not methods:
            continue
        for method in sorted(methods - {"HEAD", "OPTIONS"}):
            concrete = re.sub(r"\{[^}]+\}", "x", full_path)
            pairs.append((method, concrete))
    return pairs


# -- route matrix -----------------------------------------------------------


def test_every_non_public_admin_route_rejects_anonymous_requests(
    tmp_path: Path,
) -> None:
    with _running_admin(tmp_path) as (_server, client, _context, app):
        routes = _admin_http_routes(app)
        assert routes, "the admin route scan must not pass vacuously"
        protected = [
            (method, path)
            for method, path in routes
            if path not in PUBLIC_ADMIN_PATHS
        ]
        protected_paths = {path for _method, path in protected}
        assert {
            "/api/v1/admin/auth/refresh",
            "/api/v1/admin/auth/logout",
        } <= protected_paths

        for method, path in protected:
            result = client.request(method, path)
            assert result.status == 401, (method, path, result.body)
            envelope = result.json()["error"]
            assert envelope["code"] == "unauthenticated"
            assert envelope["details"]["reason"] == "missing"
            assert envelope["request_id"]


def test_future_feature_routes_are_authenticated_by_default(
    tmp_path: Path,
) -> None:
    def factory(_context: AdminContext) -> APIRouter:
        router = APIRouter()

        # Deliberately declares no dependencies: the parent protected router
        # must authenticate both routes and require CSRF on the unsafe one.
        @router.get("/future-probe")
        async def probe() -> dict[str, str]:
            return {"ok": "true"}

        @router.post("/future-probe")
        async def probe_write() -> dict[str, str]:
            return {"ok": "true"}

        return router

    register_feature(factory)
    try:
        with _running_admin(tmp_path) as (_server, client, context, _app):
            anonymous = client.request("GET", "/api/v1/admin/future-probe")
            assert anonymous.status == 401
            assert anonymous.json()["error"]["details"]["reason"] == "missing"

            credential = Credential(context, "Operator")
            no_csrf = client.request(
                "POST",
                "/api/v1/admin/future-probe",
                headers=credential.headers(client=client),
            )
            assert no_csrf.status == 403
            assert no_csrf.json()["error"]["details"]["reason"] == "csrf"

            allowed = client.request(
                "POST",
                "/api/v1/admin/future-probe",
                headers=credential.headers(
                    csrf=credential.csrf, client=client
                ),
            )
            assert allowed.status == 200
            assert allowed.json() == {"ok": "true"}
    finally:
        FEATURE_ROUTER_FACTORIES.remove(factory)


# -- role matrix ------------------------------------------------------------


@contextmanager
def _role_probe_routes(
    tmp_path: Path,
) -> Generator[tuple[Client, dict[str, Credential], AdminContext]]:
    def factory(context: AdminContext) -> APIRouter:
        router = APIRouter()
        superadmin_guard = require_role(context, "SuperAdmin")
        operator_write_guard = require_write(
            context, "SuperAdmin", "Operator"
        )
        session_guard = require_session(context)
        csrf_guard = require_csrf(context)

        @router.get("/role-probe/superadmin")
        async def superadmin_only(
            record: SessionRecord = Depends(superadmin_guard),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
        ) -> dict[str, str]:
            return {"role": record.access_role}

        @router.post("/role-probe/operator-write")
        async def operator_write(
            record: SessionRecord = Depends(operator_write_guard),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
        ) -> dict[str, str]:
            return {"role": record.access_role}

        @router.get("/role-probe/owned/{owner}")
        async def owned(
            owner: str,
            record: SessionRecord = Depends(session_guard),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
        ) -> dict[str, str]:
            require_ownership(context, record, owner, resource_id="exp_1")
            return {"owner": owner}

        @router.post("/role-probe/csrf")
        async def csrf_probe(
            record: SessionRecord = Depends(csrf_guard),  # ruff: ignore[fast-api-non-annotated-dependency, function-call-in-default-argument]
        ) -> dict[str, str]:
            return {"role": record.access_role}

        return router

    register_feature(factory)
    try:
        with _running_admin(tmp_path) as (_server, client, context, _app):
            credentials = {
                role: Credential(context, role)
                for role in ("SuperAdmin", "Operator", "Auditor")
            }
            credentials["OtherOperator"] = Credential(
                context, "Operator", subject="subject-operator-2"
            )
            yield client, credentials, context
    finally:
        FEATURE_ROUTER_FACTORIES.remove(factory)


def test_role_restricted_route_matrix(tmp_path: Path) -> None:
    with _role_probe_routes(tmp_path) as (client, credentials, _context):
        superadmin = credentials["SuperAdmin"]

        allowed = client.request(
            "GET",
            "/api/v1/admin/role-probe/superadmin",
            headers=superadmin.headers(client=client),
        )
        assert allowed.status == 200
        assert allowed.json() == {"role": "SuperAdmin"}

        for role in ("Operator", "Auditor"):
            denied = client.request(
                "GET",
                "/api/v1/admin/role-probe/superadmin",
                headers=credentials[role].headers(client=client),
            )
            assert denied.status == 403
            envelope = denied.json()["error"]
            assert envelope["code"] == "forbidden"
            assert envelope["details"]["reason"] == "role"
            assert envelope["details"]["required_role"] == ["SuperAdmin"]


def test_write_route_requires_csrf_origin_and_role(tmp_path: Path) -> None:
    with _role_probe_routes(tmp_path) as (client, credentials, _context):
        operator = credentials["Operator"]

        missing_csrf = client.request(
            "POST",
            "/api/v1/admin/role-probe/operator-write",
            headers=operator.headers(client=client),
        )
        assert missing_csrf.status == 403
        assert missing_csrf.json()["error"]["details"]["reason"] == "csrf"

        wrong_origin = client.request(
            "POST",
            "/api/v1/admin/role-probe/operator-write",
            headers=operator.headers(
                csrf=operator.csrf,
                origin="https://evil.example.com",
            ),
        )
        assert wrong_origin.status == 403
        assert wrong_origin.json()["error"]["details"]["reason"] == "origin"

        malformed_origin = client.request(
            "POST",
            "/api/v1/admin/role-probe/operator-write",
            headers=operator.headers(
                csrf=operator.csrf, origin="http://[not-an-origin"
            ),
        )
        assert malformed_origin.status == 403
        assert malformed_origin.json()["error"]["details"]["reason"] == (
            "origin"
        )

        auditor = credentials["Auditor"]
        auditor_denied = client.request(
            "POST",
            "/api/v1/admin/role-probe/operator-write",
            headers=auditor.headers(csrf=auditor.csrf, client=client),
        )
        assert auditor_denied.status == 403
        assert auditor_denied.json()["error"]["details"]["reason"] == "role"

        for role in ("SuperAdmin", "Operator"):
            credential = credentials[role]
            response = client.request(
                "POST",
                "/api/v1/admin/role-probe/operator-write",
                headers=credential.headers(csrf=credential.csrf, client=client),
            )
            assert response.status == 200
            assert response.json() == {"role": role}


def test_require_csrf_dependency_for_any_authenticated_role(
    tmp_path: Path,
) -> None:
    with _role_probe_routes(tmp_path) as (client, credentials, _context):
        for role in ("SuperAdmin", "Operator", "Auditor"):
            credential = credentials[role]
            missing = client.request(
                "POST",
                "/api/v1/admin/role-probe/csrf",
                headers=credential.headers(client=client),
            )
            assert missing.status == 403
            assert missing.json()["error"]["details"]["reason"] == "csrf"

            allowed = client.request(
                "POST",
                "/api/v1/admin/role-probe/csrf",
                headers=credential.headers(
                    csrf=credential.csrf, client=client
                ),
            )
            assert allowed.status == 200
            assert allowed.json() == {"role": role}


def test_ownership_policy_allows_owner_and_superadmin_only(
    tmp_path: Path,
) -> None:
    with _role_probe_routes(tmp_path) as (client, credentials, _context):
        owner = credentials["Operator"]
        other = credentials["OtherOperator"]
        auditor = credentials["Auditor"]
        superadmin = credentials["SuperAdmin"]

        assert owner_or_superadmin(_record(owner), owner.subject)
        assert owner_or_superadmin(_record(superadmin), "someone-else")
        assert not owner_or_superadmin(_record(other), owner.subject)
        assert not owner_or_superadmin(_record(auditor), owner.subject)
        assert not owner_or_superadmin(_record(owner), None)

        own = client.request(
            "GET",
            f"/api/v1/admin/role-probe/owned/{owner.subject}",
            headers=owner.headers(client=client),
        )
        assert own.status == 200

        foreign = client.request(
            "GET",
            f"/api/v1/admin/role-probe/owned/{owner.subject}",
            headers=other.headers(client=client),
        )
        assert foreign.status == 403
        assert foreign.json()["error"]["details"]["reason"] == "ownership"

        admin_view = client.request(
            "GET",
            f"/api/v1/admin/role-probe/owned/{owner.subject}",
            headers=superadmin.headers(client=client),
        )
        assert admin_view.status == 200


def _record(credential: Credential) -> SessionRecord:
    """A record-shaped view is enough for the pure ownership predicate."""
    return SessionRecord(
        session_hash="hash",
        subject=credential.subject,
        display_name="",
        email=None,
        access_role=credential.role,
        issued_at_ns=0,
        last_activity_ns=0,
        absolute_expires_ns=0,
        csrf_hash="csrf",
    )


# -- current session routes, all roles --------------------------------------


@pytest.mark.parametrize("role", ROLES)
def test_refresh_and_logout_are_available_to_every_role(
    tmp_path: Path, role: str
) -> None:
    with _running_admin(tmp_path) as (_server, client, context, _app):
        credential = Credential(context, role)

        refreshed = client.request(
            "POST",
            "/api/v1/admin/auth/refresh",
            headers=credential.headers(csrf=credential.csrf, client=client),
        )
        assert refreshed.status == 200
        payload = refreshed.json()
        assert payload["authenticated"] is True
        assert payload["access_role"] == role
        assert payload["csrf"]["token"] != credential.csrf

        logged_out = client.request(
            "POST",
            "/api/v1/admin/auth/logout",
            headers=credential.headers(
                csrf=payload["csrf"]["token"], client=client
            ),
        )
        assert logged_out.status == 204

        anonymous = client.request(
            "GET",
            "/api/v1/admin/auth/session",
            headers={"Cookie": f"mdgw_session={credential.token}"},
        )
        assert anonymous.json()["authenticated"] is False


def test_unknown_role_is_rejected_when_building_a_dependency(
    tmp_path: Path,
) -> None:
    with (
        _running_admin(tmp_path) as (_server, _client, context, _app),
        pytest.raises(ValueError, match="unknown Access Role"),
    ):
        require_role(context, "Root")
