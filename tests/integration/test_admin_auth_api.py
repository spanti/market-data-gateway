# ruff: file-ignore[implicit-namespace-package]
"""OIDC login, opaque sessions, CSRF/origin and logout over real HTTP.

A local stdlib ``http.server`` plays the Identity Provider (discovery, JWKS,
token endpoint) and signs ID Tokens with ``cryptography``-generated keys. The
Admin app is served by the real ``AsgiServer`` over HTTPS with a self-signed
certificate so the ``Secure`` cookie attributes are exercised end to end.
Everything is offline; no real provider or network is contacted.
"""

from __future__ import annotations

import base64
import hashlib
import http.client
import ipaddress
import json
import re
import ssl
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, quote, urlsplit

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.x509.oid import NameOID

from mdgw.admin.app import PUBLIC_ADMIN_PATHS
from mdgw.admin.capabilities import build_capabilities_document
from mdgw.admin.context import AdminContext
from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.config import (
    AdminAdaptersConfig,
    AdminConfig,
    AdminOidcConfig,
    load_config,
)
from mdgw.core.time import iso_to_ns
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Generator

    from mdgw.api.ws_hub import WebSocketHub

HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-admin-auth"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLIENT_ID = "mdgw-console"
CLIENT_SECRET = "client-secret-never-logged"  # ruff: ignore[hardcoded-password-string]  # fake test secret
SECRET_ENV = "MDGW_OIDC_CLIENT_SECRET"  # ruff: ignore[hardcoded-password-string]
NS_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z")
ROLE_MAPPING = {
    "mdgw-admins": "SuperAdmin",
    "mdgw-operators": "Operator",
    "mdgw-auditors": "Auditor",
}


def _restore_stdlib_ssl_context() -> None:
    """Undo an environment-injected truststore SSLContext, if present.

    Some Anaconda/pip environments monkey-patch ``ssl.SSLContext`` with
    ``pip._vendor.truststore``'s client-only wrapper. That wrapper cannot
    serve TLS, so the local HTTPS test server would abort every handshake.
    Tests restore the stdlib class; production code and defaults are
    untouched.
    """
    if ssl.SSLContext.__module__.startswith("pip._vendor.truststore"):
        from pip._vendor.truststore import (  # ruff: ignore[import-outside-top-level, import-private-name]
            _api as truststore_api,
        )

        truststore_api.extract_from_ssl()


_restore_stdlib_ssl_context()


# -- TLS material -----------------------------------------------------------


def _write_tls_files(tmp_path: Path) -> tuple[str, str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, HOST)])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address(HOST)),
                ]
            ),
            critical=False,
        )
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=None),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path = tmp_path / "tls-cert.pem"
    key_path = tmp_path / "tls-key.pem"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    return str(cert_path), str(key_path)


# -- HTTP client ------------------------------------------------------------


@dataclass
class HttpResult:
    status: int
    headers: list[tuple[str, str]]
    body: bytes
    set_cookie_headers: list[str]

    def header(self, name: str) -> str | None:
        for key, value in self.headers:
            if key.lower() == name.lower():
                return value
        return None

    @property
    def location(self) -> str | None:
        return self.header("Location")

    def json(self) -> Any:
        return json.loads(self.body)


class HttpClient:
    """Minimal HTTPS client with an explicit cookie jar and no redirects."""

    def __init__(self, base_url: str, *, context: ssl.SSLContext) -> None:
        parts = urlsplit(base_url)
        self.scheme = parts.scheme
        self.host = parts.hostname or HOST
        self.port = parts.port
        self.context = context
        self.cookies: dict[str, str] = {}
        self.request_count = 0

    @property
    def origin(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}"

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
        cookies: bool = True,
    ) -> HttpResult:
        connection: http.client.HTTPConnection
        if self.scheme == "https":
            connection = http.client.HTTPSConnection(
                self.host, self.port, context=self.context, timeout=15
            )
        else:
            connection = http.client.HTTPConnection(
                self.host, self.port, timeout=15
            )
        request_headers = dict(headers or {})
        if cookies and self.cookies:
            request_headers.setdefault(
                "Cookie",
                "; ".join(
                    f"{name}={value}"
                    for name, value in self.cookies.items()
                ),
            )
        try:
            connection.request(
                method, path, body=body, headers=request_headers
            )
            response = connection.getresponse()
            payload = response.read()
            raw_headers = list(response.getheaders())
        finally:
            connection.close()
        self.request_count += 1
        set_cookies = [
            value
            for name, value in raw_headers
            if name.lower() == "set-cookie"
        ]
        self._store_cookies(set_cookies)
        return HttpResult(
            status=int(response.status),
            headers=raw_headers,
            body=payload,
            set_cookie_headers=set_cookies,
        )

    def _store_cookies(self, set_cookies: list[str]) -> None:
        for header in set_cookies:
            jar = SimpleCookie()
            jar.load(header)
            for name, morsel in jar.items():
                max_age = morsel["max-age"]
                expired = max_age == "0"
                if not expired and morsel["expires"]:
                    try:
                        expired = (
                            datetime.strptime(
                                morsel["expires"],
                                "%a, %d %b %Y %H:%M:%S GMT",
                            ).replace(tzinfo=UTC)
                            < datetime.now(UTC)
                        )
                    except ValueError:
                        expired = False
                if expired:
                    self.cookies.pop(name, None)
                else:
                    self.cookies[name] = morsel.value

    def get(self, path: str, **kwargs: Any) -> HttpResult:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> HttpResult:
        return self.request("POST", path, **kwargs)


@dataclass
class Clock:
    now_ns: int = 0

    def __post_init__(self) -> None:
        if not self.now_ns:
            self.now_ns = time.time_ns()

    def __call__(self) -> int:
        return self.now_ns

    def advance_s(self, seconds: float) -> None:
        self.now_ns += int(seconds * 1_000_000_000)


# -- fake IdP ---------------------------------------------------------------


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _jwk(key: rsa.RSAPrivateKey, kid: str) -> dict[str, str]:
    numbers = key.public_key().public_numbers()
    return {
        "kty": "RSA",
        "kid": kid,
        "use": "sig",
        "alg": "RS256",
        "n": _b64url(
            numbers.n.to_bytes((numbers.n.bit_length() + 7) // 8, "big")
        ),
        "e": _b64url(
            numbers.e.to_bytes((numbers.e.bit_length() + 7) // 8, "big")
        ),
    }


def _sha256_b64(value: str) -> str:
    return _b64url(hashlib.sha256(value.encode("ascii")).digest())


class FakeIdp:
    """Local OIDC provider with fake discovery, JWKS and token endpoints."""

    def __init__(self) -> None:
        self.key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048
        )
        self.kid = "test-key"
        self.groups: list[str] = ["mdgw-operators"]
        self.subject = "oidc-subject-1"
        self.display_name = "Ada Operator"
        self.email = "ada@example.com"
        self.audience: str | list[str] = CLIENT_ID
        self.nonce_override: str | None = None
        self.exp_offset_s = 300
        self.iat_offset_s = -10
        self.token_status = 200
        self.discovery_status = 200
        self.secret = CLIENT_SECRET
        self.codes: dict[str, dict[str, str]] = {}
        self.counters = {"discovery": 0, "jwks": 0, "token": 0}
        self.token_request_bodies: list[dict[str, list[str]]] = []

        idp = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(  # ruff: ignore[no-self-use]  # http.server callback
                self, *_args: object
            ) -> None:
                return

            def do_GET(self) -> None:
                path = urlsplit(self.path).path
                if path == "/.well-known/openid-configuration":
                    idp.counters["discovery"] += 1
                    if idp.discovery_status != 200:
                        self._json(
                            idp.discovery_status, {"error": "server_error"}
                        )
                        return
                    self._json(
                        200,
                        {
                            "issuer": idp.issuer,
                            "authorization_endpoint": (
                                f"{idp.issuer}/authorize"
                            ),
                            "token_endpoint": f"{idp.issuer}/token",
                            "jwks_uri": f"{idp.issuer}/jwks",
                        },
                    )
                    return
                if path == "/jwks":
                    idp.counters["jwks"] += 1
                    self._json(200, {"keys": [_jwk(idp.key, idp.kid)]})
                    return
                self._json(404, {"error": "not_found"})

            def do_POST(self) -> None:
                path = urlsplit(self.path).path
                if path != "/token":
                    self._json(404, {"error": "not_found"})
                    return
                idp.counters["token"] += 1
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length).decode("ascii")
                form = parse_qs(body)
                idp.token_request_bodies.append(form)
                if idp.token_status != 200:
                    self._json(
                        idp.token_status, {"error": "invalid_grant"}
                    )
                    return
                code = (form.get("code") or [""])[0]
                registered = idp.codes.get(code)
                verifier = (form.get("code_verifier") or [""])[0]
                valid = (
                    registered is not None
                    and form.get("grant_type") == ["authorization_code"]
                    and form.get("client_id") == [CLIENT_ID]
                    and form.get("client_secret") == [idp.secret]
                    and verifier
                    and _sha256_b64(verifier)
                    == registered.get("code_challenge")
                    and (form.get("redirect_uri") or [None])[0]
                    == registered.get("redirect_uri")
                )
                if not valid:
                    self._json(400, {"error": "invalid_grant"})
                    return
                nonce = (
                    idp.nonce_override
                    if idp.nonce_override is not None
                    else registered["nonce"]
                )
                now = int(time.time())
                claims = {
                    "iss": idp.issuer,
                    "aud": idp.audience,
                    "sub": idp.subject,
                    "iat": now + idp.iat_offset_s,
                    "exp": now + idp.exp_offset_s,
                    "nonce": nonce,
                    "groups": idp.groups,
                    "name": idp.display_name,
                    "email": idp.email,
                }
                header = {"alg": "RS256", "typ": "JWT", "kid": idp.kid}
                signing_input = (
                    f"{_b64url(json.dumps(header).encode())}."
                    f"{_b64url(json.dumps(claims).encode())}"
                ).encode("ascii")
                signature = idp.key.sign(
                    signing_input, padding.PKCS1v15(), hashes.SHA256()
                )
                self._json(
                    200,
                    {
                        "access_token": "not-used",
                        "token_type": "Bearer",
                        "id_token": (
                            f"{signing_input.decode()}."
                            f"{_b64url(signature)}"
                        ),
                    },
                )

            def _json(self, status: int, payload: dict[str, Any]) -> None:
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._server = ThreadingHTTPServer((HOST, 0), Handler)
        self.port = self._server.server_address[1]
        self.issuer = f"http://{HOST}:{self.port}"
        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True
        )
        self._thread.start()

    def register_code(
        self,
        code: str,
        *,
        nonce: str,
        code_challenge: str,
        redirect_uri: str,
    ) -> None:
        self.codes[code] = {
            "nonce": nonce,
            "code_challenge": code_challenge,
            "redirect_uri": redirect_uri,
        }

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


@pytest.fixture
def idp() -> Generator[FakeIdp]:
    provider = FakeIdp()
    try:
        yield provider
    finally:
        provider.close()


# -- app fixture ------------------------------------------------------------


def _admin_config(idp: FakeIdp, tmp_path: Path) -> AdminConfig:
    return AdminConfig(
        enabled=True,
        db_path=tmp_path / "admin" / "admin.db",
        adapters=AdminAdaptersConfig(oidc=True),
        oidc=AdminOidcConfig(
            issuer=idp.issuer,
            client_id=CLIENT_ID,
            client_secret_env=SECRET_ENV,
            role_mapping=ROLE_MAPPING,
        ),
    )


@contextmanager
def _running_app(
    tmp_path: Path,
    *,
    config: AdminConfig,
    clock: Clock | None = None,
    ws_hub: WebSocketHub | None = None,
) -> Generator[tuple[AsgiServer, HttpClient, AdminContext]]:
    store = ParquetBarStore(tmp_path / "data")
    context = AdminContext(
        config=config,
        storage_root=tmp_path,
        store=store,
        environ={SECRET_ENV: CLIENT_SECRET},
        clock=clock,
        ws_hub=ws_hub,
    )
    app = create_app(store, admin=context)
    cert_path, key_path = _write_tls_files(tmp_path)
    server = AsgiServer()
    try:
        server.start(
            app,
            host=HOST,
            port=0,
            thread_name=THREAD_NAME,
            ssl_certfile=cert_path,
            ssl_keyfile=key_path,
        )
        tls_context = ssl.create_default_context(cafile=cert_path)
        client = HttpClient(server.base_url, context=tls_context)
        yield server, client, context
    finally:
        server.stop()
        store.close()


@pytest.fixture
def admin_app(
    tmp_path: Path, idp: FakeIdp
) -> Generator[tuple[AsgiServer, HttpClient, AdminContext, Clock]]:
    clock = Clock()
    with _running_app(
        tmp_path, config=_admin_config(idp, tmp_path), clock=clock
    ) as (server, client, context):
        yield server, client, context, clock


# -- login helpers ----------------------------------------------------------


@dataclass
class LoginFlow:
    login: HttpResult
    callback: HttpResult
    state: str
    nonce: str
    challenge: str
    redirect_uri: str


def _login(
    client: HttpClient,
    idp: FakeIdp,
    *,
    return_to: str = "/monitoring",
    code: str = "code-1",
) -> LoginFlow:
    login = client.get(
        f"/api/v1/admin/auth/login?return_to={quote(return_to, safe='')}"
    )
    assert login.status == 302, login.body
    location = login.location
    assert location is not None
    parts = urlsplit(location)
    assert f"{parts.scheme}://{parts.netloc}" == idp.issuer
    query = parse_qs(parts.query)
    flow = LoginFlow(
        login=login,
        callback=HttpResult(0, [], b"", []),
        state=query["state"][0],
        nonce=query["nonce"][0],
        challenge=query["code_challenge"][0],
        redirect_uri=query["redirect_uri"][0],
    )
    idp.register_code(
        code,
        nonce=flow.nonce,
        code_challenge=flow.challenge,
        redirect_uri=flow.redirect_uri,
    )
    callback = client.get(
        f"/api/v1/admin/auth/callback?code={code}&state={flow.state}"
    )
    flow.callback = callback
    return flow


def _csrf(client: HttpClient) -> str:
    payload = _session(client)
    assert payload["csrf"] is not None
    return str(payload["csrf"]["token"])


def _session(client: HttpClient) -> dict[str, Any]:
    response = client.get("/api/v1/admin/auth/session")
    assert response.status == 200
    return response.json()


def _post(
    client: HttpClient,
    path: str,
    *,
    csrf: str | None = None,
    origin: str | bool | None = True,
) -> HttpResult:
    headers: dict[str, str] = {}
    if csrf is not None:
        headers["X-CSRF-Token"] = csrf
    if origin is True:
        headers["Origin"] = client.origin
    elif isinstance(origin, str):
        headers["Origin"] = origin
    return client.post(path, headers=headers)


# -- login entry ------------------------------------------------------------


def test_login_redirects_with_pkce_state_nonce_and_transaction_cookie(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
) -> None:
    _server, client, _context, _clock = admin_app

    login = client.get(
        "/api/v1/admin/auth/login?return_to=%2Fsecurity%3Ftab%3Daudit"
    )

    assert login.status == 302
    parts = urlsplit(login.location or "")
    query = parse_qs(parts.query)
    assert parts.path == "/authorize"
    assert query["client_id"] == [CLIENT_ID]
    assert query["response_type"] == ["code"]
    assert query["code_challenge_method"] == ["S256"]
    assert query["scope"] == ["openid profile email"]
    assert query.get("return_to") is None
    assert query["redirect_uri"] == [
        f"{client.origin}/api/v1/admin/auth/callback"
    ]
    assert len(query["state"][0]) >= 32
    assert len(query["nonce"][0]) >= 32

    assert len(login.set_cookie_headers) == 1
    cookie = login.set_cookie_headers[0]
    assert cookie.startswith("mdgw_oidc_tx=")
    assert "HttpOnly" in cookie
    assert "Secure" in cookie
    assert "samesite=lax" in cookie.lower()
    assert "Path=/api/v1/admin/auth" in cookie
    assert "Domain=" not in cookie
    max_age = int(re.search(r"Max-Age=(\d+)", cookie).group(1))
    assert 0 < max_age <= 600


@pytest.mark.parametrize(
    "encoded_return_to",
    [
        "https%3A%2F%2Fevil.example%2Fsteal",
        "%2F%2Fevil.example%2Fsteal",
        "javascript%3Aalert%281%29",
        "%2Fmonitoring%00",
        "monitoring",
    ],
)
def test_login_rejects_unsafe_return_to(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    encoded_return_to: str,
) -> None:
    _server, client, _context, _clock = admin_app

    response = client.get(
        f"/api/v1/admin/auth/login?return_to={encoded_return_to}"
    )

    assert response.status == 400
    envelope = response.json()["error"]
    assert envelope["code"] == "invalid_parameter"
    assert envelope["details"]["field"] == "return_to"
    assert response.set_cookie_headers == []


def test_login_fails_closed_when_discovery_fails(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, context, _clock = admin_app
    idp.discovery_status = 503

    response = client.get("/api/v1/admin/auth/login?return_to=/")

    assert response.status == 401
    assert response.json()["error"]["details"]["reason"] == "idp_error"
    events = context.auth.store.recent_security_events()
    assert any(event.kind == "auth.login.idp_error" for event in events)


def test_login_fails_closed_when_oidc_is_not_configured(
    tmp_path: Path,
) -> None:
    config = AdminConfig(
        enabled=True,
        db_path=tmp_path / "admin" / "admin.db",
        adapters=AdminAdaptersConfig(oidc=False),
    )
    with _running_app(tmp_path, config=config) as (_server, client, _context):
        response = client.get("/api/v1/admin/auth/login?return_to=/")

    assert response.status == 401
    envelope = response.json()["error"]
    assert envelope["code"] == "unauthenticated"
    assert envelope["details"]["reason"] == "idp_error"


# -- callback and session ---------------------------------------------------


def test_callback_exchanges_code_sets_secure_session_and_returns_payload(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, _context, _clock = admin_app

    flow = _login(client, idp)

    assert flow.callback.status == 303
    assert flow.callback.location == "/monitoring"
    session_cookies = [
        header
        for header in flow.callback.set_cookie_headers
        if header.startswith("mdgw_session=")
    ]
    assert len(session_cookies) == 1
    session_cookie = session_cookies[0]
    assert "HttpOnly" in session_cookie
    assert "Secure" in session_cookie
    assert "samesite=lax" in session_cookie.lower()
    assert "Path=/" in session_cookie
    assert "Domain=" not in session_cookie
    max_age = int(re.search(r"Max-Age=(\d+)", session_cookie).group(1))
    assert 0 < max_age <= 28_800
    assert any(
        header.startswith("mdgw_oidc_tx=") and "Max-Age=0" in header
        for header in flow.callback.set_cookie_headers
    )

    # The fake IdP validated client_secret + the S256 code_verifier.
    assert idp.counters["token"] == 1
    token_form = idp.token_request_bodies[0]
    assert token_form["client_secret"] == [CLIENT_SECRET]
    assert token_form["redirect_uri"] == [flow.redirect_uri]

    session = _session(client)
    assert set(session) == {
        "authenticated",
        "server_time",
        "user",
        "access_role",
        "session",
        "csrf",
    }
    assert session["authenticated"] is True
    assert NS_TIMESTAMP.fullmatch(session["server_time"]) is not None
    assert session["user"] == {
        "subject": "oidc-subject-1",
        "display_name": "Ada Operator",
        "email": "ada@example.com",
    }
    assert session["access_role"] == "Operator"
    assert set(session["session"]) == {
        "issued_at",
        "last_activity_at",
        "idle_expires_at",
        "absolute_expires_at",
    }
    issued = iso_to_ns(session["session"]["issued_at"])
    idle = iso_to_ns(session["session"]["idle_expires_at"])
    absolute = iso_to_ns(session["session"]["absolute_expires_at"])
    assert idle - issued == 1_800 * 1_000_000_000
    assert absolute - issued == 28_800 * 1_000_000_000
    assert session["csrf"]["header_name"] == "X-CSRF-Token"
    assert len(session["csrf"]["token"]) >= 32

    status_headers = {
        name.lower(): value
        for name, value in client.get(
            "/api/v1/admin/auth/session"
        ).headers
    }
    assert status_headers["cache-control"] == "no-store"


def test_anonymous_session_probe_is_200_with_null_fields(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
) -> None:
    _server, client, _context, _clock = admin_app

    session = _session(client)

    assert session["authenticated"] is False
    assert session["user"] is None
    assert session["access_role"] is None
    assert session["session"] is None
    assert session["csrf"] is None
    assert NS_TIMESTAMP.fullmatch(session["server_time"]) is not None


def test_session_probe_does_not_extend_idle_timeout(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, _context, clock = admin_app
    _login(client, idp)
    first = _session(client)
    last_activity = first["session"]["last_activity_at"]

    clock.advance_s(1_700)
    assert _session(client)["authenticated"] is True
    clock.advance_s(99)
    assert _session(client)["session"]["last_activity_at"] == last_activity

    clock.advance_s(1)
    anonymous = _session(client)
    assert anonymous["authenticated"] is False


def test_idle_timeout_401_reason_and_security_event(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, context, clock = admin_app
    _login(client, idp)
    csrf = _csrf(client)
    clock.advance_s(1_800)

    response = _post(
        client, "/api/v1/admin/auth/refresh", csrf=csrf
    )

    assert response.status == 401
    envelope = response.json()["error"]
    assert envelope["code"] == "unauthenticated"
    assert envelope["details"]["reason"] == "idle_timeout"
    events = context.auth.store.recent_security_events()
    assert any(
        event.kind == "session.invalid" and event.reason == "idle_timeout"
        for event in events
    )


def test_invalid_session_401_reason_and_security_event(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
) -> None:
    _server, client, context, _clock = admin_app
    response = client.post(
        "/api/v1/admin/auth/refresh",
        headers={
            "Cookie": "mdgw_session=not-a-real-session",
            "Origin": client.origin,
            "X-CSRF-Token": "irrelevant",
        },
    )

    assert response.status == 401
    assert response.json()["error"]["details"]["reason"] == "invalid"
    events = context.auth.store.recent_security_events()
    assert any(
        event.kind == "session.invalid" and event.reason == "invalid"
        for event in events
    )


def test_provider_error_callback_redirects_idp_error(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
) -> None:
    _server, client, context, _clock = admin_app

    response = client.get(
        "/api/v1/admin/auth/callback?error=access_denied"
    )

    assert response.status == 303
    assert response.location == "/login?error=idp_error"
    events = context.auth.store.recent_security_events()
    assert any(event.reason == "access_denied" for event in events)


def test_session_status_rotates_csrf_without_extending_idle(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, _context, _clock = admin_app
    _login(client, idp)

    first = _session(client)
    second = _session(client)

    assert first["csrf"]["token"] != second["csrf"]["token"]
    assert first["session"]["last_activity_at"] == (
        second["session"]["last_activity_at"]
    )
    # Only the latest token is valid for a write.
    stale = client.post(
        "/api/v1/admin/auth/refresh",
        headers={
            "Cookie": f"mdgw_session={client.cookies['mdgw_session']}",
            "Origin": client.origin,
            "X-CSRF-Token": first["csrf"]["token"],
        },
    )
    assert stale.status == 403
    assert stale.json()["error"]["details"]["reason"] == "csrf"


def test_absolute_timeout_401_reason(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, _context, clock = admin_app
    _login(client, idp)
    # Keep the session genuinely active (explicit refresh) until 8 hours.
    for _ in range(28):
        csrf = _csrf(client)
        clock.advance_s(1_000)
        refreshed = _post(
            client, "/api/v1/admin/auth/refresh", csrf=csrf
        )
        assert refreshed.status == 200
    csrf = _csrf(client)
    clock.advance_s(800)

    response = _post(
        client, "/api/v1/admin/auth/refresh", csrf=csrf
    )

    assert response.status == 401
    assert response.json()["error"]["details"]["reason"] == (
        "absolute_timeout"
    )
    assert _session(client)["authenticated"] is False


def test_refresh_extends_idle_rotates_csrf_and_requires_csrf_and_origin(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, _context, clock = admin_app
    _login(client, idp)
    before = _session(client)
    csrf = before["csrf"]["token"]
    clock.advance_s(900)

    missing = _post(client, "/api/v1/admin/auth/refresh")
    assert missing.status == 403
    assert missing.json()["error"]["details"]["reason"] == "csrf"

    wrong_origin = _post(
        client,
        "/api/v1/admin/auth/refresh",
        csrf=csrf,
        origin="https://evil.example.com",
    )
    assert wrong_origin.status == 403
    assert wrong_origin.json()["error"]["details"]["reason"] == "origin"

    wrong_csrf = _post(
        client, "/api/v1/admin/auth/refresh", csrf="not-the-token"
    )
    assert wrong_csrf.status == 403
    assert wrong_csrf.json()["error"]["details"]["reason"] == "csrf"

    refreshed = _post(
        client, "/api/v1/admin/auth/refresh", csrf=csrf
    )
    assert refreshed.status == 200
    payload = refreshed.json()
    assert payload["authenticated"] is True
    assert payload["csrf"]["token"] != csrf
    assert payload["session"]["last_activity_at"] != (
        before["session"]["last_activity_at"]
    )
    assert payload["session"]["absolute_expires_at"] == (
        before["session"]["absolute_expires_at"]
    )


def test_logout_invalidates_the_server_session_and_clears_the_cookie(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, _context, _clock = admin_app
    _login(client, idp)
    csrf = _csrf(client)

    missing = _post(client, "/api/v1/admin/auth/logout")
    assert missing.status == 403
    assert missing.json()["error"]["details"]["reason"] == "csrf"

    response = _post(client, "/api/v1/admin/auth/logout", csrf=csrf)

    assert response.status == 204
    cleared = [
        header
        for header in response.set_cookie_headers
        if header.startswith("mdgw_session=")
    ]
    assert len(cleared) == 1
    assert "Max-Age=0" in cleared[0]
    assert _session(client)["authenticated"] is False

    # The old session token is invalid even if a client replays it.
    replayed = client.get(
        "/api/v1/admin/auth/session",
        headers={"Cookie": "mdgw_session=replayed"},
    )
    assert replayed.json()["authenticated"] is False


def test_session_fixation_issues_a_new_session_and_invalidates_the_old_one(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, _context, _clock = admin_app
    _login(client, idp, code="code-1")
    first_token = client.cookies["mdgw_session"]

    _login(client, idp, code="code-2")
    second_token = client.cookies["mdgw_session"]

    assert first_token != second_token
    probe = client.get(
        "/api/v1/admin/auth/session",
        headers={"Cookie": f"mdgw_session={first_token}"},
    )
    assert probe.json()["authenticated"] is False


# -- callback failure semantics --------------------------------------------


def test_callback_without_transaction_cookie_redirects_state_invalid(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
) -> None:
    _server, client, _context, _clock = admin_app

    response = client.get(
        "/api/v1/admin/auth/callback?code=x&state=y"
    )

    assert response.status == 303
    assert response.location == "/login?error=state_invalid"
    assert "mdgw_session=" not in " ".join(response.set_cookie_headers)


def test_callback_with_a_non_ascii_state_cookie_is_state_invalid(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
) -> None:
    _server, client, _context, _clock = admin_app

    response = client.get(
        "/api/v1/admin/auth/callback?code=x&state=s",
        headers={"Cookie": "mdgw_oidc_tx=café"},
    )

    assert response.status == 303
    assert response.location == "/login?error=state_invalid"


def test_callback_state_cookie_mismatch_is_rejected(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
) -> None:
    _server, client, _context, _clock = admin_app
    login = client.get(
        "/api/v1/admin/auth/login?return_to=%2Fmonitoring"
    )
    state = parse_qs(urlsplit(login.location or "").query)["state"][0]

    response = client.get(
        f"/api/v1/admin/auth/callback?code=x&state={state}",
        headers={"Cookie": "mdgw_oidc_tx=different-state"},
    )

    assert response.status == 303
    assert response.location == "/login?error=state_invalid"


def test_expired_login_transaction_redirects_state_invalid(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, _context, clock = admin_app
    login = client.get(
        "/api/v1/admin/auth/login?return_to=%2Fmonitoring"
    )
    query = parse_qs(urlsplit(login.location or "").query)
    state = query["state"][0]
    idp.register_code(
        "expired-code",
        nonce=query["nonce"][0],
        code_challenge=query["code_challenge"][0],
        redirect_uri=query["redirect_uri"][0],
    )
    clock.advance_s(301)

    response = client.get(
        f"/api/v1/admin/auth/callback?code=expired-code&state={state}"
    )

    assert response.status == 303
    assert response.location == "/login?error=state_invalid"


def test_token_endpoint_failure_redirects_idp_error(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, _context, _clock = admin_app
    idp.token_status = 400

    flow = _login(client, idp)

    assert flow.callback.status == 303
    assert flow.callback.location == "/login?error=idp_error"
    assert _session(client)["authenticated"] is False


@pytest.mark.parametrize(
    "mutation",
    [
        {"nonce_override": "a-different-nonce"},
        {"audience": "another-client"},
        {"exp_offset_s": -3_600},
        {"iat_offset_s": 3_600},
    ],
)
def test_invalid_id_token_redirects_token_invalid(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
    mutation: dict[str, Any],
) -> None:
    _server, client, _context, _clock = admin_app
    for name, value in mutation.items():
        setattr(idp, name, value)

    flow = _login(client, idp)

    assert flow.callback.status == 303
    assert flow.callback.location == "/login?error=token_invalid"


def test_unmapped_role_denies_login_and_records_a_security_event(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, context, _clock = admin_app
    idp.groups = ["not-mapped-anywhere"]
    forwarded: list[object] = []
    context.auth.security_event_sink = forwarded.append

    flow = _login(client, idp)

    assert flow.callback.location == "/login?error=role_unmapped"
    assert _session(client)["authenticated"] is False
    events = context.auth.store.recent_security_events()
    assert any(
        event.kind == "auth.role_unmapped"
        and event.reason == "role_unmapped"
        for event in events
    )
    # Ticket 03's bridge hook receives the same persisted event.
    assert [event.kind for event in forwarded] == ["auth.role_unmapped"]


def test_conflicting_roles_deny_login_and_record_a_security_event(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, context, _clock = admin_app
    idp.groups = ["mdgw-admins", "mdgw-operators"]

    flow = _login(client, idp)

    assert flow.callback.location == "/login?error=role_conflict"
    assert _session(client)["authenticated"] is False
    events = context.auth.store.recent_security_events()
    assert any(
        event.kind == "auth.role_conflict"
        and event.reason == "role_conflict"
        for event in events
    )


def test_client_cannot_choose_or_elevate_a_role(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, _context, _clock = admin_app
    # A claim that looks like a role is ignored; only groups are mapped.
    idp.groups = ["mdgw-operators"]

    _login(client, idp, return_to="/monitoring?role=SuperAdmin")

    session = _session(client)
    assert session["access_role"] == "Operator"


def test_csrf_and_origin_failures_are_persisted_as_security_events(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, context, _clock = admin_app
    _login(client, idp)
    csrf = _csrf(client)

    _post(client, "/api/v1/admin/auth/refresh", csrf=csrf, origin=None)
    _post(client, "/api/v1/admin/auth/refresh", csrf="wrong")
    client.cookies["mdgw_session"] = "invalid-token"
    client.get("/api/v1/admin/auth/session")

    events = context.auth.store.recent_security_events()
    kinds = {event.kind for event in events}
    assert {"authz.csrf_denied", "authz.origin_denied"} <= kinds


def test_secrets_and_tokens_never_appear_in_responses_or_security_events(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, context, _clock = admin_app
    idp.groups = ["not-mapped"]
    flow = _login(client, idp)
    session = _session(client)
    capabilities = client.get("/api/v1/admin/capabilities").json()

    serialized = json.dumps(
        {
            "session": session,
            "capabilities": capabilities,
            "callback_headers": flow.callback.set_cookie_headers,
        }
    )
    events = json.dumps(
        [
            {"kind": event.kind, "detail": dict(event.detail)}
            for event in context.auth.store.recent_security_events()
        ]
    )
    assert CLIENT_SECRET not in serialized
    assert CLIENT_SECRET not in events
    assert "not-mapped" not in events
    assert "id_token" not in serialized


# -- discovery / JWKS caches ------------------------------------------------


def test_discovery_and_jwks_are_cached_between_logins(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
    idp: FakeIdp,
) -> None:
    _server, client, _context, _clock = admin_app

    _login(client, idp, code="code-1")
    _login(client, idp, code="code-2")

    assert idp.counters["discovery"] == 1
    assert idp.counters["jwks"] == 1
    assert idp.counters["token"] == 2


# -- capability discovery ---------------------------------------------------


def test_capabilities_report_oidc_available_and_ws_session_enabled(
    admin_app: tuple[AsgiServer, HttpClient, AdminContext, Clock],
) -> None:
    _server, client, _context, _clock = admin_app

    document = client.get("/api/v1/admin/capabilities").json()

    adapters = {entry["name"]: entry["state"] for entry in document["adapters"]}
    assert adapters["oidc"] == "available"
    assert document["auth"]["state"] == "available"
    assert document["auth"]["required"] is True
    oidc_session = document["capabilities"]["oidc_session"]
    assert oidc_session["state"] == "enabled"
    ws = document["capabilities"]["ws_admin_session"]
    assert ws == {
        "state": "enabled",
        "path": "/api/v1/admin/ws",
        "ops": ["subscribe", "unsubscribe", "ping"],
    }


def test_default_repository_config_still_disables_admin() -> None:
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")

    assert config.admin.enabled is False
    document = build_capabilities_document(config=config.admin)
    assert document["auth"]["state"] == "unsupported"
    assert document["capabilities"]["oidc_session"] == {
        "state": "unsupported"
    }


def test_public_admin_paths_are_the_documented_explicit_set() -> None:
    assert set(PUBLIC_ADMIN_PATHS) == {
        "/api/v1/admin/capabilities",
        "/api/v1/admin/auth/login",
        "/api/v1/admin/auth/callback",
        "/api/v1/admin/auth/session",
    }
