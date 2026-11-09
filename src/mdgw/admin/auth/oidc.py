# ruff: file-ignore[suspicious-url-open-usage]  # every URL is validated http(s)
"""Offline-testable OIDC Authorization Code + PKCE client.

The API is the OIDC relying party. It fetches provider metadata and JWKS over
stdlib ``urllib`` (with an injectable transport and clock for tests), performs
the code exchange with the client secret read from the configured environment
variable, and verifies the ID Token signature (RS256/ES256) and claims
(``iss``/``aud``/``exp``/``iat``/``nonce``/``sub``) with ``cryptography``.

Nothing here logs or returns the client secret, authorization code or tokens.
Errors carry stable reason codes only, so the caller can map them onto the
contract's redirect/security-event vocabulary without leaking provider text.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import urlencode, urlsplit

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
from cryptography.hazmat.primitives.asymmetric.utils import (
    encode_dss_signature,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from mdgw.config import AdminOidcConfig

#: A provider cache entry is never trusted for longer than one hour, even if
#: the configured TTL is larger; JWKS rotates and the cache must age out.
MAX_CACHE_TTL_S = 3_600.0
MAX_RESPONSE_BYTES = 1_048_576
MAX_JWKS_KEYS = 32
_SUPPORTED_ALGS = frozenset({"RS256", "ES256"})
_ROLE_CLAIM_INVALID = "role_claim_invalid"
_ROLE_UNMAPPED = "role_unmapped"
_ROLE_CONFLICT = "role_conflict"
_EC_CURVES: dict[str, ec.EllipticCurve] = {"P-256": ec.SECP256R1()}


@dataclass(frozen=True)
class TransportResponse:
    status: int
    body: bytes


class OidcTransportError(Exception):
    """The injected/urllib transport could not complete the request."""


class OidcTransport(Protocol):
    def __call__(
        self, request: urllib.request.Request, *, timeout: float
    ) -> TransportResponse: ...


class OidcError(Exception):
    """Base class for OIDC failures with a stable machine reason."""

    reason = "idp_error"

    def __init__(self, message: str, *, reason: str | None = None) -> None:
        super().__init__(message)
        if reason is not None:
            self.reason = reason


class OidcDiscoveryError(OidcError):
    """Provider metadata could not be fetched or is not usable."""

    reason = "discovery_failed"


class OidcTokenError(OidcError):
    """The authorization code exchange failed."""

    reason = "token_exchange_failed"


class IdTokenInvalidError(OidcError):
    """The ID Token signature or claims failed verification."""

    reason = "id_token_invalid"


class RoleResolutionError(Exception):
    """The OIDC claim did not resolve to exactly one Access Role."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class OidcProvider:
    issuer: str
    authorization_endpoint: str
    token_endpoint: str
    jwks_uri: str


@dataclass(frozen=True)
class SigningKey:
    kid: str | None
    kty: str
    alg: str
    public_key: rsa.RSAPublicKey | ec.EllipticCurvePublicKey


def b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def b64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def pkce_challenge(verifier: str) -> str:
    """S256 PKCE challenge for one verifier."""
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return b64url_encode(digest)


def _default_transport(
    request: urllib.request.Request, *, timeout: float
) -> TransportResponse:
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read(MAX_RESPONSE_BYTES + 1)
            return TransportResponse(int(response.status), body)
    except urllib.error.HTTPError as error:
        try:
            body = error.read(MAX_RESPONSE_BYTES + 1)
        except (OSError, ValueError):  # pragma: no cover - read failure
            body = b""
        return TransportResponse(int(error.code), body)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        msg = "OIDC transport request failed"
        raise OidcTransportError(msg) from exc


class OidcClient:
    """Discovery/JWKS caches plus the code exchange and ID Token checks."""

    def __init__(
        self,
        config: AdminOidcConfig,
        *,
        transport: OidcTransport | None = None,
        clock: Callable[[], int] | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        self.config = config
        self._transport = _default_transport if transport is None else transport
        self._clock = time.time_ns if clock is None else clock
        self._environ = environ
        self._lock = threading.RLock()
        self._provider: OidcProvider | None = None
        self._provider_expires_ns = 0
        self._jwks: tuple[SigningKey, ...] | None = None
        self._jwks_expires_ns = 0

    # -- provider metadata -------------------------------------------------

    def discovery(self) -> OidcProvider:
        """Return provider metadata, using the bounded discovery cache."""
        now_ns = self._clock()
        with self._lock:
            cached = self._provider
            if cached is not None and now_ns < self._provider_expires_ns:
                return cached
            provider = self._fetch_provider()
            self._provider = provider
            self._provider_expires_ns = now_ns + _ttl_ns(
                self.config.discovery_ttl_s
            )
            return provider

    def _fetch_provider(self) -> OidcProvider:
        issuer = (self.config.issuer or "").rstrip("/")
        if not issuer:
            msg = "OIDC issuer is not configured"
            raise OidcDiscoveryError(msg)
        url = f"{issuer}/.well-known/openid-configuration"
        document = self._fetch_json(url, error_type=OidcDiscoveryError)
        provider_issuer = document.get("issuer")
        if not isinstance(provider_issuer, str) or (
            provider_issuer.rstrip("/") != issuer
        ):
            msg = "OIDC discovery issuer does not match the configured issuer"
            raise OidcDiscoveryError(msg)
        endpoints: dict[str, str] = {}
        for field in (
            "authorization_endpoint",
            "token_endpoint",
            "jwks_uri",
        ):
            value = document.get(field)
            if not _is_http_url(value):
                msg = f"OIDC discovery field {field} is not an http(s) URL"
                raise OidcDiscoveryError(msg)
            endpoints[field] = value
        return OidcProvider(
            issuer=provider_issuer,
            authorization_endpoint=endpoints["authorization_endpoint"],
            token_endpoint=endpoints["token_endpoint"],
            jwks_uri=endpoints["jwks_uri"],
        )

    def _fetch_json(
        self,
        url: str,
        *,
        error_type: type[OidcError],
        request: urllib.request.Request | None = None,
    ) -> dict[str, Any]:
        if request is None:
            request = urllib.request.Request(
                url,
                headers={"Accept": "application/json"},
                method="GET",
            )
        try:
            response = self._transport(
                request, timeout=self.config.http_timeout_s
            )
        except OidcTransportError as exc:
            msg = "OIDC provider request failed"
            raise error_type(msg) from exc
        if response.status != 200:
            msg = f"OIDC provider returned HTTP {response.status}"
            raise error_type(msg)
        if len(response.body) > MAX_RESPONSE_BYTES:
            msg = "OIDC provider response is too large"
            raise error_type(msg)
        try:
            document = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            msg = "OIDC provider returned malformed JSON"
            raise error_type(msg) from exc
        if not isinstance(document, dict):
            msg = "OIDC provider returned a non-object document"
            raise error_type(msg)
        return document

    # -- authorization request ---------------------------------------------

    def authorization_url(
        self,
        *,
        state: str,
        nonce: str,
        code_challenge: str,
        redirect_uri: str,
    ) -> str:
        provider = self.discovery()
        query = urlencode(
            {
                "response_type": "code",
                "client_id": self.config.client_id or "",
                "redirect_uri": redirect_uri,
                "scope": " ".join(self.config.scopes),
                "state": state,
                "nonce": nonce,
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
            }
        )
        separator = "&" if "?" in provider.authorization_endpoint else "?"
        return f"{provider.authorization_endpoint}{separator}{query}"

    # -- token exchange ----------------------------------------------------

    def exchange_code(
        self,
        *,
        code: str,
        code_verifier: str,
        redirect_uri: str,
    ) -> str:
        """Exchange one authorization code and return the raw ID Token."""
        secret = self._client_secret()
        provider = self.discovery()
        body = urlencode(
            {
                "grant_type": "authorization_code",
                "code": code,
                "client_id": self.config.client_id or "",
                "client_secret": secret,
                "code_verifier": code_verifier,
                "redirect_uri": redirect_uri,
            }
        ).encode("ascii")
        request = urllib.request.Request(
            provider.token_endpoint,
            data=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            response = self._transport(
                request, timeout=self.config.http_timeout_s
            )
        except OidcTransportError as exc:
            msg = "OIDC token request failed"
            raise OidcTokenError(msg) from exc
        if response.status != 200:
            error_code = _provider_error_code(response.body)
            suffix = f" ({error_code})" if error_code else ""
            msg = f"OIDC token endpoint rejected the code exchange{suffix}"
            raise OidcTokenError(msg)
        if len(response.body) > MAX_RESPONSE_BYTES:
            msg = "OIDC token response is too large"
            raise OidcTokenError(msg)
        try:
            document = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            msg = "OIDC token endpoint returned malformed JSON"
            raise OidcTokenError(msg) from exc
        id_token = document.get("id_token") if isinstance(document, dict) else None
        if not isinstance(id_token, str) or not id_token:
            msg = "OIDC token response did not include an ID Token"
            raise OidcTokenError(msg)
        return id_token

    def _client_secret(self) -> str:
        env_name = self.config.client_secret_env or ""
        if self._environ is not None:
            secret = self._environ.get(env_name, "")
        else:
            secret = os.environ.get(env_name, "")
        if not secret:
            msg = f"OIDC client secret is not configured in {env_name!r}"
            raise OidcTokenError(msg)
        return secret

    # -- ID Token verification ---------------------------------------------

    def verify_id_token(
        self,
        id_token: str,
        *,
        expected_nonce_hash: str,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        """Verify signature and claims; return the trusted claim set."""
        now = self._clock() if now_ns is None else int(now_ns)
        header, claims, signing_input, signature = _decode_jwt(id_token)
        alg = header.get("alg")
        if alg not in _SUPPORTED_ALGS:
            msg = f"ID Token uses an unsupported algorithm: {alg!r}"
            raise IdTokenInvalidError(msg, reason="alg")
        kid = header.get("kid")
        kid_value = kid if isinstance(kid, str) and kid else None

        keys = self._signing_keys()
        candidates = _select_keys(keys, kid_value)
        if not candidates:
            # A provider may rotate keys between a login and the next fetch.
            keys = self._signing_keys(force=True)
            candidates = _select_keys(keys, kid_value)
        if not candidates:
            msg = "ID Token was signed by an unknown key"
            raise IdTokenInvalidError(msg, reason="unknown_kid")

        if not _verify_signature(candidates, alg, signing_input, signature):
            msg = "ID Token signature verification failed"
            raise IdTokenInvalidError(msg, reason="signature")

        self._verify_claims(claims, expected_nonce_hash=expected_nonce_hash,
                            now_ns=now)
        return claims

    def _verify_claims(  # ruff: ignore[too-many-branches]  # explicit claim checks
        self,
        claims: Mapping[str, Any],
        *,
        expected_nonce_hash: str,
        now_ns: int,
    ) -> None:
        issuer = (self.config.issuer or "").rstrip("/")
        claim_issuer = claims.get("iss")
        if not isinstance(claim_issuer, str) or (
            claim_issuer.rstrip("/") != issuer
        ):
            msg = "ID Token issuer does not match"
            raise IdTokenInvalidError(msg, reason="issuer")

        audience = claims.get("aud")
        client_id = self.config.client_id or ""
        if isinstance(audience, str):
            audiences = [audience]
        elif isinstance(audience, list) and all(
            isinstance(item, str) for item in audience
        ):
            audiences = audience
        else:
            audiences = []
        if client_id not in audiences:
            msg = "ID Token audience does not include this client"
            raise IdTokenInvalidError(msg, reason="audience")

        now_s = now_ns / 1_000_000_000
        skew = float(self.config.clock_skew_s)
        exp = claims.get("exp")
        if not isinstance(exp, (int, float)) or isinstance(exp, bool):
            msg = "ID Token has no numeric expiry"
            raise IdTokenInvalidError(msg, reason="expiry")
        if now_s > float(exp) + skew:
            msg = "ID Token has expired"
            raise IdTokenInvalidError(msg, reason="expired")
        iat = claims.get("iat")
        if iat is not None:
            if not isinstance(iat, (int, float)) or isinstance(iat, bool):
                msg = "ID Token has a non-numeric issued-at"
                raise IdTokenInvalidError(msg, reason="issued_at")
            if float(iat) > now_s + skew:
                msg = "ID Token was issued in the future"
                raise IdTokenInvalidError(msg, reason="issued_in_future")

        nonce = claims.get("nonce")
        if not isinstance(nonce, str) or not nonce:
            msg = "ID Token has no nonce"
            raise IdTokenInvalidError(msg, reason="nonce")
        actual_nonce_hash = hashlib.sha256(nonce.encode("utf-8")).hexdigest()
        if not hmac.compare_digest(actual_nonce_hash, expected_nonce_hash):
            msg = "ID Token nonce does not match the login transaction"
            raise IdTokenInvalidError(msg, reason="nonce")

        subject = claims.get("sub")
        if not isinstance(subject, str) or not subject:
            msg = "ID Token has no subject"
            raise IdTokenInvalidError(msg, reason="subject")

    # -- JWKS --------------------------------------------------------------

    def _signing_keys(self, *, force: bool = False) -> tuple[SigningKey, ...]:
        now_ns = self._clock()
        with self._lock:
            cached = self._jwks
            if (
                not force
                and cached is not None
                and now_ns < self._jwks_expires_ns
            ):
                return cached
            keys = self._fetch_jwks()
            self._jwks = keys
            self._jwks_expires_ns = now_ns + _ttl_ns(self.config.jwks_ttl_s)
            return keys

    def _fetch_jwks(self) -> tuple[SigningKey, ...]:
        provider = self.discovery()
        document = self._fetch_json(
            provider.jwks_uri, error_type=OidcDiscoveryError
        )
        raw_keys = document.get("keys")
        if not isinstance(raw_keys, list):
            msg = "OIDC JWKS document has no key list"
            raise OidcDiscoveryError(msg)
        keys: list[SigningKey] = []
        for entry in raw_keys[:MAX_JWKS_KEYS]:
            key = _signing_key_from_jwk(entry)
            if key is not None:
                keys.append(key)
        if not keys:
            msg = "OIDC JWKS document has no usable signing keys"
            raise OidcDiscoveryError(msg)
        return tuple(keys)


# -- module helpers ---------------------------------------------------------


def _ttl_ns(ttl_s: float) -> int:
    bounded = min(max(float(ttl_s), 0.0), MAX_CACHE_TTL_S)
    return int(bounded * 1_000_000_000)


def _is_http_url(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parts = urlsplit(value)
    except ValueError:
        return False
    return parts.scheme in {"http", "https"} and bool(parts.netloc)


def _provider_error_code(body: bytes) -> str | None:
    """Extract only the stable provider error code, never its description."""
    try:
        document = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    if not isinstance(document, dict):
        return None
    code = document.get("error")
    if isinstance(code, str) and len(code) <= 64:
        return code
    return None


def _decode_jwt(
    token: str,
) -> tuple[dict[str, Any], dict[str, Any], bytes, bytes]:
    parts = token.split(".")
    if len(parts) != 3:
        msg = "ID Token is not a compact JWS"
        raise IdTokenInvalidError(msg, reason="malformed")
    try:
        header_document = json.loads(b64url_decode(parts[0]).decode("utf-8"))
        claims_document = json.loads(b64url_decode(parts[1]).decode("utf-8"))
        signature = b64url_decode(parts[2])
    except (ValueError, UnicodeDecodeError) as exc:
        msg = "ID Token is malformed"
        raise IdTokenInvalidError(msg, reason="malformed") from exc
    if not isinstance(header_document, dict) or not isinstance(
        claims_document, dict
    ):
        msg = "ID Token header or claims are not JSON objects"
        raise IdTokenInvalidError(msg, reason="malformed")
    signing_input = f"{parts[0]}.{parts[1]}".encode("ascii")
    return header_document, claims_document, signing_input, signature


def _b64url_int(value: object) -> int | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return int.from_bytes(b64url_decode(value), "big")
    except ValueError:
        return None


def _signing_key_from_jwk(  # ruff: ignore[too-many-return-statements]  # explicit JWK branches
    entry: object,
) -> SigningKey | None:
    if not isinstance(entry, dict):
        return None
    kty = entry.get("kty")
    kid = entry.get("kid")
    kid_value = kid if isinstance(kid, str) and kid else None
    if kty == "RSA":
        modulus = _b64url_int(entry.get("n"))
        exponent = _b64url_int(entry.get("e"))
        if modulus is None or exponent is None:
            return None
        try:
            public_key = rsa.RSAPublicNumbers(
                exponent, modulus
            ).public_key()
        except ValueError:
            return None
        return SigningKey(kid_value, "RSA", "RS256", public_key)
    if kty == "EC":
        curve = _EC_CURVES.get(str(entry.get("crv")))
        x = _b64url_int(entry.get("x"))
        y = _b64url_int(entry.get("y"))
        if curve is None or x is None or y is None:
            return None
        try:
            public_key = ec.EllipticCurvePublicNumbers(
                x, y, curve
            ).public_key()
        except ValueError:
            return None
        return SigningKey(kid_value, "EC", "ES256", public_key)
    return None


def _select_keys(
    keys: tuple[SigningKey, ...], kid: str | None
) -> tuple[SigningKey, ...]:
    if kid is None:
        return keys
    return tuple(key for key in keys if key.kid == kid)


def _verify_signature(
    keys: tuple[SigningKey, ...],
    alg: str,
    signing_input: bytes,
    signature: bytes,
) -> bool:
    for key in keys:
        if alg == "RS256" and not isinstance(key.public_key, rsa.RSAPublicKey):
            continue
        if alg == "ES256" and not isinstance(
            key.public_key, ec.EllipticCurvePublicKey
        ):
            continue
        try:  # ruff: ignore[too-many-statements-in-try-clause]  # explicit alg branches
            if alg == "RS256":
                key.public_key.verify(  # type: ignore[union-attr]
                    signature,
                    signing_input,
                    padding.PKCS1v15(),
                    hashes.SHA256(),
                )
            else:
                if len(signature) != 64:
                    continue
                r = int.from_bytes(signature[:32], "big")
                s = int.from_bytes(signature[32:], "big")
                key.public_key.verify(  # type: ignore[union-attr]
                    encode_dss_signature(r, s),
                    signing_input,
                    ec.ECDSA(hashes.SHA256()),
                )
        except InvalidSignature:
            continue
        return True
    return False


def resolve_access_role(
    claims: Mapping[str, Any],
    *,
    claim_name: str,
    role_mapping: Mapping[str, str],
) -> str:
    """Map OIDC claim values to exactly one Access Role or deny.

    The client supplies no role: only the configured claim and mapping are
    consulted. Zero matches and multiple distinct roles both deny the login.
    """
    raw = claims.get(claim_name)
    if raw is None:
        groups: list[str] = []
    elif isinstance(raw, str):
        groups = [raw]
    elif isinstance(raw, list) and all(
        isinstance(item, str) for item in raw
    ):
        groups = list(raw)
    else:
        msg = f"OIDC claim {claim_name!r} is not a string or list of strings"
        raise RoleResolutionError(_ROLE_CLAIM_INVALID, msg)

    roles = {role_mapping[group] for group in groups if group in role_mapping}
    if not roles:
        msg = "OIDC claims did not map to any configured Access Role"
        raise RoleResolutionError(_ROLE_UNMAPPED, msg)
    if len(roles) > 1:
        msg = "OIDC claims mapped to conflicting Access Roles"
        raise RoleResolutionError(_ROLE_CONFLICT, msg)
    return roles.pop()


__all__ = [
    "MAX_CACHE_TTL_S",
    "IdTokenInvalidError",
    "OidcClient",
    "OidcDiscoveryError",
    "OidcError",
    "OidcProvider",
    "OidcTokenError",
    "OidcTransportError",
    "RoleResolutionError",
    "TransportResponse",
    "pkce_challenge",
    "resolve_access_role",
]
