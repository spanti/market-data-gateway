# ruff: file-ignore[raise-vanilla-args,raw-string-in-exception,too-many-return-statements,too-many-branches,too-many-statements-in-try-clause,raise-within-try]
"""Length-prefixed, token-authenticated loopback control plane.

The Admin API and the Collector run in different processes. Windows builds of
pyzmq cannot bind ``ipc://``, so this module owns the transport both sides
use: a loopback TCP socket carrying **one** bounded JSON frame per request.

Wire shape (transport wrapper)::

    {"schema_version": 1, "correlation_id": "corr_…", "token": "…",
     "message": <mdgw.admin.control.ControlEnvelope>}

The server compares the token with :func:`hmac.compare_digest`, validates the
envelope with the shared allowlist and per-action Pydantic payload schema
*before* dispatching, echoes the correlation id, and answers with::

    {"schema_version": 1, "correlation_id": "corr_…", "ok": true,
     "result": {...}}
    {"schema_version": 1, "correlation_id": "corr_…", "ok": false,
     "error": {"code": "...", "message": "...", "details": {...}}}

There is no generic command, path, shell string or free-form parameter: only
registered actions with registered payload models can ever reach a handler.

Extension point for ticket 06 (Backfill jobs)
---------------------------------------------
Define the payload model here (or in the owning feature module) and add it to
:func:`collector_control_registry` with ``ControlTarget.COLLECTOR``. Then call
``ControlPlaneServer.register(action, handler)`` on the Collector side. The
server validates against the same registry before dispatch, so an action that
is not registered cannot be invoked. Feature handlers should stay off
``CollectorService.start/stop`` by registering through this seam.
"""

from __future__ import annotations

import contextlib
import json
import logging
import socket
import struct
import threading
from collections.abc import Mapping
from typing import Any, Literal
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)

from mdgw.admin.control import (
    ActionNotAllowedError,
    ControlActionRegistry,
    ControlEnvelope,
    ControlError,
    ControlMessageTooLargeError,
    ControlTarget,
    ControlTimeoutError,
    build_control_message,
    validate_control_message,
)
from mdgw.service.control_wire import (
    decode_json_object,
    encode_json,
    is_loopback_host,
    token_matches,
)

logger = logging.getLogger(__name__)

CONTROL_PLANE_PROTOCOL_VERSION = 1
ACTION_PING = "ping"
FEED_SUBSCRIPTION_SYNC_ACTION = "feed.subscription.sync"
BACKFILL_JOB_START_ACTION = "backfill.job.start"
BACKFILL_JOB_STATUS_ACTION = "backfill.job.status"
BACKFILL_JOB_CANCEL_ACTION = "backfill.job.cancel"
GAPS_SNAPSHOT_ACTION = "gaps.snapshot"

#: Largest frame accepted or produced by one request/response.
DEFAULT_MAX_MESSAGE_BYTES = 65_536
DEFAULT_REQUEST_TIMEOUT_S = 5.0
_ACCEPT_POLL_S = 0.2
_MAX_CORRELATION_LENGTH = 128
_MAX_SUBSCRIPTIONS_PER_SYNC = 5_000


class ControlTransportError(RuntimeError):
    """The peer could not be reached, timed out, or sent an unusable frame."""

    def __init__(self, reason: str, message: str | None = None) -> None:
        super().__init__(message or reason)
        self.reason = reason


class ControlRequestError(RuntimeError):
    """The server rejected a validated request; nothing was acknowledged."""

    def __init__(
        self,
        code: str,
        message: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})


# -- payload schemas ---------------------------------------------------------


class EmptyPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class FeedSubscriptionDesired(BaseModel):
    """One subscription's desired state as dispatched to the Collector."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    subscription_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"
    )
    feed: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=128)
    instrument_id: str = Field(min_length=3, max_length=256)
    data_type: Literal["bar"] = "bar"
    interval: Literal["1m", "5m", "15m", "1h", "1d"]
    enabled: bool
    deleted: bool = False
    version: int = Field(ge=1)


class FeedSubscriptionSyncPayload(BaseModel):
    """The complete desired subscription set, replacing the previous one."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    sync_version: int = Field(ge=1)
    subscriptions: list[FeedSubscriptionDesired] = Field(
        default_factory=list, max_length=_MAX_SUBSCRIPTIONS_PER_SYNC
    )


class _BackfillJobScopedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    job_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"
    )


class BackfillJobStatusPayload(_BackfillJobScopedPayload):
    """Look up one in-flight Backfill Job in the Collector's live registry."""


class BackfillJobCancelPayload(_BackfillJobScopedPayload):
    """Request cooperative cancellation of one Backfill Job."""


class BackfillJobStartPayload(_BackfillJobScopedPayload):
    """Execute one persisted Backfill Job range.

    The plan mode is validated against the target interval so the Collector
    can never be asked to fetch a native high-interval history: ``1m`` is the
    only native interval, everything higher is aggregated from ``1m``.
    """

    instrument_id: str = Field(min_length=3, max_length=256)
    interval: Literal["1m", "5m", "15m", "1h", "1d"]
    start_ns: int = Field(ge=0)
    end_ns: int = Field(gt=0)
    mode: Literal["native_interval", "one_minute_plus_aggregation"]

    @model_validator(mode="after")
    def _range_and_plan_are_consistent(self) -> BackfillJobStartPayload:
        if self.start_ns >= self.end_ns:
            msg = "Backfill Job start_ns must be before end_ns"
            raise ValueError(msg)
        expected_mode = (
            "native_interval"
            if self.interval == "1m"
            else "one_minute_plus_aggregation"
        )
        if self.mode != expected_mode:
            msg = (
                "Backfill Job plan mode does not match the target interval: "
                f"{self.mode!r} for {self.interval!r}"
            )
            raise ValueError(msg)
        return self


def collector_control_registry() -> ControlActionRegistry:
    """The allowlist shared by the Collector server and the Admin client.

    Ticket 06 extends this function (or passes its own registry into both the
    server and the client) instead of editing the transport.
    """
    registry = ControlActionRegistry()
    registry.register(
        ACTION_PING, targets=(ControlTarget.COLLECTOR,), payload=EmptyPayload
    )
    registry.register(
        FEED_SUBSCRIPTION_SYNC_ACTION,
        targets=(ControlTarget.COLLECTOR,),
        payload=FeedSubscriptionSyncPayload,
    )
    registry.register(
        BACKFILL_JOB_START_ACTION,
        targets=(ControlTarget.COLLECTOR,),
        payload=BackfillJobStartPayload,
    )
    registry.register(
        BACKFILL_JOB_STATUS_ACTION,
        targets=(ControlTarget.COLLECTOR,),
        payload=BackfillJobStatusPayload,
    )
    registry.register(
        BACKFILL_JOB_CANCEL_ACTION,
        targets=(ControlTarget.COLLECTOR,),
        payload=BackfillJobCancelPayload,
    )
    # Ticket 07: read-only projection of the Collector's process-local
    # ``gap_unrecoverable`` list. The API persists it; the Collector's Dataset
    # and HWM are never touched by this action.
    registry.register(
        GAPS_SNAPSHOT_ACTION,
        targets=(ControlTarget.COLLECTOR,),
        payload=EmptyPayload,
    )
    return registry


# -- framing -----------------------------------------------------------------


def write_frame(
    payload: Mapping[str, Any], *, max_message_bytes: int | None = None
) -> bytes:
    """Serialize one JSON frame with a 4-byte big-endian length prefix."""
    body = encode_json(payload)
    if max_message_bytes is not None and len(body) > max_message_bytes:
        msg = f"control message exceeds {max_message_bytes} bytes"
        raise ValueError(msg)
    return struct.pack("!I", len(body)) + body


def read_frame(data: bytes, *, max_message_bytes: int | None = None) -> Any:
    """Parse a complete length-prefixed frame already held in memory."""
    if len(data) < 4:
        msg = "control frame is truncated"
        raise ValueError(msg)
    length = struct.unpack("!I", data[:4])[0]
    if max_message_bytes is not None and length > max_message_bytes:
        msg = f"control message exceeds {max_message_bytes} bytes"
        raise ValueError(msg)
    if len(data) - 4 != length:
        msg = "control frame length does not match its payload"
        raise ValueError(msg)
    try:
        return json.loads(data[4:].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        msg = "control frame is not valid UTF-8 JSON"
        raise ValueError(msg) from exc


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise ConnectionError("peer closed the control connection")
        chunks.extend(chunk)
    return bytes(chunks)


def _recv_frame(sock: socket.socket, max_message_bytes: int) -> Any:
    header = _recv_exact(sock, 4)
    (length,) = struct.unpack("!I", header)
    if length <= 0 or length > max_message_bytes:
        msg = f"control message exceeds {max_message_bytes} bytes"
        raise ValueError(msg)
    return read_frame(header + _recv_exact(sock, length))


def _error_payload(
    correlation_id: str | None,
    code: str,
    message: str,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": CONTROL_PLANE_PROTOCOL_VERSION,
        "correlation_id": correlation_id,
        "ok": False,
        "error": {
            "code": code,
            "message": message,
            "details": dict(details or {}),
        },
    }


# -- server ------------------------------------------------------------------


class ControlPlaneServer:
    """Loopback TCP listener that validates and dispatches allowlisted actions."""

    def __init__(  # ruff: ignore[too-many-arguments]  # explicit transport knobs
        self,
        *,
        host: str,
        port: int,
        token: str,
        target: ControlTarget = ControlTarget.COLLECTOR,
        registry: ControlActionRegistry | None = None,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
        request_timeout_s: float = DEFAULT_REQUEST_TIMEOUT_S,
    ) -> None:
        if not is_loopback_host(host):
            msg = "the control plane must bind a loopback address"
            raise ValueError(msg)
        if not token:
            msg = "a non-empty control-plane token is required"
            raise ValueError(msg)
        if max_message_bytes <= 0:
            msg = "max_message_bytes must be positive"
            raise ValueError(msg)
        if request_timeout_s <= 0:
            msg = "request_timeout_s must be positive"
            raise ValueError(msg)
        self.host = host
        self._requested_port = int(port)
        self._token = token
        self.target = ControlTarget(target)
        self._registry = (
            collector_control_registry() if registry is None else registry
        )
        self.max_message_bytes = int(max_message_bytes)
        self.request_timeout_s = float(request_timeout_s)
        self._handlers: dict[str, Any] = {}
        self._stop = threading.Event()
        self._listener: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._port: int | None = None

    @property
    def port(self) -> int:
        if self._port is None:
            msg = "ControlPlaneServer.start() must be called first"
            raise RuntimeError(msg)
        return self._port

    @property
    def registry(self) -> ControlActionRegistry:
        return self._registry

    def allowed_actions(self) -> tuple[str, ...]:
        return tuple(
            action
            for action in self._registry.allowed_for(self.target)
            if action in self._handlers
        )

    def register(self, action: str, handler: Any) -> None:
        """Attach the handler for one *already registered* action."""
        if self._registry.resolve(action) is None:
            msg = f"control action is not registered: {action!r}"
            raise ValueError(msg)
        if action in self._handlers:
            msg = f"duplicate control handler: {action!r}"
            raise ValueError(msg)
        self._handlers[action] = handler

    def start(self) -> None:
        if self._listener is not None:
            return
        family = socket.AF_INET6 if ":" in self.host else socket.AF_INET
        listener = socket.socket(family, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind((self.host, self._requested_port))
            listener.listen(16)
            listener.settimeout(_ACCEPT_POLL_S)
        except BaseException:
            listener.close()
            raise
        self._listener = listener
        self._port = int(listener.getsockname()[1])
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._accept_loop,
            name="mdgw-control-plane",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "control plane listening",
            extra={
                "event": "control_plane.started",
                "host": self.host,
                "port": self._port,
            },
        )

    def stop(self) -> None:
        self._stop.set()
        listener = self._listener
        self._listener = None
        if listener is not None:
            with contextlib.suppress(OSError):  # close is best effort
                listener.close()
        thread = self._thread
        self._thread = None
        if thread is not None:
            thread.join(timeout=2.0)
        self._port = None

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            listener = self._listener
            if listener is None:
                return
            try:
                connection, _address = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            thread = threading.Thread(
                target=self._serve_connection,
                args=(connection,),
                name="mdgw-control-plane-request",
                daemon=True,
            )
            thread.start()

    def _serve_connection(self, connection: socket.socket) -> None:
        correlation_id: str | None = None
        try:
            connection.settimeout(self.request_timeout_s)
            try:
                header = _recv_exact(connection, 4)
            except (ConnectionError, TimeoutError, OSError):
                return
            (length,) = struct.unpack("!I", header)
            if length <= 0 or length > self.max_message_bytes:
                self._send(
                    connection,
                    _error_payload(
                        correlation_id,
                        "message_too_large",
                        "the control message exceeds the byte bound",
                    ),
                )
                return
            try:
                body = _recv_exact(connection, length)
            except (ConnectionError, TimeoutError, OSError):
                return
            response = self._handle_frame(body)
            self._send(connection, response)
        finally:
            with contextlib.suppress(OSError):  # pragma: no cover
                connection.close()

    def _send(
        self, connection: socket.socket, payload: Mapping[str, Any]
    ) -> None:
        try:
            connection.sendall(
                write_frame(
                    payload, max_message_bytes=self.max_message_bytes
                )
            )
        except (OSError, ValueError):  # pragma: no cover - peer vanished
            logger.debug("control plane response could not be sent")

    def _handle_frame(self, body: bytes) -> dict[str, Any]:
        wrapper = decode_json_object(body)
        if wrapper is None:
            return _error_payload(
                None, "invalid_message", "the control frame is not valid JSON"
            )
        correlation = wrapper.get("correlation_id")
        if not isinstance(correlation, str) or not correlation:
            correlation = None
        if wrapper.get("schema_version") != CONTROL_PLANE_PROTOCOL_VERSION:
            return _error_payload(
                correlation,
                "invalid_message",
                "unsupported control plane protocol version",
            )
        provided = wrapper.get("token")
        if not isinstance(provided, str) or not token_matches(
            self._token, provided
        ):
            return _error_payload(
                correlation, "unauthorized", "the control token is invalid"
            )
        message = wrapper.get("message")
        if not isinstance(message, dict):
            return _error_payload(
                correlation,
                "invalid_message",
                "the control message is missing",
            )
        if correlation is not None and message.get("correlation_id") != correlation:
            return _error_payload(
                correlation,
                "invalid_message",
                "the transport and message correlation ids differ",
            )
        try:
            message_target = ControlTarget(message.get("target"))
        except ValueError:
            return _error_payload(
                correlation,
                "invalid_message",
                "the control message target is invalid",
            )
        if message_target != self.target:
            return _error_payload(
                correlation,
                "target_mismatch",
                "the control message targets another service",
            )
        try:
            envelope: ControlEnvelope = validate_control_message(
                message,
                max_message_bytes=self.max_message_bytes,
                max_timeout_ms=int(self.request_timeout_s * 1000),
                registry=self._registry,
            )
        except ControlMessageTooLargeError:
            return _error_payload(
                correlation,
                "message_too_large",
                "the control message exceeds the byte bound",
            )
        except ControlTimeoutError:
            return _error_payload(
                correlation,
                "invalid_message",
                "the requested timeout exceeds the server bound",
            )
        except ActionNotAllowedError:
            return _error_payload(
                correlation,
                "unknown_action",
                "the action is not in the control allowlist",
            )
        except ControlError:
            return _error_payload(
                correlation,
                "invalid_message",
                "the control message failed schema validation",
            )
        handler = self._handlers.get(envelope.action)
        if handler is None:
            return _error_payload(
                correlation,
                "unknown_action",
                "the action is not served by this control plane",
            )
        spec = self._registry.resolve(envelope.action)
        if spec is None:  # pragma: no cover - validate already proved this
            return _error_payload(
                correlation, "unknown_action", "the action is not registered"
            )
        try:
            payload = spec.payload.model_validate(envelope.payload)
            result = handler(payload)
            if not isinstance(result, Mapping):
                raise TypeError("control handler must return a mapping")
            encoded = dict(result)
            json.dumps(encoded)
        except (ValidationError, TypeError, ValueError):
            logger.warning(
                "control handler returned an invalid result",
                extra={
                    "event": "control_plane.handler_failed",
                    "action": envelope.action,
                },
            )
            return _error_payload(
                correlation,
                "handler_failed",
                "the control handler failed",
            )
        except Exception:
            logger.exception(
                "control handler failed",
                extra={
                    "event": "control_plane.handler_failed",
                    "action": envelope.action,
                },
            )
            return _error_payload(
                correlation,
                "handler_failed",
                "the control handler failed",
            )
        return {
            "schema_version": CONTROL_PLANE_PROTOCOL_VERSION,
            "correlation_id": correlation,
            "ok": True,
            "result": encoded,
        }


# -- client ------------------------------------------------------------------


class ControlPlaneClient:
    """One-request-per-connection client for the loopback control plane."""

    def __init__(  # ruff: ignore[too-many-arguments]  # explicit transport knobs
        self,
        *,
        host: str,
        port: int,
        token: str,
        target: ControlTarget = ControlTarget.COLLECTOR,
        registry: ControlActionRegistry | None = None,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
        timeout_s: float = DEFAULT_REQUEST_TIMEOUT_S,
    ) -> None:
        if not is_loopback_host(host):
            msg = "the control plane must connect a loopback address"
            raise ValueError(msg)
        if not token:
            msg = "a non-empty control-plane token is required"
            raise ValueError(msg)
        if max_message_bytes <= 0:
            msg = "max_message_bytes must be positive"
            raise ValueError(msg)
        if timeout_s <= 0:
            msg = "timeout_s must be positive"
            raise ValueError(msg)
        self.host = host
        self.port = int(port)
        self._token = token
        self.target = ControlTarget(target)
        self._registry = (
            collector_control_registry() if registry is None else registry
        )
        self.max_message_bytes = int(max_message_bytes)
        self.timeout_s = float(timeout_s)

    def call(
        self,
        action: str,
        payload: Mapping[str, Any] | BaseModel | None = None,
        *,
        correlation_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """Send and validate one request; never treats a timeout as success."""
        correlation = (
            f"corr_{uuid4().hex}" if correlation_id is None else correlation_id
        )
        raw_payload = (
            payload.model_dump(mode="json")
            if isinstance(payload, BaseModel)
            else dict(payload or {})
        )
        effective_timeout_ms = (
            int(self.timeout_s * 1000)
            if timeout_ms is None
            else int(timeout_ms)
        )
        try:
            message = build_control_message(
                self.target,
                action,
                correlation_id=correlation,
                payload=raw_payload,
                timeout_ms=effective_timeout_ms,
                registry=self._registry,
            )
        except ControlError as exc:
            raise ControlTransportError("invalid_request", str(exc)) from exc
        wrapper = {
            "schema_version": CONTROL_PLANE_PROTOCOL_VERSION,
            "correlation_id": correlation,
            "token": self._token,
            "message": message,
        }
        wait_s = min(self.timeout_s, message["timeout_ms"] / 1000.0)
        try:
            with socket.create_connection(
                (self.host, self.port), timeout=wait_s
            ) as sock:
                sock.settimeout(wait_s)
                sock.sendall(
                    write_frame(
                        wrapper, max_message_bytes=self.max_message_bytes
                    )
                )
                response = _recv_frame(sock, self.max_message_bytes)
        except TimeoutError as exc:
            raise ControlTransportError(
                "timeout", "the control request timed out"
            ) from exc
        except ConnectionRefusedError as exc:
            raise ControlTransportError(
                "collector_unavailable",
                "the collector control plane is unreachable",
            ) from exc
        except (OSError, ConnectionError, ValueError) as exc:
            raise ControlTransportError(
                "collector_unavailable", "the control request failed"
            ) from exc
        if not isinstance(response, dict):
            raise ControlTransportError(
                "invalid_response", "the control response is not an object"
            )
        if response.get("schema_version") != CONTROL_PLANE_PROTOCOL_VERSION:
            raise ControlTransportError(
                "invalid_response", "unsupported control response version"
            )
        if response.get("correlation_id") != correlation:
            raise ControlTransportError(
                "correlation_mismatch",
                "the control response correlation id does not match",
            )
        if response.get("ok") is not True:
            error = response.get("error")
            if not isinstance(error, dict):
                raise ControlTransportError(
                    "invalid_response", "the control error is malformed"
                )
            code = error.get("code")
            message_text = error.get("message")
            details = error.get("details")
            raise ControlRequestError(
                code if isinstance(code, str) else "invalid_response",
                message_text
                if isinstance(message_text, str)
                else "the control request was rejected",
                details if isinstance(details, dict) else {},
            )
        result = response.get("result")
        if not isinstance(result, dict):
            raise ControlTransportError(
                "invalid_response", "the control result is not an object"
            )
        return result


__all__ = [
    "ACTION_PING",
    "BACKFILL_JOB_CANCEL_ACTION",
    "BACKFILL_JOB_START_ACTION",
    "BACKFILL_JOB_STATUS_ACTION",
    "CONTROL_PLANE_PROTOCOL_VERSION",
    "DEFAULT_MAX_MESSAGE_BYTES",
    "DEFAULT_REQUEST_TIMEOUT_S",
    "FEED_SUBSCRIPTION_SYNC_ACTION",
    "GAPS_SNAPSHOT_ACTION",
    "BackfillJobCancelPayload",
    "BackfillJobStartPayload",
    "BackfillJobStatusPayload",
    "ControlPlaneClient",
    "ControlPlaneServer",
    "ControlRequestError",
    "ControlTransportError",
    "EmptyPayload",
    "FeedSubscriptionDesired",
    "FeedSubscriptionSyncPayload",
    "collector_control_registry",
    "read_frame",
    "write_frame",
]
