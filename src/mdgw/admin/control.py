"""Allow-listed, schema-validated control messages for future IPC.

Later tickets let the API ask the Collector or the Supervisor to perform a
small, fixed set of actions. This module is the shared message contract: a
versioned envelope, a correlation id, a bounded timeout, an allow-listed
action name and a per-action Pydantic payload schema. Unknown actions, wrong
targets, extra payload fields and oversized messages are rejected before any
IPC adapter sees them.

There is deliberately no generic command, path or shell string in the
envelope. Actual IPC adapters (loopback TCP on Windows) arrive in later
tickets and must only transport these validated messages.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
)

from mdgw.core.time import datetime_to_ns, iso_to_ns, ns_to_iso, utc_now

CONTROL_PROTOCOL_VERSION = 1
DEFAULT_MAX_CONTROL_TIMEOUT_MS = 30_000
DEFAULT_MAX_MESSAGE_BYTES = 65_536
DEFAULT_CONTROL_TIMEOUT_MS = 5_000

_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
_ACTION_PATTERN = r"^[a-z][a-z0-9_.]{0,63}$"
_TIMESTAMP_PATTERN = (
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$"
)
_ACTION_RE = re.compile(_ACTION_PATTERN)
_TIMESTAMP_RE = re.compile(_TIMESTAMP_PATTERN)


class ControlError(ValueError):
    """Base class for rejected control messages."""


class ControlValidationError(ControlError):
    """The envelope or payload failed schema validation."""

    def __init__(
        self,
        message: str,
        *,
        errors: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(message)
        self.errors = list(errors or [])


class ControlMessageTooLargeError(ControlError):
    """The raw message exceeds the configured byte bound."""


class ActionNotAllowedError(ControlValidationError):
    """The action is unknown or not allowed for the target service."""


class ControlTimeoutError(ControlValidationError):
    """The requested timeout exceeds the caller's bound."""


class ControlTarget(StrEnum):
    """The only two services reachable through the control channel."""

    COLLECTOR = "collector"
    SUPERVISOR = "supervisor"


class ControlEnvelope(BaseModel):
    """One bounded control request; extra fields are rejected."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1]
    message_id: str = Field(
        min_length=1, max_length=128, pattern=_ID_PATTERN
    )
    correlation_id: str = Field(
        min_length=1, max_length=128, pattern=_ID_PATTERN
    )
    target: ControlTarget
    action: str = Field(
        min_length=1, max_length=64, pattern=_ACTION_PATTERN
    )
    sent_at: str = Field(pattern=_TIMESTAMP_PATTERN)
    timeout_ms: int = Field(
        gt=0, le=DEFAULT_MAX_CONTROL_TIMEOUT_MS
    )
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("sent_at")
    @classmethod
    def _valid_timestamp(cls, value: str) -> str:
        if _TIMESTAMP_RE.match(value) is None:
            msg = "sent_at must be an RFC3339 UTC timestamp with a trailing 'Z'"
            raise ValueError(msg)
        try:
            iso_to_ns(value)
        except ValueError as exc:
            msg = f"sent_at is not a valid timestamp: {value!r}"
            raise ValueError(msg) from exc
        return value


@dataclass(frozen=True)
class ControlActionSpec:
    """One allow-listed action and the payload schema it accepts."""

    name: str
    targets: frozenset[ControlTarget]
    payload: type[BaseModel]


class ControlActionRegistry:
    """Per-target action allowlist; empty by construction unless registered."""

    def __init__(self) -> None:
        self._actions: dict[str, ControlActionSpec] = {}

    def register(
        self,
        name: str,
        *,
        targets: tuple[str | ControlTarget, ...],
        payload: type[BaseModel],
    ) -> None:
        if _ACTION_RE.match(name) is None:
            msg = f"invalid control action name: {name!r}"
            raise ValueError(msg)
        if name in self._actions:
            msg = f"duplicate control action: {name!r}"
            raise ValueError(msg)
        target_set = frozenset(ControlTarget(target) for target in targets)
        if not target_set:
            msg = "a control action must allow at least one target"
            raise ValueError(msg)
        if not (isinstance(payload, type) and issubclass(payload, BaseModel)):
            msg = "payload must be a Pydantic BaseModel subclass"
            raise TypeError(msg)
        self._actions[name] = ControlActionSpec(
            name=name, targets=target_set, payload=payload
        )

    def resolve(self, name: str) -> ControlActionSpec | None:
        return self._actions.get(name)

    def allowed_for(self, target: str | ControlTarget) -> tuple[str, ...]:
        resolved = ControlTarget(target)
        return tuple(
            sorted(
                name
                for name, spec in self._actions.items()
                if resolved in spec.targets
            )
        )


class _EmptyPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


_DEFAULT_REGISTRY = ControlActionRegistry()
_DEFAULT_REGISTRY.register(
    "ping",
    targets=(ControlTarget.COLLECTOR, ControlTarget.SUPERVISOR),
    payload=_EmptyPayload,
)


def default_registry() -> ControlActionRegistry:
    """The process-wide action allowlist; later tickets register more here."""
    return _DEFAULT_REGISTRY


def validate_control_message(
    raw: bytes | bytearray | str | Mapping[str, Any],
    *,
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    max_timeout_ms: int = DEFAULT_MAX_CONTROL_TIMEOUT_MS,
    registry: ControlActionRegistry | None = None,
) -> ControlEnvelope:
    """Validate one control message before it reaches an IPC adapter."""
    resolved_registry = default_registry() if registry is None else registry
    data = _decode_message(raw, max_message_bytes=max_message_bytes)
    try:
        envelope = ControlEnvelope.model_validate(data)
    except ValidationError as exc:
        msg = "control message failed schema validation"
        raise ControlValidationError(
            msg,
            errors=exc.errors(),
        ) from exc

    spec = resolved_registry.resolve(envelope.action)
    if spec is None or envelope.target not in spec.targets:
        msg = (
            f"control action {envelope.action!r} is not allowed for "
            f"target {envelope.target.value!r}"
        )
        raise ActionNotAllowedError(msg)

    if envelope.timeout_ms > max_timeout_ms:
        msg = (
            f"control timeout {envelope.timeout_ms}ms exceeds the bound "
            f"of {max_timeout_ms}ms"
        )
        raise ControlTimeoutError(msg)

    try:
        spec.payload.model_validate(envelope.payload)
    except ValidationError as exc:
        msg = (
            f"payload for control action {envelope.action!r} is invalid"
        )
        raise ControlValidationError(
            msg,
            errors=exc.errors(),
        ) from exc
    return envelope


def build_control_message(  # ruff: ignore[too-many-arguments]  # explicit message contract
    target: str | ControlTarget,
    action: str,
    *,
    correlation_id: str,
    payload: Mapping[str, Any] | None = None,
    timeout_ms: int = DEFAULT_CONTROL_TIMEOUT_MS,
    message_id: str | None = None,
    sent_at: str | None = None,
    registry: ControlActionRegistry | None = None,
) -> dict[str, Any]:
    """Build and validate one control message as a JSON-ready dict."""
    message = {
        "schema_version": CONTROL_PROTOCOL_VERSION,
        "message_id": (
            f"msg_{uuid4().hex}" if message_id is None else message_id
        ),
        "correlation_id": correlation_id,
        "target": ControlTarget(target).value,
        "action": action,
        "sent_at": (
            ns_to_iso(datetime_to_ns(utc_now()))
            if sent_at is None
            else sent_at
        ),
        "timeout_ms": timeout_ms,
        "payload": dict(payload or {}),
    }
    envelope = validate_control_message(message, registry=registry)
    return envelope.model_dump(mode="json")


def _decode_message(
    raw: bytes | bytearray | str | Mapping[str, Any],
    *,
    max_message_bytes: int,
) -> Any:
    if max_message_bytes <= 0:
        msg = "max_message_bytes must be positive"
        raise ValueError(msg)
    if isinstance(raw, (bytes, bytearray)):
        if len(raw) > max_message_bytes:
            msg = f"control message exceeds {max_message_bytes} bytes"
            raise ControlMessageTooLargeError(msg)
        try:
            text = bytes(raw).decode("utf-8")
        except UnicodeDecodeError as exc:
            msg = "control message is not valid UTF-8"
            raise ControlValidationError(msg) from exc
        return _load_json(text)
    if isinstance(raw, str):
        if len(raw.encode("utf-8")) > max_message_bytes:
            msg = f"control message exceeds {max_message_bytes} bytes"
            raise ControlMessageTooLargeError(msg)
        return _load_json(raw)
    if isinstance(raw, Mapping):
        try:
            serialized = json.dumps(
                dict(raw), separators=(",", ":"), ensure_ascii=False
            )
        except (TypeError, ValueError) as exc:
            msg = "control message is not JSON serializable"
            raise ControlValidationError(msg) from exc
        if len(serialized.encode("utf-8")) > max_message_bytes:
            msg = f"control message exceeds {max_message_bytes} bytes"
            raise ControlMessageTooLargeError(msg)
        return dict(raw)
    msg = f"unsupported control message type: {type(raw).__name__}"
    raise ControlValidationError(msg)


def _load_json(text: str) -> Any:
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        msg = f"control message is not valid JSON: {exc.msg}"
        raise ControlValidationError(msg) from exc
    if not isinstance(data, dict):
        msg = "control message must be a JSON object"
        raise ControlValidationError(msg)
    return data


__all__ = [
    "CONTROL_PROTOCOL_VERSION",
    "DEFAULT_CONTROL_TIMEOUT_MS",
    "DEFAULT_MAX_CONTROL_TIMEOUT_MS",
    "DEFAULT_MAX_MESSAGE_BYTES",
    "ActionNotAllowedError",
    "ControlActionRegistry",
    "ControlActionSpec",
    "ControlEnvelope",
    "ControlError",
    "ControlMessageTooLargeError",
    "ControlTarget",
    "ControlTimeoutError",
    "ControlValidationError",
    "build_control_message",
    "default_registry",
    "validate_control_message",
]
