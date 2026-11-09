# ruff: file-ignore[implicit-namespace-package]
"""Offline control-channel envelope for future Collector/Supervisor IPC.

Ticket 01 delivers the message contract only: an allow-listed action, a
schema-validated payload, a bounded timeout and a correlation id. There is no
arbitrary command, path or parameter concatenation, and unknown actions are
rejected before any adapter can see them.
"""

from __future__ import annotations

import json

import pytest
from pydantic import BaseModel, ConfigDict

from mdgw.admin.control import (
    CONTROL_PROTOCOL_VERSION,
    DEFAULT_MAX_CONTROL_TIMEOUT_MS,
    DEFAULT_MAX_MESSAGE_BYTES,
    ActionNotAllowedError,
    ControlActionRegistry,
    ControlMessageTooLargeError,
    ControlTimeoutError,
    ControlValidationError,
    build_control_message,
    default_registry,
    validate_control_message,
)

NOW = "2026-11-09T12:00:00.000000000Z"


class EmptyPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _message(**overrides):
    message = {
        "schema_version": CONTROL_PROTOCOL_VERSION,
        "message_id": "msg_01",
        "correlation_id": "corr_01",
        "target": "collector",
        "action": "ping",
        "sent_at": NOW,
        "timeout_ms": 1000,
        "payload": {},
    }
    message.update(overrides)
    return message


def test_a_valid_ping_envelope_is_accepted() -> None:
    envelope = validate_control_message(_message())

    assert envelope.schema_version == CONTROL_PROTOCOL_VERSION
    assert envelope.target.value == "collector"
    assert envelope.action == "ping"
    assert envelope.correlation_id == "corr_01"
    assert envelope.payload == {}


def test_the_same_envelope_is_accepted_as_json_bytes() -> None:
    raw = json.dumps(_message()).encode("utf-8")

    envelope = validate_control_message(raw)

    assert envelope.action == "ping"


def test_ping_is_allowed_for_both_targets() -> None:
    assert validate_control_message(
        _message(target="supervisor")
    ).target.value == "supervisor"


@pytest.mark.parametrize(
    "overrides",
    [
        {"schema_version": None},
        {"action": "shell.exec"},
        {"correlation_id": ""},
        {"correlation_id": "not allowed"},
        {"correlation_id": "x" * 129},
        {"message_id": ""},
        {"target": "browser"},
        {"sent_at": "2026-11-09 12:00:00"},
        {"sent_at": "not-a-time"},
        {"timeout_ms": 0},
        {"timeout_ms": DEFAULT_MAX_CONTROL_TIMEOUT_MS + 1},
        {"payload": {"command": "rm -rf /"}},
        {"unexpected": True},
    ],
)
def test_invalid_envelopes_are_rejected(overrides) -> None:
    message = _message(**overrides)
    if overrides.get("schema_version", "keep") is None:
        message.pop("schema_version")

    with pytest.raises(ControlValidationError):
        validate_control_message(message)


def test_short_timeouts_can_be_bounded_lower_per_call() -> None:
    with pytest.raises(ControlTimeoutError):
        validate_control_message(_message(timeout_ms=5000), max_timeout_ms=1000)

    envelope = validate_control_message(
        _message(timeout_ms=1000), max_timeout_ms=1000
    )
    assert envelope.timeout_ms == 1000


def test_oversized_messages_are_rejected_before_parsing() -> None:
    raw = json.dumps(_message()).encode("utf-8")

    with pytest.raises(ControlMessageTooLargeError):
        validate_control_message(raw, max_message_bytes=10)


def test_malformed_json_is_a_validation_error() -> None:
    with pytest.raises(ControlValidationError, match="JSON"):
        validate_control_message(b"{not-json")


def test_non_object_json_is_rejected() -> None:
    with pytest.raises(ControlValidationError):
        validate_control_message(b"[]")


def test_actions_are_allow_listed_per_target() -> None:
    registry = ControlActionRegistry()
    registry.register("ping", targets=("collector",), payload=EmptyPayload)
    registry.register(
        "test.only_collector",
        targets=("collector",),
        payload=EmptyPayload,
    )
    registry.register(
        "test.only_supervisor",
        targets=("supervisor",),
        payload=EmptyPayload,
    )

    validate_control_message(
        _message(action="test.only_collector", target="collector"),
        registry=registry,
    )
    with pytest.raises(ActionNotAllowedError):
        validate_control_message(
            _message(action="test.only_collector", target="supervisor"),
            registry=registry,
        )
    with pytest.raises(ActionNotAllowedError):
        validate_control_message(
            _message(action="not.registered", target="collector"),
            registry=registry,
        )


def test_payload_is_validated_against_the_registered_schema() -> None:
    with pytest.raises(ControlValidationError):
        validate_control_message(_message(payload={"extra": 1}))

    registry = ControlActionRegistry()
    registry.register("ping", targets=("collector",), payload=EmptyPayload)
    with pytest.raises(ControlValidationError):
        validate_control_message(
            _message(payload={"extra": 1}), registry=registry
        )


def test_registry_rejects_duplicate_or_malformed_actions() -> None:
    registry = ControlActionRegistry()
    registry.register("ping", targets=("collector",), payload=EmptyPayload)

    with pytest.raises(ValueError, match="duplicate"):
        registry.register("ping", targets=("supervisor",), payload=EmptyPayload)
    with pytest.raises(ValueError, match="action"):
        registry.register("Bad Action", targets=("collector",), payload=EmptyPayload)


def test_default_registry_only_exposes_ping() -> None:
    assert default_registry().allowed_for("collector") == ("ping",)
    assert default_registry().allowed_for("supervisor") == ("ping",)


def test_build_control_message_returns_a_valid_envelope() -> None:
    message = build_control_message(
        "collector",
        "ping",
        correlation_id="corr_build",
        message_id="msg_build",
        timeout_ms=250,
        sent_at=NOW,
    )

    assert message["schema_version"] == CONTROL_PROTOCOL_VERSION
    assert message["correlation_id"] == "corr_build"
    assert message["timeout_ms"] == 250
    assert validate_control_message(message).message_id == "msg_build"


def test_build_control_message_rejects_unknown_actions() -> None:
    with pytest.raises(ActionNotAllowedError):
        build_control_message(
            "collector", "os.execute", correlation_id="corr", sent_at=NOW
        )


def test_defaults_are_bounded() -> None:
    assert DEFAULT_MAX_CONTROL_TIMEOUT_MS == 30_000
    assert DEFAULT_MAX_MESSAGE_BYTES == 65_536
