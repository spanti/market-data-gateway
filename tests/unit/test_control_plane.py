# ruff: file-ignore[implicit-namespace-package,hardcoded-password-string,hardcoded-password-func-arg]
"""Loopback TCP control plane: bounded framing, auth, allowlist, timeouts.

The transport is the only way an Admin write can reach the Collector. These
tests exercise it over a real loopback socket (Windows cannot bind ``ipc://``):
a wrong token, an oversized or malformed frame, an action outside the
allowlist, a target mismatch and a slow handler all fail without dispatching
the handler.
"""

from __future__ import annotations

import json
import socket
import struct
import time

import pytest

from mdgw.admin.control import ControlTarget
from mdgw.service.control_plane import (
    API_RATE_LIMIT_ACTION,
    BACKFILL_JOB_CANCEL_ACTION,
    BACKFILL_JOB_START_ACTION,
    BACKFILL_JOB_STATUS_ACTION,
    FEED_SUBSCRIPTION_SYNC_ACTION,
    WS_SLOW_CONSUMER_ACTION,
    BackfillJobCancelPayload,
    BackfillJobStartPayload,
    BackfillJobStatusPayload,
    ControlPlaneClient,
    ControlPlaneServer,
    ControlRequestError,
    ControlTransportError,
    FeedSubscriptionDesired,
    FeedSubscriptionSyncPayload,
    collector_control_registry,
    read_frame,
    write_frame,
)

TOKEN = "test-control-token"


def _server(*, handler=None):
    registry = collector_control_registry()
    server = ControlPlaneServer(
        host="127.0.0.1",
        port=0,
        token=TOKEN,
        target=ControlTarget.COLLECTOR,
        registry=registry,
        max_message_bytes=4096,
        request_timeout_s=2.0,
    )
    received: list[object] = []

    def ping(payload):
        received.append(payload)
        return {"pong": True}

    server.register("ping", ping)
    if handler is not None:
        server.register(FEED_SUBSCRIPTION_SYNC_ACTION, handler)
    server.start()
    return server, received


def _client(server, **overrides):
    options = {
        "host": "127.0.0.1",
        "port": server.port,
        "token": TOKEN,
        "registry": collector_control_registry(),
        "max_message_bytes": 4096,
        "timeout_s": 2.0,
    }
    options.update(overrides)
    return ControlPlaneClient(**options)


def _raw_roundtrip(port: int, payload: bytes, *, timeout: float = 2.0) -> dict:
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.sendall(struct.pack("!I", len(payload)) + payload)
        length = struct.unpack("!I", _recv_exact(sock, 4))[0]
        return json.loads(_recv_exact(sock, length))


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            msg = "connection closed early"
            raise AssertionError(msg)
        chunks.extend(chunk)
    return bytes(chunks)


def test_the_collector_allowlist_contains_the_ticket_04_and_06_actions() -> None:
    registry = collector_control_registry()

    assert set(registry.allowed_for("collector")) == {
        "ping",
        FEED_SUBSCRIPTION_SYNC_ACTION,
        API_RATE_LIMIT_ACTION,
        WS_SLOW_CONSUMER_ACTION,
        BACKFILL_JOB_START_ACTION,
        BACKFILL_JOB_STATUS_ACTION,
        BACKFILL_JOB_CANCEL_ACTION,
    }
    assert registry.allowed_for("supervisor") == ()


def test_backfill_start_payload_requires_a_consistent_plan() -> None:
    payload = BackfillJobStartPayload(
        job_id="bf_0123456789abcdef0123456789abcdef",
        instrument_id="binance:BTC/USDT",
        interval="1h",
        start_ns=1_700_000_000_000_000_000,
        end_ns=1_700_003_600_000_000_000,
        mode="one_minute_plus_aggregation",
    )
    assert payload.model_dump(mode="json")["interval"] == "1h"

    with pytest.raises(ValueError, match="start_ns must be before end_ns"):
        BackfillJobStartPayload(
            job_id="bf_x",
            instrument_id="binance:BTC/USDT",
            interval="1h",
            start_ns=1_700_003_600_000_000_000,
            end_ns=1_700_000_000_000_000_000,
            mode="one_minute_plus_aggregation",
        )
    with pytest.raises(ValueError, match="plan mode does not match"):
        BackfillJobStartPayload(
            job_id="bf_x",
            instrument_id="binance:BTC/USDT",
            interval="1h",
            start_ns=1_700_000_000_000_000_000,
            end_ns=1_700_003_600_000_000_000,
            mode="native_interval",
        )
    with pytest.raises(ValueError, match="plan mode does not match"):
        BackfillJobStartPayload(
            job_id="bf_x",
            instrument_id="binance:BTC/USDT",
            interval="1m",
            start_ns=1_700_000_000_000_000_000,
            end_ns=1_700_003_600_000_000_000,
            mode="one_minute_plus_aggregation",
        )


def test_backfill_status_and_cancel_payloads_are_tightly_scoped() -> None:
    assert BackfillJobStatusPayload(job_id="bf_x").job_id == "bf_x"
    assert BackfillJobCancelPayload(job_id="bf_x").job_id == "bf_x"
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        BackfillJobStatusPayload(job_id="bf_x", extra=True)  # type: ignore[call-arg]


def test_a_backfill_action_is_dispatched_only_after_schema_validation() -> None:
    registry = collector_control_registry()
    server = ControlPlaneServer(
        host="127.0.0.1",
        port=0,
        token=TOKEN,
        registry=registry,
        max_message_bytes=4096,
        request_timeout_s=2.0,
    )
    received: list[object] = []
    server.register("ping", lambda _payload: {"pong": True})
    server.register(
        BACKFILL_JOB_START_ACTION,
        lambda payload: received.append(payload)
        or {"job_id": payload.job_id, "state": "queued", "accepted": True},
    )
    server.start()
    client = ControlPlaneClient(
        host="127.0.0.1",
        port=server.port,
        token=TOKEN,
        registry=registry,
        max_message_bytes=4096,
        timeout_s=2.0,
    )
    try:
        result = client.call(
            BACKFILL_JOB_START_ACTION,
            {
                "job_id": "bf_x",
                "instrument_id": "binance:BTC/USDT",
                "interval": "1m",
                "start_ns": 1_700_000_000_000_000_000,
                "end_ns": 1_700_003_600_000_000_000,
                "mode": "native_interval",
            },
            correlation_id="corr_backfill",
        )
        assert result["accepted"] is True
        assert len(received) == 1

        with pytest.raises(ControlTransportError):
            client.call(
                BACKFILL_JOB_START_ACTION,
                {
                    "job_id": "bf_x",
                    "instrument_id": "binance:BTC/USDT",
                    "interval": "1m",
                    "start_ns": 1,
                    "end_ns": 2,
                    "mode": "native_interval",
                    "unexpected": True,
                },
            )
    finally:
        server.stop()


def test_a_valid_request_is_dispatched_and_acknowledged() -> None:
    server, received = _server()
    try:
        result = _client(server).call(
            "ping", {}, correlation_id="corr_ok"
        )
    finally:
        server.stop()

    assert result == {"pong": True}
    assert len(received) == 1
    assert received[0].model_dump() == {}


def test_feed_sync_payload_round_trips_through_the_transport() -> None:
    def sync(payload):
        return {
            "subscriptions": [
                {"subscription_id": item.subscription_id, "state": "applied"}
                for item in payload.subscriptions
            ]
        }

    server, _ = _server(handler=sync)
    payload = FeedSubscriptionSyncPayload(
        sync_version=3,
        subscriptions=[
            FeedSubscriptionDesired(
                subscription_id="fsub_1",
                feed="fake",
                venue="binance",
                instrument_id="binance:BTC/USDT",
                interval="1m",
                enabled=True,
                deleted=False,
                version=3,
            )
        ],
    )
    try:
        result = _client(server).call(
            FEED_SUBSCRIPTION_SYNC_ACTION,
            payload,
            correlation_id="corr_sync",
        )
    finally:
        server.stop()

    assert result["subscriptions"][0]["subscription_id"] == "fsub_1"
    assert result["subscriptions"][0]["state"] == "applied"


def test_a_wrong_token_is_rejected_before_dispatch() -> None:
    server, received = _server()
    try:
        with pytest.raises(ControlRequestError) as error:
            _client(server, token="wrong").call("ping", {})
    finally:
        server.stop()

    assert error.value.code == "unauthorized"
    assert received == []


def test_an_action_outside_the_allowlist_is_rejected() -> None:
    server, received = _server()
    try:
        response = _raw_roundtrip(
            server.port,
            json.dumps(
                {
                    "schema_version": 1,
                    "correlation_id": "corr_bad",
                    "token": TOKEN,
                    "message": {
                        "schema_version": 1,
                        "message_id": "msg_bad",
                        "correlation_id": "corr_bad",
                        "target": "collector",
                        "action": "shell.exec",
                        "sent_at": "2026-11-09T12:00:00.000000000Z",
                        "timeout_ms": 500,
                        "payload": {},
                    },
                }
            ).encode("utf-8"),
        )
    finally:
        server.stop()

    assert response["ok"] is False
    assert response["error"]["code"] == "unknown_action"
    assert received == []


def test_a_target_mismatch_is_rejected_before_dispatch() -> None:
    server, received = _server()
    try:
        response = _raw_roundtrip(
            server.port,
            json.dumps(
                {
                    "schema_version": 1,
                    "correlation_id": "corr_target",
                    "token": TOKEN,
                    "message": {
                        "schema_version": 1,
                        "message_id": "msg_target",
                        "correlation_id": "corr_target",
                        "target": "supervisor",
                        "action": "ping",
                        "sent_at": "2026-11-09T12:00:00.000000000Z",
                        "timeout_ms": 500,
                        "payload": {},
                    },
                }
            ).encode("utf-8"),
        )
    finally:
        server.stop()

    assert response["ok"] is False
    assert response["error"]["code"] == "target_mismatch"
    assert received == []


def test_an_oversized_frame_is_rejected_without_reading_it() -> None:
    server, received = _server()
    try:
        oversized = b"{" + b" " * 8192
        with socket.create_connection(
            ("127.0.0.1", server.port), timeout=2.0
        ) as sock:
            sock.sendall(struct.pack("!I", len(oversized)))
            length = struct.unpack("!I", _recv_exact(sock, 4))[0]
            response = json.loads(_recv_exact(sock, length))
    finally:
        server.stop()

    assert response["ok"] is False
    assert response["error"]["code"] == "message_too_large"
    assert received == []


def test_malformed_json_is_rejected() -> None:
    server, received = _server()
    try:
        response = _raw_roundtrip(server.port, b"not-json")
    finally:
        server.stop()

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_message"
    assert received == []


def test_an_invalid_payload_schema_is_rejected_before_the_handler() -> None:
    server, _received = _server(handler=lambda _payload: {"unused": True})
    try:
        response = _raw_roundtrip(
            server.port,
            json.dumps(
                {
                    "schema_version": 1,
                    "correlation_id": "corr_schema",
                    "token": TOKEN,
                    "message": {
                        "schema_version": 1,
                        "message_id": "msg_schema",
                        "correlation_id": "corr_schema",
                        "target": "collector",
                        "action": FEED_SUBSCRIPTION_SYNC_ACTION,
                        "sent_at": "2026-11-09T12:00:00.000000000Z",
                        "timeout_ms": 500,
                        "payload": {"sync_version": 1, "unexpected": True},
                    },
                }
            ).encode("utf-8"),
        )
    finally:
        server.stop()

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_message"


def test_a_client_timeout_does_not_turn_into_a_success() -> None:
    server, _ = _server()

    def slow(_payload):
        time.sleep(0.5)
        return {"late": True}

    server.register(FEED_SUBSCRIPTION_SYNC_ACTION, slow)
    client = _client(server, timeout_s=0.1)
    try:
        with pytest.raises(ControlTransportError) as error:
            client.call(
                FEED_SUBSCRIPTION_SYNC_ACTION,
                FeedSubscriptionSyncPayload(sync_version=1, subscriptions=[]),
                timeout_ms=100,
            )
    finally:
        server.stop()

    assert error.value.reason == "timeout"


def test_a_handler_error_is_reported_without_a_fake_ack() -> None:
    def failing(_payload):
        msg = "boom"
        raise RuntimeError(msg)

    server, _ = _server(handler=failing)
    try:
        with pytest.raises(ControlRequestError) as error:
            _client(server).call(
                FEED_SUBSCRIPTION_SYNC_ACTION,
                FeedSubscriptionSyncPayload(sync_version=1, subscriptions=[]),
            )
    finally:
        server.stop()

    assert error.value.code == "handler_failed"


def test_frames_are_length_prefixed_and_bounded() -> None:
    payload = {"hello": "world"}
    frame = write_frame(payload)
    assert frame[:4] == struct.pack("!I", len(frame) - 4)
    assert read_frame(frame) == payload

    with pytest.raises(ValueError, match="exceeds"):
        read_frame(b"\x00\x00\x10\x00{}", max_message_bytes=16)
