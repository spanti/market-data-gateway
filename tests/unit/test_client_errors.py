"""ClientError mapping for REST/control errors and URL construction."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mdgw_client import ClientError, GatewayClient, ReplayFailedError
from mdgw_client.client import derive_ws_url
from mdgw_client.errors import (
    error_from_control_response,
    error_from_rest_body,
    replay_error_from_end_of_session,
)
from mdgw_client.history import build_bars_url

BTC = "binance:BTC/USDT"


def test_client_error_exposes_the_documented_fields() -> None:
    error = ClientError(
        "invalid_range",
        "bad range",
        status=400,
        details={"limit": 10},
        request_id="req-1",
    )
    assert error.code == "invalid_range"
    assert error.message == "bad range"
    assert error.status == 400
    assert error.details == {"limit": 10}
    assert error.request_id == "req-1"
    assert str(error) == "invalid_range: bad range"


def test_rest_error_body_preserves_the_server_code_and_request_id() -> None:
    body = json.dumps(
        {
            "error": {
                "code": "range_too_large",
                "message": "too many Bars",
                "details": {"row_count": 200000},
                "request_id": "req-42",
            }
        }
    ).encode()

    error = error_from_rest_body(body, status=400)

    assert isinstance(error, ClientError)
    assert error.code == "range_too_large"
    assert error.message == "too many Bars"
    assert error.status == 400
    assert error.details == {"row_count": 200000}
    assert error.request_id == "req-42"


def test_non_json_rest_error_falls_back_to_http_error() -> None:
    error = error_from_rest_body(b"<html>oops</html>", status=502)
    assert error.code == "http_error"
    assert error.status == 502

    missing = error_from_rest_body(json.dumps({"detail": "x"}).encode(), status=500)
    assert missing.code == "http_error"


def test_control_error_response_maps_to_client_error() -> None:
    error = error_from_control_response(
        {
            "ok": False,
            "error": {
                "code": "invalid_parameter",
                "message": "seek requires the session to be paused",
            },
        }
    )
    assert error.code == "invalid_parameter"
    assert "paused" in error.message


def test_malformed_control_error_is_a_protocol_error() -> None:
    error = error_from_control_response({"ok": False, "error": "nope"})
    assert error.code == "protocol_error"


def test_replay_error_end_of_session_maps_to_replay_failed_error() -> None:
    error = replay_error_from_end_of_session(
        {
            "op": "end_of_session",
            "session_id": "rpl-1",
            "reason": "error",
            "bars_sent": 3,
            "error": {
                "code": "schema_version_mismatch",
                "message": "Dataset schema version is not supported",
            },
        }
    )
    assert isinstance(error, ReplayFailedError)
    assert isinstance(error, ClientError)
    assert error.code == "schema_version_mismatch"
    assert error.message == "Dataset schema version is not supported"
    assert error.bars_sent == 3


def test_malformed_replay_error_end_of_session_is_a_protocol_error() -> None:
    error = replay_error_from_end_of_session(
        {"op": "end_of_session", "reason": "error", "error": "nope"}
    )
    assert isinstance(error, ReplayFailedError)
    assert error.code == "protocol_error"
    assert error.bars_sent is None


def test_replay_error_eos_raises_a_sticky_failure_not_stop_iteration() -> None:
    from mdgw_client.replay import ReplaySession, _SessionChannel

    class _StubClient:
        def _release_session(self, _session) -> None:
            pass

    channel = _SessionChannel(10)
    session = ReplaySession(
        client=_StubClient(),  # type: ignore[arg-type] - close-only dependency
        session_id="rpl-sticky",
        publish_endpoint="tcp://127.0.0.1:1",
        channel=channel,
    )
    event = {
        "op": "end_of_session",
        "session_id": "rpl-sticky",
        "reason": "error",
        "bars_sent": 2,
        "error": {"code": "internal_error", "message": "replay read failed"},
    }
    channel.offer(
        ("replay.rpl-sticky.control", json.dumps(event).encode()), critical=True
    )
    try:
        with pytest.raises(ReplayFailedError) as first:
            session.next_event(timeout=1.0)
        assert first.value.code == "internal_error"
        assert first.value.message == "replay read failed"
        assert first.value.bars_sent == 2
        assert session.ended is True
        assert session.end_of_session == event

        # The failure is sticky: a repeated call never looks like normal
        # replay completion (`StopIteration`), including ``__iter__``.
        with pytest.raises(ReplayFailedError) as second:
            session.next_event(timeout=1.0)
        assert second.value.code == "internal_error"
        with pytest.raises(ReplayFailedError):
            list(session)
    finally:
        session.close()


def test_a_normal_end_of_session_still_raises_stop_iteration() -> None:
    from mdgw_client.replay import ReplaySession, _SessionChannel

    class _StubClient:
        def _release_session(self, _session) -> None:
            pass

    channel = _SessionChannel(10)
    session = ReplaySession(
        client=_StubClient(),  # type: ignore[arg-type] - close-only dependency
        session_id="rpl-normal",
        publish_endpoint="tcp://127.0.0.1:1",
        channel=channel,
    )
    event = {
        "op": "end_of_session",
        "session_id": "rpl-normal",
        "reason": "exhausted",
        "bars_sent": 0,
    }
    channel.offer(
        ("replay.rpl-normal.control", json.dumps(event).encode()), critical=True
    )
    try:
        with pytest.raises(StopIteration):
            session.next_event(timeout=1.0)
        assert session.end_of_session == event
        with pytest.raises(StopIteration):
            session.next_event(timeout=1.0)
    finally:
        session.close()


def test_bars_url_encodes_interval_instrument_and_timestamps() -> None:
    url = build_bars_url(
        "http://127.0.0.1:8080/",
        "1m",
        instrument=BTC,
        start=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end="2024-01-02T00:00:00.123456789Z",
        limit=10,
    )
    assert url.startswith("http://127.0.0.1:8080/api/v1/bars?")
    assert "interval=1m" in url
    assert "instrument=binance%3ABTC%2FUSDT" in url
    assert "start=2024-01-01T00%3A00%3A00.000000000Z" in url
    assert "end=2024-01-02T00%3A00%3A00.123456789Z" in url
    assert "limit=10" in url
    assert url.endswith("&format=arrow")


def test_bars_url_omits_optional_parameters() -> None:
    url = build_bars_url("http://host:1", "5m")
    assert url == "http://host:1/api/v1/bars?interval=5m&format=arrow"


def test_bars_url_rejects_a_negative_or_boolean_limit() -> None:
    with pytest.raises(ValueError):
        build_bars_url("http://host:1", "1m", limit=-1)
    with pytest.raises(ValueError):
        build_bars_url("http://host:1", "1m", limit=True)


def test_ws_url_is_derived_from_the_base_url() -> None:
    assert derive_ws_url("http://127.0.0.1:8080") == "ws://127.0.0.1:8080/ws"
    assert derive_ws_url("https://example.test") == "wss://example.test/ws"
    assert derive_ws_url("http://host:1/api") == "ws://host:1/api/ws"


def test_replay_is_unavailable_without_both_endpoints() -> None:
    client = GatewayClient("http://127.0.0.1:1")
    with pytest.raises(ClientError) as excinfo:
        _ = client.replay
    assert excinfo.value.code == "replay_unavailable"
    client.close()


def test_ws_unavailable_hint_names_the_installable_dependency(monkeypatch) -> None:
    import builtins

    from mdgw_client.live import _import_ws_connect

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "websockets" or name.startswith("websockets."):
            raise ImportError("websockets is not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(ClientError) as excinfo:
        _import_ws_connect()

    assert excinfo.value.code == "ws_unavailable"
    assert "websockets" in excinfo.value.message
    assert "mdgw-client[ws]" not in excinfo.value.message


def test_history_keeps_decimal_exact_in_the_arrow_table() -> None:
    # A tiny direct check that the SDK never coerces decimals on read paths.
    import pyarrow as pa

    from mdgw_client.schema import BAR_SCHEMA

    table = pa.Table.from_pylist(
        [
            {
                "schema_version": 1,
                "data_type": "bar",
                "instrument_id": BTC,
                "interval": "1m",
                "open_ts": 0,
                "close_ts": 60_000_000_000,
                "event_ts": 60_000_000_000,
                "ingest_ts": 60_000_000_000,
                "feed": "fake",
                "seq": None,
                "aggregation_source": "external",
                "open": Decimal("65000.100000000000000001"),
                "high": Decimal("1"),
                "low": Decimal("1"),
                "close": Decimal("1"),
                "volume": Decimal("1"),
                "trade_count": None,
                "revision": 0,
            }
        ],
        schema=BAR_SCHEMA,
    )
    assert table.column("open").to_pylist() == [
        Decimal("65000.100000000000000001")
    ]
