from __future__ import annotations

import io
import json
import logging

from mdgw.logging import JsonFormatter, configure_json_logging


def test_json_formatter_emits_required_and_correlation_fields() -> None:
    record = logging.LogRecord(
        "mdgw.api",
        logging.INFO,
        __file__,
        1,
        "request %s",
        ("done",),
        None,
    )
    record.event = "api.http_request"
    record.request_id = "req-1"
    record.session_id = "sess-1"
    record.instrument_id = "binance:BTC/USDT"
    record.feed = "fake"
    record.duration_ms = 12.5
    record.status = 200

    payload = json.loads(JsonFormatter(service="api").format(record))

    assert payload["level"] == "INFO"
    assert payload["logger"] == "mdgw.api"
    assert payload["service"] == "api"
    assert payload["event"] == "api.http_request"
    assert payload["message"] == "request done"
    assert payload["request_id"] == "req-1"
    assert payload["session_id"] == "sess-1"
    assert payload["instrument_id"] == "binance:BTC/USDT"
    assert payload["feed"] == "fake"
    assert payload["duration_ms"] == 12.5
    assert payload["status"] == 200
    assert payload["ts"].endswith("Z")


def test_configure_json_logging_writes_parseable_lines() -> None:
    root = logging.getLogger()
    previous_handlers = root.handlers[:]
    previous_level = root.level
    stream = io.StringIO()
    try:
        configure_json_logging("INFO", service="collector", stream=stream)
        logging.getLogger("mdgw.test").info(
            "collector started",
            extra={"event": "service.started", "feed": "fake"},
        )
        lines = stream.getvalue().splitlines()
    finally:
        root.handlers[:] = previous_handlers
        root.setLevel(previous_level)

    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["service"] == "collector"
    assert payload["event"] == "service.started"
    assert payload["feed"] == "fake"
    assert payload["message"] == "collector started"


def test_explicit_service_field_overrides_the_default() -> None:
    record = logging.LogRecord(
        "mdgw.test", logging.WARNING, __file__, 1, "child", None, None
    )
    record.service = "api"

    payload = json.loads(JsonFormatter(service="supervisor").format(record))

    assert payload["service"] == "api"


def test_optional_endpoint_and_request_fields_are_emitted() -> None:
    record = logging.LogRecord(
        "mdgw.replay", logging.INFO, __file__, 1, "started", None, None
    )
    record.event = "replay.started"
    record.control_endpoint = "tcp://127.0.0.1:5557"
    record.publish_endpoint = "tcp://127.0.0.1:5558"
    record.method = "GET"
    record.path = "/api/v1/bars"
    record.topics = ["market.bar.1m.binance:BTC/USDT"]
    record.http = "http://127.0.0.1:8080"

    payload = json.loads(JsonFormatter(service="replay").format(record))

    assert payload["control_endpoint"] == "tcp://127.0.0.1:5557"
    assert payload["publish_endpoint"] == "tcp://127.0.0.1:5558"
    assert payload["method"] == "GET"
    assert payload["path"] == "/api/v1/bars"
    assert payload["topics"] == ["market.bar.1m.binance:BTC/USDT"]
    assert payload["http"] == "http://127.0.0.1:8080"
