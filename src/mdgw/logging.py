"""Structured JSON logging for the Service Roles.

Library code only creates module loggers and passes structured fields through
``extra=``; ``configure_json_logging`` is called once by the CLI, so importing
any ``mdgw`` module never mutates the global logging configuration.

Every line is one JSON object with at least ``ts``, ``level``, ``logger``,
``service``, ``event`` and ``message``. Correlation fields such as
``request_id``, ``session_id``, ``instrument_id``, ``feed``, ``duration_ms``
and ``status`` are included when the emitting call site supplies them.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import IO

# Fields the formatter surfaces verbatim when present on a LogRecord.
_OPTIONAL_FIELDS = (
    "request_id",
    "session_id",
    "instrument_id",
    "feed",
    "duration_ms",
    "status",
    "method",
    "path",
    "topics",
    "control_endpoint",
    "publish_endpoint",
    "http",
)


class JsonFormatter(logging.Formatter):
    """Render one LogRecord as a single JSON line."""

    def __init__(self, *, service: str | None = None) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": _iso_timestamp(record.created),
            "level": record.levelname,
            "logger": record.name,
            "service": getattr(record, "service", None) or self.service or "mdgw",
            "event": getattr(record, "event", None) or "log",
            "message": record.getMessage(),
        }
        for name in _OPTIONAL_FIELDS:
            value = getattr(record, name, None)
            if value is not None:
                payload[name] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_json_logging(
    level: str | int = "INFO",
    *,
    service: str | None = None,
    stream: IO[str] | None = None,
) -> logging.Handler:
    """Install one JSON StreamHandler on the root logger and return it.

    The CLI calls this once per process. Replacing the root handlers makes the
    configuration deterministic when tests call ``main`` in-process.
    """
    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setFormatter(JsonFormatter(service=service))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(_coerce_level(level))
    return handler


def _coerce_level(level: str | int) -> int:
    if isinstance(level, int):
        return level
    return logging.getLevelNamesMapping().get(str(level).upper(), logging.INFO)


def _iso_timestamp(created: float) -> str:
    return (
        datetime.fromtimestamp(created, tz=UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


__all__ = ["JsonFormatter", "configure_json_logging"]
