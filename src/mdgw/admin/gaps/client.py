"""Admin-side client for the Collector's ``gaps.snapshot`` control action.

The API never reads the Collector's memory directly. This module sends the
allow-listed, empty-payload request over the shared loopback control plane and
turns transport failures into an explicit error the polling runtime can log
without treating "no answer" as "no gaps".
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from mdgw.service.control_plane import (
    GAPS_SNAPSHOT_ACTION,
    ControlPlaneClient,
    ControlRequestError,
    ControlTransportError,
    EmptyPayload,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mdgw.config import AdminControlConfig

REASON_NOT_CONFIGURED = "not_configured"
REASON_COLLECTOR_UNAVAILABLE = "collector_unavailable"
REASON_TIMEOUT = "timeout"
REASON_REJECTED = "collector_rejected"
REASON_INVALID_RESPONSE = "invalid_response"


class GapSnapshotError(RuntimeError):
    """The Collector did not return a usable snapshot."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


class CollectorGapClient:
    """Typed adapter over the shared control-plane transport."""

    def __init__(
        self,
        config: AdminControlConfig,
        *,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        self._config = config
        self._environ = environ

    def _token(self) -> str:
        source = self._environ if self._environ is not None else os.environ
        return str(source.get(self._config.token_env, "") or "")

    @property
    def configured(self) -> bool:
        return self._config.port > 0 and bool(self._token())

    def snapshot(self) -> dict[str, Any]:
        """Fetch the Collector's current in-memory Gap snapshot."""
        token = self._token()
        if self._config.port <= 0 or not token:
            raise GapSnapshotError(
                REASON_NOT_CONFIGURED,
                "the collector control plane is not configured",
            )
        transport = ControlPlaneClient(
            host=self._config.host,
            port=self._config.port,
            token=token,
            max_message_bytes=self._config.max_message_bytes,
            timeout_s=self._config.timeout_s,
        )
        try:
            result = transport.call(
                GAPS_SNAPSHOT_ACTION,
                EmptyPayload(),
                timeout_ms=int(self._config.timeout_s * 1000),
            )
        except ControlTransportError as exc:
            reason = (
                REASON_TIMEOUT
                if exc.reason == "timeout"
                else REASON_COLLECTOR_UNAVAILABLE
            )
            raise GapSnapshotError(reason, str(exc)) from exc
        except ControlRequestError as exc:
            raise GapSnapshotError(REASON_REJECTED, str(exc)) from exc
        if not isinstance(result.get("gaps"), list):
            raise GapSnapshotError(
                REASON_INVALID_RESPONSE,
                "the collector gap snapshot is malformed",
            )
        return result


__all__ = [
    "REASON_COLLECTOR_UNAVAILABLE",
    "REASON_INVALID_RESPONSE",
    "REASON_NOT_CONFIGURED",
    "REASON_REJECTED",
    "REASON_TIMEOUT",
    "CollectorGapClient",
    "GapSnapshotError",
]
