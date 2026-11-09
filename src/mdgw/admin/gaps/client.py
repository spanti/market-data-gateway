"""Admin-side client for the Collector's ``gaps.snapshot`` control action.

The API never reads the Collector's memory directly. This module sends the
allow-listed, empty-payload request over the shared loopback control plane and
turns transport failures into an explicit error the polling runtime can log
without treating "no answer" as "no gaps".
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mdgw.admin.collector_control import (
    REASON_COLLECTOR_REJECTED,
    REASON_COLLECTOR_UNAVAILABLE,
    REASON_INVALID_RESPONSE,
    REASON_NOT_CONFIGURED,
    REASON_TIMEOUT,
    CollectorControlEndpoint,
    CollectorControlError,
)
from mdgw.service.control_plane import (
    GAPS_SNAPSHOT_ACTION,
    EmptyPayload,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mdgw.config import AdminControlConfig

REASON_REJECTED = REASON_COLLECTOR_REJECTED


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
        self._endpoint = CollectorControlEndpoint(config, environ=environ)

    @property
    def configured(self) -> bool:
        return self._endpoint.configured

    def snapshot(self) -> dict[str, Any]:
        """Fetch the Collector's current in-memory Gap snapshot."""
        try:
            result = self._endpoint.call(GAPS_SNAPSHOT_ACTION, EmptyPayload())
        except CollectorControlError as exc:
            raise GapSnapshotError(exc.reason, exc.message) from exc
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
