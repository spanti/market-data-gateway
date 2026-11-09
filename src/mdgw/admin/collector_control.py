"""Shared endpoint helper for the Admin -> Collector control plane.

Three Admin features (Feed Subscriptions, Backfill Jobs and the Gap snapshot
poller) speak the same loopback control-plane transport with the same token,
timeout and failure vocabulary. This module owns that lookup/construction once;
each feature keeps its own typed client and maps the shared errors onto its
own public error type.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from mdgw.service.control_plane import (
    ControlPlaneClient,
    ControlRequestError,
    ControlTransportError,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pydantic import BaseModel

    from mdgw.config import AdminControlConfig

REASON_NOT_CONFIGURED = "not_configured"
REASON_COLLECTOR_UNAVAILABLE = "collector_unavailable"
REASON_TIMEOUT = "timeout"
REASON_NOT_APPLIED = "not_applied"
REASON_COLLECTOR_REJECTED = "collector_rejected"
REASON_INVALID_RESPONSE = "invalid_response"


class CollectorControlError(RuntimeError):
    """The Collector control exchange failed, with a stable reason."""

    def __init__(
        self, reason: str, message: str, *, code: str | None = None
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.code = code or reason


class CollectorControlEndpoint:
    """Token lookup and ``ControlPlaneClient`` construction for one config."""

    def __init__(
        self,
        config: AdminControlConfig,
        *,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        self._config = config
        self._environ = environ

    def token(self) -> str:
        source = self._environ if self._environ is not None else os.environ
        return str(source.get(self._config.token_env, "") or "")

    @property
    def configured(self) -> bool:
        return self._config.collector_port > 0 and bool(self.token())

    def transport(self) -> ControlPlaneClient:
        token = self.token()
        if self._config.collector_port <= 0 or not token:
            raise CollectorControlError(
                REASON_NOT_CONFIGURED,
                "the collector control plane is not configured",
            )
        return ControlPlaneClient(
            host=self._config.collector_host,
            port=self._config.collector_port,
            token=token,
            max_message_bytes=self._config.max_message_bytes,
            timeout_s=self._config.timeout_s,
        )

    def call(
        self,
        action: str,
        payload: Mapping[str, Any] | BaseModel | None = None,
        *,
        correlation_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """Send one validated request, raising ``CollectorControlError``."""
        try:
            return self.transport().call(
                action,
                payload,
                correlation_id=correlation_id,
                timeout_ms=(
                    int(self._config.timeout_s * 1000)
                    if timeout_ms is None
                    else timeout_ms
                ),
            )
        except ControlTransportError as exc:
            reason = (
                REASON_TIMEOUT
                if exc.reason == "timeout"
                else REASON_COLLECTOR_UNAVAILABLE
            )
            raise CollectorControlError(reason, str(exc)) from exc
        except ControlRequestError as exc:
            raise CollectorControlError(
                REASON_COLLECTOR_REJECTED,
                "the collector rejected the control request",
                code=exc.code,
            ) from exc


__all__ = [
    "REASON_COLLECTOR_REJECTED",
    "REASON_COLLECTOR_UNAVAILABLE",
    "REASON_INVALID_RESPONSE",
    "REASON_NOT_APPLIED",
    "REASON_NOT_CONFIGURED",
    "REASON_TIMEOUT",
    "CollectorControlEndpoint",
    "CollectorControlError",
]
