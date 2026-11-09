"""Admin-side client for Collector Backfill Job control.

The API never executes a job. It persists the job, then sends the allow-listed
``backfill.job.start`` / ``backfill.job.status`` / ``backfill.job.cancel``
messages over the loopback control plane. This module validates the
Collector's answer and turns transport failures into explicit dispatch errors;
it never invents an acknowledgement or a running state.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from mdgw.service.control_plane import (
    BACKFILL_JOB_CANCEL_ACTION,
    BACKFILL_JOB_START_ACTION,
    BACKFILL_JOB_STATUS_ACTION,
    BackfillJobCancelPayload,
    BackfillJobStartPayload,
    BackfillJobStatusPayload,
    ControlPlaneClient,
    ControlRequestError,
    ControlTransportError,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mdgw.config import AdminControlConfig

REASON_NOT_CONFIGURED = "not_configured"
REASON_COLLECTOR_UNAVAILABLE = "collector_unavailable"
REASON_TIMEOUT = "timeout"
REASON_REJECTED = "collector_rejected"
REASON_INVALID_RESPONSE = "invalid_response"

COLLECTOR_STATES = frozenset(
    {
        "unknown",
        "queued",
        "running",
        "succeeded",
        "partially_succeeded",
        "failed",
        "cancelling",
        "cancelled",
    }
)


class BackfillDispatchError(RuntimeError):
    """The Collector did not confirm the requested action."""

    def __init__(self, reason: str, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.code = code or reason


class CollectorBackfillClient:
    """Thin typed adapter over the shared control-plane transport."""

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

    def _transport(self) -> ControlPlaneClient:
        token = self._token()
        if self._config.port <= 0 or not token:
            raise BackfillDispatchError(
                REASON_NOT_CONFIGURED,
                "the collector control plane is not configured",
            )
        return ControlPlaneClient(
            host=self._config.host,
            port=self._config.port,
            token=token,
            max_message_bytes=self._config.max_message_bytes,
            timeout_s=self._config.timeout_s,
        )

    def start_job(  # ruff: ignore[too-many-arguments] - explicit job identity
        self,
        *,
        job_id: str,
        instrument_id: str,
        interval: str,
        start_ns: int,
        end_ns: int,
        mode: str,
        correlation_id: str,
    ) -> dict[str, Any]:
        payload = BackfillJobStartPayload(
            job_id=job_id,
            instrument_id=instrument_id,
            interval=interval,
            start_ns=start_ns,
            end_ns=end_ns,
            mode=mode,
        )
        return self._validated_call(
            BACKFILL_JOB_START_ACTION,
            payload,
            correlation_id=correlation_id,
            expect_accept=True,
        )

    def job_status(self, job_id: str, *, correlation_id: str) -> dict[str, Any]:
        return self._validated_call(
            BACKFILL_JOB_STATUS_ACTION,
            BackfillJobStatusPayload(job_id=job_id),
            correlation_id=correlation_id,
        )

    def cancel_job(self, job_id: str, *, correlation_id: str) -> dict[str, Any]:
        return self._validated_call(
            BACKFILL_JOB_CANCEL_ACTION,
            BackfillJobCancelPayload(job_id=job_id),
            correlation_id=correlation_id,
        )

    def _validated_call(
        self,
        action: str,
        payload: Any,
        *,
        correlation_id: str,
        expect_accept: bool = False,
    ) -> dict[str, Any]:
        try:
            result = self._transport().call(
                action,
                payload,
                correlation_id=correlation_id,
                timeout_ms=int(self._config.timeout_s * 1000),
            )
        except ControlTransportError as exc:
            reason = (
                REASON_TIMEOUT
                if exc.reason == "timeout"
                else REASON_COLLECTOR_UNAVAILABLE
            )
            raise BackfillDispatchError(reason, str(exc)) from exc
        except ControlRequestError as exc:
            raise BackfillDispatchError(
                REASON_REJECTED,
                "the collector rejected the backfill request",
                code=exc.code,
            ) from exc
        state = result.get("state")
        job_id = result.get("job_id")
        if not isinstance(job_id, str) or state not in COLLECTOR_STATES:
            raise BackfillDispatchError(
                REASON_INVALID_RESPONSE,
                "the collector response is malformed",
            )
        if expect_accept:
            accepted = result.get("accepted")
            if accepted is False:
                reason = result.get("reason")
                raise BackfillDispatchError(
                    str(reason) if isinstance(reason, str) else REASON_REJECTED,
                    "the collector did not accept the backfill job",
                    code=str(reason)
                    if isinstance(reason, str)
                    else REASON_REJECTED,
                )
            if accepted is not True:
                raise BackfillDispatchError(
                    REASON_INVALID_RESPONSE,
                    "the collector start acknowledgement is malformed",
                )
        return result


__all__ = [
    "COLLECTOR_STATES",
    "REASON_COLLECTOR_UNAVAILABLE",
    "REASON_INVALID_RESPONSE",
    "REASON_NOT_CONFIGURED",
    "REASON_REJECTED",
    "REASON_TIMEOUT",
    "BackfillDispatchError",
    "CollectorBackfillClient",
]
