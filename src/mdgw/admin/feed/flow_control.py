# ruff: file-ignore[raise-vanilla-args,raw-string-in-exception,blind-except]
"""API Rate Limit and WebSocket Slow Consumer: two independent hot resources.

Each resource has its own version, desired settings, observed state and audit
action. API Rate Limit mutates the token-bucket policy consulted by the HTTP
middleware; WebSocket Slow Consumer mutates the hub's queue/lag_notice policy.
Neither includes a Venue limit, write-queue bound or storage backpressure.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from mdgw.admin.audit.models import AuditTarget
from mdgw.admin.audit.writer import actor_from_session
from mdgw.admin.errors import ConflictError, InvalidParameterError
from mdgw.admin.feed.models import (
    API_RATE_LIMIT_RESOURCE,
    OBSERVED_APPLIED,
    WS_SLOW_CONSUMER_RESOURCE,
    FlowControlResource,
    last_error_wire,
)
from mdgw.admin.feed.service import parse_if_match
from mdgw.core.time import ns_to_iso

if TYPE_CHECKING:
    from collections.abc import Callable

    from mdgw.admin.auth.store import SessionRecord
    from mdgw.admin.context import AdminContext
    from mdgw.admin.feed.runtime import FeedAdminRuntime
    from mdgw.admin.feed.store import FeedStore

ACTION_API_RATE_LIMIT_UPDATE = "flow_control.api_rate_limit.update"
ACTION_WS_SLOW_CONSUMER_UPDATE = "flow_control.ws_slow_consumer.update"
_TARGET_TYPE = "flow_control"

_ACTIONS = {
    API_RATE_LIMIT_RESOURCE: ACTION_API_RATE_LIMIT_UPDATE,
    WS_SLOW_CONSUMER_RESOURCE: ACTION_WS_SLOW_CONSUMER_UPDATE,
}


class ApiRateLimitUpdateBody(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool
    requests_per_second: float = Field(gt=0)
    burst: int = Field(ge=0)
    max_ws_sessions_per_ip: int = Field(ge=0)


class WsSlowConsumerUpdateBody(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool
    queue_size: int = Field(ge=1)
    lag_notice_interval_ms: int = Field(ge=0)


class FlowControlService:
    """Read and hot-update the two versioned flow-control sub-resources."""

    def __init__(
        self,
        *,
        context: AdminContext,
        store: FeedStore,
        runtime: FeedAdminRuntime,
        clock: Callable[[], int],
    ) -> None:
        self._context = context
        self._store = store
        self._runtime = runtime
        self._clock = clock

    def get(self) -> dict[str, Any]:
        return {
            API_RATE_LIMIT_RESOURCE: self._require(
                API_RATE_LIMIT_RESOURCE
            ).to_wire(),
            WS_SLOW_CONSUMER_RESOURCE: self._require(
                WS_SLOW_CONSUMER_RESOURCE
            ).to_wire(),
        }

    async def update_api_rate_limit(
        self,
        body: ApiRateLimitUpdateBody,
        *,
        if_match: str | None,
        session: SessionRecord,
        request_id: str,
        reason: str | None = None,
    ) -> dict[str, Any]:
        return await self._update_impl(
            API_RATE_LIMIT_RESOURCE,
            settings=body.model_dump(mode="json"),
            if_match=if_match,
            session=session,
            request_id=request_id,
            reason=reason,
        )

    async def update_ws_slow_consumer(
        self,
        body: WsSlowConsumerUpdateBody,
        *,
        if_match: str | None,
        session: SessionRecord,
        request_id: str,
        reason: str | None = None,
    ) -> dict[str, Any]:
        return await self._update_impl(
            WS_SLOW_CONSUMER_RESOURCE,
            settings=body.model_dump(mode="json"),
            if_match=if_match,
            session=session,
            request_id=request_id,
            reason=reason,
        )

    async def _update_impl(  # ruff: ignore[too-many-arguments]  # explicit write fields
        self,
        resource: str,
        *,
        settings: dict[str, Any],
        if_match: str | None,
        session: SessionRecord,
        request_id: str,
        reason: str | None,
    ) -> dict[str, Any]:
        current = self._require(resource)
        expected = parse_if_match(if_match)
        if expected != current.version:
            raise _version_conflict(current)

        actor = actor_from_session(session)
        now = int(self._clock())
        version = current.version + 1
        handle = self._context.audit.begin(
            actor=actor,
            action=_ACTIONS[resource],
            target=AuditTarget(type=_TARGET_TYPE, id=resource),
            request_id=request_id,
            reason=reason,
            before=current.to_wire(),
        )
        apply_failure: str | None = None
        try:
            await self._runtime.apply_flow_control(
                resource, settings=settings, version=version
            )
        except Exception:
            apply_failure = "the runtime rejected the flow-control policy"

        observed_state = (
            OBSERVED_APPLIED if apply_failure is None else "rejected"
        )
        drift_reason = None if apply_failure is None else "not_applied"
        last_error = (
            None
            if apply_failure is None
            else last_error_wire(
                code="apply_failed",
                message=apply_failure,
                at_ns=now,
                request_id=request_id,
            )
        )
        updated = FlowControlResource(
            **{
                **current.__dict__,
                "version": version,
                "settings": settings,
                "observed_version": version,
                "observed_state": observed_state,
                "observed_at_ns": now,
                "drift_reason": drift_reason,
                "last_error": last_error,
                "updated_at_ns": now,
                "updated_by": actor.subject,
            }
        )
        if not self._store.update_flow(
            resource,
            expected_version=current.version,
            settings=settings,
            version=version,
            observed_version=version,
            observed_state=observed_state,
            observed_at_ns=now,
            drift_reason=drift_reason,
            last_error=last_error,
            updated_by=actor.subject,
        ):
            handle.failed(failure_reason="version_conflict")
            raise _version_conflict(self._require(resource))
        wire = updated.to_wire()
        if apply_failure is None:
            handle.succeeded(after=wire)
        else:
            handle.failed(
                failure_reason="apply_failed", after=wire
            )
        return wire

    def apply_persisted(self) -> None:
        """Apply desired settings after an API restart and mark them observed."""
        now = int(self._clock())
        for resource in (API_RATE_LIMIT_RESOURCE, WS_SLOW_CONSUMER_RESOURCE):
            current = self._require(resource)
            try:
                self._runtime.apply_flow_control_now(
                    resource, settings=current.settings, version=current.version
                )
            except Exception:
                self._store.mark_flow_observed(
                    resource,
                    desired_version=current.version,
                    observed_version=current.observed_version,
                    observed_state="rejected",
                    observed_at_ns=now,
                    drift_reason="not_applied",
                    last_error={
                        "code": "apply_failed",
                        "message": "the runtime rejected the persisted policy",
                        "at": _iso(now),
                        "request_id": "startup",
                    },
                )
                continue
            self._store.mark_flow_observed(
                resource,
                desired_version=current.version,
                observed_version=current.version,
                observed_state=OBSERVED_APPLIED,
                observed_at_ns=now,
                drift_reason=None,
                last_error=None,
            )

    def _require(self, resource: str) -> FlowControlResource:
        row = self._store.get_flow(resource)
        if row is not None:
            return row
        # The migration seeds both rows; a missing row means the management DB
        # was tampered with. Fail loudly instead of inventing settings.
        raise InvalidParameterError(
            "flow-control resource is missing",
            details={"resource": resource},
        )


def _version_conflict(current: FlowControlResource) -> ConflictError:
    return ConflictError(
        "the flow-control resource was modified by another request",
        details={
            "reason": "version_conflict",
            "current_version": current.version,
            "current": current.to_wire(),
        },
    )


def _iso(ns: int) -> str:
    return ns_to_iso(ns)


__all__ = [
    "ACTION_API_RATE_LIMIT_UPDATE",
    "ACTION_WS_SLOW_CONSUMER_UPDATE",
    "ApiRateLimitUpdateBody",
    "FlowControlService",
    "WsSlowConsumerUpdateBody",
]
