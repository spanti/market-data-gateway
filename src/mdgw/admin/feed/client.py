"""Admin-side client for the Collector control plane.

This is the only place the Admin API sends desired Feed Subscription state.
The transport itself is ``mdgw.service.control_plane`` (loopback TCP, shared
token, bounded length-prefixed JSON); this module translates database rows
into the allow-listed ``feed.subscription.sync`` payload and turns transport
failures into an explicit *drift* reason. It never invents an acknowledgement.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mdgw.admin.collector_control import (
    REASON_COLLECTOR_REJECTED,
    REASON_COLLECTOR_UNAVAILABLE,
    REASON_NOT_APPLIED,
    REASON_NOT_CONFIGURED,
    REASON_TIMEOUT,
    CollectorControlEndpoint,
    CollectorControlError,
)
from mdgw.core.hashing import sha256_hex
from mdgw.service.control_plane import (
    FEED_SUBSCRIPTION_SYNC_ACTION,
    FeedSubscriptionDesired,
    FeedSubscriptionSyncPayload,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from mdgw.admin.feed.models import FeedSubscription
    from mdgw.config import AdminControlConfig


class CollectorDispatchError(RuntimeError):
    """Dispatch failed before any acknowledgement; Desired must be kept."""

    def __init__(
        self,
        reason: str,
        message: str,
        *,
        code: str = "collector_unavailable",
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.code = code


class CollectorControlClient:
    """Versioned desired-state dispatch to the Collector."""

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

    def sync_subscriptions(
        self,
        subscriptions: Sequence[FeedSubscription],
        *,
        request_id: str,
    ) -> dict[str, dict[str, Any]]:
        """Dispatch the full desired set and return per-id acknowledgements.

        The Collector response is ``{subscription_id: {state, reason, version,
        collector_instance_id}}``. Anything malformed raises a dispatch error
        with ``reason="not_applied"``; no observed state is set from it.
        """
        desired = [
            FeedSubscriptionDesired(
                subscription_id=row.subscription_id,
                feed=row.feed,
                venue=row.venue,
                instrument_id=row.instrument_id,
                data_type="bar",
                interval=row.interval,
                enabled=row.desired_enabled,
                deleted=row.desired_deleted,
                version=row.desired_version,
            )
            for row in subscriptions
        ]
        sync_version = max(1, sum(row.desired_version for row in subscriptions))
        payload = FeedSubscriptionSyncPayload(
            sync_version=sync_version, subscriptions=desired
        )
        correlation = sha256_hex(request_id)[:40]
        try:
            result = self._endpoint.call(
                FEED_SUBSCRIPTION_SYNC_ACTION,
                payload,
                correlation_id=f"corr_{correlation}",
            )
        except CollectorControlError as exc:
            # Transport failures keep the stable ``collector_unavailable``
            # code (the reason distinguishes timeout); a server-side refusal
            # carries the Collector's own error code.
            code = (
                exc.code
                if exc.reason == REASON_COLLECTOR_REJECTED
                else "collector_unavailable"
            )
            raise CollectorDispatchError(
                exc.reason, exc.message, code=code
            ) from exc
        collector_instance_id = result.get("collector_instance_id")
        entries = result.get("subscriptions")
        if not isinstance(collector_instance_id, str) or not isinstance(
            entries, list
        ):
            raise CollectorDispatchError(
                REASON_NOT_APPLIED,
                "the collector acknowledgement is malformed",
                code="invalid_response",
            )
        acknowledged: dict[str, dict[str, Any]] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            subscription_id = entry.get("subscription_id")
            state = entry.get("state")
            version = entry.get("version")
            reason = entry.get("reason")
            if (
                not isinstance(subscription_id, str)
                or state not in {"applied", "rejected"}
                or isinstance(version, bool)
                or not isinstance(version, int)
                or not (reason is None or isinstance(reason, str))
            ):
                continue
            acknowledged[subscription_id] = {
                "state": state,
                "reason": reason,
                "version": version,
                "collector_instance_id": collector_instance_id,
            }
        return acknowledged


__all__ = [
    "REASON_COLLECTOR_REJECTED",
    "REASON_COLLECTOR_UNAVAILABLE",
    "REASON_NOT_APPLIED",
    "REASON_NOT_CONFIGURED",
    "REASON_TIMEOUT",
    "CollectorControlClient",
    "CollectorDispatchError",
]
