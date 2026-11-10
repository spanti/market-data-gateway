"""Feed Subscription and flow-control value objects and wire rendering.

Desired, Observed, Drift and Last Error are separate values by design: only a
Collector acknowledgement (or an explicit collector-unavailable/not-applied
dispatch failure) touches the observed side, and Desired is never rolled back
automatically. ``to_wire`` is the only place that renders the contract JSON.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from mdgw.core.schema import INTERVALS
from mdgw.core.time import ns_to_iso

DATA_TYPE_BAR = "bar"
BAR_INTERVALS: tuple[str, ...] = tuple(INTERVALS)

OBSERVED_PENDING = "pending"
OBSERVED_APPLYING = "applying"
OBSERVED_APPLIED = "applied"
OBSERVED_REJECTED = "rejected"
OBSERVED_UNKNOWN = "unknown"
OBSERVED_STATES: tuple[str, ...] = (
    OBSERVED_PENDING,
    OBSERVED_APPLYING,
    OBSERVED_APPLIED,
    OBSERVED_REJECTED,
    OBSERVED_UNKNOWN,
)

DRIFT_NOT_APPLIED = "not_applied"
DRIFT_COLLECTOR_REJECTED = "collector_rejected"
DRIFT_COLLECTOR_UNAVAILABLE = "collector_unavailable"
DRIFT_DELETION_PENDING = "deletion_pending"
DRIFT_REASONS: tuple[str, ...] = (
    DRIFT_NOT_APPLIED,
    DRIFT_COLLECTOR_REJECTED,
    DRIFT_COLLECTOR_UNAVAILABLE,
    DRIFT_DELETION_PENDING,
)

API_RATE_LIMIT_RESOURCE = "api_rate_limit"
WS_SLOW_CONSUMER_RESOURCE = "ws_slow_consumer"
FLOW_CONTROL_RESOURCES: tuple[str, ...] = (
    API_RATE_LIMIT_RESOURCE,
    WS_SLOW_CONSUMER_RESOURCE,
)

#: List filter values: the desired tri-state plus every observed state.
FEED_STATE_FILTERS: tuple[str, ...] = (
    "enabled",
    "disabled",
    "deleted",
    *OBSERVED_STATES,
)

DEFAULT_API_RATE_LIMIT: dict[str, Any] = {
    # Rate limiting stays off unless the deployment enables it; the admin
    # resource can hot-enable it later without a restart.
    "enabled": False,
    "requests_per_second": 100.0,
    "burst": 200,
    "max_ws_sessions_per_ip": 16,
}
DEFAULT_WS_SLOW_CONSUMER: dict[str, Any] = {
    "enabled": True,
    "queue_size": 1000,
    "lag_notice_interval_ms": 1000,
}


def last_error_wire(
    *,
    code: str,
    message: str,
    at_ns: int,
    request_id: str,
) -> dict[str, str]:
    return {
        "code": code,
        "message": message,
        "at": ns_to_iso(at_ns),
        "request_id": request_id,
    }


def parse_last_error(value: str | None) -> dict[str, Any] | None:
    if not value:
        return None
    try:
        parsed = json.loads(value)
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None


@dataclass(frozen=True)
class FeedSubscription:
    """One `feed_subscriptions` row."""

    subscription_id: str
    feed: str
    venue: str
    instrument_id: str
    interval: str
    desired_enabled: bool
    desired_deleted: bool
    desired_version: int
    desired_updated_at_ns: int
    desired_updated_by: str
    created_at_ns: int
    created_by: str
    updated_at_ns: int
    observed_enabled: bool
    observed_deleted: bool
    observed_version: int
    observed_state: str
    observed_at_ns: int | None
    collector_instance_id: str | None
    drift_in_sync: bool
    drift_reason: str | None
    drift_since_ns: int
    last_error: dict[str, Any] | None
    data_type: str = DATA_TYPE_BAR

    @property
    def observed_at(self) -> str:
        return ns_to_iso(
            self.observed_at_ns
            if self.observed_at_ns is not None
            else self.created_at_ns
        )

    def to_wire(self) -> dict[str, Any]:
        return {
            "subscription_id": self.subscription_id,
            "feed": self.feed,
            "venue": self.venue,
            "instrument_id": self.instrument_id,
            "data_type": self.data_type,
            "interval": self.interval,
            "desired": {
                "enabled": self.desired_enabled,
                "deleted": self.desired_deleted,
                "version": self.desired_version,
                "updated_at": ns_to_iso(self.desired_updated_at_ns),
                "updated_by": self.desired_updated_by,
            },
            "observed": {
                "enabled": self.observed_enabled,
                "deleted": self.observed_deleted,
                "version": self.observed_version,
                "state": self.observed_state,
                "observed_at": self.observed_at,
                "collector_instance_id": self.collector_instance_id or "",
            },
            "drift": {
                "in_sync": self.drift_in_sync,
                "desired_version": self.desired_version,
                "observed_version": self.observed_version,
                "since": ns_to_iso(self.drift_since_ns),
                "reason": self.drift_reason,
            },
            "last_error": self.last_error,
            "created_at": ns_to_iso(self.created_at_ns),
            "created_by": self.created_by,
            "updated_at": ns_to_iso(self.updated_at_ns),
        }


@dataclass(frozen=True)
class FlowControlResource:
    """One `flow_control` row (desired settings plus observed state)."""

    resource: str
    version: int
    settings: dict[str, Any]
    observed_version: int
    observed_state: str
    observed_at_ns: int | None
    drift_reason: str | None
    last_error: dict[str, Any] | None
    updated_at_ns: int
    updated_by: str

    def to_wire(self) -> dict[str, Any]:
        wire: dict[str, Any] = dict(self.settings)
        wire.update(
            {
                "version": self.version,
                "apply_mode": "hot",
                "observed": {
                    "version": self.observed_version,
                    "state": self.observed_state,
                    "observed_at": (
                        ns_to_iso(self.observed_at_ns)
                        if self.observed_at_ns is not None
                        else ns_to_iso(self.updated_at_ns)
                    ),
                },
                "drift": {
                    "in_sync": (
                        self.observed_version == self.version
                        and self.drift_reason is None
                    ),
                    "desired_version": self.version,
                    "observed_version": self.observed_version,
                    "reason": self.drift_reason,
                },
                "last_error": self.last_error,
            }
        )
        return wire


__all__ = [
    "API_RATE_LIMIT_RESOURCE",
    "BAR_INTERVALS",
    "DATA_TYPE_BAR",
    "DEFAULT_API_RATE_LIMIT",
    "DEFAULT_WS_SLOW_CONSUMER",
    "DRIFT_COLLECTOR_REJECTED",
    "DRIFT_COLLECTOR_UNAVAILABLE",
    "DRIFT_DELETION_PENDING",
    "DRIFT_NOT_APPLIED",
    "DRIFT_REASONS",
    "FEED_STATE_FILTERS",
    "FLOW_CONTROL_RESOURCES",
    "OBSERVED_APPLIED",
    "OBSERVED_REJECTED",
    "OBSERVED_STATES",
    "OBSERVED_UNKNOWN",
    "WS_SLOW_CONSUMER_RESOURCE",
    "FeedSubscription",
    "FlowControlResource",
    "last_error_wire",
    "parse_last_error",
]
