"""One Admin Feed/Flow-Control runtime per API process.

The runtime owns the database-backed services, the Collector control client
and the two mutable local policies:

* ``rate_limit_policy`` is read by the HTTP rate-limit middleware on every
  request (the limiter resets its buckets when the version changes);
* WebSocket Slow Consumer settings are pushed into the attached hub.

It also tracks the Collector control adapter's truthful state for capability
discovery: ``available`` only after a successful control-plane exchange.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from starlette.datastructures import Headers

from mdgw.admin.capabilities import AdapterStatus
from mdgw.admin.feed.client import CollectorControlClient
from mdgw.admin.feed.flow_control import FlowControlService
from mdgw.admin.feed.models import (
    API_RATE_LIMIT_RESOURCE,
    DEFAULT_API_RATE_LIMIT,
    DEFAULT_WS_SLOW_CONSUMER,
    WS_SLOW_CONSUMER_RESOURCE,
)
from mdgw.admin.feed.service import FeedSubscriptionService
from mdgw.admin.feed.store import FeedStore
from mdgw.core.rate_limit import RateLimitPolicy

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mdgw.admin.context import AdminContext
    from mdgw.api.ws_hub import WebSocketHub
    from mdgw.core.rate_limit import RateLimiter

logger = logging.getLogger(__name__)


class FeedAdminRuntime:
    """Feed Subscription + flow-control services for one Admin context."""

    def __init__(self, context: AdminContext) -> None:
        self._context = context
        self._clock = context.audit.now_ns
        self.store = FeedStore(context.db, clock=self._clock)
        defaults = context.flow_control_defaults
        if defaults is not None:
            self.store.reconcile_flow_seed(
                API_RATE_LIMIT_RESOURCE,
                defaults.get(API_RATE_LIMIT_RESOURCE, DEFAULT_API_RATE_LIMIT),
                now_ns=self._clock(),
            )
            self.store.reconcile_flow_seed(
                WS_SLOW_CONSUMER_RESOURCE,
                defaults.get(
                    WS_SLOW_CONSUMER_RESOURCE, DEFAULT_WS_SLOW_CONSUMER
                ),
                now_ns=self._clock(),
            )
        self.client = CollectorControlClient(
            context.config.control,
            environ=context.resolved_environ,
        )
        self.flow_control = FlowControlService(
            context=context,
            store=self.store,
            runtime=self,
            clock=self._clock,
        )
        self.subscriptions = FeedSubscriptionService(
            context=context,
            store=self.store,
            client=self.client,
            instrument_store=context.instrument_store,
            clock=self._clock,
            adapter_enabled=context.config.adapters.collector_control,
            runtime=self,
        )
        self._rate_limiter: RateLimiter | None = None
        self._ws_hub: WebSocketHub | None = None
        self._rate_policy = _rate_policy_from(
            self.store.get_flow(API_RATE_LIMIT_RESOURCE)
        )
        self._ws_settings = _ws_settings_from(
            self.store.get_flow(WS_SLOW_CONSUMER_RESOURCE)
        )
        self._control_ok: bool | None = None
        self._control_reason: str | None = None

    # -- rate-limit policy source -----------------------------------------

    def rate_limit_policy(self) -> RateLimitPolicy:
        return self._rate_policy

    def resolve_client_ip(self, scope: Any) -> str:
        """Resolve the real source IP through the trusted-proxy policy.

        Flow control must key on the operator-configured forwarder chain, not
        on a raw ``scope["client"]`` peer. The resolver ignores forwarded
        headers unless the peer is a configured trusted proxy.
        """
        client = scope.get("client")
        peer = client[0] if isinstance(client, tuple) and client else ""
        return self._context.client_ip_resolver.resolve_from_headers(
            str(peer), Headers(scope=scope)
        )

    def bind_rate_limiter(self, limiter: RateLimiter | None) -> None:
        self._rate_limiter = limiter

    def bind_ws_hub(self, hub: WebSocketHub | None) -> None:
        self._ws_hub = hub

    async def apply_flow_control(
        self, resource: str, *, settings: Mapping[str, Any], version: int
    ) -> None:
        self.apply_flow_control_now(
            resource, settings=settings, version=version
        )

    def apply_flow_control_now(
        self, resource: str, *, settings: Mapping[str, Any], version: int
    ) -> None:
        """Apply one resource; safe before the event loop starts (startup)."""
        if resource == API_RATE_LIMIT_RESOURCE:
            self._rate_policy = _rate_policy_from_settings(settings, version)
            return
        if resource == WS_SLOW_CONSUMER_RESOURCE:
            validated = _ws_settings_from_settings(settings)
            self._ws_settings = validated
            hub = self._ws_hub
            if hub is not None:
                hub.apply_flow_control(
                    queue_size=int(validated["queue_size"]),
                    lag_notice_interval_ms=int(
                        validated["lag_notice_interval_ms"]
                    ),
                )
            return
        msg = f"unknown flow-control resource: {resource!r}"
        raise ValueError(msg)

    def reconcile_runtime_defaults(
        self, *, rate_limit: Any, hub: WebSocketHub | None
    ) -> None:
        """Seed untouched resources from the process's actual startup config.

        A row already written by an operator is never overwritten; only the
        migration/startup ``system`` rows are reconciled. This keeps the
        Reported Observed state truthful when ``create_app`` is embedded with
        an explicitly configured hub or rate limit.
        """
        now = self._clock()
        if rate_limit is not None:
            self.store.reconcile_flow_seed(
                API_RATE_LIMIT_RESOURCE,
                {
                    "enabled": bool(rate_limit.enabled),
                    "requests_per_second": float(
                        rate_limit.requests_per_second
                    ),
                    "burst": int(rate_limit.burst),
                    "max_ws_sessions_per_ip": int(
                        rate_limit.max_ws_sessions_per_ip
                    ),
                },
                now_ns=now,
            )
        if hub is not None:
            self.store.reconcile_flow_seed(
                WS_SLOW_CONSUMER_RESOURCE,
                {
                    "enabled": True,
                    "queue_size": hub.queue_size,
                    "lag_notice_interval_ms": hub.lag_notice_interval_ms,
                },
                now_ns=now,
            )
        self._rate_policy = _rate_policy_from(
            self.store.get_flow(API_RATE_LIMIT_RESOURCE)
        )
        self._ws_settings = _ws_settings_from(
            self.store.get_flow(WS_SLOW_CONSUMER_RESOURCE)
        )

    # -- startup -----------------------------------------------------------

    def on_startup(self) -> None:
        self.flow_control.apply_persisted()
        self.subscriptions.redispatch_on_startup()

    # -- adapter state -----------------------------------------------------

    def record_dispatch_success(self) -> None:
        self._control_ok = True
        self._control_reason = None

    def record_dispatch_failure(self, reason: str, _message: str) -> None:
        self._control_ok = False
        # Bounded, non-secret text only.
        self._control_reason = f"collector control is unavailable ({reason})"

    def adapter_status(self) -> AdapterStatus:
        if not self._context.config.adapters.collector_control:
            return AdapterStatus(
                "unsupported", "disabled by configuration"
            )
        if not self.client.configured:
            return AdapterStatus(
                "unavailable",
                "collector control is not configured "
                "(port or token missing)",
            )
        if self._control_ok is None:
            return AdapterStatus(
                "unavailable", "collector control is not connected"
            )
        if not self._control_ok:
            return AdapterStatus(
                "unavailable",
                self._control_reason or "collector control is unavailable",
            )
        return AdapterStatus("available")


def _rate_policy_from(row: Any) -> RateLimitPolicy:
    if row is None:
        return _rate_policy_from_settings(DEFAULT_API_RATE_LIMIT, 1)
    return _rate_policy_from_settings(row.settings, row.version)


def _rate_policy_from_settings(
    settings: Mapping[str, Any], version: int
) -> RateLimitPolicy:
    try:
        rate = float(settings["requests_per_second"])
        burst = int(settings["burst"])
        max_ws = int(settings["max_ws_sessions_per_ip"])
        enabled = bool(settings["enabled"])
    except (KeyError, TypeError, ValueError):
        defaults = DEFAULT_API_RATE_LIMIT
        return RateLimitPolicy(
            enabled=bool(defaults["enabled"]),
            requests_per_second=float(defaults["requests_per_second"]),
            burst=int(defaults["burst"]),
            max_ws_sessions_per_ip=int(defaults["max_ws_sessions_per_ip"]),
            version=max(1, int(version)),
        )
    return RateLimitPolicy(
        enabled=enabled,
        requests_per_second=rate,
        burst=burst,
        max_ws_sessions_per_ip=max_ws,
        version=max(1, int(version)),
    )


def _ws_settings_from(row: Any) -> dict[str, Any]:
    if row is None:
        return dict(DEFAULT_WS_SLOW_CONSUMER)
    try:
        return _ws_settings_from_settings(row.settings)
    except ValueError:
        return dict(DEFAULT_WS_SLOW_CONSUMER)


def _ws_settings_from_settings(
    settings: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        enabled = bool(settings["enabled"])
        queue_size = int(settings["queue_size"])
        interval_ms = int(settings["lag_notice_interval_ms"])
    except (KeyError, TypeError, ValueError) as exc:
        msg = "invalid WebSocket slow-consumer settings"
        raise ValueError(msg) from exc
    if queue_size < 1 or interval_ms < 0:
        msg = "invalid WebSocket slow-consumer settings"
        raise ValueError(msg)
    return {
        "enabled": enabled,
        "queue_size": queue_size,
        "lag_notice_interval_ms": interval_ms,
    }


__all__ = ["FeedAdminRuntime"]
