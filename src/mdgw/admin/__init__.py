"""Admin API package: controlled `/api/v1/admin` foundation.

Ticket 01 owns the routing boundary, unified errors, the independent
management SQLite store, trusted-proxy client resolution, capability
discovery and the offline control-channel envelope. Later tickets add OIDC,
audit, feed subscriptions, lifecycle control, backfill, export and metrics.
"""
