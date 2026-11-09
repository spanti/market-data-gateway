"""Authenticated, server-owned Prometheus template proxy (ticket 09).

The Operator Console never sends PromQL. It asks for a named template with
declared parameters; the server maps it to one fixed query, validates every
range/step/point limit and returns a bounded series payload.
"""

from __future__ import annotations

from mdgw.admin.monitoring.routes import create_monitoring_router

__all__ = ["create_monitoring_router"]
