"""FastAPI route modules, split by resource.

Each module exposes a `create_router(...)` factory so `create_app` can bind
per-application state without module-level globals. Bars (ticket 06) and
WebSocket (ticket 07) start as empty routers and are filled in place.
"""

from mdgw.api.routes import bars, instruments, ops, websocket

__all__ = ["bars", "instruments", "ops", "websocket"]
