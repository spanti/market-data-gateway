"""Route-template resolution shared by the middleware and WebSocket route.

Metric labels and rate-limit bucket keys use the *route template* (for example
``/api/v1/instruments/{instrument_id:path}``), never the concrete request path.
Keeping these helpers in one module stops the label rules from drifting between
the request middleware, the pre-routing rate limiter and the WebSocket route.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from starlette.routing import Match

if TYPE_CHECKING:
    from collections.abc import Iterable, MutableMapping

#: Fixed template for requests that match no route. It keeps unmatched paths
#: out of rate-limit buckets and metric labels, bounding their cardinality.
UNMATCHED_ROUTE = "<unmatched>"

#: ASGI scope key where pre-routing middleware publishes the template it
#: resolved, so later middleware can label the request without a route.
ROUTE_TEMPLATE_SCOPE_KEY = "mdgw_route_template"


def matched_route_template(scope: MutableMapping[str, Any]) -> str | None:
    """Return the template of the route routing already selected, if any."""
    route = scope.get("route")
    path = getattr(route, "path", None)
    if isinstance(path, str) and path:
        return path
    return None


def route_template(
    scope: MutableMapping[str, Any],
    *,
    fallback: str = "",
) -> str:
    """Resolve a request's route template.

    Prefers the route routing selected, then a template published earlier in
    the middleware chain, then ``fallback`` (the raw path when it is empty).
    """
    template = matched_route_template(scope)
    if template is None:
        preset = scope.get(ROUTE_TEMPLATE_SCOPE_KEY)
        if isinstance(preset, str) and preset:
            template = preset
    if template is None:
        template = fallback or str(scope.get("path", ""))
    return template


def _leaf_routes(routes: Iterable[Any]) -> Iterable[Any]:
    """Yield routes carrying a concrete template.

    FastAPI 0.162 stores routers included via ``include_router`` behind an
    ``_IncludedRouter`` wrapper whose ``matches`` delegates to the wrapped
    router but which has no ``path`` of its own. Unwrap those so the rate
    limiter sees the same templates the router will eventually use.
    """
    for route in routes:
        original = getattr(route, "original_router", None)
        nested = getattr(original, "routes", None)
        if nested is not None:
            yield from _leaf_routes(nested)
        else:
            yield route


def pre_match_route_template(
    scope: MutableMapping[str, Any],
    routes: Iterable[Any],
) -> str:
    """Resolve the template before routing, or the fixed unmatched sentinel.

    The rate limiter runs outside the router, so ``scope["route"]`` is not set
    yet. Matching the app's routes here produces the same template while still
    collapsing unknown paths onto one bounded bucket.
    """
    for route in _leaf_routes(routes):
        match, _ = route.matches(scope)
        if match is Match.NONE:
            continue
        path = getattr(route, "path", None)
        if isinstance(path, str) and path:
            return path
    return UNMATCHED_ROUTE


__all__ = [
    "ROUTE_TEMPLATE_SCOPE_KEY",
    "UNMATCHED_ROUTE",
    "matched_route_template",
    "pre_match_route_template",
    "route_template",
]
