"""Closed HTTP label normalization for Prometheus metrics (Slice 15A2).

Every label attached to an Atlas Prometheus metric must come from a
bounded, reviewed allowlist -- never a raw request path, header, query
string, or exception message. This module is the single place that
normalizes HTTP method/route/status into such bounded label values; see
:mod:`atlas.observability.metrics.catalog` for non-HTTP label
normalization (workflow node names, provider ids, etc. -- those are
already closed application-level enums and need no separate allowlist).

``normalize_http_route`` never emits an arbitrary ``route.path_format``.
The caller passes the FastAPI route template string only when a route
actually matched; this module then looks the result up in
``_APPROVED_HTTP_ROUTES`` and emits only the mapped canonical label. A
matched route whose template is not a key in the allowlist (e.g. a future
route added without updating this module) becomes ``"other"``, never its
own raw template -- this fails safe (a bounded but momentarily
uninformative label) rather than silently growing the label's cardinality.

The allowlist keys are ``APIRoute.path_format`` values, *not* the full
mounted URL path: FastAPI's ``include_router`` does not flatten a nested
router's prefix into each sub-route's own ``path_format`` (verified
against the installed FastAPI's ``_IncludedRouter`` lazy-routing
behavior -- e.g. the route mounted at ``/v1/research-jobs`` reports
``path_format == "/research-jobs"``, without the parent router's ``/v1``
prefix). The dict's *values* restore the full, human-readable mounted path
for the emitted label, so a dashboard still reads ``/v1/research-jobs``
rather than the prefix-stripped internal template.
"""

from __future__ import annotations

from typing import Final

#: Approved HTTP methods. Anything else (including malformed/unusual
#: methods a client might send) normalizes to ``"other"``.
_APPROVED_HTTP_METHODS: Final[frozenset[str]] = frozenset(
    {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
)
_OTHER_METHOD: Final[str] = "other"

#: Explicit, reviewed allowlist mapping each Atlas route's raw
#: ``path_format`` to the canonical, full-mounted-path label it emits as
#: ``route``. Kept in sync by hand with the actual FastAPI routers
#: (``atlas.api.v1.router`` and the routes ``atlas.main`` defines
#: directly) -- adding a new route requires adding its ``path_format``
#: here too, or it renders as ``"other"`` rather than silently becoming a
#: new label value.
_APPROVED_HTTP_ROUTES: Final[dict[str, str]] = {
    "/health": "/health",
    "/ready": "/ready",
    "/metrics": "/metrics",
    "/research-jobs": "/v1/research-jobs",
    "/research-jobs/{job_id}": "/v1/research-jobs/{job_id}",
    "/research-jobs/{job_id}/evaluation": "/v1/research-jobs/{job_id}/evaluation",
    "/research-jobs/{job_id}/citations": "/v1/research-jobs/{job_id}/citations",
    "/research-jobs/{job_id}/review-decisions": (
        "/v1/research-jobs/{job_id}/review-decisions"
    ),
    "/evidence/documents": "/v1/evidence/documents",
    "/evidence/items/{evidence_item_id}": "/v1/evidence/items/{evidence_item_id}",
}
_UNMATCHED_ROUTE: Final[str] = "unmatched"
_OTHER_ROUTE: Final[str] = "other"

#: Exact status codes with dedicated label values; everything else
#: collapses to its ``NxxCode_other`` bucket, or ``"other"`` if the code is
#: not a valid three-digit HTTP status at all (e.g. ``0``, a negative
#: value, or ``999`` from a misbehaving handler).
_EXACT_STATUS_CODES: Final[frozenset[int]] = frozenset(
    {200, 202, 404, 409, 422, 429, 500, 503}
)


def normalize_http_method(method: str) -> str:
    """Map an HTTP method to one of the approved labels, else ``"other"``."""
    upper = method.upper()
    if upper in _APPROVED_HTTP_METHODS:
        return upper
    return _OTHER_METHOD


def normalize_http_route(route_template: str | None) -> str:
    """Map a matched route's raw ``path_format`` to its canonical approved label.

    ``route_template=None`` means no route matched at all (e.g. a 404 for
    a path FastAPI could not route) and always returns ``"unmatched"``.
    A non-``None`` template that is not a key in the reviewed allowlist
    returns ``"other"`` rather than the raw template itself.
    """
    if route_template is None:
        return _UNMATCHED_ROUTE
    return _APPROVED_HTTP_ROUTES.get(route_template, _OTHER_ROUTE)


def normalize_http_status(status_code: int) -> str:
    """Map a status code to an exact approved code or a bounded ``*xx_other`` bucket."""
    if status_code in _EXACT_STATUS_CODES:
        return str(status_code)
    if 100 <= status_code < 200:
        return "1xx_other"
    if 200 <= status_code < 300:
        return "2xx_other"
    if 300 <= status_code < 400:
        return "3xx_other"
    if 400 <= status_code < 500:
        return "4xx_other"
    if 500 <= status_code < 600:
        return "5xx_other"
    return "other"
