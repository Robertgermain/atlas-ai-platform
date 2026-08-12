"""FastAPI application entrypoint.

Logging setup (Slice 15A1): :func:`configure_logging` runs at module
import time, before ``app``/its routers are constructed. When served via
``uvicorn atlas.main:app`` (the Dockerfile's own ``CMD``), Uvicorn's own
``Config.__init__`` already ran its own logging setup before this module
is ever imported (verified against the installed ``uvicorn`` package --
see ``atlas.observability.logging``'s module docstring), so this call
always runs strictly after Uvicorn's and can safely reconfigure the
``uvicorn``/``uvicorn.error``/``uvicorn.access`` loggers without being
overwritten afterward. Only this module's own ``/ready`` boundary is
converted to structured logging in this slice; ``atlas.api.errors``'s own
already-sanitized (fixed-string) warnings are deliberately left
unconverted for a later slice -- see ``docs/TECHNICAL_DESIGN.md``.

Metrics (Slice 15A2): ``_HttpMetricsMiddleware`` is a pure ASGI
middleware (not Starlette's ``BaseHTTPMiddleware``, which buffers the
whole response and has known interactions with streaming and exception
propagation) so it observes the real status code and post-routing
``scope["route"]`` for every request, including ones that raise or
stream, without altering FastAPI's own exception handling. ``/metrics``
is unauthenticated by design: Kubernetes ``Service``/``NetworkPolicy``
(Milestone 18+) is the intended exposure-control boundary, and
application user authentication (Milestone 16) must never become a
prerequisite for a Prometheus scrape.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError
from starlette.types import Message, Receive, Scope, Send

from atlas.api.errors import register_exception_handlers
from atlas.api.v1.router import api_v1_router
from atlas.observability.events import Event
from atlas.observability.logging import configure_logging, log_event
from atlas.observability.metrics import (
    AtlasMetrics,
    default_metrics,
    normalize_http_method,
    normalize_http_route,
    normalize_http_status,
    render_metrics_safe,
)

configure_logging(service_role="api")

logger = logging.getLogger(__name__)


class _HttpMetricsMiddleware:
    """Observes ``atlas_http_request*`` for every HTTP request.

    Reads ``scope["route"]`` only after awaiting the downstream app, by
    which point Starlette's router has already set it (or left it unset,
    for a request no route matched). The wrapped ``send`` only ever peeks
    at the ``status`` of the ``http.response.start`` message -- it never
    buffers or alters the response body, so streaming responses pass
    through unchanged.
    """

    def __init__(
        self,
        app: Callable[[Scope, Receive, Send], Awaitable[None]],
        metrics: AtlasMetrics,
    ) -> None:
        self._app = app
        self._metrics = metrics

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        method = normalize_http_method(str(scope.get("method", "")))
        status_code: int | None = None

        async def _send(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
            await send(message)

        started_at = time.perf_counter()
        try:
            await self._app(scope, receive, _send)
        finally:
            duration_seconds = time.perf_counter() - started_at
            route_obj = scope.get("route")
            route_template = (
                getattr(route_obj, "path_format", None)
                if route_obj is not None
                else None
            )
            route = normalize_http_route(route_template)
            status = (
                normalize_http_status(status_code)
                if status_code is not None
                else "other"
            )
            self._metrics.observe_http_request(
                method=method,
                route=route,
                status=status,
                duration_seconds=duration_seconds,
            )


app = FastAPI(title="Atlas AI Platform", version="0.1.0")
register_exception_handlers(app)
app.add_middleware(_HttpMetricsMiddleware, metrics=default_metrics())
app.include_router(api_v1_router)


@app.get("/health")
def health() -> dict[str, str]:
    """Return service liveness status."""
    return {"status": "ok"}


@app.get("/ready")
def ready() -> JSONResponse:
    """Return service readiness based on PostgreSQL connectivity."""
    try:
        from atlas.persistence.db import get_engine
        from atlas.persistence.readiness import check_postgres_ready

        check_postgres_ready(get_engine())
    except SQLAlchemyError:
        log_event(logger, Event.READINESS_CHECK_FAILED, level=logging.WARNING)
        default_metrics().observe_database_readiness_failure()
        return JSONResponse({"status": "not_ready"}, status_code=503)
    return JSONResponse({"status": "ready"})


@app.get("/metrics")
def metrics() -> Response:
    """Serve this process's Prometheus registry. Unauthenticated by design.

    A ``generate_latest()`` failure never propagates a raw traceback to the
    caller: :func:`render_metrics_safe` contains and sanitizes it, and this
    route reflects that as a controlled ``503`` (Slice 15A2 correction).
    """
    body, content_type, status = render_metrics_safe(default_metrics())
    return Response(content=body, media_type=content_type, status_code=status)
