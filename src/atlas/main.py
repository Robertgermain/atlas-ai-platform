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
"""

from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from atlas.api.errors import register_exception_handlers
from atlas.api.v1.router import api_v1_router
from atlas.observability.events import Event
from atlas.observability.logging import configure_logging, log_event

configure_logging(service_role="api")

logger = logging.getLogger(__name__)

app = FastAPI(title="Atlas AI Platform", version="0.1.0")
register_exception_handlers(app)
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
        return JSONResponse({"status": "not_ready"}, status_code=503)
    return JSONResponse({"status": "ready"})
