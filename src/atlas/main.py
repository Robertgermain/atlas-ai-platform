"""FastAPI application entrypoint."""

from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from atlas.api.errors import register_exception_handlers
from atlas.api.v1.router import api_v1_router

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
        logger.warning("Readiness check failed: database unavailable")
        return JSONResponse({"status": "not_ready"}, status_code=503)
    return JSONResponse({"status": "ready"})
