"""Structured API error helpers and FastAPI exception handlers."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError

from atlas.api.schemas.research_jobs import ErrorBody, ErrorResponse
from atlas.application.exceptions import (
    IdempotencyConflictError,
    ResearchJobLookupError,
)

logger = logging.getLogger(__name__)


def error_response(
    *,
    status_code: int,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    """Build a structured Atlas API error response."""
    payload = ErrorResponse(
        error=ErrorBody(code=code, message=message, details=details or {}),
    )
    return JSONResponse(
        status_code=status_code,
        content=payload.model_dump(),
    )


def register_exception_handlers(app: FastAPI) -> None:
    """Attach Atlas API exception handlers to the application."""

    @app.exception_handler(RequestValidationError)
    async def request_validation_error_handler(
        _request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        issues: list[dict[str, Any]] = []
        for error in exc.errors():
            issues.append(
                {
                    "loc": list(error.get("loc", ())),
                    "msg": str(error.get("msg", "Invalid value.")),
                    "type": str(error.get("type", "value_error")),
                }
            )
        return error_response(
            status_code=422,
            code="request_validation_failed",
            message="Request validation failed.",
            details={"issues": issues},
        )

    @app.exception_handler(ResearchJobLookupError)
    async def research_job_lookup_error_handler(
        _request: Request,
        exc: ResearchJobLookupError,
    ) -> JSONResponse:
        return error_response(
            status_code=404,
            code="research_job_not_found",
            message="Research job not found.",
            details={"job_id": exc.job_id},
        )

    @app.exception_handler(IdempotencyConflictError)
    async def idempotency_conflict_error_handler(
        _request: Request,
        _exc: IdempotencyConflictError,
    ) -> JSONResponse:
        return error_response(
            status_code=409,
            code="idempotency_key_conflict",
            message=(
                "Idempotency key was already used with a different request payload."
            ),
        )

    @app.exception_handler(OperationalError)
    async def operational_error_handler(
        _request: Request,
        _exc: OperationalError,
    ) -> JSONResponse:
        logger.warning("Research-job API database unavailable")
        return error_response(
            status_code=503,
            code="service_unavailable",
            message="Service temporarily unavailable.",
        )
