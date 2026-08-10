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
from atlas.coordination.errors import RateLimitExceededError
from atlas.embeddings.errors import (
    EmbeddingAuthConfigError,
    EmbeddingConflictError,
    EmbeddingInvalidRequestError,
    EmbeddingProviderError,
    EmbeddingRateLimitedError,
    EmbeddingTimeoutError,
)
from atlas.evaluation.errors import (
    EvaluationConflictError,
    EvaluationInProgressError,
    EvaluationNotFoundError,
)
from atlas.evidence.errors import (
    CitationIntegrityError,
    ClaimEvidenceRequiredError,
    EvidenceNotFoundError,
    EvidenceTooLargeError,
    EvidenceValidationError,
    ReportArtifactConflictError,
    UrlCanonicalizationError,
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

    @app.exception_handler(RateLimitExceededError)
    async def rate_limit_exceeded_handler(
        _request: Request,
        exc: RateLimitExceededError,
    ) -> JSONResponse:
        response = error_response(
            status_code=429,
            code="rate_limit_exceeded",
            message="Too many requests. Please retry later.",
            details={"retry_after_seconds": exc.retry_after_seconds},
        )
        response.headers["Retry-After"] = str(exc.retry_after_seconds)
        return response

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

    @app.exception_handler(EvidenceNotFoundError)
    async def evidence_not_found_handler(
        _request: Request,
        exc: EvidenceNotFoundError,
    ) -> JSONResponse:
        return error_response(
            status_code=404,
            code="evidence_item_not_found",
            message="Evidence item not found.",
            details={"evidence_item_id": exc.evidence_item_id},
        )

    @app.exception_handler(EvidenceValidationError)
    async def evidence_validation_handler(
        _request: Request,
        _exc: EvidenceValidationError,
    ) -> JSONResponse:
        return error_response(
            status_code=422,
            code="evidence_validation_failed",
            message="Evidence request validation failed.",
        )

    @app.exception_handler(EvidenceTooLargeError)
    async def evidence_too_large_handler(
        _request: Request,
        _exc: EvidenceTooLargeError,
    ) -> JSONResponse:
        return error_response(
            status_code=422,
            code="evidence_too_large",
            message="Evidence content exceeds allowed size limits.",
        )

    @app.exception_handler(UrlCanonicalizationError)
    async def url_canonicalization_handler(
        _request: Request,
        _exc: UrlCanonicalizationError,
    ) -> JSONResponse:
        return error_response(
            status_code=422,
            code="url_canonicalization_failed",
            message="URL could not be canonicalized for source identity.",
        )

    @app.exception_handler(CitationIntegrityError)
    async def citation_integrity_handler(
        _request: Request,
        _exc: CitationIntegrityError,
    ) -> JSONResponse:
        return error_response(
            status_code=422,
            code="citation_integrity_failed",
            message="Citation references evidence unavailable to this research job.",
        )

    @app.exception_handler(ClaimEvidenceRequiredError)
    async def claim_evidence_required_handler(
        _request: Request,
        _exc: ClaimEvidenceRequiredError,
    ) -> JSONResponse:
        return error_response(
            status_code=422,
            code="claim_evidence_required",
            message="Every claim must cite at least one evidence item.",
        )

    @app.exception_handler(ReportArtifactConflictError)
    async def report_artifact_conflict_handler(
        _request: Request,
        _exc: ReportArtifactConflictError,
    ) -> JSONResponse:
        return error_response(
            status_code=409,
            code="report_artifact_conflict",
            message=(
                "A final report artifact already exists for this workflow "
                "execution with different content or citations."
            ),
        )

    @app.exception_handler(EmbeddingInvalidRequestError)
    async def embedding_invalid_request_handler(
        _request: Request,
        _exc: EmbeddingInvalidRequestError,
    ) -> JSONResponse:
        return error_response(
            status_code=422,
            code="embedding_invalid_request",
            message="Embedding request validation failed.",
        )

    @app.exception_handler(EmbeddingConflictError)
    async def embedding_conflict_handler(
        _request: Request,
        _exc: EmbeddingConflictError,
    ) -> JSONResponse:
        return error_response(
            status_code=409,
            code="embedding_conflict",
            message="Embedding write conflicted with an existing profile row.",
        )

    @app.exception_handler(EmbeddingAuthConfigError)
    async def embedding_auth_config_handler(
        _request: Request,
        _exc: EmbeddingAuthConfigError,
    ) -> JSONResponse:
        logger.warning("Embedding provider authentication or configuration failed")
        return error_response(
            status_code=503,
            code="embedding_auth_config",
            message="Embedding provider is unavailable.",
        )

    @app.exception_handler(EmbeddingTimeoutError)
    async def embedding_timeout_handler(
        _request: Request,
        _exc: EmbeddingTimeoutError,
    ) -> JSONResponse:
        logger.warning("Embedding provider timed out")
        return error_response(
            status_code=503,
            code="embedding_timeout",
            message="Embedding provider timed out.",
        )

    @app.exception_handler(EmbeddingRateLimitedError)
    async def embedding_rate_limited_handler(
        _request: Request,
        _exc: EmbeddingRateLimitedError,
    ) -> JSONResponse:
        logger.warning("Embedding provider rate limited")
        return error_response(
            status_code=503,
            code="embedding_rate_limited",
            message="Embedding provider is temporarily unavailable.",
        )

    @app.exception_handler(EmbeddingProviderError)
    async def embedding_provider_handler(
        _request: Request,
        _exc: EmbeddingProviderError,
    ) -> JSONResponse:
        logger.warning("Embedding provider failed")
        return error_response(
            status_code=503,
            code="embedding_provider_failed",
            message="Embedding provider failed.",
        )

    @app.exception_handler(EvaluationNotFoundError)
    async def evaluation_not_found_handler(
        _request: Request,
        _exc: EvaluationNotFoundError,
    ) -> JSONResponse:
        return error_response(
            status_code=404,
            code="evaluation_not_found",
            message="Evaluation run not found.",
        )

    @app.exception_handler(EvaluationConflictError)
    async def evaluation_conflict_handler(
        _request: Request,
        _exc: EvaluationConflictError,
    ) -> JSONResponse:
        return error_response(
            status_code=409,
            code="evaluation_conflict",
            message="Evaluation fingerprint conflict.",
        )

    @app.exception_handler(EvaluationInProgressError)
    async def evaluation_in_progress_handler(
        _request: Request,
        _exc: EvaluationInProgressError,
    ) -> JSONResponse:
        return error_response(
            status_code=409,
            code="evaluation_in_progress",
            message="Evaluation is already in progress.",
        )
