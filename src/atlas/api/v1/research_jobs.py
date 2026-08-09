"""HTTP routes for research jobs."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, status
from fastapi.exceptions import RequestValidationError

from atlas.api.deps import provide_research_job_service
from atlas.api.schemas.research_jobs import (
    CreateResearchJobRequest,
    ErrorResponse,
    ResearchJobResponse,
)
from atlas.application.research_jobs import ResearchJobService

MAX_IDEMPOTENCY_KEY_LENGTH = 128

router = APIRouter(prefix="/research-jobs", tags=["research-jobs"])


def _validated_idempotency_key(raw_key: str) -> str:
    cleaned = raw_key.strip()
    if not cleaned:
        raise RequestValidationError(
            [
                {
                    "type": "string_too_short",
                    "loc": ("header", "Idempotency-Key"),
                    "msg": "Idempotency-Key must be a non-empty string.",
                    "input": "",
                }
            ]
        )
    if len(cleaned) > MAX_IDEMPOTENCY_KEY_LENGTH:
        raise RequestValidationError(
            [
                {
                    "type": "string_too_long",
                    "loc": ("header", "Idempotency-Key"),
                    "msg": (
                        "Idempotency-Key must be at most "
                        f"{MAX_IDEMPOTENCY_KEY_LENGTH} characters."
                    ),
                    "input": "",
                }
            ]
        )
    return cleaned


@router.post(
    "",
    response_model=ResearchJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        409: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
def create_research_job(
    body: CreateResearchJobRequest,
    service: Annotated[ResearchJobService, Depends(provide_research_job_service)],
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=MAX_IDEMPOTENCY_KEY_LENGTH,
        ),
    ],
) -> ResearchJobResponse:
    """Create a PENDING research job or replay a matching idempotent request."""
    cleaned_key = _validated_idempotency_key(idempotency_key)
    job = service.submit(body.question, idempotency_key=cleaned_key)
    return ResearchJobResponse.from_domain(job)


@router.get(
    "/{job_id}",
    response_model=ResearchJobResponse,
    responses={
        404: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
def get_research_job(
    job_id: str,
    service: Annotated[ResearchJobService, Depends(provide_research_job_service)],
) -> ResearchJobResponse:
    """Return a research job by id."""
    job = service.get(job_id)
    return ResearchJobResponse.from_domain(job)
