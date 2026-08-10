"""HTTP routes for operator review decisions."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, Response, status
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import IntegrityError, OperationalError

from atlas.api.deps import provide_review_service, provide_settings
from atlas.api.schemas.research_jobs import ErrorResponse
from atlas.api.schemas.review import ReviewDecisionRequest, ReviewDecisionResponse
from atlas.application.review import (
    ReviewConflictError,
    ReviewNotFoundError,
    ReviewReadinessError,
    ReviewService,
)
from atlas.config.settings import Settings

MAX_IDEMPOTENCY_KEY_LENGTH = 128

router = APIRouter(
    prefix="/research-jobs",
    tags=["review"],
)


@router.post(
    "/{job_id}/review-decisions",
    response_model=ReviewDecisionResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        202: {"model": ReviewDecisionResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
def create_review_decision(
    job_id: str,
    body: ReviewDecisionRequest,
    response: Response,
    service: Annotated[ReviewService, Depends(provide_review_service)],
    settings: Annotated[Settings, Depends(provide_settings)],
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=MAX_IDEMPOTENCY_KEY_LENGTH,
        ),
    ],
) -> ReviewDecisionResponse:
    """Submit a human review decision for a job awaiting review."""
    if not settings.review_api_enabled:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Not Found")

    cleaned_key = idempotency_key.strip()
    if not cleaned_key:
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

    try:
        decision_id, decision_status = service.submit_decision(
            job_id=job_id,
            decision=body.decision,
            actor_id=body.actor_id,
            idempotency_key=cleaned_key,
            evaluation_run_id=body.evaluation_run_id,
        )
    except ReviewNotFoundError as exc:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Not Found") from exc
    except ReviewReadinessError as exc:
        from fastapi import HTTPException

        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ReviewConflictError as exc:
        from fastapi import HTTPException

        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except IntegrityError as exc:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=409,
            detail="Review decision conflict",
        ) from exc
    except OperationalError as exc:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=503,
            detail="Database temporarily unavailable",
        ) from exc
    except ValueError as exc:
        raise RequestValidationError(
            [
                {
                    "type": "value_error",
                    "loc": ("body",),
                    "msg": str(exc),
                    "input": body.model_dump(),
                }
            ]
        ) from exc

    response.status_code = status.HTTP_202_ACCEPTED
    return ReviewDecisionResponse(
        id=decision_id,
        research_job_id=job_id,
        decision=body.decision,
        actor_id=body.actor_id,
        status=decision_status,
    )
