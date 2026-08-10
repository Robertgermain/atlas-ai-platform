"""HTTP routes for research jobs."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, status
from fastapi.exceptions import RequestValidationError

from atlas.api.deps import (
    provide_evaluation_service,
    provide_report_artifact_service,
    provide_research_job_service,
)
from atlas.api.schemas.evaluation import (
    EvaluationDetailResponse,
    EvaluationSummaryResponse,
)
from atlas.api.schemas.evidence import JobCitationsHttpResponse
from atlas.api.schemas.research_jobs import (
    CreateResearchJobRequest,
    ErrorResponse,
    ResearchJobResponse,
)
from atlas.application.research_jobs import ResearchJobService
from atlas.evaluation.contracts import EvaluationRunResult
from atlas.evaluation.errors import EvaluationNotFoundError
from atlas.evaluation.service import EvaluationService
from atlas.evidence.service import ReportArtifactService

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


def _summary_from_run(run: EvaluationRunResult) -> EvaluationSummaryResponse:
    return EvaluationSummaryResponse(
        passed=run.passed,
        aggregate_score=run.aggregate_score,
        profile=run.evaluation_profile,
        disposition_hint=run.disposition_hint,
    )


def _prefer_succeeded_or_latest(
    runs: list[EvaluationRunResult],
) -> EvaluationRunResult | None:
    if not runs:
        return None
    succeeded = [run for run in runs if run.status == "SUCCEEDED"]
    if succeeded:
        return succeeded[-1]
    return runs[-1]


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
    evaluation_service: Annotated[
        EvaluationService,
        Depends(provide_evaluation_service),
    ],
) -> ResearchJobResponse:
    """Return a research job by id, optionally with evaluation summary."""
    job = service.get(job_id)
    response = ResearchJobResponse.from_domain(job)
    selected = _prefer_succeeded_or_latest(evaluation_service.get_by_job(job_id))
    if selected is None:
        return response
    return response.model_copy(
        update={"evaluation_summary": _summary_from_run(selected)}
    )


@router.get(
    "/{job_id}/evaluation",
    response_model=EvaluationDetailResponse,
    responses={
        404: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
def get_research_job_evaluation(
    job_id: str,
    job_service: Annotated[ResearchJobService, Depends(provide_research_job_service)],
    evaluation_service: Annotated[
        EvaluationService,
        Depends(provide_evaluation_service),
    ],
) -> EvaluationDetailResponse:
    """Return the latest SUCCEEDED evaluation run, else the latest run detail."""
    job_service.get(job_id)
    selected = _prefer_succeeded_or_latest(evaluation_service.get_by_job(job_id))
    if selected is None:
        raise EvaluationNotFoundError()
    return EvaluationDetailResponse.from_result(selected)


@router.get(
    "/{job_id}/citations",
    response_model=JobCitationsHttpResponse,
    responses={
        404: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
def get_research_job_citations(
    job_id: str,
    job_service: Annotated[ResearchJobService, Depends(provide_research_job_service)],
    report_service: Annotated[
        ReportArtifactService,
        Depends(provide_report_artifact_service),
    ],
) -> JobCitationsHttpResponse:
    """Return claim → evidence → document → source citations for a job."""
    # Ensure the job exists (404) even when no artifact has been written yet.
    job_service.get(job_id)
    payload = report_service.get_job_citations(job_id)
    return JobCitationsHttpResponse.from_domain(payload)
