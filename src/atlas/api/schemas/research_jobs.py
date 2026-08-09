"""Pydantic contracts for research-job HTTP APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from atlas.domain import ResearchJob, ResearchJobStatus

MIN_RESEARCH_QUESTION_LENGTH = 1
MAX_RESEARCH_QUESTION_LENGTH = 8000

NormalizedResearchQuestion = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=MIN_RESEARCH_QUESTION_LENGTH,
        max_length=MAX_RESEARCH_QUESTION_LENGTH,
    ),
]


class CreateResearchJobRequest(BaseModel):
    """Request body for creating a research job."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {"question": "What are the main risks of multi-agent research systems?"}
            ]
        }
    )

    question: NormalizedResearchQuestion


class ResearchJobResponse(BaseModel):
    """Public representation of a research job."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "id": "11111111-1111-4111-8111-111111111111",
                    "question": "What are the main risks of multi-agent systems?",
                    "status": "PENDING",
                    "created_at": "2026-08-08T12:00:00Z",
                    "updated_at": "2026-08-08T12:00:00Z",
                    "started_at": None,
                    "finished_at": None,
                    "result": None,
                    "failure_reason": None,
                }
            ]
        }
    )

    id: str
    question: str
    status: ResearchJobStatus
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    result: str | None
    failure_reason: str | None

    @classmethod
    def from_domain(cls, job: ResearchJob) -> Self:
        """Build an API response from a domain research job."""
        return cls(
            id=job.id,
            question=job.question,
            status=job.status,
            created_at=job.created_at,
            updated_at=job.updated_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
            result=job.result,
            failure_reason=job.failure_reason,
        )


class ErrorBody(BaseModel):
    """Machine-readable error payload."""

    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    """Structured Atlas API error envelope."""

    error: ErrorBody
