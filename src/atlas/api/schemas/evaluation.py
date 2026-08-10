"""Pydantic contracts for evaluation HTTP APIs."""

from __future__ import annotations

from typing import Annotated, Self

from pydantic import BaseModel, Field

from atlas.evaluation.contracts import (
    DimensionResult,
    DispositionHint,
    EvaluationProfile,
    EvaluationRunResult,
    EvaluationRunStatus,
)


class EvaluationSummaryResponse(BaseModel):
    """Sanitized evaluation summary attached to research-job responses."""

    passed: bool | None
    aggregate_score: Annotated[float, Field(ge=0.0, le=1.0)] | None
    profile: EvaluationProfile
    disposition_hint: DispositionHint | None


class EvaluationDetailResponse(BaseModel):
    """Sanitized evaluation detail without tokens, fingerprints, or bodies."""

    run_id: str
    research_job_id: str
    workflow_execution_id: str
    evaluation_profile: EvaluationProfile
    evaluation_attempt: int
    status: EvaluationRunStatus
    passed: bool | None
    aggregate_score: Annotated[float, Field(ge=0.0, le=1.0)] | None
    disposition_hint: DispositionHint | None
    dimensions: list[DimensionResult] = Field(default_factory=list)
    grader_versions: dict[str, str] = Field(default_factory=dict)

    @classmethod
    def from_result(cls, result: EvaluationRunResult) -> Self:
        """Build a public detail response from a durable evaluation result."""
        return cls(
            run_id=result.run_id,
            research_job_id=result.research_job_id,
            workflow_execution_id=result.workflow_execution_id,
            evaluation_profile=result.evaluation_profile,
            evaluation_attempt=result.evaluation_attempt,
            status=result.status,
            passed=result.passed,
            aggregate_score=result.aggregate_score,
            disposition_hint=result.disposition_hint,
            dimensions=list(result.dimensions),
            grader_versions=dict(result.grader_versions),
        )
