"""Public evaluation API representation of frozen profile identities."""

from __future__ import annotations

from atlas.api.schemas.evaluation import (
    EvaluationDetailResponse,
    EvaluationSummaryResponse,
)
from atlas.evaluation.contracts import (
    EVALUATION_PROFILE_CANDIDATE,
    EVALUATION_PROFILE_CANDIDATE_FAKE,
    EVALUATION_PROFILE_V1,
    EvaluationProfile,
    EvaluationRunResult,
)

_PROFILES: tuple[EvaluationProfile, ...] = (
    EVALUATION_PROFILE_CANDIDATE,
    EVALUATION_PROFILE_CANDIDATE_FAKE,
    EVALUATION_PROFILE_V1,
)


def _result(profile: EvaluationProfile) -> EvaluationRunResult:
    return EvaluationRunResult(
        run_id="run-1",
        research_job_id="job-1",
        workflow_execution_id="exec-1",
        evaluation_profile=profile,
        evaluation_attempt=1,
        status="SUCCEEDED",
        input_fingerprint="a" * 64,
        passed=True,
        aggregate_score=1.0,
        disposition_hint="complete",
        dimensions=[],
        grader_versions={"semantic_groundedness": "semantic_groundedness.v1"},
    )


def test_detail_and_summary_accept_all_approved_profiles() -> None:
    for profile in _PROFILES:
        result = _result(profile)
        detail = EvaluationDetailResponse.from_result(result)
        dumped = detail.model_dump()
        assert dumped["evaluation_profile"] == profile
        assert "input_fingerprint" not in dumped
        summary = EvaluationSummaryResponse(
            passed=True,
            aggregate_score=1.0,
            profile=profile,
            disposition_hint="complete",
        )
        assert summary.model_dump()["profile"] == profile
        assert "input_fingerprint" not in summary.model_dump()
