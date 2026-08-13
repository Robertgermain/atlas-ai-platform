"""Fake semantic grader and skipped-dimension behavior."""

from __future__ import annotations

from atlas.evaluation.aggregation import aggregate_dimensions
from atlas.evaluation.contracts import DimensionResult, EvaluationCandidateInput
from atlas.evaluation.graders import (
    FakeSemanticGroundednessGrader,
    skipped_semantic_dimension,
)
from atlas.evaluation.llm_grader import FakeSemanticGroundednessGrader as ReexportedFake
from atlas.evidence.contracts import ClaimStructured
from tests.evaluation.semantic_helpers import semantic_request_for_candidate


def _candidate(
    *,
    claims: list[ClaimStructured] | None = None,
    evidence_item_ids: list[str] | None = None,
) -> EvaluationCandidateInput:
    return EvaluationCandidateInput(
        job_id="job-llm-fake",
        question="Semantic grader fixture",
        plan=["Clarify evaluationgate scope"],
        findings=["Clarify evaluationgate scope observed"],
        draft="Clarify evaluationgate scope in the draft.",
        claims=list(claims or []),
        evidence_item_ids=list(evidence_item_ids or []),
    )


def test_fake_semantic_groundedness_grader_works() -> None:
    assert FakeSemanticGroundednessGrader is ReexportedFake
    grader = FakeSemanticGroundednessGrader()
    linked = _candidate(
        claims=[ClaimStructured(text="Supported", evidence_item_ids=["ev-1"])],
        evidence_item_ids=["ev-1"],
    )
    ok = grader.grade(semantic_request_for_candidate(linked, {"ev-1"}))
    assert ok.name == "semantic_groundedness"
    assert ok.method == "llm"
    assert ok.passed is True
    assert ok.score == 1.0
    assert ok.failure_codes == []

    weak = grader.grade(semantic_request_for_candidate(linked, set()))
    assert weak.passed is False
    assert weak.score == 0.5
    assert "SEMANTIC_UNCLEAR" in weak.failure_codes


def test_skipped_semantic_dimension_when_grader_none() -> None:
    skipped = skipped_semantic_dimension()
    assert skipped.method == "skipped"
    assert skipped.passed is True
    assert skipped.weight == 0.0

    dimensions = [
        DimensionResult(
            name="citation_integrity",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=True,
            is_provisional=False,
        ),
        DimensionResult(
            name="tool_use",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=True,
            is_provisional=False,
        ),
        DimensionResult(
            name="report_structure",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=True,
            is_provisional=True,
        ),
        DimensionResult(
            name="coverage",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=False,
            is_provisional=True,
        ),
        DimensionResult(
            name="completeness",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=False,
            is_provisional=True,
        ),
        DimensionResult(
            name="lexical_id_groundedness",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=False,
            is_provisional=True,
        ),
        skipped,
    ]
    aggregate, passed, stamped = aggregate_dimensions(dimensions)
    assert passed is True
    assert aggregate == 1.0
    semantic = next(item for item in stamped if item.name == "semantic_groundedness")
    assert semantic.method == "skipped"
    assert semantic.passed is True
    assert semantic.weight == 0.0
