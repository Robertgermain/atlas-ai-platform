"""Semantic groundedness grader: Fake offline and LangChain live (Slice 15C1).

Default worker composition remains ``skipped``. Live mode is explicit
``ATLAS_SEMANTIC_GRADER_MODE=live`` and uses Atlas model composition, never
direct provider SDK business logic.
"""

from __future__ import annotations

from collections.abc import Sequence

from atlas.evaluation.aggregation import SEMANTIC_PASS_THRESHOLD, weight_for
from atlas.evaluation.contracts import DimensionResult
from atlas.evaluation.graders import FakeSemanticGroundednessGrader
from atlas.evaluation.semantic_contracts import (
    LIVE_SEMANTIC_GRADER_VERSION,
    SEMANTIC_FAILURE_UNCLEAR,
    SEMANTIC_FAILURE_UNSUPPORTED,
    SEMANTIC_FAILURE_WEAK,
    SEMANTIC_PROMPT_VERSION,
    SemanticGradeRequest,
    SemanticGroundednessOutput,
    support_label_for_score,
)
from atlas.models.service import ModelInvocationService
from atlas.observability.langsmith import attach_run_metadata

__all__ = [
    "FakeSemanticGroundednessGrader",
    "LangChainSemanticGroundednessGrader",
    "LiveSemanticGroundednessGrader",
    "SemanticGroundednessOutput",
    "aggregate_semantic_claim_scores",
    "dimension_from_semantic_output",
    "support_label_for_score",
]


def aggregate_semantic_claim_scores(scores: Sequence[float]) -> float:
    """Atlas-owned semantic aggregate: arithmetic mean of per-claim scores.

    Empty claims are an explicit deterministic pass (``1.0``). This is the
    empty-claim path; it is not a mean of an empty sequence.
    """
    if not scores:
        return 1.0
    return sum(scores) / len(scores)


def dimension_from_semantic_output(
    output: SemanticGroundednessOutput,
) -> DimensionResult:
    """Map validated per-claim scores onto the provisional semantic dimension.

    Ordinal correctness is enforced in ``ModelInvocationService.evaluate_semantic``
    before the ledger is marked SUCCEEDED. The dimension score is the Atlas
    arithmetic mean. Support labels are derived from that mapping. A passing
    dimension never carries failure codes; unsupported claims do not veto the
    mean.
    """
    score = aggregate_semantic_claim_scores([item.score for item in output.claims])
    passed = score >= SEMANTIC_PASS_THRESHOLD
    codes: list[str] = []
    if not passed:
        labels = {support_label_for_score(item.score) for item in output.claims}
        if "unsupported" in labels:
            codes.append(SEMANTIC_FAILURE_UNSUPPORTED)
        if "unclear" in labels:
            codes.append(SEMANTIC_FAILURE_UNCLEAR)
        if not codes:
            codes.append(SEMANTIC_FAILURE_WEAK)
    return DimensionResult(
        name="semantic_groundedness",
        score=score,
        passed=passed,
        method="llm",
        is_hard=False,
        is_provisional=True,
        failure_codes=codes,
        weight=weight_for("semantic_groundedness", semantic_present=True),
    )


class LangChainSemanticGroundednessGrader:
    """Live semantic grader over ``ModelInvocationService.evaluate_semantic``."""

    version: str = LIVE_SEMANTIC_GRADER_VERSION
    prompt_version: str = SEMANTIC_PROMPT_VERSION

    def __init__(
        self,
        service: ModelInvocationService,
        *,
        workflow_execution_id: str,
    ) -> None:
        self._service = service
        self._workflow_execution_id = workflow_execution_id

    def grade(self, request: SemanticGradeRequest) -> DimensionResult:
        if not request.claims:
            result = dimension_from_semantic_output(
                SemanticGroundednessOutput(claims=[])
            )
            attach_run_metadata(
                {
                    "atlas.semantic_grader_outcome": "quality_pass",
                    "atlas.grader_version": self.version,
                    "atlas.prompt_version": self.prompt_version,
                }
            )
            return result

        output, _meta = self._service.evaluate_semantic(
            request,
            workflow_execution_id=self._workflow_execution_id,
        )
        result = dimension_from_semantic_output(output)
        attach_run_metadata(
            {
                "atlas.semantic_grader_outcome": (
                    "quality_pass" if result.passed else "quality_fail"
                ),
                "atlas.grader_version": self.version,
                "atlas.prompt_version": self.prompt_version,
                "atlas.evaluation_passed": result.passed,
                "atlas.evaluation_score": result.score,
            }
        )
        return result


LiveSemanticGroundednessGrader = LangChainSemanticGroundednessGrader
