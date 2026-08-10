"""Opt-in live evaluation placeholder — deferred, not verification evidence."""

from __future__ import annotations

import pytest

from atlas.evaluation.contracts import EvaluationCandidateInput
from atlas.evaluation.errors import EvaluationTerminalError
from atlas.evaluation.llm_grader import DeferredSemanticGroundednessPort


def test_deferred_semantic_port_fails_closed() -> None:
    """Live semantic evaluation is deferred; the scaffold must not pretend to work."""
    grader = DeferredSemanticGroundednessPort()
    candidate = EvaluationCandidateInput(
        job_id="deferred-job",
        question="Deferred semantic probe",
        plan=["Review market outlook"],
        findings=["Market outlook remains mixed"],
        draft="Market outlook remains mixed in this draft.",
    )
    with pytest.raises(EvaluationTerminalError):
        grader.grade(candidate)


@pytest.mark.skip(
    reason=(
        "Live LangChain semantic groundedness is deferred until later in "
        "Milestone 12; a skipped placeholder is not live-verification evidence."
    )
)
def test_live_semantic_eval_deferred_not_implemented() -> None:
    raise AssertionError("live semantic grader is not implemented")
