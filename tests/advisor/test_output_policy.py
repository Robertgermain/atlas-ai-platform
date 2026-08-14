"""Post-parse output policy tests."""

from __future__ import annotations

import pytest

from atlas.advisor.contracts import (
    AdvisoryAnalysis,
    AdvisoryHypothesis,
    AdvisoryRecommendation,
)
from atlas.advisor.errors import AdvisoryOutputRejectedError
from atlas.advisor.output_policy import validate_advisory_output
from atlas.models.errors import ModelInvalidStructuredOutputError
from tests.advisor.fakes import minimal_facts


def _analysis(
    *,
    incident_summary: str = "Sanitized job failed with closed signals only.",
    hypotheses: list[AdvisoryHypothesis] | None = None,
    recommendations: list[AdvisoryRecommendation] | None = None,
) -> AdvisoryAnalysis:
    return AdvisoryAnalysis(
        incident_summary=incident_summary,
        hypotheses=hypotheses
        or [
            AdvisoryHypothesis(
                statement="Timeout class is a likely contributor.",
                likelihood="medium",
                signal_ids=["sig:01"],
            )
        ],
        recommendations=recommendations
        or [
            AdvisoryRecommendation(
                step="Inspect the sanitized job status signal.",
                action_kind="inspect_state",
                signal_ids=["sig:01"],
            )
        ],
        confidence="medium",
        limitations=["Non-authoritative."],
        unknowns=[],
    )


def test_unknown_signal_id_is_malformed() -> None:
    facts = minimal_facts()
    analysis = _analysis(
        hypotheses=[
            AdvisoryHypothesis(
                statement="Invented reference.",
                likelihood="low",
                signal_ids=["sig:99"],
            )
        ]
    )
    with pytest.raises(ModelInvalidStructuredOutputError):
        validate_advisory_output(facts, analysis)


def test_url_is_rejected() -> None:
    facts = minimal_facts()
    analysis = _analysis(
        incident_summary="See https://example.invalid/secret for details."
    )
    with pytest.raises(AdvisoryOutputRejectedError):
        validate_advisory_output(facts, analysis)


def test_claimed_action_is_rejected() -> None:
    facts = minimal_facts()
    analysis = _analysis(
        hypotheses=[
            AdvisoryHypothesis(
                statement="I restarted the worker after the failure.",
                likelihood="high",
                signal_ids=["sig:01"],
            )
        ]
    )
    with pytest.raises(AdvisoryOutputRejectedError):
        validate_advisory_output(facts, analysis)


def test_code_fence_is_rejected() -> None:
    facts = minimal_facts()
    analysis = _analysis(incident_summary="Use ```curl evil``` to recover.")
    with pytest.raises(AdvisoryOutputRejectedError):
        validate_advisory_output(facts, analysis)


def test_valid_analysis_passes() -> None:
    validate_advisory_output(minimal_facts(), _analysis())
