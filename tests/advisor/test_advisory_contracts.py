"""Closed schema and illegal signal-combination tests."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from atlas.advisor.contracts import (
    AdvisoryAnalysis,
    AdvisoryIncidentFacts,
    EvaluationRunSignal,
    JobStatusSignal,
    ModelOutcomeCountSignal,
    RecoveryDecisionSignal,
)
from tests.advisor.fakes import minimal_facts


def test_facts_reject_extra_fields() -> None:
    with pytest.raises(ValidationError):
        AdvisoryIncidentFacts.model_validate(
            {
                "facts_version": "advisory.incident.v1",
                "research_job_id": "job-1",
                "signals": [
                    {
                        "signal_type": "job.status",
                        "signal_id": "sig:01",
                        "status": "FAILED",
                    }
                ],
                "question": "must-not-appear",
            }
        )


def test_job_status_rejects_unrelated_count() -> None:
    with pytest.raises(ValidationError):
        JobStatusSignal.model_validate(
            {
                "signal_type": "job.status",
                "signal_id": "sig:01",
                "status": "FAILED",
                "count": 3,
            }
        )


def test_evaluation_run_succeeded_requires_score() -> None:
    with pytest.raises(ValidationError):
        EvaluationRunSignal.model_validate(
            {
                "signal_type": "evaluation.run",
                "signal_id": "sig:01",
                "status": "SUCCEEDED",
                "profile": "evaluation.candidate.v1",
            }
        )


def test_unknown_signal_type_rejected() -> None:
    with pytest.raises(ValidationError):
        AdvisoryIncidentFacts.model_validate(
            {
                "facts_version": "advisory.incident.v1",
                "research_job_id": "job-1",
                "signals": [
                    {
                        "signal_type": "invented.kind",
                        "signal_id": "sig:01",
                    }
                ],
            }
        )


def test_model_provider_must_be_closed() -> None:
    with pytest.raises(ValidationError):
        ModelOutcomeCountSignal.model_validate(
            {
                "signal_type": "model.outcome_count",
                "signal_id": "sig:01",
                "node_name": "plan",
                "provider": "evil-provider",
                "status": "FAILED",
                "count": 1,
            }
        )


def test_analysis_rejects_mutation_action_kind() -> None:
    with pytest.raises(ValidationError):
        AdvisoryAnalysis.model_validate(
            {
                "schema_version": "advisory.analysis.v1",
                "incident_summary": "summary text without urls",
                "hypotheses": [
                    {
                        "statement": "a hypothesis",
                        "likelihood": "low",
                        "signal_ids": ["sig:01"],
                    }
                ],
                "recommendations": [
                    {
                        "step": "retry the job",
                        "action_kind": "retry",
                        "signal_ids": ["sig:01"],
                    }
                ],
                "confidence": "low",
            }
        )


def test_minimal_facts_are_valid() -> None:
    facts = minimal_facts()
    assert facts.signals[0].signal_id == "sig:01"


def test_retry_requires_attempt_number() -> None:
    with pytest.raises(ValidationError):
        RecoveryDecisionSignal.model_validate(
            {
                "signal_type": "recovery.decision",
                "signal_id": "sig:01",
                "decision": "retry",
                "failure_category": "TRANSIENT_TIMEOUT",
                "reason_code": "TRANSIENT_RETRY",
            }
        )


def test_non_retry_rejects_attempt_number() -> None:
    with pytest.raises(ValidationError):
        RecoveryDecisionSignal.model_validate(
            {
                "signal_type": "recovery.decision",
                "signal_id": "sig:01",
                "decision": "complete",
                "failure_category": "QUALITY_STRUCTURE",
                "reason_code": "EVALUATION_PASSED",
                "attempt_number": 1,
            }
        )
