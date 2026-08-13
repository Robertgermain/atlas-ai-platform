"""Quality vs availability for the semantic grader (Slice 15C1)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy import select, text
from sqlalchemy.orm import Session, sessionmaker

from atlas.api.schemas.evaluation import EvaluationDetailResponse
from atlas.domain import ResearchJob
from atlas.evaluation.contracts import (
    EVALUATION_PROFILE,
    DimensionResult,
    EvaluationCandidateInput,
)
from atlas.evaluation.graders import FakeSemanticGroundednessGrader
from atlas.evaluation.runner import EvaluationRunner
from atlas.evaluation.semantic_contracts import SemanticGradeRequest
from atlas.evaluation.service import EvaluationService
from atlas.evidence.contracts import ClaimStructured
from atlas.models.errors import (
    ModelAttemptOwnershipLostError,
    ModelAuthConfigError,
    ModelInvalidStructuredOutputError,
    ModelTimeoutError,
)
from atlas.observability.metrics.catalog import AtlasMetrics
from atlas.persistence.db import session_scope
from atlas.persistence.models.evaluation import EvaluationRunModel
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository
from atlas.recovery.policy import AttemptCounts, decide_for_exception
from atlas.workflow.graph import evaluate_node


def _create_job_and_execution(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
) -> str:
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, "Semantic availability question"),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="a" * 64,
        )
        return workflow_repo.create_execution(
            session,
            research_job_id=job_id,
            thread_id=job_id,
            at=at,
        )


def _claim_for_job(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
) -> str:
    import secrets

    token = secrets.token_hex(32)
    with session_scope(session_factory) as session:
        session.execute(
            text(
                """
                UPDATE research_jobs
                SET status = 'RUNNING',
                    started_at = COALESCE(started_at, NOW()),
                    updated_at = NOW(),
                    claim_token = :token,
                    lease_expires_at = :lease
                WHERE id = :job_id
                """
            ),
            {
                "token": token,
                "lease": datetime.now(UTC) + timedelta(minutes=5),
                "job_id": job_id,
            },
        )
    return token


def _candidate(job_id: str) -> EvaluationCandidateInput:
    return EvaluationCandidateInput(
        job_id=job_id,
        question="Runner finalization probe",
        plan=["Clarify research scope carefully"],
        findings=["Clarify research scope carefully in findings"],
        draft="Clarify research scope carefully in the draft.",
        claims=[],
        evidence_item_ids=[],
        tool_summary=[],
        evaluation_profile=EVALUATION_PROFILE,
    )


def _sample_count(metrics: AtlasMetrics, metric_name: str, **labels: str) -> float:
    total = 0.0
    for family in metrics.registry.collect():
        for sample in family.samples:
            if sample.name != metric_name:
                continue
            if all(sample.labels.get(k) == v for k, v in labels.items()):
                total += sample.value
    return total


class _RaisingSemanticGrader:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc
        self.version = "semantic_groundedness.v1"

    def grade(self, request: SemanticGradeRequest) -> DimensionResult:
        del request
        raise self._exc


def test_valid_quality_fail_succeeds_with_semantic_dimension(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = f"sem-quality-fail-{uuid4()}"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    claim = _claim_for_job(session_factory, job_id=job_id)
    metrics = AtlasMetrics(CollectorRegistry())
    service = EvaluationService(session_factory=session_factory, metrics=metrics)
    runner = EvaluationRunner(
        evaluation_service=service,
        semantic_grader=FakeSemanticGroundednessGrader(),
        load_excerpt_sources=lambda _ids: [],
        metrics=metrics,
    )
    candidate = EvaluationCandidateInput(
        job_id=job_id,
        question="Quality fail probe with enough tokens here",
        plan=["Clarify quality fail scope carefully"],
        findings=["Clarify quality fail scope carefully in findings"],
        draft="Clarify quality fail scope carefully in the draft.",
        claims=[
            ClaimStructured(text="Unsupported claim text", evidence_item_ids=["ev-1"])
        ],
        evidence_item_ids=["ev-1"],
        evaluation_profile=EVALUATION_PROFILE,
    )
    result = runner.run(
        candidate=candidate,
        workflow_execution_id=execution_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=claim,
    )
    assert result.status == "SUCCEEDED"
    semantic = next(
        item for item in result.dimensions if item.name == "semantic_groundedness"
    )
    assert semantic.passed is False
    assert semantic.method == "llm"
    detail = EvaluationDetailResponse.from_result(result)
    assert detail.status == "SUCCEEDED"
    assert any(item.name == "semantic_groundedness" for item in detail.dimensions)
    assert (
        _sample_count(
            metrics, "atlas_semantic_grader_outcomes_total", outcome="quality_fail"
        )
        == 1
    )
    assert (
        _sample_count(
            metrics,
            "atlas_evaluation_dimension_outcomes_total",
            dimension="semantic_groundedness",
            outcome="failed",
        )
        == 1
    )


def test_timeout_fails_evaluation_without_semantic_dimension(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = f"sem-timeout-{uuid4()}"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    claim = _claim_for_job(session_factory, job_id=job_id)
    metrics = AtlasMetrics(CollectorRegistry())
    service = EvaluationService(session_factory=session_factory, metrics=metrics)
    runner = EvaluationRunner(
        evaluation_service=service,
        semantic_grader=_RaisingSemanticGrader(ModelTimeoutError()),
        metrics=metrics,
    )
    with pytest.raises(ModelTimeoutError):
        runner.run(
            candidate=_candidate(job_id),
            workflow_execution_id=execution_id,
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            job_claim_token=claim,
        )
    with session_scope(session_factory) as session:
        row = session.execute(select(EvaluationRunModel)).scalar_one()
        assert row.status == "FAILED"
        result = EvaluationService(session_factory=session_factory).get_latest_for_job(
            job_id
        )
    assert result is not None
    assert result.status == "FAILED"
    assert result.dimensions == []
    detail = EvaluationDetailResponse.from_result(result)
    assert detail.status == "FAILED"
    assert detail.dimensions == []
    assert "semantic_groundedness" not in detail.grader_versions
    assert (
        _sample_count(
            metrics, "atlas_semantic_grader_outcomes_total", outcome="timeout"
        )
        == 1
    )
    assert (
        _sample_count(
            metrics,
            "atlas_evaluation_dimension_outcomes_total",
            dimension="semantic_groundedness",
        )
        == 0
    )
    assert (
        _sample_count(
            metrics,
            "atlas_evaluation_runs_total",
            profile="evaluation.candidate.v1",
            outcome="failed",
        )
        == 1
    )


def test_auth_and_repeated_malformed_are_permanent(
    session_factory: sessionmaker[Session],
) -> None:
    counts = AttemptCounts(
        repair_count=0, job_retry_count=0, evaluation_attempt_count=0
    )
    timeout = decide_for_exception(exc=ModelTimeoutError(), counts=counts)
    assert timeout.action == "retry"
    auth = decide_for_exception(exc=ModelAuthConfigError(), counts=counts)
    assert auth.action == "terminal"
    malformed = decide_for_exception(
        exc=ModelInvalidStructuredOutputError(), counts=counts
    )
    assert malformed.action == "terminal"

    job_id = f"sem-auth-{uuid4()}"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    claim = _claim_for_job(session_factory, job_id=job_id)
    metrics = AtlasMetrics(CollectorRegistry())
    service = EvaluationService(session_factory=session_factory, metrics=metrics)
    runner = EvaluationRunner(
        evaluation_service=service,
        semantic_grader=_RaisingSemanticGrader(ModelAuthConfigError()),
        metrics=metrics,
    )
    with pytest.raises(ModelAuthConfigError):
        runner.run(
            candidate=_candidate(job_id),
            workflow_execution_id=execution_id,
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            job_claim_token=claim,
        )
    assert (
        _sample_count(
            metrics, "atlas_semantic_grader_outcomes_total", outcome="auth_config"
        )
        == 1
    )
    assert (
        _sample_count(
            metrics,
            "atlas_evaluation_dimension_outcomes_total",
            dimension="semantic_groundedness",
        )
        == 0
    )


def test_ownership_lost_does_not_finalize_failure(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = f"sem-own-run-{uuid4()}"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    claim = _claim_for_job(session_factory, job_id=job_id)
    metrics = AtlasMetrics(CollectorRegistry())
    service = EvaluationService(session_factory=session_factory, metrics=metrics)
    runner = EvaluationRunner(
        evaluation_service=service,
        semantic_grader=_RaisingSemanticGrader(ModelAttemptOwnershipLostError()),
        metrics=metrics,
    )
    with pytest.raises(ModelAttemptOwnershipLostError):
        runner.run(
            candidate=_candidate(job_id),
            workflow_execution_id=execution_id,
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            job_claim_token=claim,
        )
    with session_scope(session_factory) as session:
        row = session.execute(select(EvaluationRunModel)).scalar_one()
        assert row.status == "IN_PROGRESS"
    assert (
        _sample_count(
            metrics,
            "atlas_semantic_grader_outcomes_total",
            outcome="ownership_lost",
        )
        == 1
    )
    assert _sample_count(metrics, "atlas_evaluation_runs_total", outcome="failed") == 0


def test_skipped_observes_skipped_not_quality_failure(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = f"sem-skip-{uuid4()}"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    claim = _claim_for_job(session_factory, job_id=job_id)
    metrics = AtlasMetrics(CollectorRegistry())
    service = EvaluationService(session_factory=session_factory, metrics=metrics)
    runner = EvaluationRunner(
        evaluation_service=service,
        metrics=metrics,
    )
    result = runner.run(
        candidate=_candidate(job_id),
        workflow_execution_id=execution_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=claim,
    )
    assert result.status == "SUCCEEDED"
    semantic = next(
        item for item in result.dimensions if item.name == "semantic_groundedness"
    )
    assert semantic.method == "skipped"
    assert semantic.weight == 0.0
    assert (
        _sample_count(
            metrics, "atlas_semantic_grader_outcomes_total", outcome="skipped"
        )
        == 1
    )
    assert (
        _sample_count(
            metrics,
            "atlas_evaluation_dimension_outcomes_total",
            dimension="semantic_groundedness",
            outcome="failed",
        )
        == 0
    )


def test_evaluate_node_does_not_wrap_model_error() -> None:
    class _Runner:
        def provenance_ok_for_claims(self, **_kwargs: object) -> bool:
            return True

        def run(self, **_kwargs: object) -> object:
            raise ModelTimeoutError()

    state = {
        "job_id": "job-node",
        "question": "Evaluate node model error probe",
        "plan": ["Clarify node error scope carefully"],
        "findings": ["Clarify node error scope carefully in findings"],
        "draft": "Clarify node error scope carefully in the draft.",
        "claims": [],
        "evidence_item_ids": [],
        "tool_summary": [],
        "repair_count": 0,
        "evaluation_attempt": 0,
    }
    runtime = SimpleNamespace(
        context=SimpleNamespace(
            evaluation_runner=_Runner(),
            workflow_execution_id="exec-node",
            job_claim_token="c" * 64,
        )
    )
    with pytest.raises(ModelTimeoutError):
        evaluate_node(state, runtime)  # type: ignore[arg-type]
