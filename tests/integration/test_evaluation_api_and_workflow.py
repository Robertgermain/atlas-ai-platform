"""Evaluation HTTP APIs and LangGraph evaluate→policy workflow paths."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from atlas.api.deps import provide_session_factory
from atlas.application.worker import ResearchJobWorker
from atlas.domain import ResearchJob
from atlas.evaluation.contracts import (
    EVALUATION_PROFILE,
    EvaluationCandidateInput,
    EvaluationRunResult,
)
from atlas.main import app
from atlas.persistence.db import reset_engine_cache, session_scope
from atlas.persistence.models.evaluation import EvaluationRunModel
from atlas.persistence.models.evidence import ReportArtifactModel
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.workflow import LangGraphResearchProcessor, create_checkpoint_runtime


def _api_client(session_factory: sessionmaker[Session]) -> TestClient:
    reset_engine_cache()
    app.dependency_overrides[provide_session_factory] = lambda: session_factory
    return TestClient(app)


def test_evaluation_api_404_when_missing(
    session_factory: sessionmaker[Session],
    test_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ATLAS_DATABASE_URL", test_database_url)
    client = _api_client(session_factory)
    job_id = "eval-api-missing"
    with session_scope(session_factory) as session:
        SqlAlchemyResearchJobRepository().add(
            session,
            ResearchJob.create(job_id, "No evaluation yet"),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="1" * 64,
        )
    try:
        response = client.get(f"/v1/research-jobs/{job_id}/evaluation")
        assert response.status_code == 404
        body = response.json()
        assert body["error"]["code"] == "evaluation_not_found"
    finally:
        app.dependency_overrides.clear()
        reset_engine_cache()


def test_evaluation_happy_path_api_and_summary(
    session_factory: sessionmaker[Session],
    test_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ATLAS_DATABASE_URL", test_database_url)
    client = _api_client(session_factory)
    runtime = create_checkpoint_runtime(test_database_url)
    processor = LangGraphResearchProcessor(
        checkpointer=runtime.checkpointer,
        session_factory=session_factory,
    )
    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=SqlAlchemyResearchJobRepository(),
        processor=processor,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=30.0,
        lease_seconds=60.0,
    )
    question = "Slice 12A evaluation happy path cited report"
    try:
        created = client.post(
            "/v1/research-jobs",
            json={"question": question},
            headers={"Idempotency-Key": "eval-happy-key"},
        )
        assert created.status_code == 202
        job_id = created.json()["id"]
        assert worker.run_once() is True

        fetched = client.get(f"/v1/research-jobs/{job_id}")
        assert fetched.status_code == 200
        job_body = fetched.json()
        assert job_body["status"] == "COMPLETED"
        summary = job_body["evaluation_summary"]
        assert summary is not None
        assert summary["passed"] is True
        assert summary["profile"] == EVALUATION_PROFILE

        evaluation = client.get(f"/v1/research-jobs/{job_id}/evaluation")
        assert evaluation.status_code == 200
        detail = evaluation.json()
        assert detail["status"] == "SUCCEEDED"
        assert detail["passed"] is True
        assert detail["dimensions"]

        with session_factory() as session:
            reports = session.execute(
                select(func.count())
                .select_from(ReportArtifactModel)
                .where(ReportArtifactModel.research_job_id == job_id)
            ).scalar_one()
            runs = session.execute(
                select(func.count())
                .select_from(EvaluationRunModel)
                .where(EvaluationRunModel.research_job_id == job_id)
            ).scalar_one()
        assert reports == 1
        assert runs == 1

        citations = client.get(f"/v1/research-jobs/{job_id}/citations")
        assert citations.status_code == 200
        assert citations.json()["report_artifact_id"] is not None
    finally:
        worker.close()
        runtime.close()
        app.dependency_overrides.clear()
        reset_engine_cache()


class _FailingEvaluationRunner:
    """Custom runner that returns a durable-looking failed evaluation."""

    def run(
        self,
        *,
        candidate: EvaluationCandidateInput,
        workflow_execution_id: str,
        deadline: datetime,
        job_claim_token: str,
        provenance_ok: bool = True,
    ) -> EvaluationRunResult:
        del deadline, provenance_ok, job_claim_token
        return EvaluationRunResult(
            run_id=str(uuid4()),
            research_job_id=candidate.job_id,
            workflow_execution_id=workflow_execution_id or "fake-execution",
            evaluation_profile=candidate.evaluation_profile,
            evaluation_attempt=candidate.evaluation_attempt,
            status="SUCCEEDED",
            input_fingerprint="2" * 64,
            passed=False,
            aggregate_score=0.2,
            disposition_hint="terminal",
            dimensions=[],
            grader_versions={},
        )

    def provenance_ok_for_claims(
        self,
        *,
        job_id: str,
        claims: list[Any],
    ) -> bool:
        del job_id, claims
        return True


class _FailingEvalProcessor(LangGraphResearchProcessor):
    def _build_context(
        self,
        *,
        workflow_execution_id: str,
        hooks: object,
        job_claim_token: str,
        job_id: str = "",
    ) -> Any:
        base = super()._build_context(
            workflow_execution_id=workflow_execution_id,
            hooks=hooks,  # type: ignore[arg-type]
            job_claim_token=job_claim_token,
            job_id=job_id,
        )
        return replace(base, evaluation_runner=_FailingEvaluationRunner())


def test_evaluation_fail_path_blocks_report(
    session_factory: sessionmaker[Session],
    test_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ATLAS_DATABASE_URL", test_database_url)
    client = _api_client(session_factory)
    runtime = create_checkpoint_runtime(test_database_url)
    processor = _FailingEvalProcessor(
        checkpointer=runtime.checkpointer,
        session_factory=session_factory,
    )
    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=SqlAlchemyResearchJobRepository(),
        processor=processor,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=30.0,
        lease_seconds=60.0,
    )
    try:
        created = client.post(
            "/v1/research-jobs",
            json={"question": "Slice 12A evaluation fail path"},
            headers={"Idempotency-Key": "eval-fail-key"},
        )
        assert created.status_code == 202
        job_id = created.json()["id"]
        assert worker.run_once() is True

        fetched = client.get(f"/v1/research-jobs/{job_id}")
        assert fetched.status_code == 200
        body = fetched.json()
        assert body["status"] == "FAILED"
        assert "EvaluationTerminalError" in (body.get("failure_reason") or "")

        with session_factory() as session:
            reports = session.execute(
                select(func.count())
                .select_from(ReportArtifactModel)
                .where(ReportArtifactModel.research_job_id == job_id)
            ).scalar_one()
        assert reports == 0

        citations = client.get(f"/v1/research-jobs/{job_id}/citations")
        assert citations.status_code == 200
        assert citations.json()["report_artifact_id"] is None
    finally:
        worker.close()
        runtime.close()
        app.dependency_overrides.clear()
        reset_engine_cache()


def test_completed_job_replay_no_duplicate_evaluation_or_artifacts(
    session_factory: sessionmaker[Session],
    test_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ATLAS_DATABASE_URL", test_database_url)
    client = _api_client(session_factory)
    runtime = create_checkpoint_runtime(test_database_url)
    counters: dict[str, int] = {}
    processor = LangGraphResearchProcessor(
        checkpointer=runtime.checkpointer,
        session_factory=session_factory,
        node_counters=counters,
    )
    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=SqlAlchemyResearchJobRepository(),
        processor=processor,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=30.0,
        lease_seconds=60.0,
    )
    question = "Slice 12A evaluation replay idempotency"
    try:
        created = client.post(
            "/v1/research-jobs",
            json={"question": question},
            headers={"Idempotency-Key": "eval-replay-key"},
        )
        assert created.status_code == 202
        job_id = created.json()["id"]
        assert worker.run_once() is True

        with session_factory() as session:
            first_reports = session.execute(
                select(func.count())
                .select_from(ReportArtifactModel)
                .where(ReportArtifactModel.research_job_id == job_id)
            ).scalar_one()
            first_runs = session.execute(
                select(func.count())
                .select_from(EvaluationRunModel)
                .where(EvaluationRunModel.research_job_id == job_id)
            ).scalar_one()
        assert first_reports == 1
        assert first_runs == 1

        replayed = processor(question, job_id=job_id, claim_token="c" * 64)
        from atlas.application.job_processing import CompletedProcessing

        assert isinstance(replayed, CompletedProcessing)
        assert "Question:" in replayed.result
        assert counters.get("evaluate") == 1
        assert counters.get("complete") == 1

        with session_factory() as session:
            second_reports = session.execute(
                select(func.count())
                .select_from(ReportArtifactModel)
                .where(ReportArtifactModel.research_job_id == job_id)
            ).scalar_one()
            second_runs = session.execute(
                select(func.count())
                .select_from(EvaluationRunModel)
                .where(EvaluationRunModel.research_job_id == job_id)
            ).scalar_one()
        assert second_reports == first_reports
        assert second_runs == first_runs
    finally:
        worker.close()
        runtime.close()
        app.dependency_overrides.clear()
        reset_engine_cache()
