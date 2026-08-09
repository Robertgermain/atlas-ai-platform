"""API → worker → LangGraph → API integration coverage."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from atlas.api.deps import provide_session_factory
from atlas.application.worker import ResearchJobWorker
from atlas.domain import ResearchJob, ResearchJobStatus
from atlas.main import app
from atlas.persistence.db import reset_engine_cache, session_scope
from atlas.persistence.models.workflow import (
    WorkflowExecutionModel,
    WorkflowNodeExecutionModel,
)
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository
from atlas.workflow import LangGraphResearchProcessor, create_checkpoint_runtime

T0 = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)


def _api_client(session_factory: sessionmaker[Session]) -> TestClient:
    reset_engine_cache()
    app.dependency_overrides[provide_session_factory] = lambda: session_factory
    return TestClient(app)


def _assert_report_structure(result: str, question: str) -> None:
    assert "Question:" in result
    assert question in result
    assert "Plan:" in result
    assert "Findings:" in result
    assert "Draft:" in result


def test_api_create_then_langgraph_worker_completes(
    test_database_url: str,
    session_factory: sessionmaker[Session],
) -> None:
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
        processing_timeout_seconds=15.0,
        lease_seconds=30.0,
    )
    question = "Worker graph path"
    try:
        created = client.post(
            "/v1/research-jobs",
            json={"question": question},
            headers={"Idempotency-Key": "workflow-api-key"},
        )
        assert created.status_code == 202
        job_id = created.json()["id"]

        assert worker.run_once() is True

        fetched = client.get(f"/v1/research-jobs/{job_id}")
        assert fetched.status_code == 200
        body = fetched.json()
        assert body["status"] == "COMPLETED"
        _assert_report_structure(body["result"], question)

        with session_factory() as session:
            executions = session.scalars(
                select(WorkflowExecutionModel).where(
                    WorkflowExecutionModel.research_job_id == job_id
                )
            ).all()
            assert len(executions) == 1
            assert executions[0].status == "COMPLETED"
            assert executions[0].thread_id == job_id
            nodes = session.scalars(
                select(WorkflowNodeExecutionModel).where(
                    WorkflowNodeExecutionModel.workflow_execution_id == executions[0].id
                )
            ).all()
            assert {node.node_name for node in nodes} == {
                "validate",
                "plan",
                "research",
                "draft",
                "complete",
            }
            assert all(node.attempt == 1 for node in nodes)
            assert all(node.status == "COMPLETED" for node in nodes)
    finally:
        worker.close()
        runtime.close()
        app.dependency_overrides.clear()
        reset_engine_cache()


def test_interrupt_then_worker_resume_creates_second_execution(
    test_database_url: str,
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyResearchJobRepository()
    job_id = "workflow-reclaim-1"
    question = "Resume after reclaim"
    with session_scope(session_factory) as session:
        repo.add(
            session,
            ResearchJob.create(job_id, question, at=T0),
            idempotency_key="workflow-reclaim-key",
            request_fingerprint="c" * 64,
        )

    runtime_a = create_checkpoint_runtime(test_database_url)
    counters: dict[str, int] = {}
    processor_a = LangGraphResearchProcessor(
        checkpointer=runtime_a.checkpointer,
        session_factory=session_factory,
        interrupt_after=["plan"],
        node_counters=counters,
    )
    try:
        try:
            processor_a(question, job_id=job_id)
            raise AssertionError("expected interrupt before completion")
        except RuntimeError as exc:
            assert "interrupted" in str(exc).lower()
    finally:
        runtime_a.close()
        del processor_a

    assert counters == {"validate": 1, "plan": 1}

    with session_factory() as session:
        first = session.scalars(
            select(WorkflowExecutionModel).where(
                WorkflowExecutionModel.research_job_id == job_id
            )
        ).one()
        assert first.status == "FAILED"
        first_id = first.id

    runtime_b = create_checkpoint_runtime(test_database_url)
    processor_b = LangGraphResearchProcessor(
        checkpointer=runtime_b.checkpointer,
        session_factory=session_factory,
        node_counters=counters,
    )
    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=repo,
        processor=processor_b,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=15.0,
        lease_seconds=30.0,
    )
    try:
        assert worker.run_once() is True
    finally:
        worker.close()
        runtime_b.close()

    with session_scope(session_factory) as session:
        loaded = repo.get(session, job_id)
        executions = session.scalars(
            select(WorkflowExecutionModel)
            .where(WorkflowExecutionModel.research_job_id == job_id)
            .order_by(WorkflowExecutionModel.started_at)
        ).all()

    assert loaded is not None
    assert loaded.status is ResearchJobStatus.COMPLETED
    _assert_report_structure(loaded.result or "", question)
    assert len(executions) == 2
    assert executions[0].id == first_id
    assert executions[0].status == "FAILED"
    assert executions[1].status == "COMPLETED"
    assert executions[1].thread_id == job_id
    assert counters["validate"] == 1
    assert counters["plan"] == 1
    assert counters["research"] == 1
    assert counters["draft"] == 1
    assert counters["complete"] == 1


def test_new_processing_attempt_abandons_prior_running_execution(
    test_database_url: str,
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    job_id = "workflow-abandon-1"
    question = "Abandon prior attempt"
    with session_scope(session_factory) as session:
        repo.add(
            session,
            ResearchJob.create(job_id, question, at=T0),
            idempotency_key="workflow-abandon-key",
            request_fingerprint="d" * 64,
        )
        prior_id = workflow_repo.create_execution(
            session,
            research_job_id=job_id,
            thread_id=job_id,
            at=T0,
        )

    runtime = create_checkpoint_runtime(test_database_url)
    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=repo,
        processor=LangGraphResearchProcessor(
            checkpointer=runtime.checkpointer,
            session_factory=session_factory,
        ),
        poll_interval_seconds=0.01,
        processing_timeout_seconds=15.0,
        lease_seconds=30.0,
    )
    try:
        assert worker.run_once() is True
    finally:
        worker.close()
        runtime.close()

    with session_factory() as session:
        prior = session.get(WorkflowExecutionModel, prior_id)
        executions = session.scalars(
            select(WorkflowExecutionModel)
            .where(WorkflowExecutionModel.research_job_id == job_id)
            .order_by(WorkflowExecutionModel.started_at)
        ).all()

    assert prior is not None
    assert prior.status == "ABANDONED"
    assert len(executions) == 2
    assert executions[1].status == "COMPLETED"
