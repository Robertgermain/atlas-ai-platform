"""PostgreSQL persistence of sanitized node failure errors."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from atlas.domain import ResearchJob
from atlas.persistence.db import session_scope
from atlas.persistence.models.workflow import WorkflowNodeExecutionModel
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository
from atlas.workflow.processor import sanitize_node_error

T0 = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)


def test_persisted_node_failure_error_is_safe(
    session_factory: sessionmaker[Session],
) -> None:
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    job_id = "safe-error-job"
    secret = "sk-secret-value"

    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, "Safe error question", at=T0),
            idempotency_key="safe-error-key",
            request_fingerprint="e" * 64,
        )
        execution_id = workflow_repo.create_execution(
            session,
            research_job_id=job_id,
            thread_id=job_id,
            at=T0,
        )
        attempt = workflow_repo.begin_node_attempt(
            session,
            workflow_execution_id=execution_id,
            node_name="research",
            at=T0,
        )
        workflow_repo.fail_node_attempt(
            session,
            workflow_execution_id=execution_id,
            node_name="research",
            attempt=attempt,
            error=sanitize_node_error(RuntimeError(f"leak={secret}")),
            at=T0,
        )

    with session_factory() as session:
        row = session.scalars(
            select(WorkflowNodeExecutionModel).where(
                WorkflowNodeExecutionModel.workflow_execution_id == execution_id,
                WorkflowNodeExecutionModel.node_name == "research",
                WorkflowNodeExecutionModel.attempt == attempt,
            )
        ).one()

    assert row.status == "FAILED"
    assert row.error == "RuntimeError: node execution failed"
    assert secret not in (row.error or "")
    assert "leak=" not in (row.error or "")
