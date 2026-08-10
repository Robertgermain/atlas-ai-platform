"""Race tests: stale workers cannot terminalize executions after reclaim."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session, sessionmaker

from atlas.domain import ResearchJob, ResearchJobStatus
from atlas.persistence.db import session_scope
from atlas.persistence.mappers.research_job import apply_domain_to_orm, to_domain
from atlas.persistence.models import ResearchJobModel
from atlas.persistence.models.workflow import WorkflowExecutionModel
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository

CLAIM_A = "a" * 64
CLAIM_B = "b" * 64


def _setup_owned_by_a(session_factory: sessionmaker[Session], job_id: str) -> str:
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, "Exec claim race"),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="c" * 64,
        )
        model = session.get(ResearchJobModel, job_id)
        assert model is not None
        job = to_domain(model)
        at = datetime.now(UTC)
        job.start(at=at)
        apply_domain_to_orm(job, model)
        model.claim_token = CLAIM_A
        model.lease_expires_at = at + timedelta(hours=1)
        execution_id = workflow_repo.create_execution(
            session, research_job_id=job_id, at=at
        )
        assert job_repo.set_active_workflow_execution(
            session,
            job_id=job_id,
            claim_token=CLAIM_A,
            execution_id=execution_id,
            at=at,
        )
        return execution_id


def _expire_and_reclaim_as_b(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
) -> None:
    past = datetime.now(UTC) - timedelta(seconds=1)
    with session_scope(session_factory) as session:
        model = session.get(ResearchJobModel, job_id)
        assert model is not None
        model.lease_expires_at = past
        session.flush()
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        model = session.get(ResearchJobModel, job_id)
        assert model is not None
        model.claim_token = CLAIM_B
        model.lease_expires_at = at + timedelta(hours=1)
        session.flush()


def test_stale_a_cannot_complete_after_b_reclaim(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "exec-race-complete"
    execution_id = _setup_owned_by_a(session_factory, job_id=job_id)
    _expire_and_reclaim_as_b(session_factory, job_id=job_id)
    at = datetime.now(UTC)
    workflow_repo = SqlAlchemyWorkflowRepository()
    with session_scope(session_factory) as session:
        assert (
            workflow_repo.complete_execution_for_claim(
                session,
                execution_id=execution_id,
                research_job_id=job_id,
                claim_token=CLAIM_A,
                at=at,
            )
            is False
        )
        exec_model = session.get(WorkflowExecutionModel, execution_id)
        assert exec_model is not None
        assert exec_model.status == "RUNNING"


def test_stale_a_cannot_fail_after_b_reclaim(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "exec-race-fail"
    execution_id = _setup_owned_by_a(session_factory, job_id=job_id)
    _expire_and_reclaim_as_b(session_factory, job_id=job_id)
    at = datetime.now(UTC)
    workflow_repo = SqlAlchemyWorkflowRepository()
    with session_scope(session_factory) as session:
        assert (
            workflow_repo.fail_execution_for_claim(
                session,
                execution_id=execution_id,
                research_job_id=job_id,
                claim_token=CLAIM_A,
                at=at,
            )
            is False
        )
        exec_model = session.get(WorkflowExecutionModel, execution_id)
        assert exec_model is not None
        assert exec_model.status == "RUNNING"


def test_stale_a_cannot_abandon_after_b_reclaim(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "exec-race-abandon"
    execution_id = _setup_owned_by_a(session_factory, job_id=job_id)
    _expire_and_reclaim_as_b(session_factory, job_id=job_id)
    at = datetime.now(UTC)
    workflow_repo = SqlAlchemyWorkflowRepository()
    with session_scope(session_factory) as session:
        assert (
            workflow_repo.abandon_execution_for_claim(
                session,
                execution_id=execution_id,
                research_job_id=job_id,
                claim_token=CLAIM_A,
                at=at,
            )
            is False
        )
        exec_model = session.get(WorkflowExecutionModel, execution_id)
        assert exec_model is not None
        assert exec_model.status == "RUNNING"


def test_b_can_resume_and_finalize_normally(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "exec-race-b-finalize"
    execution_id = _setup_owned_by_a(session_factory, job_id=job_id)
    _expire_and_reclaim_as_b(session_factory, job_id=job_id)
    at = datetime.now(UTC)
    workflow_repo = SqlAlchemyWorkflowRepository()
    job_repo = SqlAlchemyResearchJobRepository()
    with session_scope(session_factory) as session:
        assert workflow_repo.complete_execution_for_claim(
            session,
            execution_id=execution_id,
            research_job_id=job_id,
            claim_token=CLAIM_B,
            at=at,
        )
        assert job_repo.finalize_completion(
            session,
            job_id=job_id,
            claim_token=CLAIM_B,
            result="ok",
            at=at,
        )
    with session_scope(session_factory) as session:
        exec_model = session.get(WorkflowExecutionModel, execution_id)
        model = session.get(ResearchJobModel, job_id)
        assert exec_model is not None
        assert model is not None
        assert exec_model.status == "COMPLETED"
        assert model.status == "COMPLETED"


def test_a_exception_path_does_not_alter_b_execution(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "exec-race-exception"
    execution_id = _setup_owned_by_a(session_factory, job_id=job_id)
    _expire_and_reclaim_as_b(session_factory, job_id=job_id)
    at = datetime.now(UTC)
    workflow_repo = SqlAlchemyWorkflowRepository()
    job_repo = SqlAlchemyResearchJobRepository()
    with session_scope(session_factory) as session:
        assert (
            workflow_repo.fail_execution_for_claim(
                session,
                execution_id=execution_id,
                research_job_id=job_id,
                claim_token=CLAIM_A,
                at=at,
            )
            is False
        )
        assert (
            job_repo.finalize_failure(
                session,
                job_id=job_id,
                claim_token=CLAIM_A,
                reason="stale",
                at=at,
            )
            is False
        )
    with session_scope(session_factory) as session:
        exec_model = session.get(WorkflowExecutionModel, execution_id)
        model = session.get(ResearchJobModel, job_id)
        assert exec_model is not None
        assert model is not None
        assert exec_model.status == "RUNNING"
        assert model.status == "RUNNING"
        assert model.claim_token == CLAIM_B


def test_operator_reject_fails_execution_without_worker_claim(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "exec-race-operator-reject"
    execution_id = _setup_owned_by_a(session_factory, job_id=job_id)
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        assert job_repo.transition_awaiting_review(
            session, job_id=job_id, claim_token=CLAIM_A, at=at
        )
    reject_at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        model = session.get(ResearchJobModel, job_id, with_for_update=True)
        assert model is not None
        assert model.status == ResearchJobStatus.AWAITING_REVIEW.value
        assert workflow_repo.fail_execution(
            session, execution_id=execution_id, at=reject_at
        )
        assert job_repo.fail_from_review(
            session,
            job_id=job_id,
            reason="Rejected by operator review",
            at=reject_at,
        )
    with session_scope(session_factory) as session:
        exec_model = session.get(WorkflowExecutionModel, execution_id)
        model = session.get(ResearchJobModel, job_id)
        assert exec_model is not None
        assert model is not None
        assert exec_model.status == "FAILED"
        assert model.status == "FAILED"


def test_retry_schedule_abandons_only_when_claim_valid(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "exec-race-retry-abandon"
    execution_id = _setup_owned_by_a(session_factory, job_id=job_id)
    _expire_and_reclaim_as_b(session_factory, job_id=job_id)
    at = datetime.now(UTC)
    job_repo = SqlAlchemyResearchJobRepository()
    with session_scope(session_factory) as session:
        assert (
            job_repo.schedule_retry(
                session,
                job_id=job_id,
                claim_token=CLAIM_A,
                next_attempt_at=at + timedelta(seconds=5),
                at=at,
                abandon_execution_id=execution_id,
            )
            is False
        )
        exec_model = session.get(WorkflowExecutionModel, execution_id)
        model = session.get(ResearchJobModel, job_id)
        assert exec_model is not None
        assert model is not None
        assert exec_model.status == "RUNNING"
        assert model.status == "RUNNING"
        assert model.job_retry_count == 0
        assert model.claim_token == CLAIM_B

    with session_scope(session_factory) as session:
        assert job_repo.schedule_retry(
            session,
            job_id=job_id,
            claim_token=CLAIM_B,
            next_attempt_at=at + timedelta(seconds=5),
            at=at,
            abandon_execution_id=execution_id,
        )
        exec_model = session.get(WorkflowExecutionModel, execution_id)
        model = session.get(ResearchJobModel, job_id)
        assert exec_model is not None
        assert model is not None
        assert exec_model.status == "ABANDONED"
        assert model.status == "PENDING"
        assert model.job_retry_count == 1
