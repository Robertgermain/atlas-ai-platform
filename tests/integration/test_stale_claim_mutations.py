"""Race tests: stale claim cannot mutate after ownership loss."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session, sessionmaker

from atlas.domain import ResearchJob
from atlas.persistence.db import session_scope
from atlas.persistence.mappers.research_job import apply_domain_to_orm, to_domain
from atlas.persistence.models import ResearchJobModel
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
            ResearchJob.create(job_id, "Stale claim race"),
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


def _transfer_to_b(session_factory: sessionmaker[Session], job_id: str) -> None:
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        model = session.get(ResearchJobModel, job_id)
        assert model is not None
        model.claim_token = CLAIM_B
        model.lease_expires_at = at + timedelta(hours=1)
        session.flush()


def test_stale_worker_cannot_bind_increment_review_or_retry(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "stale-claim-mutations"
    execution_id = _setup_owned_by_a(session_factory, job_id=job_id)
    _transfer_to_b(session_factory, job_id=job_id)
    at = datetime.now(UTC)
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()

    with session_scope(session_factory) as session:
        new_exec = workflow_repo.create_execution(
            session, research_job_id=job_id, at=at
        )
        assert (
            job_repo.set_active_workflow_execution(
                session,
                job_id=job_id,
                claim_token=CLAIM_A,
                execution_id=new_exec,
                at=at,
            )
            is False
        )
        assert (
            job_repo.increment_repair_count(
                session, job_id=job_id, claim_token=CLAIM_A, at=at
            )
            is False
        )
        assert (
            job_repo.increment_evaluation_attempt_count(
                session, job_id=job_id, claim_token=CLAIM_A, at=at
            )
            is False
        )
        assert (
            job_repo.transition_awaiting_review(
                session, job_id=job_id, claim_token=CLAIM_A, at=at
            )
            is False
        )
        assert (
            job_repo.schedule_retry(
                session,
                job_id=job_id,
                claim_token=CLAIM_A,
                next_attempt_at=at + timedelta(seconds=5),
                at=at,
            )
            is False
        )
        assert (
            job_repo.finalize_completion(
                session,
                job_id=job_id,
                claim_token=CLAIM_A,
                result="should-not-complete",
                at=at,
            )
            is False
        )
        assert (
            job_repo.finalize_failure(
                session,
                job_id=job_id,
                claim_token=CLAIM_A,
                reason="should-not-fail",
                at=at,
            )
            is False
        )

    with session_scope(session_factory) as session:
        model = session.get(ResearchJobModel, job_id)
        assert model is not None
        assert model.status == "RUNNING"
        assert model.claim_token == CLAIM_B
        assert model.repair_count == 0
        assert model.evaluation_attempt_count == 0
        assert model.active_workflow_execution_id == execution_id


def test_expired_lease_matching_token_fails_all_mutations(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "expired-lease-mutations"
    _setup_owned_by_a(session_factory, job_id=job_id)
    past = datetime.now(UTC) - timedelta(seconds=1)
    with session_scope(session_factory) as session:
        model = session.get(ResearchJobModel, job_id)
        assert model is not None
        model.lease_expires_at = past
        session.flush()

    at = datetime.now(UTC)
    job_repo = SqlAlchemyResearchJobRepository()
    with session_scope(session_factory) as session:
        assert (
            job_repo.increment_repair_count(
                session, job_id=job_id, claim_token=CLAIM_A, at=at
            )
            is False
        )
        assert (
            job_repo.increment_evaluation_attempt_count(
                session, job_id=job_id, claim_token=CLAIM_A, at=at
            )
            is False
        )
        assert (
            job_repo.transition_awaiting_review(
                session, job_id=job_id, claim_token=CLAIM_A, at=at
            )
            is False
        )
        assert (
            job_repo.schedule_retry(
                session,
                job_id=job_id,
                claim_token=CLAIM_A,
                next_attempt_at=at + timedelta(seconds=5),
                at=at,
            )
            is False
        )
        assert (
            job_repo.finalize_completion(
                session,
                job_id=job_id,
                claim_token=CLAIM_A,
                result="nope",
                at=at,
            )
            is False
        )
