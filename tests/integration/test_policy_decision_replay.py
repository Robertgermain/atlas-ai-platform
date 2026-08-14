"""Integration tests for transaction-safe policy decision replay."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import Session, sessionmaker

from atlas.domain import ResearchJob
from atlas.persistence.db import session_scope
from atlas.persistence.models import ResearchJobModel
from atlas.persistence.models.recovery import (
    JobRecoveryAttemptModel,
    PolicyDecisionModel,
)
from atlas.persistence.repositories.recovery import SqlAlchemyRecoveryRepository
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository
from atlas.recovery.errors import PolicyDecisionConflictError
from atlas.recovery.fingerprint import fingerprint_policy_decision
from tests.integration.research_job_fixtures import bind_profile_and_start_claimed_job

CLAIM = "a" * 64


def _setup_claimed_job(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
) -> str:
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, "Policy replay"),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="c" * 64,
        )
        model = session.get(ResearchJobModel, job_id)
        assert model is not None
        at = datetime.now(UTC)
        bind_profile_and_start_claimed_job(
            model,
            at=at,
            claim_token=CLAIM,
            lease_expires_at=at + timedelta(hours=1),
        )
        execution_id = workflow_repo.create_execution(
            session, research_job_id=job_id, at=at
        )
        assert job_repo.set_active_workflow_execution(
            session,
            job_id=job_id,
            claim_token=CLAIM,
            execution_id=execution_id,
            at=at,
        )
        return execution_id


def _fingerprint(
    *,
    job_id: str,
    execution_id: str,
    decision: str = "retry",
    repair_count: int = 0,
    job_retry_count: int = 0,
) -> str:
    return fingerprint_policy_decision(
        research_job_id=job_id,
        workflow_execution_id=execution_id,
        evaluation_run_id=None,
        decision=decision,
        failure_category="TRANSIENT_TIMEOUT",
        reason_code="TRANSIENT_RETRY",
        repair_count=repair_count,
        job_retry_count=job_retry_count,
        evaluation_attempt_count=0,
    )


def test_new_policy_insert_returns_new_id(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "policy-insert-new"
    execution_id = _setup_claimed_job(session_factory, job_id=job_id)
    repo = SqlAlchemyRecoveryRepository()
    proposed = str(uuid4())
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        returned = repo.insert_policy_decision(
            session,
            id=proposed,
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            evaluation_run_id=None,
            decision="retry",
            failure_category="TRANSIENT_TIMEOUT",
            reason_code="TRANSIENT_RETRY",
            decision_fingerprint=_fingerprint(job_id=job_id, execution_id=execution_id),
            created_at=at,
        )
    assert returned == proposed


def test_replay_returns_existing_id(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "policy-insert-replay"
    execution_id = _setup_claimed_job(session_factory, job_id=job_id)
    repo = SqlAlchemyRecoveryRepository()
    fp = _fingerprint(job_id=job_id, execution_id=execution_id)
    first = str(uuid4())
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        assert (
            repo.insert_policy_decision(
                session,
                id=first,
                research_job_id=job_id,
                workflow_execution_id=execution_id,
                evaluation_run_id=None,
                decision="retry",
                failure_category="TRANSIENT_TIMEOUT",
                reason_code="TRANSIENT_RETRY",
                decision_fingerprint=fp,
                created_at=at,
            )
            == first
        )
    second = str(uuid4())
    with session_scope(session_factory) as session:
        returned = repo.insert_policy_decision(
            session,
            id=second,
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            evaluation_run_id=None,
            decision="retry",
            failure_category="TRANSIENT_TIMEOUT",
            reason_code="TRANSIENT_RETRY",
            decision_fingerprint=fp,
            created_at=at,
        )
    assert returned == first
    assert returned != second
    with session_scope(session_factory) as session:
        rows = session.scalars(
            select(PolicyDecisionModel).where(
                PolicyDecisionModel.research_job_id == job_id
            )
        ).all()
    assert len(rows) == 1


def test_outer_transaction_survives_policy_replay(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "policy-outer-tx"
    execution_id = _setup_claimed_job(session_factory, job_id=job_id)
    repo = SqlAlchemyRecoveryRepository()
    job_repo = SqlAlchemyResearchJobRepository()
    fp = _fingerprint(job_id=job_id, execution_id=execution_id)
    first = str(uuid4())
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        repo.insert_policy_decision(
            session,
            id=first,
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            evaluation_run_id=None,
            decision="retry",
            failure_category="TRANSIENT_TIMEOUT",
            reason_code="TRANSIENT_RETRY",
            decision_fingerprint=fp,
            created_at=at,
        )

    with session_scope(session_factory) as session:
        returned = repo.insert_policy_decision(
            session,
            id=str(uuid4()),
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            evaluation_run_id=None,
            decision="retry",
            failure_category="TRANSIENT_TIMEOUT",
            reason_code="TRANSIENT_RETRY",
            decision_fingerprint=fp,
            created_at=at,
        )
        assert returned == first
        assert job_repo.increment_repair_count(
            session, job_id=job_id, claim_token=CLAIM, at=datetime.now(UTC)
        )

    with session_scope(session_factory) as session:
        _, _, _ = job_repo.get_attempt_counts(session, job_id=job_id)
        model = session.get(ResearchJobModel, job_id)
        assert model is not None
        assert model.repair_count == 1


def test_retry_replay_references_existing_policy_no_duplicate_recovery(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "policy-retry-replay"
    execution_id = _setup_claimed_job(session_factory, job_id=job_id)
    repo = SqlAlchemyRecoveryRepository()
    job_repo = SqlAlchemyResearchJobRepository()
    fp = _fingerprint(job_id=job_id, execution_id=execution_id)
    decision_id = str(uuid4())
    recovery_id = str(uuid4())
    at = datetime.now(UTC)
    next_at = at + timedelta(seconds=5)

    with session_scope(session_factory) as session:
        authoritative = repo.insert_policy_decision(
            session,
            id=decision_id,
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            evaluation_run_id=None,
            decision="retry",
            failure_category="TRANSIENT_TIMEOUT",
            reason_code="TRANSIENT_RETRY",
            decision_fingerprint=fp,
            created_at=at,
        )
        assert authoritative == decision_id
        repo.insert_recovery_attempt(
            session,
            id=recovery_id,
            research_job_id=job_id,
            policy_decision_id=authoritative,
            abandoned_workflow_execution_id=execution_id,
            attempt_number=1,
            next_attempt_at=next_at,
            created_at=at,
        )
        assert job_repo.schedule_retry(
            session,
            job_id=job_id,
            claim_token=CLAIM,
            next_attempt_at=next_at,
            at=at,
            abandon_execution_id=execution_id,
        )

    with session_scope(session_factory) as session:
        replay_id = repo.insert_policy_decision(
            session,
            id=str(uuid4()),
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            evaluation_run_id=None,
            decision="retry",
            failure_category="TRANSIENT_TIMEOUT",
            reason_code="TRANSIENT_RETRY",
            decision_fingerprint=fp,
            created_at=datetime.now(UTC),
        )
        assert replay_id == decision_id
        existing = repo.get_recovery_attempt_by_policy(
            session, policy_decision_id=replay_id
        )
        assert existing is not None
        assert existing.id == recovery_id
        assert existing.policy_decision_id == decision_id
        # Replay must not create another recovery row or re-increment counters.
        attempts = session.scalars(
            select(JobRecoveryAttemptModel).where(
                JobRecoveryAttemptModel.research_job_id == job_id
            )
        ).all()
        model = session.get(ResearchJobModel, job_id)
        assert model is not None
        assert len(attempts) == 1
        assert model.job_retry_count == 1
        assert model.status == "PENDING"


def test_forced_exception_after_replay_preserves_committed_policy(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "policy-forced-rollback"
    execution_id = _setup_claimed_job(session_factory, job_id=job_id)
    repo = SqlAlchemyRecoveryRepository()
    job_repo = SqlAlchemyResearchJobRepository()
    fp = _fingerprint(job_id=job_id, execution_id=execution_id)
    first = str(uuid4())
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        repo.insert_policy_decision(
            session,
            id=first,
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            evaluation_run_id=None,
            decision="retry",
            failure_category="TRANSIENT_TIMEOUT",
            reason_code="TRANSIENT_RETRY",
            decision_fingerprint=fp,
            created_at=at,
        )

    with pytest.raises(RuntimeError, match="forced"):
        with session_scope(session_factory) as session:
            returned = repo.insert_policy_decision(
                session,
                id=str(uuid4()),
                research_job_id=job_id,
                workflow_execution_id=execution_id,
                evaluation_run_id=None,
                decision="retry",
                failure_category="TRANSIENT_TIMEOUT",
                reason_code="TRANSIENT_RETRY",
                decision_fingerprint=fp,
                created_at=at,
            )
            assert returned == first
            assert job_repo.increment_repair_count(
                session, job_id=job_id, claim_token=CLAIM, at=datetime.now(UTC)
            )
            raise RuntimeError("forced")

    with session_scope(session_factory) as session:
        rows = session.scalars(
            select(PolicyDecisionModel).where(
                PolicyDecisionModel.research_job_id == job_id
            )
        ).all()
        model = session.get(ResearchJobModel, job_id)
        assert model is not None
    assert len(rows) == 1
    assert rows[0].id == first
    assert model.repair_count == 0


def test_inconsistent_fields_fail_closed(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "policy-conflict"
    execution_id = _setup_claimed_job(session_factory, job_id=job_id)
    repo = SqlAlchemyRecoveryRepository()
    fp = _fingerprint(job_id=job_id, execution_id=execution_id)
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        repo.insert_policy_decision(
            session,
            id=str(uuid4()),
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            evaluation_run_id=None,
            decision="retry",
            failure_category="TRANSIENT_TIMEOUT",
            reason_code="TRANSIENT_RETRY",
            decision_fingerprint=fp,
            created_at=at,
        )
    with pytest.raises(PolicyDecisionConflictError):
        with session_scope(session_factory) as session:
            # Same fingerprint key but different stored decision fields.
            session.execute(
                text(
                    "UPDATE policy_decisions SET decision = 'terminal' "
                    "WHERE research_job_id = :job_id"
                ),
                {"job_id": job_id},
            )
            session.flush()
            repo.insert_policy_decision(
                session,
                id=str(uuid4()),
                research_job_id=job_id,
                workflow_execution_id=execution_id,
                evaluation_run_id=None,
                decision="retry",
                failure_category="TRANSIENT_TIMEOUT",
                reason_code="TRANSIENT_RETRY",
                decision_fingerprint=fp,
                created_at=at,
            )
