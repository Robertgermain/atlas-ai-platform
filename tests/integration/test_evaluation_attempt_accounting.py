"""Integration tests for job-global evaluation attempt accounting."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from atlas.domain import ResearchJob
from atlas.evaluation.contracts import EVALUATION_PROFILE
from atlas.evaluation.errors import EvaluationAttemptCapError
from atlas.evaluation.service import EvaluationService
from atlas.persistence.db import session_scope
from atlas.persistence.models import ResearchJobModel
from atlas.persistence.models.evaluation import EvaluationRunModel
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository
from atlas.recovery.policy import MAX_EVALUATION_ATTEMPTS
from tests.integration.research_job_fixtures import bind_profile_and_start_claimed_job

CLAIM = "a" * 64
FINGERPRINT = "b" * 64


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
            ResearchJob.create(job_id, "Eval attempt accounting"),
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
            evaluation_profile=EVALUATION_PROFILE,
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


def test_new_evaluation_increments_job_global_count(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-attempt-1"
    execution_id = _setup_claimed_job(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)
    run_id, token, replay = service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint=FINGERPRINT,
        job_id=job_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=CLAIM,
    )
    assert replay is None
    assert run_id
    assert token

    with session_scope(session_factory) as session:
        _, _, eac = SqlAlchemyResearchJobRepository().get_attempt_counts(
            session, job_id=job_id
        )
    assert eac == 1


def test_second_attempt_on_new_key_increments_again(
    session_factory: sessionmaker[Session],
) -> None:
    """Repair-style second evaluation attempt bumps the job-global counter."""
    job_id = "eval-attempt-2"
    execution_id = _setup_claimed_job(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)
    deadline = datetime.now(UTC) + timedelta(minutes=5)
    service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint=FINGERPRINT,
        job_id=job_id,
        deadline=deadline,
        job_claim_token=CLAIM,
    )
    service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=2,
        fingerprint="d" * 64,
        job_id=job_id,
        deadline=deadline,
        job_claim_token=CLAIM,
    )
    with session_scope(session_factory) as session:
        _, _, eac = SqlAlchemyResearchJobRepository().get_attempt_counts(
            session, job_id=job_id
        )
    assert eac == 2


def test_reclaim_same_attempt_does_not_double_increment(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-attempt-replay"
    execution_id = _setup_claimed_job(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)
    service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint=FINGERPRINT,
        job_id=job_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=CLAIM,
    )
    with session_scope(session_factory) as session:
        row = session.scalars(
            select(EvaluationRunModel).where(
                EvaluationRunModel.research_job_id == job_id
            )
        ).one()
        row.deadline_at = datetime.now(UTC) - timedelta(seconds=1)
        session.flush()

    service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint=FINGERPRINT,
        job_id=job_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=CLAIM,
    )
    with session_scope(session_factory) as session:
        _, _, eac = SqlAlchemyResearchJobRepository().get_attempt_counts(
            session, job_id=job_id
        )
    assert eac == 1


def test_fourth_evaluation_allowed_fifth_blocked(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-attempt-four-then-five"
    execution_id = _setup_claimed_job(session_factory, job_id=job_id)
    with session_scope(session_factory) as session:
        model = session.get(ResearchJobModel, job_id)
        assert model is not None
        model.evaluation_attempt_count = MAX_EVALUATION_ATTEMPTS - 1
        session.flush()

    service = EvaluationService(session_factory=session_factory)
    service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=MAX_EVALUATION_ATTEMPTS,
        fingerprint=FINGERPRINT,
        job_id=job_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=CLAIM,
    )
    with session_scope(session_factory) as session:
        _, _, eac = SqlAlchemyResearchJobRepository().get_attempt_counts(
            session, job_id=job_id
        )
    assert eac == MAX_EVALUATION_ATTEMPTS

    with pytest.raises(EvaluationAttemptCapError):
        service.begin_or_resume(
            execution_id=execution_id,
            profile=EVALUATION_PROFILE,
            attempt=MAX_EVALUATION_ATTEMPTS + 1,
            fingerprint="e" * 64,
            job_id=job_id,
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            job_claim_token=CLAIM,
        )


def test_crash_before_commit_does_not_increment(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If create_run fails after increment in the same TX, counter rolls back."""
    job_id = "eval-attempt-crash"
    execution_id = _setup_claimed_job(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)

    def _boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("simulated crash after reserve")

    monkeypatch.setattr(service._repository, "create_run", _boom)
    with pytest.raises(RuntimeError, match="simulated crash"):
        service.begin_or_resume(
            execution_id=execution_id,
            profile=EVALUATION_PROFILE,
            attempt=1,
            fingerprint=FINGERPRINT,
            job_id=job_id,
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            job_claim_token=CLAIM,
        )
    with session_scope(session_factory) as session:
        _, _, eac = SqlAlchemyResearchJobRepository().get_attempt_counts(
            session, job_id=job_id
        )
    assert eac == 0


def test_fifth_evaluation_create_blocked(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-attempt-cap"
    execution_id = _setup_claimed_job(session_factory, job_id=job_id)
    with session_scope(session_factory) as session:
        model = session.get(ResearchJobModel, job_id)
        assert model is not None
        model.evaluation_attempt_count = MAX_EVALUATION_ATTEMPTS
        session.flush()

    service = EvaluationService(session_factory=session_factory)
    with pytest.raises(EvaluationAttemptCapError):
        service.begin_or_resume(
            execution_id=execution_id,
            profile=EVALUATION_PROFILE,
            attempt=MAX_EVALUATION_ATTEMPTS + 1,
            fingerprint=FINGERPRINT,
            job_id=job_id,
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            job_claim_token=CLAIM,
        )
    with session_scope(session_factory) as session:
        _, _, eac = SqlAlchemyResearchJobRepository().get_attempt_counts(
            session, job_id=job_id
        )
    assert eac == MAX_EVALUATION_ATTEMPTS
