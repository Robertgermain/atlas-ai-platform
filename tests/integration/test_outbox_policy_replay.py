"""Policy-replay paths must not enqueue duplicate outbox events."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from atlas.domain import ResearchJob
from atlas.eventing.builders import (
    build_research_job_awaiting_review,
    build_research_job_retry_scheduled,
)
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.outbox import SqlAlchemyOutboxRepository
from atlas.persistence.repositories.recovery import SqlAlchemyRecoveryRepository
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository
from atlas.recovery.fingerprint import fingerprint_policy_decision

T0 = datetime(2026, 8, 10, 17, 0, 0, tzinfo=UTC)


@pytest.fixture
def session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )


def _seed_running_job(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
    claim_token: str,
) -> str:
    job_repo = SqlAlchemyResearchJobRepository()
    wf_repo = SqlAlchemyWorkflowRepository()
    job = ResearchJob.create(job_id, "policy replay question", at=T0)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            job,
            idempotency_key=f"idem-{job_id}",
            request_fingerprint="d" * 64,
        )
        claimed = job_repo.claim_next(
            session,
            now=T0,
            lease_expires_at=T0 + timedelta(seconds=90),
            claim_token=claim_token,
        )
        assert claimed is not None
        execution_id = wf_repo.create_execution(
            session,
            research_job_id=job_id,
            at=T0,
        )
        assert job_repo.set_active_workflow_execution(
            session,
            job_id=job_id,
            claim_token=claim_token,
            execution_id=execution_id,
            at=T0,
        )
        return execution_id


def test_awaiting_review_policy_replay_no_duplicate_event(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "job-await-replay"
    claim_token = "a" * 64
    execution_id = _seed_running_job(
        session_factory,
        job_id=job_id,
        claim_token=claim_token,
    )

    job_repo = SqlAlchemyResearchJobRepository()
    recovery = SqlAlchemyRecoveryRepository()
    outbox = SqlAlchemyOutboxRepository()
    decision_id = "decision-await-1"
    fp = fingerprint_policy_decision(
        research_job_id=job_id,
        workflow_execution_id=execution_id,
        evaluation_run_id=None,
        decision="await_review",
        failure_category="NEEDS_HUMAN_REVIEW",
        reason_code="AWAIT_REVIEW_POLICY",
        repair_count=0,
        job_retry_count=0,
        evaluation_attempt_count=1,
    )

    def _once(*, proposed_id: str) -> str:
        with session_scope(session_factory) as session:
            authoritative = recovery.insert_policy_decision(
                session,
                id=proposed_id,
                research_job_id=job_id,
                workflow_execution_id=execution_id,
                evaluation_run_id=None,
                decision="await_review",
                failure_category="NEEDS_HUMAN_REVIEW",
                reason_code="AWAIT_REVIEW_POLICY",
                decision_fingerprint=fp,
                created_at=T0,
            )
            created = authoritative == proposed_id
            if created:
                ok = job_repo.transition_awaiting_review(
                    session,
                    job_id=job_id,
                    claim_token=claim_token,
                    at=T0,
                )
                assert ok is True
                outbox.enqueue(
                    session,
                    build_research_job_awaiting_review(
                        research_job_id=job_id,
                        workflow_execution_id=execution_id,
                        entered_review_at=T0,
                    ),
                )
            return authoritative

    first = _once(proposed_id=decision_id)
    second = _once(proposed_id="decision-await-2")
    assert first == decision_id
    assert second == decision_id

    with session_scope(session_factory) as session:
        rows = outbox.list_for_aggregate(
            session, aggregate_type="research_job", aggregate_id=job_id
        )
    assert [row.event_type for row in rows] == ["research_job.awaiting_review"]


def test_retry_scheduled_policy_replay_no_duplicate_event(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "job-retry-replay"
    claim_token = "b" * 64
    execution_id = _seed_running_job(
        session_factory,
        job_id=job_id,
        claim_token=claim_token,
    )

    job_repo = SqlAlchemyResearchJobRepository()
    recovery = SqlAlchemyRecoveryRepository()
    outbox = SqlAlchemyOutboxRepository()
    decision_id = "decision-retry-1"
    next_at = T0 + timedelta(seconds=5)
    fp = fingerprint_policy_decision(
        research_job_id=job_id,
        workflow_execution_id=execution_id,
        evaluation_run_id=None,
        decision="retry",
        failure_category="TRANSIENT_PROVIDER",
        reason_code="TRANSIENT_RETRY",
        repair_count=0,
        job_retry_count=0,
        evaluation_attempt_count=0,
    )

    def _once(*, proposed_id: str) -> str:
        with session_scope(session_factory) as session:
            authoritative = recovery.insert_policy_decision(
                session,
                id=proposed_id,
                research_job_id=job_id,
                workflow_execution_id=execution_id,
                evaluation_run_id=None,
                decision="retry",
                failure_category="TRANSIENT_PROVIDER",
                reason_code="TRANSIENT_RETRY",
                decision_fingerprint=fp,
                created_at=T0,
            )
            created = authoritative == proposed_id
            if created:
                recovery.insert_recovery_attempt(
                    session,
                    id="recovery-1",
                    research_job_id=job_id,
                    policy_decision_id=authoritative,
                    abandoned_workflow_execution_id=execution_id,
                    attempt_number=1,
                    next_attempt_at=next_at,
                    created_at=T0,
                )
                ok = job_repo.schedule_retry(
                    session,
                    job_id=job_id,
                    claim_token=claim_token,
                    next_attempt_at=next_at,
                    at=T0,
                    abandon_execution_id=execution_id,
                )
                assert ok is True
                outbox.enqueue(
                    session,
                    build_research_job_retry_scheduled(
                        research_job_id=job_id,
                        abandoned_workflow_execution_id=execution_id,
                        job_retry_count=1,
                        next_attempt_at=next_at,
                        occurred_at=T0,
                    ),
                )
            return authoritative

    first = _once(proposed_id=decision_id)
    second = _once(proposed_id="decision-retry-2")
    assert first == decision_id
    assert second == decision_id

    with session_scope(session_factory) as session:
        rows = outbox.list_for_aggregate(
            session, aggregate_type="research_job", aggregate_id=job_id
        )
    assert [row.event_type for row in rows] == ["research_job.retry_scheduled"]
    assert rows[0].payload["job_retry_count"] == 1
