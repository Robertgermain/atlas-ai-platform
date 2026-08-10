"""PostgreSQL tests for atomic outbox insertion with domain mutations."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from atlas.application.research_jobs import ResearchJobService
from atlas.application.worker import ResearchJobWorker
from atlas.domain import ResearchJob, ResearchJobStatus
from atlas.eventing.builders import build_research_job_completed
from atlas.outbox.errors import OutboxEnqueueError
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.outbox import SqlAlchemyOutboxRepository
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository

T0 = datetime(2026, 8, 10, 16, 0, 0, tzinfo=UTC)


@pytest.fixture
def session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )


class _BoomOutbox:
    """Forces outbox construction/insertion failure."""

    def enqueue(self, session: Session, event: object) -> None:
        del session, event
        raise OutboxEnqueueError("ForcedEnqueueFailure")


def test_created_event_atomic_with_job_insert(
    session_factory: sessionmaker[Session],
) -> None:
    service = ResearchJobService(
        session_factory=session_factory,
        repository=SqlAlchemyResearchJobRepository(),
        outbox=SqlAlchemyOutboxRepository(),
        id_factory=lambda: "job-created-atomic",
    )
    job = service.submit("atomic create?", idempotency_key="idem-created-1")
    assert job.id == "job-created-atomic"

    outbox = SqlAlchemyOutboxRepository()
    with session_scope(session_factory) as session:
        rows = outbox.list_for_aggregate(
            session,
            aggregate_type="research_job",
            aggregate_id=job.id,
        )
    assert len(rows) == 1
    assert rows[0].event_type == "research_job.created"
    assert rows[0].payload["research_job_id"] == job.id


def test_idempotent_job_replay_emits_exactly_one_created_event(
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyResearchJobRepository()
    outbox = SqlAlchemyOutboxRepository()
    service = ResearchJobService(
        session_factory=session_factory,
        repository=repo,
        outbox=outbox,
        id_factory=lambda: str(uuid4()),
    )
    first = service.submit("same q", idempotency_key="idem-replay-1")
    second = service.submit("same q", idempotency_key="idem-replay-1")
    assert first.id == second.id

    with session_scope(session_factory) as session:
        rows = outbox.list_for_aggregate(
            session,
            aggregate_type="research_job",
            aggregate_id=first.id,
        )
    assert len(rows) == 1
    assert rows[0].event_type == "research_job.created"


def test_created_outbox_failure_rolls_back_job(
    session_factory: sessionmaker[Session],
) -> None:
    service = ResearchJobService(
        session_factory=session_factory,
        repository=SqlAlchemyResearchJobRepository(),
        outbox=_BoomOutbox(),
        id_factory=lambda: "job-rollback-create",
    )
    with pytest.raises(OutboxEnqueueError):
        service.submit("should roll back", idempotency_key="idem-rollback-1")

    with session_scope(session_factory) as session:
        job = SqlAlchemyResearchJobRepository().get(session, "job-rollback-create")
        count = session.execute(text("SELECT count(*) FROM outbox_events")).scalar_one()
    assert job is None
    assert count == 0


def test_completion_event_claim_fencing(
    session_factory: sessionmaker[Session],
) -> None:
    job_repo = SqlAlchemyResearchJobRepository()
    outbox = SqlAlchemyOutboxRepository()
    job = ResearchJob.create("job-complete-fence", "q", at=T0)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            job,
            idempotency_key="idem-complete-fence",
            request_fingerprint="a" * 64,
        )
        claimed = job_repo.claim_next(
            session,
            now=T0,
            lease_expires_at=T0 + timedelta(seconds=90),
            claim_token="a" * 64,
        )
    assert claimed is not None

    with session_scope(session_factory) as session:
        owned = job_repo.finalize_completion(
            session,
            job_id=claimed.job.id,
            claim_token=claimed.claim_token,
            result="done",
            at=T0 + timedelta(seconds=1),
        )
        assert owned is True
        outbox.enqueue(
            session,
            build_research_job_completed(
                research_job_id=claimed.job.id,
                completed_at=T0 + timedelta(seconds=1),
            ),
        )

    # Stale token cannot complete again / enqueue another via fencing.
    with session_scope(session_factory) as session:
        owned_stale = job_repo.finalize_completion(
            session,
            job_id=claimed.job.id,
            claim_token=claimed.claim_token,
            result="again",
            at=T0 + timedelta(seconds=2),
        )
    assert owned_stale is False

    with session_scope(session_factory) as session:
        rows = outbox.list_for_aggregate(
            session,
            aggregate_type="research_job",
            aggregate_id=claimed.job.id,
        )
    assert [row.event_type for row in rows] == ["research_job.completed"]


def test_worker_completion_outbox_failure_no_false_terminal(
    session_factory: sessionmaker[Session],
) -> None:
    job_repo = SqlAlchemyResearchJobRepository()
    job = ResearchJob.create("job-worker-outbox-fail", "q", at=T0)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            job,
            idempotency_key="idem-worker-outbox-fail",
            request_fingerprint="b" * 64,
        )

    def _processor(
        question: str,
        *,
        job_id: str,
        claim_token: str,
        continuation_mode: object = None,
        active_workflow_execution_id: str | None = None,
    ) -> object:
        del question, claim_token, continuation_mode, active_workflow_execution_id
        from atlas.application.job_processing import CompletedProcessing

        return CompletedProcessing(result="ok", workflow_execution_id="exec-1")

    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=job_repo,
        processor=_processor,  # type: ignore[arg-type]
        poll_interval_seconds=0.01,
        processing_timeout_seconds=5.0,
        lease_seconds=90.0,
        outbox=_BoomOutbox(),
    )
    try:
        with pytest.raises(OutboxEnqueueError):
            worker.run_once()
    finally:
        worker.close()

    with session_scope(session_factory) as session:
        loaded = job_repo.get(session, "job-worker-outbox-fail")
        count = session.execute(text("SELECT count(*) FROM outbox_events")).scalar_one()
    assert loaded is not None
    assert loaded.status is ResearchJobStatus.RUNNING
    assert count == 0


def test_worker_failure_outbox_failure_does_not_recurse(
    session_factory: sessionmaker[Session],
) -> None:
    job_repo = SqlAlchemyResearchJobRepository()
    job = ResearchJob.create("job-worker-fail-outbox", "q", at=T0)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            job,
            idempotency_key="idem-worker-fail-outbox",
            request_fingerprint="c" * 64,
        )

    def _processor(
        question: str,
        *,
        job_id: str,
        claim_token: str,
        continuation_mode: object = None,
        active_workflow_execution_id: str | None = None,
    ) -> object:
        del question, claim_token, continuation_mode, active_workflow_execution_id
        from atlas.application.job_processing import TerminalFailed

        return TerminalFailed(reason_code="BoomCode", workflow_execution_id=None)

    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=job_repo,
        processor=_processor,  # type: ignore[arg-type]
        poll_interval_seconds=0.01,
        processing_timeout_seconds=5.0,
        lease_seconds=90.0,
        outbox=_BoomOutbox(),
    )
    try:
        with pytest.raises(OutboxEnqueueError):
            worker.run_once()
    finally:
        worker.close()

    with session_scope(session_factory) as session:
        loaded = job_repo.get(session, "job-worker-fail-outbox")
        count = session.execute(text("SELECT count(*) FROM outbox_events")).scalar_one()
    assert loaded is not None
    assert loaded.status is ResearchJobStatus.RUNNING
    assert count == 0
