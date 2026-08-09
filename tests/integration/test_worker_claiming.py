"""PostgreSQL integration tests for worker claiming and fencing."""

from __future__ import annotations

import secrets
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from atlas.application.worker import ResearchJobWorker
from atlas.domain import ResearchJob, ResearchJobStatus
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository

T0 = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)


def _seed_pending(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
    question: str,
) -> None:
    repo = SqlAlchemyResearchJobRepository()
    with session_scope(session_factory) as session:
        repo.add(
            session,
            ResearchJob.create(job_id, question, at=T0),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="a" * 64,
        )


def test_claim_next_starts_pending_job(
    session_factory: sessionmaker[Session],
) -> None:
    _seed_pending(session_factory, job_id="claim-1", question="Q1")
    repo = SqlAlchemyResearchJobRepository()
    now = T0 + timedelta(seconds=1)
    token = secrets.token_hex(32)

    with session_scope(session_factory) as session:
        claimed = repo.claim_next(
            session,
            now=now,
            lease_expires_at=now + timedelta(seconds=30),
            claim_token=token,
        )

    assert claimed is not None
    assert claimed.claim_token == token
    assert claimed.job.status is ResearchJobStatus.RUNNING
    assert claimed.job.started_at == now

    with session_factory() as session:
        row = (
            session.execute(
                text(
                    """
                    SELECT status, claim_token, lease_expires_at
                    FROM research_jobs WHERE id = :id
                    """
                ),
                {"id": "claim-1"},
            )
            .mappings()
            .one()
        )
    assert row["status"] == "RUNNING"
    assert row["claim_token"] == token
    assert row["lease_expires_at"] is not None


def test_stale_token_finalize_rejected_after_reclaim(
    session_factory: sessionmaker[Session],
) -> None:
    """Worker A loses fencing after Worker B reclaims an expired lease."""
    _seed_pending(session_factory, job_id="fence-1", question="Fence me")
    repo = SqlAlchemyResearchJobRepository()

    now_a = T0 + timedelta(seconds=1)
    token_a = secrets.token_hex(32)
    with session_scope(session_factory) as session:
        claimed_a = repo.claim_next(
            session,
            now=now_a,
            lease_expires_at=now_a + timedelta(seconds=30),
            claim_token=token_a,
        )
    assert claimed_a is not None
    assert claimed_a.claim_token == token_a

    with session_factory() as session:
        session.execute(
            text(
                """
                UPDATE research_jobs
                SET lease_expires_at = :expired
                WHERE id = :id
                """
            ),
            {"id": "fence-1", "expired": now_a - timedelta(seconds=1)},
        )
        session.commit()

    now_b = now_a + timedelta(seconds=60)
    token_b = secrets.token_hex(32)
    with session_scope(session_factory) as session:
        claimed_b = repo.claim_next(
            session,
            now=now_b,
            lease_expires_at=now_b + timedelta(seconds=30),
            claim_token=token_b,
        )
    assert claimed_b is not None
    assert claimed_b.claim_token == token_b
    assert token_b != token_a

    with session_scope(session_factory) as session:
        owned_a = repo.finalize_completion(
            session,
            job_id="fence-1",
            claim_token=token_a,
            result="stale result from A",
            at=now_b + timedelta(seconds=1),
        )
    assert owned_a is False

    with session_scope(session_factory) as session:
        owned_b = repo.finalize_completion(
            session,
            job_id="fence-1",
            claim_token=token_b,
            result="Research completed for: Fence me",
            at=now_b + timedelta(seconds=2),
        )
    assert owned_b is True

    with session_scope(session_factory) as session:
        loaded = repo.get(session, "fence-1")
    assert loaded is not None
    assert loaded.status is ResearchJobStatus.COMPLETED
    assert loaded.result == "Research completed for: Fence me"

    with session_factory() as session:
        row = (
            session.execute(
                text(
                    """
                    SELECT claim_token, lease_expires_at, result
                    FROM research_jobs WHERE id = :id
                    """
                ),
                {"id": "fence-1"},
            )
            .mappings()
            .one()
        )
    assert row["claim_token"] is None
    assert row["lease_expires_at"] is None
    assert row["result"] == "Research completed for: Fence me"


def test_concurrent_claim_uses_skip_locked(
    session_factory: sessionmaker[Session],
) -> None:
    """Two sessions race: while A holds the row lock, B must skip it."""
    _seed_pending(session_factory, job_id="skip-locked-1", question="Only one")
    repo = SqlAlchemyResearchJobRepository()
    now = T0 + timedelta(seconds=1)
    a_locked = Event()
    b_finished = Event()
    outcomes: dict[str, object] = {}

    def claimer_a() -> None:
        session = session_factory()
        try:
            claimed = repo.claim_next(
                session,
                now=now,
                lease_expires_at=now + timedelta(seconds=30),
                claim_token=secrets.token_hex(32),
            )
            outcomes["a"] = claimed
            a_locked.set()
            assert b_finished.wait(timeout=5.0)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def claimer_b() -> None:
        assert a_locked.wait(timeout=5.0)
        with session_scope(session_factory) as session:
            claimed = repo.claim_next(
                session,
                now=now,
                lease_expires_at=now + timedelta(seconds=30),
                claim_token=secrets.token_hex(32),
            )
        outcomes["b"] = claimed
        b_finished.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        future_a = pool.submit(claimer_a)
        future_b = pool.submit(claimer_b)
        future_a.result(timeout=10.0)
        future_b.result(timeout=10.0)

    assert outcomes["a"] is not None
    assert outcomes["b"] is None


def test_concurrent_claims_two_pending_jobs(
    session_factory: sessionmaker[Session],
) -> None:
    """Two workers each claim a distinct job under concurrent START."""
    _seed_pending(session_factory, job_id="pair-a", question="A")
    _seed_pending(session_factory, job_id="pair-b", question="B")
    repo = SqlAlchemyResearchJobRepository()
    now = T0 + timedelta(seconds=1)
    start = Event()
    claimed_ids: list[str] = []

    def claim_one() -> None:
        assert start.wait(timeout=5.0)
        with session_scope(session_factory) as session:
            claimed = repo.claim_next(
                session,
                now=now,
                lease_expires_at=now + timedelta(seconds=30),
                claim_token=secrets.token_hex(32),
            )
        assert claimed is not None
        claimed_ids.append(claimed.job.id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(claim_one)
        second = pool.submit(claim_one)
        start.set()
        first.result(timeout=10.0)
        second.result(timeout=10.0)

    assert sorted(claimed_ids) == ["pair-a", "pair-b"]


def test_worker_processes_job_end_to_end(
    test_database_url: str,
    session_factory: sessionmaker[Session],
) -> None:
    from atlas.workflow import LangGraphResearchProcessor, create_checkpoint_runtime

    _seed_pending(session_factory, job_id="worker-e2e", question="End to end")
    runtime = create_checkpoint_runtime(test_database_url)
    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=SqlAlchemyResearchJobRepository(),
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

    repo = SqlAlchemyResearchJobRepository()
    with session_scope(session_factory) as session:
        loaded = repo.get(session, "worker-e2e")
    assert loaded is not None
    assert loaded.status is ResearchJobStatus.COMPLETED
    assert loaded.result is not None
    assert "Question:" in loaded.result
    assert "End to end" in loaded.result
    assert "Plan:" in loaded.result
    assert "Findings:" in loaded.result
    assert "Draft:" in loaded.result
