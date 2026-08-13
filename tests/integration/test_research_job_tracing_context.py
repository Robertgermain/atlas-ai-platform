"""PostgreSQL integration tests for durable trace-context propagation
(Slice 15A3): atomic first-parent consumption, concurrent-claim exclusivity,
and immediate crash/lease-reclaim before any workflow execution exists.

See ``atlas.persistence.repositories.research_job.claim_next`` and
``alembic/versions/20260812_0014_tracing_context.py`` for the full durable
contract this exercises.
"""

from __future__ import annotations

import secrets
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from atlas.application.ports import ClaimedResearchJob
from atlas.domain import ResearchJob
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository

T0 = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)

_VALID_TRACEPARENT = "00-" + "a" * 32 + "-" + "b" * 16 + "-01"


def _seed_pending(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
    traceparent: str | None,
) -> None:
    repo = SqlAlchemyResearchJobRepository()
    with session_scope(session_factory) as session:
        repo.add(
            session,
            ResearchJob.create(job_id, "question", at=T0),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="a" * 64,
            traceparent=traceparent,
        )


def test_first_claim_with_stored_traceparent_consumes_it_and_grants_parent(
    session_factory: sessionmaker[Session],
) -> None:
    _seed_pending(
        session_factory, job_id="trace-first-claim", traceparent=_VALID_TRACEPARENT
    )
    repo = SqlAlchemyResearchJobRepository()
    now = T0 + timedelta(seconds=1)

    with session_scope(session_factory) as session:
        claimed = repo.claim_next(
            session,
            now=now,
            lease_expires_at=now + timedelta(seconds=30),
            claim_token=secrets.token_hex(32),
        )

    assert claimed is not None
    assert claimed.traceparent == _VALID_TRACEPARENT
    assert claimed.use_traceparent_as_parent is True

    with session_factory() as session:
        row = (
            session.execute(
                text(
                    "SELECT initial_traceparent_consumed_at FROM research_jobs"
                    " WHERE id = :id"
                ),
                {"id": "trace-first-claim"},
            )
            .mappings()
            .one()
        )
    assert row["initial_traceparent_consumed_at"] is not None


def test_job_without_stored_traceparent_never_grants_parent_eligibility(
    session_factory: sessionmaker[Session],
) -> None:
    _seed_pending(session_factory, job_id="trace-none", traceparent=None)
    repo = SqlAlchemyResearchJobRepository()
    now = T0 + timedelta(seconds=1)

    with session_scope(session_factory) as session:
        claimed = repo.claim_next(
            session,
            now=now,
            lease_expires_at=now + timedelta(seconds=30),
            claim_token=secrets.token_hex(32),
        )

    assert claimed is not None
    assert claimed.traceparent is None
    assert claimed.use_traceparent_as_parent is False

    with session_factory() as session:
        row = (
            session.execute(
                text(
                    "SELECT initial_traceparent_consumed_at FROM research_jobs"
                    " WHERE id = :id"
                ),
                {"id": "trace-none"},
            )
            .mappings()
            .one()
        )
    assert row["initial_traceparent_consumed_at"] is None


def test_immediate_crash_reclaim_before_workflow_execution_never_reuses_parent(
    session_factory: sessionmaker[Session],
) -> None:
    """A crash/lease reclaim of a row whose first claim already consumed
    the marker -- but never got as far as creating a workflow execution --
    must see the marker already non-null and must never be granted
    ``use_traceparent_as_parent=True`` a second time."""
    _seed_pending(
        session_factory, job_id="trace-crash-reclaim", traceparent=_VALID_TRACEPARENT
    )
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
    assert claimed_a.use_traceparent_as_parent is True
    # Simulate a crash: no workflow execution is ever created, and the
    # lease is left to expire without any finalize/abandon call.

    with session_factory() as session:
        session.execute(
            text("UPDATE research_jobs SET lease_expires_at = :expired WHERE id = :id"),
            {"id": "trace-crash-reclaim", "expired": now_a - timedelta(seconds=1)},
        )
        session.commit()

    now_b = now_a + timedelta(seconds=60)
    with session_scope(session_factory) as session:
        claimed_b = repo.claim_next(
            session,
            now=now_b,
            lease_expires_at=now_b + timedelta(seconds=30),
            claim_token=secrets.token_hex(32),
        )

    assert claimed_b is not None
    assert claimed_b.traceparent == _VALID_TRACEPARENT
    assert claimed_b.use_traceparent_as_parent is False


def test_review_continuation_reclaim_never_reuses_parent(
    session_factory: sessionmaker[Session],
) -> None:
    """A later legitimate RUNNING re-claim (e.g. the worker process
    restarting mid-lease without crashing) must also see the marker
    already consumed and never be granted direct-parent eligibility
    twice, even though it is a normal fencing reclaim rather than a
    crash."""
    _seed_pending(
        session_factory,
        job_id="trace-second-running-claim",
        traceparent=_VALID_TRACEPARENT,
    )
    repo = SqlAlchemyResearchJobRepository()
    now_a = T0 + timedelta(seconds=1)

    with session_scope(session_factory) as session:
        claimed_a = repo.claim_next(
            session,
            now=now_a,
            lease_expires_at=now_a + timedelta(seconds=5),
            claim_token=secrets.token_hex(32),
        )
    assert claimed_a is not None
    assert claimed_a.use_traceparent_as_parent is True

    now_b = now_a + timedelta(seconds=10)
    with session_scope(session_factory) as session:
        claimed_b = repo.claim_next(
            session,
            now=now_b,
            lease_expires_at=now_b + timedelta(seconds=30),
            claim_token=secrets.token_hex(32),
        )

    assert claimed_b is not None
    assert claimed_b.job.id == "trace-second-running-claim"
    assert claimed_b.use_traceparent_as_parent is False


def test_concurrent_claim_consumes_the_marker_at_most_once(
    session_factory: sessionmaker[Session],
) -> None:
    """Two sessions race to claim the same row; ``FOR UPDATE SKIP LOCKED``
    means only one ever observes the row at all, so only one claim can
    ever see ``use_traceparent_as_parent=True`` for this row, ever."""
    _seed_pending(
        session_factory, job_id="trace-concurrent", traceparent=_VALID_TRACEPARENT
    )
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

    claimed_a = outcomes["a"]
    assert isinstance(claimed_a, ClaimedResearchJob)
    assert claimed_a.use_traceparent_as_parent is True
    # B was skipped entirely by SKIP LOCKED (row still held by A's open
    # transaction), so B claimed nothing for this row.
    assert outcomes["b"] is None
