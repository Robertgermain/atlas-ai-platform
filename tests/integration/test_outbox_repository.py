"""PostgreSQL integration tests for the transactional outbox repository."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from atlas.eventing import (
    build_research_job_created,
    build_research_job_failed,
)
from atlas.outbox.errors import OutboxEnqueueError
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.outbox import SqlAlchemyOutboxRepository

T0 = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)
T1 = T0 + timedelta(seconds=10)
T2 = T0 + timedelta(seconds=20)
LEASE = T0 + timedelta(seconds=30)


@pytest.fixture
def session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )


def test_enqueue_and_monotonic_outbox_position(
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyOutboxRepository()
    with session_scope(session_factory) as session:
        e1 = build_research_job_created(research_job_id="job-a", created_at=T0)
        e2 = build_research_job_created(research_job_id="job-b", created_at=T1)
        repo.enqueue(session, e1)
        repo.enqueue(session, e2)
        rows = repo.list_for_aggregate(
            session, aggregate_type="research_job", aggregate_id="job-a"
        )
        assert len(rows) == 1
        assert rows[0].event_id == e1.event_id
        assert rows[0].payload["research_job_id"] == "job-a"
        all_rows = list(
            session.execute(
                text(
                    "SELECT event_id, outbox_position FROM outbox_events "
                    "ORDER BY outbox_position ASC"
                )
            )
        )
        assert all_rows[0][1] < all_rows[1][1]


def test_payload_size_database_constraint(
    session_factory: sessionmaker[Session],
) -> None:
    event = build_research_job_failed(
        research_job_id="job-size",
        failed_at=T0,
        reason_class="X",
    )
    # Bypass application serializer and insert an oversized JSONB payload.
    with pytest.raises((IntegrityError, OutboxEnqueueError)):
        with session_scope(session_factory) as session:
            session.execute(
                text(
                    """
                    INSERT INTO outbox_events (
                        event_id, event_type, event_version, aggregate_type,
                        aggregate_id, occurred_at, payload, created_at,
                        publish_attempts
                    ) VALUES (
                        :event_id, 'research_job.failed', 1, 'research_job',
                        'job-size', :occurred_at,
                        CAST(:payload AS jsonb), :occurred_at, 0
                    )
                    """
                ),
                {
                    "event_id": str(event.event_id),
                    "occurred_at": T0,
                    "payload": (
                        '{"research_job_id":"job-size","failed_at":"'
                        + T0.isoformat()
                        + '","reason_class":"'
                        + ("R" * 20000)
                        + '"}'
                    ),
                },
            )


def test_claim_ordering_batch_size_and_attempt_increment(
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyOutboxRepository()
    events = [
        build_research_job_created(research_job_id=f"job-{i}", created_at=T0)
        for i in range(3)
    ]
    with session_scope(session_factory) as session:
        for event in events:
            repo.enqueue(session, event)

    with session_scope(session_factory) as session:
        claimed = repo.claim_batch(
            session,
            claimant_token="a" * 64,
            at=T0,
            lease_expires_at=LEASE,
            batch_size=2,
        )
    assert len(claimed) == 2
    assert claimed[0].outbox_position < claimed[1].outbox_position
    assert claimed[0].publish_attempts == 1
    assert claimed[1].publish_attempts == 1
    assert [c.event.event_id for c in claimed] == [
        events[0].event_id,
        events[1].event_id,
    ]

    # Head-of-line: earliest unpublished (events[0]) still has an unexpired
    # lease, so a second claimant must not leapfrog to events[2].
    with session_scope(session_factory) as session:
        claimed2 = repo.claim_batch(
            session,
            claimant_token="b" * 64,
            at=T0,
            lease_expires_at=LEASE,
            batch_size=50,
        )
    assert claimed2 == []


def test_leased_head_blocks_later_positions(
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyOutboxRepository()
    events = [
        build_research_job_created(research_job_id=f"job-hol-{i}", created_at=T0)
        for i in range(3)
    ]
    with session_scope(session_factory) as session:
        for event in events:
            repo.enqueue(session, event)
        # Lease only the global head; leave later rows unclaimed.
        claimed = repo.claim_batch(
            session,
            claimant_token="a" * 64,
            at=T0,
            lease_expires_at=LEASE,
            batch_size=1,
        )
    assert len(claimed) == 1
    assert claimed[0].event.event_id == events[0].event_id

    with session_scope(session_factory) as session:
        blocked = repo.claim_batch(
            session,
            claimant_token="b" * 64,
            at=T0,
            lease_expires_at=LEASE,
            batch_size=50,
        )
    assert blocked == []

    # After head lease expiry, claim is contiguous from the head.
    with session_scope(session_factory) as session:
        reclaimed = repo.claim_batch(
            session,
            claimant_token="b" * 64,
            at=LEASE,
            lease_expires_at=LEASE + timedelta(seconds=30),
            batch_size=50,
        )
    assert [c.event.event_id for c in reclaimed] == [
        events[0].event_id,
        events[1].event_id,
        events[2].event_id,
    ]
    assert reclaimed[0].publish_attempts == 2
    assert reclaimed[1].publish_attempts == 1
    assert reclaimed[2].publish_attempts == 1


def test_lease_expiry_reclaim_and_stale_token_rejection(
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyOutboxRepository()
    event = build_research_job_created(research_job_id="job-lease", created_at=T0)
    with session_scope(session_factory) as session:
        repo.enqueue(session, event)

    token_a = "a" * 64
    with session_scope(session_factory) as session:
        claimed = repo.claim_batch(
            session,
            claimant_token=token_a,
            at=T0,
            lease_expires_at=T1,
            batch_size=10,
        )
    assert len(claimed) == 1

    # Matching token after lease expiry (before reclaim) must not finalize.
    with session_scope(session_factory) as session:
        assert (
            repo.mark_published(
                session,
                event_id=event.event_id,
                claimant_token=token_a,
                at=T1,
            )
            is False
        )
        assert (
            repo.release_failed_claim(
                session,
                event_id=event.event_id,
                claimant_token=token_a,
                at=T1,
                error_class="Timeout",
            )
            is False
        )

    token_b = "b" * 64
    with session_scope(session_factory) as session:
        reclaimed = repo.claim_batch(
            session,
            claimant_token=token_b,
            at=T1,
            lease_expires_at=T2,
            batch_size=10,
        )
    assert len(reclaimed) == 1
    assert reclaimed[0].publish_attempts == 2

    # Stale previous owner cannot finalize after reclaim.
    with session_scope(session_factory) as session:
        assert (
            repo.mark_published(
                session,
                event_id=event.event_id,
                claimant_token=token_a,
                at=T1,
            )
            is False
        )
        assert (
            repo.mark_published(
                session,
                event_id=event.event_id,
                claimant_token=token_b,
                at=T1,
            )
            is True
        )

    with session_scope(session_factory) as session:
        row = repo.get_by_event_id(session, event.event_id)
        assert row is not None
        assert row.published_at == T1
        assert row.publish_claim_token is None
        assert row.publish_lease_expires_at is None
        assert row.last_publish_error_class is None


def test_release_failed_claim_stores_sanitized_class_only(
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyOutboxRepository()
    event = build_research_job_created(research_job_id="job-fail", created_at=T0)
    token = "c" * 64
    with session_scope(session_factory) as session:
        repo.enqueue(session, event)
        repo.claim_batch(
            session,
            claimant_token=token,
            at=T0,
            lease_expires_at=LEASE,
            batch_size=1,
        )
    with session_scope(session_factory) as session:
        assert (
            repo.release_failed_claim(
                session,
                event_id=event.event_id,
                claimant_token=token,
                at=T0,
                error_class="RuntimeError",
            )
            is True
        )
        row = repo.get_by_event_id(session, event.event_id)
        assert row is not None
        assert row.last_publish_error_class == "RuntimeError"
        assert row.publish_claim_token is None
        assert "Traceback" not in (row.last_publish_error_class or "")
        assert "secret" not in str(row.payload).lower()


def test_event_id_in_rebuilt_envelope_matches_row(
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyOutboxRepository()
    event = build_research_job_created(research_job_id="job-id", created_at=T0)
    with session_scope(session_factory) as session:
        repo.enqueue(session, event)
        claimed = repo.claim_batch(
            session,
            claimant_token="d" * 64,
            at=T0,
            lease_expires_at=LEASE,
            batch_size=1,
        )
    assert claimed[0].event.event_id == event.event_id
    assert claimed[0].event_id == event.event_id


def test_locked_head_row_cannot_leapfrog_later_positions(
    session_factory: sessionmaker[Session],
) -> None:
    """While another session holds FOR UPDATE on the head, claim_batch is empty."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    from sqlalchemy import select

    from atlas.persistence.models.outbox import OutboxEventModel

    repo = SqlAlchemyOutboxRepository()
    events = [
        build_research_job_created(research_job_id=f"job-lock-hol-{i}", created_at=T0)
        for i in range(3)
    ]
    with session_scope(session_factory) as session:
        for event in events:
            repo.enqueue(session, event)

    head_locked = Event()
    b_finished = Event()
    outcomes: dict[str, object] = {}

    def holder_a() -> None:
        session = session_factory()
        try:
            head = session.scalars(
                select(OutboxEventModel)
                .where(OutboxEventModel.published_at.is_(None))
                .order_by(OutboxEventModel.outbox_position.asc())
                .limit(1)
                .with_for_update()
            ).one()
            assert head.event_id == events[0].event_id
            head_locked.set()
            assert b_finished.wait(timeout=5.0)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def claimant_b() -> None:
        assert head_locked.wait(timeout=5.0)
        with session_scope(session_factory) as session:
            claimed = repo.claim_batch(
                session,
                claimant_token="b" * 64,
                at=T0,
                lease_expires_at=LEASE,
                batch_size=50,
            )
        outcomes["b"] = claimed
        b_finished.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        future_a = pool.submit(holder_a)
        future_b = pool.submit(claimant_b)
        future_a.result(timeout=10.0)
        future_b.result(timeout=10.0)

    assert outcomes["b"] == []
    with session_scope(session_factory) as session:
        for event in events:
            row = repo.get_by_event_id(session, event.event_id)
            assert row is not None
            assert row.publish_claim_token is None
            assert row.published_at is None
            assert row.publish_attempts == 0
