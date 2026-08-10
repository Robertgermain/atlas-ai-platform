"""PostgreSQL integration tests for outbox relay orchestration."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from atlas.eventing import build_research_job_created
from atlas.outbox.clock import ControllableClock
from atlas.outbox.errors import RelayOwnershipError
from atlas.outbox.fakes import ClockAdvancingProducer, FakeEventProducer
from atlas.outbox.relay import OutboxRelay
from atlas.outbox.relay_lock import PostgresOutboxRelayLock
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.outbox import SqlAlchemyOutboxRepository

T0 = datetime(2026, 8, 10, 15, 0, 0, tzinfo=UTC)
T1 = T0 + timedelta(seconds=60)


@pytest.fixture
def session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )


def _relay(
    *,
    engine: Engine,
    session_factory: sessionmaker[Session],
    producer: object,
    clock: ControllableClock,
    publish_lease_seconds: float = 30.0,
    batch_size: int = 50,
) -> tuple[OutboxRelay, PostgresOutboxRelayLock]:
    lock = PostgresOutboxRelayLock(engine)
    lock.acquire()
    relay = OutboxRelay(
        session_factory=session_factory,
        repository=SqlAlchemyOutboxRepository(),
        producer=producer,  # type: ignore[arg-type]
        lock=lock,
        batch_size=batch_size,
        publish_lease_seconds=publish_lease_seconds,
        clock=clock,
    )
    return relay, lock


def test_relay_publishes_outside_claim_transaction(
    engine: Engine,
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyOutboxRepository()
    event = build_research_job_created(research_job_id="job-relay", created_at=T0)
    with session_scope(session_factory) as session:
        repo.enqueue(session, event)

    clock = ControllableClock(T0)
    producer = FakeEventProducer()
    relay, lock = _relay(
        engine=engine,
        session_factory=session_factory,
        producer=producer,
        clock=clock,
    )
    try:
        published = relay.run_once()
    finally:
        lock.release()

    assert published == 1
    assert len(producer.published) == 1
    assert producer.published[0].event_id == event.event_id

    with session_scope(session_factory) as session:
        row = repo.get_by_event_id(session, event.event_id)
        assert row is not None
        assert row.published_at == T0
        assert row.publish_claim_token is None
        assert row.publish_attempts == 1


def test_relay_producer_failure_releases_claim(
    engine: Engine,
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyOutboxRepository()
    event = build_research_job_created(research_job_id="job-fail-pub", created_at=T0)
    with session_scope(session_factory) as session:
        repo.enqueue(session, event)

    clock = ControllableClock(T0)
    producer = FakeEventProducer(fail_on_event_ids={event.event_id})
    relay, lock = _relay(
        engine=engine,
        session_factory=session_factory,
        producer=producer,
        clock=clock,
    )
    try:
        published = relay.run_once()
    finally:
        lock.release()

    assert published == 0
    with session_scope(session_factory) as session:
        row = repo.get_by_event_id(session, event.event_id)
        assert row is not None
        assert row.published_at is None
        assert row.publish_claim_token is None
        assert row.last_publish_error_class == "RuntimeError"
        assert row.publish_attempts == 1


def test_slow_producer_past_lease_cannot_mark_or_release(
    engine: Engine,
    session_factory: sessionmaker[Session],
) -> None:
    """Fresh post-producer ``at`` must reject mark/release after lease expiry."""
    repo = SqlAlchemyOutboxRepository()
    event = build_research_job_created(research_job_id="job-slow", created_at=T0)
    with session_scope(session_factory) as session:
        repo.enqueue(session, event)

    clock = ControllableClock(T0)
    # Claim lease is claim_at + 5s; producer advances 10s before mark/release.
    producer = ClockAdvancingProducer(
        clock=clock,
        advance_by=timedelta(seconds=10),
    )
    relay, lock = _relay(
        engine=engine,
        session_factory=session_factory,
        producer=producer,
        clock=clock,
        publish_lease_seconds=5.0,
    )
    try:
        published = relay.run_once()
    finally:
        lock.release()

    assert published == 0
    assert len(producer.published) == 1  # producer acked
    with session_scope(session_factory) as session:
        row = repo.get_by_event_id(session, event.event_id)
        assert row is not None
        assert row.published_at is None
        # Stale owner could not release either (lease already expired).
        assert row.publish_claim_token is not None
        assert row.publish_lease_expires_at == T0 + timedelta(seconds=5)
        assert row.last_publish_error_class is None


def test_stale_owner_cannot_overwrite_later_claimant(
    engine: Engine,
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyOutboxRepository()
    event = build_research_job_created(research_job_id="job-stale", created_at=T0)
    with session_scope(session_factory) as session:
        repo.enqueue(session, event)

    clock = ControllableClock(T0)
    producer = ClockAdvancingProducer(
        clock=clock,
        advance_by=timedelta(seconds=10),
    )
    relay, lock = _relay(
        engine=engine,
        session_factory=session_factory,
        producer=producer,
        clock=clock,
        publish_lease_seconds=5.0,
    )
    try:
        assert relay.run_once() == 0
    finally:
        lock.release()

    # Later claimant reclaims after lease expiry and finalizes.
    with session_scope(session_factory) as session:
        reclaimed = repo.claim_batch(
            session,
            claimant_token="b" * 64,
            at=T0 + timedelta(seconds=10),
            lease_expires_at=T1,
            batch_size=1,
        )
    assert len(reclaimed) == 1
    assert reclaimed[0].publish_attempts == 2

    with session_scope(session_factory) as session:
        assert (
            repo.mark_published(
                session,
                event_id=event.event_id,
                claimant_token="a" * 64,  # arbitrary stale token
                at=T0 + timedelta(seconds=10),
            )
            is False
        )
        assert (
            repo.mark_published(
                session,
                event_id=event.event_id,
                claimant_token="b" * 64,
                at=T0 + timedelta(seconds=10),
            )
            is True
        )


def test_failure_stops_later_events_in_batch(
    engine: Engine,
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyOutboxRepository()
    events = [
        build_research_job_created(research_job_id=f"job-ord-{i}", created_at=T0)
        for i in range(3)
    ]
    with session_scope(session_factory) as session:
        for event in events:
            repo.enqueue(session, event)

    clock = ControllableClock(T0)
    producer = FakeEventProducer(fail_on_event_ids={events[0].event_id})
    relay, lock = _relay(
        engine=engine,
        session_factory=session_factory,
        producer=producer,
        clock=clock,
        batch_size=50,
    )
    try:
        published = relay.run_once()
    finally:
        lock.release()

    assert published == 0
    assert producer.attempts == [events[0]]
    assert producer.published == []

    with session_scope(session_factory) as session:
        rows = [repo.get_by_event_id(session, event.event_id) for event in events]
        assert rows[0] is not None and rows[1] is not None and rows[2] is not None
        assert rows[0].published_at is None
        assert rows[0].last_publish_error_class == "RuntimeError"
        assert rows[0].publish_claim_token is None
        # Later claimed rows were released without publishing.
        assert rows[1].published_at is None
        assert rows[1].publish_claim_token is None
        assert rows[1].last_publish_error_class == "EarlierEventPublishFailure"
        assert rows[2].published_at is None
        assert rows[2].publish_claim_token is None
        assert rows[2].last_publish_error_class == "EarlierEventPublishFailure"


def test_ordered_recovery_after_earlier_failure(
    engine: Engine,
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyOutboxRepository()
    events = [
        build_research_job_created(research_job_id=f"job-rec-{i}", created_at=T0)
        for i in range(2)
    ]
    with session_scope(session_factory) as session:
        for event in events:
            repo.enqueue(session, event)

    clock = ControllableClock(T0)
    failing = FakeEventProducer(fail_on_event_ids={events[0].event_id})
    relay, lock = _relay(
        engine=engine,
        session_factory=session_factory,
        producer=failing,
        clock=clock,
    )
    try:
        assert relay.run_once() == 0
    finally:
        lock.release()

    # Later run: event N succeeds, then N+1 publishes in order.
    clock.set(T0 + timedelta(seconds=1))
    succeeding = FakeEventProducer()
    relay, lock = _relay(
        engine=engine,
        session_factory=session_factory,
        producer=succeeding,
        clock=clock,
    )
    try:
        published = relay.run_once()
    finally:
        lock.release()

    assert published == 2
    assert [event.event_id for event in succeeding.published] == [
        events[0].event_id,
        events[1].event_id,
    ]
    with session_scope(session_factory) as session:
        row0 = repo.get_by_event_id(session, events[0].event_id)
        row1 = repo.get_by_event_id(session, events[1].event_id)
        assert row0 is not None and row1 is not None
        assert row0.published_at is not None
        assert row1.published_at is not None
        assert row0.outbox_position < row1.outbox_position
        # publish_attempts counts claims: fail claim + success claim for each
        # row that was claimed in both the failing and recovering runs.
        assert row0.publish_attempts == 2
        assert row1.publish_attempts == 2


def test_lost_mark_ownership_stops_later_events(
    engine: Engine,
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyOutboxRepository()
    events = [
        build_research_job_created(research_job_id=f"job-lost-{i}", created_at=T0)
        for i in range(2)
    ]
    with session_scope(session_factory) as session:
        for event in events:
            repo.enqueue(session, event)

    clock = ControllableClock(T0)
    producer = ClockAdvancingProducer(
        clock=clock,
        advance_by=timedelta(seconds=10),
    )
    relay, lock = _relay(
        engine=engine,
        session_factory=session_factory,
        producer=producer,
        clock=clock,
        publish_lease_seconds=5.0,
    )
    try:
        published = relay.run_once()
    finally:
        lock.release()

    assert published == 0
    # Only the first event was offered to the producer.
    assert len(producer.attempts) == 1
    assert producer.attempts[0].event_id == events[0].event_id
    assert len(producer.published) == 1

    with session_scope(session_factory) as session:
        row0 = repo.get_by_event_id(session, events[0].event_id)
        row1 = repo.get_by_event_id(session, events[1].event_id)
        assert row0 is not None and row1 is not None
        assert row0.published_at is None
        assert row1.published_at is None
        # Remaining row was not published. Release may no-op once the shared
        # lease has already expired; either way the row is reclaimable in order.
        assert row1.publish_claim_token is None or (
            row1.publish_lease_expires_at is not None
            and row1.publish_lease_expires_at <= clock()
        )


def test_recovery_after_lost_mark_preserves_order(
    engine: Engine,
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyOutboxRepository()
    events = [
        build_research_job_created(research_job_id=f"job-gap-ord-{i}", created_at=T0)
        for i in range(2)
    ]
    with session_scope(session_factory) as session:
        for event in events:
            repo.enqueue(session, event)

    clock = ControllableClock(T0)
    slow = ClockAdvancingProducer(
        clock=clock,
        advance_by=timedelta(seconds=10),
    )
    relay, lock = _relay(
        engine=engine,
        session_factory=session_factory,
        producer=slow,
        clock=clock,
        publish_lease_seconds=5.0,
    )
    try:
        assert relay.run_once() == 0
    finally:
        lock.release()

    # Resume after lease expiry with a fast producer: N then N+1 in order.
    clock.set(T0 + timedelta(seconds=10))
    fast = FakeEventProducer()
    relay, lock = _relay(
        engine=engine,
        session_factory=session_factory,
        producer=fast,
        clock=clock,
        publish_lease_seconds=30.0,
    )
    try:
        published = relay.run_once()
    finally:
        lock.release()

    assert published == 2
    assert [event.event_id for event in fast.published] == [
        events[0].event_id,
        events[1].event_id,
    ]


def test_crash_before_publish_leaves_unpublished(
    session_factory: sessionmaker[Session],
) -> None:
    """Claim then abandon without publish — row remains reclaimable."""
    repo = SqlAlchemyOutboxRepository()
    event = build_research_job_created(research_job_id="job-crash", created_at=T0)
    with session_scope(session_factory) as session:
        repo.enqueue(session, event)
        claimed = repo.claim_batch(
            session,
            claimant_token="a" * 64,
            at=T0,
            lease_expires_at=T0 + timedelta(seconds=5),
            batch_size=1,
        )
    assert len(claimed) == 1
    assert claimed[0].publish_attempts == 1
    with session_scope(session_factory) as session:
        reclaimed = repo.claim_batch(
            session,
            claimant_token="b" * 64,
            at=T0 + timedelta(seconds=5),
            lease_expires_at=T1,
            batch_size=1,
        )
    assert len(reclaimed) == 1
    assert reclaimed[0].event.event_id == event.event_id
    assert reclaimed[0].publish_attempts == 2


def test_producer_success_missing_db_ack_republishes_after_lease(
    engine: Engine,
    session_factory: sessionmaker[Session],
) -> None:
    """At-least-once gap: ack without mark_published → republish same event_id."""
    repo = SqlAlchemyOutboxRepository()
    event = build_research_job_created(research_job_id="job-gap", created_at=T0)
    with session_scope(session_factory) as session:
        repo.enqueue(session, event)
        claimed = repo.claim_batch(
            session,
            claimant_token="a" * 64,
            at=T0,
            lease_expires_at=T0 + timedelta(seconds=5),
            batch_size=1,
        )
    producer = FakeEventProducer()
    producer.publish(claimed[0].event)

    clock = ControllableClock(T0 + timedelta(seconds=5))
    relay, lock = _relay(
        engine=engine,
        session_factory=session_factory,
        producer=producer,
        clock=clock,
        publish_lease_seconds=30.0,
    )
    try:
        published = relay.run_once()
    finally:
        lock.release()

    assert published == 1
    assert len(producer.published) == 2
    assert (
        producer.published[0].event_id
        == producer.published[1].event_id
        == event.event_id
    )


def test_singleton_relay_advisory_lock_exclusion(engine: Engine) -> None:
    first = PostgresOutboxRelayLock(engine)
    second = PostgresOutboxRelayLock(engine)
    first.acquire()
    try:
        with pytest.raises(RelayOwnershipError):
            second.acquire()
        assert second.held is False
    finally:
        first.release()

    second.acquire()
    try:
        assert second.held is True
        with engine.connect() as connection:
            held = connection.execute(
                text("SELECT pg_try_advisory_lock(738192013)")
            ).scalar_one()
            assert held is False
            connection.execute(text("SELECT pg_advisory_unlock(738192013)"))
    finally:
        second.release()


def test_publish_attempts_counts_claims_not_producer_calls(
    session_factory: sessionmaker[Session],
) -> None:
    """``publish_attempts`` increments on claim, even before producer I/O."""
    repo = SqlAlchemyOutboxRepository()
    event = build_research_job_created(research_job_id="job-attempts", created_at=T0)
    with session_scope(session_factory) as session:
        repo.enqueue(session, event)
        claimed = repo.claim_batch(
            session,
            claimant_token="a" * 64,
            at=T0,
            lease_expires_at=T0 + timedelta(seconds=5),
            batch_size=1,
        )
    assert claimed[0].publish_attempts == 1
    with session_scope(session_factory) as session:
        row = repo.get_by_event_id(session, event.event_id)
        assert row is not None
        assert row.publish_attempts == 1
        # No producer call occurred; attempts still reflect the claim.
        assert row.published_at is None


def test_crash_recovery_head_of_line_blocks_until_lease_expires(
    engine: Engine,
    session_factory: sessionmaker[Session],
) -> None:
    """Crashed relay's unexpired claims block a replacement until leases expire."""
    repo = SqlAlchemyOutboxRepository()
    events = [
        build_research_job_created(research_job_id=f"job-crash-hol-{i}", created_at=T0)
        for i in range(3)
    ]
    with session_scope(session_factory) as session:
        for event in events:
            repo.enqueue(session, event)

    # Relay A holds the singleton lock, claims N and N+1, then "crashes"
    # (advisory connection closes) without publishing or releasing claims.
    lock_a = PostgresOutboxRelayLock(engine)
    lock_a.acquire()
    lease_expires = T0 + timedelta(seconds=30)
    with session_scope(session_factory) as session:
        claimed = repo.claim_batch(
            session,
            claimant_token="a" * 64,
            at=T0,
            lease_expires_at=lease_expires,
            batch_size=2,
        )
    assert [c.event.event_id for c in claimed] == [
        events[0].event_id,
        events[1].event_id,
    ]
    # Crash: close the advisory connection without clearing outbox claims.
    lock_a.abandon_connection()

    clock = ControllableClock(T0 + timedelta(seconds=1))
    producer_b = FakeEventProducer()
    relay_b, lock_b = _relay(
        engine=engine,
        session_factory=session_factory,
        producer=producer_b,
        clock=clock,
        publish_lease_seconds=30.0,
    )
    try:
        assert relay_b.run_once() == 0
        assert producer_b.published == []
        assert producer_b.attempts == []
        with session_scope(session_factory) as session:
            row2 = repo.get_by_event_id(session, events[2].event_id)
            assert row2 is not None
            assert row2.publish_claim_token is None
            assert row2.published_at is None
            assert row2.publish_attempts == 0

        # After lease expiry, publish N, N+1, then N+2 in exact order.
        clock.set(lease_expires)
        published = relay_b.run_once()
    finally:
        lock_b.release()

    assert published == 3
    assert [event.event_id for event in producer_b.published] == [
        events[0].event_id,
        events[1].event_id,
        events[2].event_id,
    ]
    with session_scope(session_factory) as session:
        row0 = repo.get_by_event_id(session, events[0].event_id)
        row1 = repo.get_by_event_id(session, events[1].event_id)
        row2 = repo.get_by_event_id(session, events[2].event_id)
        assert row0 is not None and row1 is not None and row2 is not None
        assert row0.published_at is not None
        assert row1.published_at is not None
        assert row2.published_at is not None
        assert row0.outbox_position < row1.outbox_position < row2.outbox_position
        assert row0.publish_attempts == 2
        assert row1.publish_attempts == 2
        assert row2.publish_attempts == 1
