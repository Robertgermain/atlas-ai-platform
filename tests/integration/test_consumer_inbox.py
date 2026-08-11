"""Real-PostgreSQL inbox/dedup/projection tests for Slice 13C2A.

Uses a fake Kafka message/consumer double (``atlas.consumer.fakes``) so
these tests exercise the real ``SqlAlchemyInboxRepository`` and
``SqlAlchemyResearchJobProjectionRepository`` against a real database
without requiring a Kafka broker. Real-broker end-to-end coverage lives in
``test_consumer_kafka.py``.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session, sessionmaker

from atlas.consumer.fakes import FakeKafkaConsumer, build_kafka_message_for_event
from atlas.consumer.identity import RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1
from atlas.consumer.runner import ConsumerRunner, ProcessOutcome
from atlas.eventing import (
    DomainEvent,
    build_research_job_completed,
    build_research_job_created,
    build_research_job_failed,
)
from atlas.persistence.db import session_scope
from atlas.persistence.models.consumer import (
    ConsumerDeadLetterModel,
    ConsumerInboxModel,
    ResearchJobEventProjectionModel,
)
from atlas.persistence.repositories.consumer_dead_letter import (
    SqlAlchemyDeadLetterRepository,
)
from atlas.persistence.repositories.consumer_inbox import SqlAlchemyInboxRepository
from atlas.persistence.repositories.research_job_projection import (
    SqlAlchemyResearchJobProjectionRepository,
)

T0 = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)
_CONSUMER_ID = RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1


class _CountingProjection:
    """Wraps the real repository and counts ``apply()`` invocations."""

    def __init__(self) -> None:
        self._inner = SqlAlchemyResearchJobProjectionRepository()
        self.apply_calls = 0

    def apply(self, session: Session, event: DomainEvent, *, at: datetime) -> None:
        self.apply_calls += 1
        self._inner.apply(session, event, at=at)


def _runner(
    session_factory: sessionmaker[Session],
    consumer: FakeKafkaConsumer,
    *,
    projection: _CountingProjection | None = None,
    clock: Callable[[], datetime] = lambda: T0,
) -> tuple[ConsumerRunner, _CountingProjection]:
    projection = projection or _CountingProjection()
    runner = ConsumerRunner(
        consumer=consumer,  # type: ignore[arg-type]
        session_factory=session_factory,
        inbox=SqlAlchemyInboxRepository(),
        projection=projection,
        dead_letters=SqlAlchemyDeadLetterRepository(),
        consumer_id=_CONSUMER_ID,
        poll_timeout_seconds=1.0,
        clock=clock,
    )
    return runner, projection


def test_new_event_is_recorded_and_applied_atomically(
    session_factory: sessionmaker[Session],
) -> None:
    event = build_research_job_created(
        research_job_id="job-inbox-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    runner, projection = _runner(session_factory, consumer)

    outcome = runner.run_once()

    assert outcome == ProcessOutcome.APPLIED
    assert projection.apply_calls == 1
    assert consumer.committed == [message]

    with session_scope(session_factory) as session:
        inbox_row = session.get(ConsumerInboxModel, (_CONSUMER_ID, event.event_id))
        assert inbox_row is not None
        assert inbox_row.event_type == "research_job.created"
        assert inbox_row.kafka_partition == 0
        assert inbox_row.kafka_offset == 1

        projection_row = session.get(ResearchJobEventProjectionModel, "job-inbox-1")
        assert projection_row is not None
        assert projection_row.last_event_id == event.event_id
        assert projection_row.last_event_type == "research_job.created"


def test_duplicate_event_id_is_skipped_but_offset_still_commits(
    session_factory: sessionmaker[Session],
) -> None:
    event = build_research_job_created(
        research_job_id="job-inbox-2", created_at=T0, event_id=uuid4()
    )
    message_1 = build_kafka_message_for_event(event, partition=0, offset=10)
    message_2 = build_kafka_message_for_event(event, partition=0, offset=10)
    consumer = FakeKafkaConsumer([message_1, message_2])
    runner, projection = _runner(session_factory, consumer)

    first = runner.run_once()
    second = runner.run_once()

    assert first == ProcessOutcome.APPLIED
    assert second == ProcessOutcome.DUPLICATE
    assert projection.apply_calls == 1  # effect applied exactly once
    assert consumer.committed == [message_1, message_2]  # both offsets committed


def test_lifecycle_order_violation_is_rejected_and_rolled_back(
    session_factory: sessionmaker[Session],
) -> None:
    research_job_id = "job-inbox-3"
    created = build_research_job_created(
        research_job_id=research_job_id, created_at=T0, event_id=uuid4()
    )
    completed = build_research_job_completed(
        research_job_id=research_job_id,
        completed_at=T0 + timedelta(minutes=5),
        event_id=uuid4(),
    )
    stray_after_terminal = build_research_job_failed(
        research_job_id=research_job_id,
        failed_at=T0 + timedelta(minutes=10),
        reason_class="StrayOrderingAnomaly",
        event_id=uuid4(),
    )
    consumer = FakeKafkaConsumer(
        [
            build_kafka_message_for_event(created, offset=1),
            build_kafka_message_for_event(completed, offset=2),
            build_kafka_message_for_event(stray_after_terminal, offset=3),
        ]
    )
    runner, projection = _runner(session_factory, consumer)

    assert runner.run_once() == ProcessOutcome.APPLIED
    assert runner.run_once() == ProcessOutcome.APPLIED
    assert projection.apply_calls == 2

    # A lifecycle-order violation is permanent, record-specific poison
    # (Slice 13C2B): it is dead-lettered, not raised, and its offset is
    # still committed so the poisoned record never blocks the partition.
    outcome = runner.run_once()
    assert outcome == ProcessOutcome.DEAD_LETTERED

    assert len(consumer.committed) == 3

    with session_scope(session_factory) as session:
        inbox_row = session.get(
            ConsumerInboxModel, (_CONSUMER_ID, stray_after_terminal.event_id)
        )
        assert inbox_row is None

        projection_row = session.get(ResearchJobEventProjectionModel, research_job_id)
        assert projection_row is not None
        assert projection_row.last_event_id == completed.event_id
        assert projection_row.last_event_type == "research_job.completed"

        dead_letter = (
            session.query(ConsumerDeadLetterModel)
            .filter_by(consumer_id=_CONSUMER_ID, kafka_partition=0, kafka_offset=3)
            .one()
        )
        assert dead_letter.failure_code == "lifecycle_order_violation"
        assert dead_letter.event_id == stray_after_terminal.event_id
        assert dead_letter.replay_eligible is True


def test_crash_before_db_commit_leaves_no_partial_state_and_offset_uncommitted(
    session_factory: sessionmaker[Session],
) -> None:
    """A failure applying the effect must roll back the whole transaction.

    Simulates the "crash before PostgreSQL commit" window via fault
    injection (mirroring ``ClockAdvancingProducer``'s precedent) rather
    than an actual process kill: the effect raises once, so nothing is
    written and the Kafka offset is never committed. A retry of the exact
    same record then succeeds cleanly with a single applied effect.
    """
    event = build_research_job_created(
        research_job_id="job-inbox-4", created_at=T0, event_id=uuid4()
    )
    message_1 = build_kafka_message_for_event(event, partition=0, offset=20)
    message_2 = build_kafka_message_for_event(event, partition=0, offset=20)
    consumer = FakeKafkaConsumer([message_1, message_2])

    class _FaultyOnce:
        def __init__(self, inner: _CountingProjection) -> None:
            self.inner = inner
            self._raise_next = True

        def apply(self, session: Session, event: DomainEvent, *, at: datetime) -> None:
            if self._raise_next:
                self._raise_next = False
                raise RuntimeError("synthetic-crash-before-commit")
            self.inner.apply(session, event, at=at)

    inner = _CountingProjection()
    faulty = _FaultyOnce(inner)
    runner = ConsumerRunner(
        consumer=consumer,  # type: ignore[arg-type]
        session_factory=session_factory,
        inbox=SqlAlchemyInboxRepository(),
        projection=faulty,
        dead_letters=SqlAlchemyDeadLetterRepository(),
        consumer_id=_CONSUMER_ID,
        poll_timeout_seconds=1.0,
        clock=lambda: T0,
    )

    with pytest.raises(RuntimeError, match="synthetic-crash-before-commit"):
        runner.run_once()
    assert consumer.committed == []

    with session_scope(session_factory) as session:
        assert session.get(ConsumerInboxModel, (_CONSUMER_ID, event.event_id)) is None
        assert session.get(ResearchJobEventProjectionModel, "job-inbox-4") is None

    outcome = runner.run_once()
    assert outcome == ProcessOutcome.APPLIED
    assert inner.apply_calls == 1
    assert consumer.committed == [message_2]

    with session_scope(session_factory) as session:
        inbox_row = session.get(ConsumerInboxModel, (_CONSUMER_ID, event.event_id))
        assert inbox_row is not None
        projection_row = session.get(ResearchJobEventProjectionModel, "job-inbox-4")
        assert projection_row is not None
        assert projection_row.last_event_id == event.event_id


def test_crash_after_db_commit_before_offset_ack_then_redelivery(
    session_factory: sessionmaker[Session],
) -> None:
    """The DB effect must be durable even if the offset commit itself fails.

    Simulates "crash after PostgreSQL commit but before the Kafka offset
    acknowledgment" by making the *offset* commit (not the DB transaction)
    fail once. Redelivery of the same record after "restart" must then be
    recognized as a duplicate -- the effect is never reapplied -- while the
    offset finally commits.
    """
    event = build_research_job_created(
        research_job_id="job-inbox-5", created_at=T0, event_id=uuid4()
    )
    message_1 = build_kafka_message_for_event(event, partition=0, offset=30)
    message_2 = build_kafka_message_for_event(event, partition=0, offset=30)
    consumer = FakeKafkaConsumer([message_1, message_2])
    consumer.raise_on_commit = RuntimeError("synthetic-crash-before-offset-ack")
    runner, projection = _runner(session_factory, consumer)

    with pytest.raises(RuntimeError, match="synthetic-crash-before-offset-ack"):
        runner.run_once()
    assert projection.apply_calls == 1  # the DB effect *did* commit
    assert consumer.committed == []  # but the offset was never acknowledged

    with session_scope(session_factory) as session:
        inbox_row = session.get(ConsumerInboxModel, (_CONSUMER_ID, event.event_id))
        assert inbox_row is not None  # durable despite the offset-ack failure

    # "Restart": the offset-commit fault is cleared, and the same record is
    # redelivered (offset was never advanced).
    consumer.raise_on_commit = None
    outcome = runner.run_once()

    assert outcome == ProcessOutcome.DUPLICATE
    assert projection.apply_calls == 1  # effect never reapplied
    assert consumer.committed == [message_2]
