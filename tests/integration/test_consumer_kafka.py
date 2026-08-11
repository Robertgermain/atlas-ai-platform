"""Real-Kafka + real-PostgreSQL end-to-end tests for Slice 13C2A.

Uses the actual fixed, allowlisted consumer group id (Slice 13C2A has no
arbitrary-group escape hatch), so every test that publishes its own
messages first calls ``seed_consumer_group_offset`` to seed that group's
committed offset to the topic's current end -- otherwise a test would
either resume from a previous local run's committed position (Compose's
named volume persists) or, for a brand-new group, from the entire
topic history under ``auto.offset.reset=earliest``. See
``tests/integration/kafka_support.py`` for the helper's rationale.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy.orm import Session, sessionmaker

from atlas.consumer.identity import RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1
from atlas.consumer.kafka_consumer import KafkaEventConsumer
from atlas.consumer.runner import ConsumerRunner, ProcessOutcome
from atlas.eventing import build_research_job_created
from atlas.outbox.kafka_producer import KafkaEventProducer
from atlas.persistence.db import session_scope
from atlas.persistence.models.consumer import (
    ConsumerInboxModel,
    ResearchJobEventProjectionModel,
)
from atlas.persistence.repositories.consumer_inbox import SqlAlchemyInboxRepository
from atlas.persistence.repositories.research_job_projection import (
    SqlAlchemyResearchJobProjectionRepository,
)
from tests.integration.kafka_support import (
    get_topic_end_offset,
    seed_consumer_group_offset,
)

T0 = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)
_CONSUMER_ID = RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1


def _seed_group_to_end(kafka_bootstrap_servers: str) -> None:
    seed_consumer_group_offset(
        kafka_bootstrap_servers,
        group_id=RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1,
        offset=get_topic_end_offset(kafka_bootstrap_servers),
    )


def _new_consumer(kafka_bootstrap_servers: str) -> KafkaEventConsumer:
    return KafkaEventConsumer(
        bootstrap_servers=kafka_bootstrap_servers,
        group_id=RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1,
        session_timeout_seconds=10.0,
        max_poll_interval_seconds=60.0,
    )


def _new_runner(
    consumer: KafkaEventConsumer, session_factory: sessionmaker[Session]
) -> ConsumerRunner:
    return ConsumerRunner(
        consumer=consumer,
        session_factory=session_factory,
        inbox=SqlAlchemyInboxRepository(),
        projection=SqlAlchemyResearchJobProjectionRepository(),
        consumer_id=_CONSUMER_ID,
        poll_timeout_seconds=2.0,
    )


def _run_until(
    runner: ConsumerRunner, outcomes_needed: int, *, max_attempts: int = 30
) -> list[ProcessOutcome]:
    """Poll until ``outcomes_needed`` non-``NO_MESSAGE`` outcomes are seen."""
    seen: list[ProcessOutcome] = []
    for _ in range(max_attempts):
        outcome = runner.run_once()
        if outcome is not ProcessOutcome.NO_MESSAGE:
            seen.append(outcome)
        if len(seen) >= outcomes_needed:
            break
    return seen


def test_end_to_end_publish_and_consume_updates_projection(
    kafka_bootstrap_servers: str,
    session_factory: sessionmaker[Session],
) -> None:
    _seed_group_to_end(kafka_bootstrap_servers)
    research_job_id = f"kafka-e2e-{uuid4().hex}"
    event = build_research_job_created(
        research_job_id=research_job_id, created_at=T0, event_id=uuid4()
    )

    producer = KafkaEventProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        delivery_timeout_seconds=10.0,
        socket_timeout_seconds=10.0,
    )
    try:
        producer.publish(event)
    finally:
        producer.close(timeout_seconds=10.0)

    consumer = _new_consumer(kafka_bootstrap_servers)
    try:
        runner = _new_runner(consumer, session_factory)
        outcomes = _run_until(runner, 1)
    finally:
        consumer.close()

    assert outcomes == [ProcessOutcome.APPLIED]
    with session_scope(session_factory) as session:
        inbox_row = session.get(ConsumerInboxModel, (_CONSUMER_ID, event.event_id))
        assert inbox_row is not None
        projection_row = session.get(ResearchJobEventProjectionModel, research_job_id)
        assert projection_row is not None
        assert projection_row.last_event_id == event.event_id


def test_real_duplicate_delivery_after_offset_rewind_is_deduped(
    kafka_bootstrap_servers: str,
    session_factory: sessionmaker[Session],
) -> None:
    _seed_group_to_end(kafka_bootstrap_servers)
    research_job_id = f"kafka-dup-{uuid4().hex}"
    event = build_research_job_created(
        research_job_id=research_job_id, created_at=T0, event_id=uuid4()
    )
    start_offset = get_topic_end_offset(kafka_bootstrap_servers)

    producer = KafkaEventProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        delivery_timeout_seconds=10.0,
        socket_timeout_seconds=10.0,
    )
    try:
        producer.publish(event)
    finally:
        producer.close(timeout_seconds=10.0)

    consumer = _new_consumer(kafka_bootstrap_servers)
    try:
        runner = _new_runner(consumer, session_factory)
        first_outcomes = _run_until(runner, 1)
    finally:
        consumer.close()
    assert first_outcomes == [ProcessOutcome.APPLIED]

    # Rewind the real, fixed consumer group back to this record's offset --
    # a genuine Kafka redelivery of an already-fully-processed record, not a
    # fabricated one.
    seed_consumer_group_offset(
        kafka_bootstrap_servers,
        group_id=RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1,
        offset=start_offset,
    )

    consumer_2 = _new_consumer(kafka_bootstrap_servers)
    try:
        runner_2 = _new_runner(consumer_2, session_factory)
        second_outcomes = _run_until(runner_2, 1)
    finally:
        consumer_2.close()

    assert second_outcomes == [ProcessOutcome.DUPLICATE]
    with session_scope(session_factory) as session:
        projection_row = session.get(ResearchJobEventProjectionModel, research_job_id)
        assert projection_row is not None
        assert projection_row.last_event_id == event.event_id


class _FaultyCommitOnce:
    """Wraps a real ``KafkaEventConsumer``; fails exactly one offset commit."""

    def __init__(self, inner: KafkaEventConsumer) -> None:
        self._inner = inner
        self._raise_next = True

    def poll(self, timeout_seconds: float) -> object | None:
        return self._inner.poll(timeout_seconds)

    def commit_message(self, message: object) -> None:
        if self._raise_next:
            self._raise_next = False
            raise RuntimeError("synthetic-crash-before-offset-ack")
        self._inner.commit_message(message)  # type: ignore[arg-type]


def test_crash_before_real_offset_ack_causes_genuine_kafka_redelivery(
    kafka_bootstrap_servers: str,
    session_factory: sessionmaker[Session],
) -> None:
    """The DB effect is durable even though the *real* offset commit failed.

    Because the real broker never receives the offset commit, restarting
    with a fresh, unwrapped consumer in the same group causes Kafka itself
    -- not a test fabrication -- to redeliver the record. The inbox
    recognizes it as a duplicate.
    """
    _seed_group_to_end(kafka_bootstrap_servers)
    research_job_id = f"kafka-crash-{uuid4().hex}"
    event = build_research_job_created(
        research_job_id=research_job_id, created_at=T0, event_id=uuid4()
    )

    producer = KafkaEventProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        delivery_timeout_seconds=10.0,
        socket_timeout_seconds=10.0,
    )
    try:
        producer.publish(event)
    finally:
        producer.close(timeout_seconds=10.0)

    consumer = _new_consumer(kafka_bootstrap_servers)
    faulty = _FaultyCommitOnce(consumer)
    try:
        runner = ConsumerRunner(
            consumer=faulty,  # type: ignore[arg-type]
            session_factory=session_factory,
            inbox=SqlAlchemyInboxRepository(),
            projection=SqlAlchemyResearchJobProjectionRepository(),
            consumer_id=_CONSUMER_ID,
            poll_timeout_seconds=2.0,
        )
        raised = False
        for _ in range(30):
            try:
                outcome = runner.run_once()
            except RuntimeError as exc:
                assert "synthetic-crash-before-offset-ack" in str(exc)
                raised = True
                break
            if outcome is not ProcessOutcome.NO_MESSAGE:
                raise AssertionError(
                    "Expected the injected offset-commit failure before any "
                    "outcome could be returned."
                )
        assert raised, "Injected offset-commit failure never triggered."
    finally:
        consumer.close()

    with session_scope(session_factory) as session:
        inbox_row = session.get(ConsumerInboxModel, (_CONSUMER_ID, event.event_id))
        assert inbox_row is not None  # durable despite the offset-ack failure

    # "Restart": fresh consumer, same real group -- Kafka redelivers because
    # the offset commit never reached the broker.
    consumer_2 = _new_consumer(kafka_bootstrap_servers)
    try:
        runner_2 = _new_runner(consumer_2, session_factory)
        outcomes = _run_until(runner_2, 1)
    finally:
        consumer_2.close()

    assert outcomes == [ProcessOutcome.DUPLICATE]


def test_consumer_restart_resumes_from_the_last_committed_offset(
    kafka_bootstrap_servers: str,
    session_factory: sessionmaker[Session],
) -> None:
    """A fresh consumer instance in the same group must not reprocess history.

    Non-destructive "restart": closes one ``KafkaEventConsumer`` and opens
    a new one in the same real group, proving group-managed offsets (not
    an in-process cursor) are what makes resumption correct.
    """
    _seed_group_to_end(kafka_bootstrap_servers)
    research_job_id_1 = f"kafka-restart-{uuid4().hex}"
    research_job_id_2 = f"kafka-restart-{uuid4().hex}"
    event_1 = build_research_job_created(
        research_job_id=research_job_id_1, created_at=T0, event_id=uuid4()
    )
    event_2 = build_research_job_created(
        research_job_id=research_job_id_2, created_at=T0, event_id=uuid4()
    )

    producer = KafkaEventProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        delivery_timeout_seconds=10.0,
        socket_timeout_seconds=10.0,
    )
    try:
        producer.publish(event_1)
    finally:
        producer.close(timeout_seconds=10.0)

    consumer_1 = _new_consumer(kafka_bootstrap_servers)
    try:
        runner_1 = _new_runner(consumer_1, session_factory)
        outcomes_1 = _run_until(runner_1, 1)
    finally:
        consumer_1.close()
    assert outcomes_1 == [ProcessOutcome.APPLIED]

    producer_2 = KafkaEventProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        delivery_timeout_seconds=10.0,
        socket_timeout_seconds=10.0,
    )
    try:
        producer_2.publish(event_2)
    finally:
        producer_2.close(timeout_seconds=10.0)

    consumer_2 = _new_consumer(kafka_bootstrap_servers)
    try:
        runner_2 = _new_runner(consumer_2, session_factory)
        outcomes_2 = _run_until(runner_2, 1)
    finally:
        consumer_2.close()

    # Only the second job's event is observed by the restarted consumer --
    # it never reprocesses the first job.
    assert outcomes_2 == [ProcessOutcome.APPLIED]
    with session_scope(session_factory) as session:
        assert (
            session.get(ResearchJobEventProjectionModel, research_job_id_1) is not None
        )
        assert (
            session.get(ResearchJobEventProjectionModel, research_job_id_2) is not None
        )
