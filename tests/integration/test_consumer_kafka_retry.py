"""Real-Kafka + real-PostgreSQL retry/deadline/ordering tests (Slice 13C2B).

Complements ``test_consumer_kafka.py`` (which covers the base poll-decode-
apply-commit path) with the three real-Kafka guarantees that only a genuine
broker can demonstrate:

1. bounded in-process retry (with real ``time.sleep`` backoff) completes and
   commits without the consumer group rebalancing the partition away --
   proven with real ``on_assign``/``on_revoke`` callbacks, not a mock;
2. the runner's own processing deadline terminates the record *before*
   Kafka's real ``max.poll.interval.ms`` would itself force a rebalance;
3. a second, already-published record is never resolved until the first
   (head-of-line) record's retry episode fully resolves, because
   ``ConsumerRunner.run_once()`` calls ``poll()`` at most once per record
   and never again during that record's retry loop.

Each test builds its own thin, real ``confluent_kafka.Consumer`` (mirroring
``KafkaEventConsumer``'s fixed, allowlisted configuration) instead of using
``KafkaEventConsumer`` directly, purely to attach ``on_assign``/``on_revoke``
rebalance-listener callbacks that the production adapter intentionally does
not expose.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from uuid import UUID, uuid4

from confluent_kafka import Consumer, Message, TopicPartition
from sqlalchemy.orm import Session, sessionmaker

from atlas.consumer.errors import ProcessingDeadlineExceededError
from atlas.consumer.fakes import build_dbapi_error
from atlas.consumer.identity import (
    CLIENT_ID_BY_CONSUMER_GROUP_ID,
    RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1,
)
from atlas.consumer.ports import ApplyEffect, InboxOutcome, InboxRepository
from atlas.consumer.runner import ConsumerRunner, ProcessOutcome
from atlas.consumer.timing import RetryTimingParameters
from atlas.eventing import build_research_job_created
from atlas.eventing.contracts import DomainEvent
from atlas.eventing.topic import RESEARCH_JOB_EVENTS_TOPIC_V1
from atlas.outbox.kafka_producer import KafkaEventProducer
from atlas.persistence.repositories.consumer_dead_letter import (
    SqlAlchemyDeadLetterRepository,
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


class _RebalanceTrackingConsumer:
    """A real ``confluent_kafka.Consumer`` with assign/revoke instrumentation.

    Mirrors ``KafkaEventConsumer``'s exact, fixed configuration (same real
    allowlisted group id and client id) so it participates in the same
    consumer group any production instance would. The only reason this
    exists instead of using ``KafkaEventConsumer`` directly is that the
    production adapter deliberately exposes no rebalance-listener seam --
    this test-only wrapper adds one so a real rebalance (or its absence)
    can be asserted on, rather than assumed.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        session_timeout_seconds: float,
        max_poll_interval_seconds: float,
    ) -> None:
        self.assign_count = 0
        self.revoke_count = 0
        group_id = RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1
        client_id = CLIENT_ID_BY_CONSUMER_GROUP_ID[group_id]

        def _on_assign(_consumer: Consumer, _partitions: list[TopicPartition]) -> None:
            self.assign_count += 1

        def _on_revoke(_consumer: Consumer, _partitions: list[TopicPartition]) -> None:
            self.revoke_count += 1

        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "client.id": client_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "session.timeout.ms": int(session_timeout_seconds * 1000),
                "max.poll.interval.ms": int(max_poll_interval_seconds * 1000),
            }
        )
        self._consumer.subscribe(
            [RESEARCH_JOB_EVENTS_TOPIC_V1], on_assign=_on_assign, on_revoke=_on_revoke
        )

    def poll(self, timeout_seconds: float) -> Message | None:
        return self._consumer.poll(timeout_seconds)

    def commit_message(self, message: Message) -> None:
        self._consumer.commit(message=message, asynchronous=False)

    def committed_offset(self) -> int:
        (tp,) = self._consumer.committed(
            [TopicPartition(RESEARCH_JOB_EVENTS_TOPIC_V1, 0)], timeout=10.0
        )
        return int(tp.offset)

    def close(self) -> None:
        self._consumer.close()


class _FailForEventThenDelegate:
    """Wraps a real inbox; injects transient DB errors for one target event.

    ``call_times`` records a wall-clock timestamp for every call keyed by
    event id, regardless of whether that call raises -- this is what lets
    the head-of-line-ordering test prove a second record's first call
    happened only after the first record's *final* (successful) call.
    """

    def __init__(
        self,
        inner: InboxRepository,
        *,
        fail_for_event_id: UUID,
        fail_count: int,
    ) -> None:
        self._inner = inner
        self._fail_for_event_id = fail_for_event_id
        self._remaining = fail_count
        self.call_times: dict[UUID, list[float]] = {}

    def record_and_apply(
        self,
        session: Session,
        *,
        consumer_id: str,
        event: DomainEvent,
        kafka_partition: int,
        kafka_offset: int,
        at: datetime,
        apply_effect: ApplyEffect,
    ) -> InboxOutcome:
        self.call_times.setdefault(event.event_id, []).append(time.monotonic())
        if event.event_id == self._fail_for_event_id and self._remaining > 0:
            self._remaining -= 1
            raise build_dbapi_error(sqlstate="08006")
        return self._inner.record_and_apply(
            session,
            consumer_id=consumer_id,
            event=event,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            at=at,
            apply_effect=apply_effect,
        )


def _seed_group_to_end(kafka_bootstrap_servers: str) -> None:
    seed_consumer_group_offset(
        kafka_bootstrap_servers,
        group_id=RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1,
        offset=get_topic_end_offset(kafka_bootstrap_servers),
    )


def _publish(kafka_bootstrap_servers: str, event: DomainEvent) -> None:
    producer = KafkaEventProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        delivery_timeout_seconds=10.0,
        socket_timeout_seconds=10.0,
    )
    try:
        producer.publish(event)
    finally:
        producer.close(timeout_seconds=10.0)


def test_real_kafka_retry_succeeds_within_budget_without_rebalance(
    kafka_bootstrap_servers: str,
    session_factory: sessionmaker[Session],
) -> None:
    """Bounded in-process retry (real sleep-based backoff) never rebalances.

    The retry episode's total wall-clock time (two short backoffs) stays
    comfortably under both the runner's own deadline and Kafka's real
    ``max.poll.interval.ms``/``session.timeout.ms`` -- so the partition
    assignment observed via real ``on_assign``/``on_revoke`` callbacks never
    changes mid-retry, proving the retry loop genuinely never calls
    ``poll()`` again and never triggers a group rebalance.
    """
    _seed_group_to_end(kafka_bootstrap_servers)
    research_job_id = f"kafka-retry-ok-{uuid4().hex}"
    event = build_research_job_created(
        research_job_id=research_job_id, created_at=T0, event_id=uuid4()
    )
    _publish(kafka_bootstrap_servers, event)

    consumer = _RebalanceTrackingConsumer(
        bootstrap_servers=kafka_bootstrap_servers,
        session_timeout_seconds=10.0,
        max_poll_interval_seconds=15.0,
    )
    inbox = _FailForEventThenDelegate(
        SqlAlchemyInboxRepository(),
        fail_for_event_id=event.event_id,
        fail_count=2,
    )
    try:
        runner = ConsumerRunner(
            consumer=consumer,
            session_factory=session_factory,
            inbox=inbox,
            projection=SqlAlchemyResearchJobProjectionRepository(),
            dead_letters=SqlAlchemyDeadLetterRepository(),
            consumer_id=_CONSUMER_ID,
            poll_timeout_seconds=2.0,
            max_poll_interval_seconds=15.0,
            timing_params=RetryTimingParameters(
                max_attempts=3,
                base_seconds=0.2,
                max_backoff_seconds=1.0,
                jitter_max_seconds=0.0,
                safety_margin_seconds=1.0,
                db_connect_timeout_seconds=0.01,
                db_pool_timeout_seconds=0.01,
                db_statement_timeout_seconds=0.01,
                processing_overhead_seconds=0.0,
                max_db_round_trips_per_attempt=8,
            ),
        )

        outcome: ProcessOutcome | None = None
        for _ in range(30):
            outcome = runner.run_once()
            if outcome is not ProcessOutcome.NO_MESSAGE:
                break
        # Snapshot rebalance counters *before* the deliberate group-leave in
        # ``finally`` below -- ``close()`` itself triggers a (correct, but
        # irrelevant to this assertion) revoke, which must not be conflated
        # with an unwanted *mid-retry* revoke.
        assign_count_during_retry = consumer.assign_count
        revoke_count_during_retry = consumer.revoke_count
    finally:
        consumer.close()

    assert outcome is ProcessOutcome.APPLIED
    assert inbox.call_times[event.event_id] == sorted(inbox.call_times[event.event_id])
    assert len(inbox.call_times[event.event_id]) == 3
    # The genuinely real group never rebalanced during the retry episode:
    # exactly the initial join, never a mid-retry revoke.
    assert assign_count_during_retry == 1
    assert revoke_count_during_retry == 0


def test_real_kafka_deadline_terminates_before_kafkas_max_poll_interval(
    kafka_bootstrap_servers: str,
    session_factory: sessionmaker[Session],
) -> None:
    """The runner's own deadline fires well inside Kafka's real poll budget.

    Kafka's real ``max.poll.interval.ms`` is configured generously (30s) so
    it would not itself revoke the partition during this short test. The
    runner's own timing parameters instead make the very first backoff
    unaffordable, so ``ProcessingDeadlineExceededError`` terminates the
    record quickly, with the offset left uncommitted -- proving the
    application-level safety margin, not Kafka's own liveness enforcement,
    is what bounds a stuck retry episode in practice.
    """
    _seed_group_to_end(kafka_bootstrap_servers)
    research_job_id = f"kafka-deadline-{uuid4().hex}"
    event = build_research_job_created(
        research_job_id=research_job_id, created_at=T0, event_id=uuid4()
    )
    start_offset = get_topic_end_offset(kafka_bootstrap_servers)
    _publish(kafka_bootstrap_servers, event)

    consumer = _RebalanceTrackingConsumer(
        bootstrap_servers=kafka_bootstrap_servers,
        session_timeout_seconds=25.0,
        max_poll_interval_seconds=30.0,
    )
    inbox = _FailForEventThenDelegate(
        SqlAlchemyInboxRepository(),
        fail_for_event_id=event.event_id,
        fail_count=1,
    )
    try:
        runner = ConsumerRunner(
            consumer=consumer,
            session_factory=session_factory,
            inbox=inbox,
            projection=SqlAlchemyResearchJobProjectionRepository(),
            dead_letters=SqlAlchemyDeadLetterRepository(),
            consumer_id=_CONSUMER_ID,
            poll_timeout_seconds=2.0,
            # A small runner-level budget, independent of Kafka's real one.
            max_poll_interval_seconds=3.0,
            timing_params=RetryTimingParameters(
                max_attempts=3,
                base_seconds=10.0,
                max_backoff_seconds=10.0,
                jitter_max_seconds=0.0,
                safety_margin_seconds=1.0,
                db_connect_timeout_seconds=0.01,
                db_pool_timeout_seconds=0.01,
                db_statement_timeout_seconds=0.01,
                processing_overhead_seconds=0.0,
                max_db_round_trips_per_attempt=1,
            ),
        )

        started = time.monotonic()
        raised = False
        for _ in range(30):
            try:
                outcome = runner.run_once()
            except ProcessingDeadlineExceededError:
                raised = True
                break
            if outcome is not ProcessOutcome.NO_MESSAGE:
                raise AssertionError("Expected the deadline to fire, not an outcome.")
        elapsed = time.monotonic() - started

        assert raised, "Expected ProcessingDeadlineExceededError."
        # Well inside Kafka's real 30s max.poll.interval.ms -- the runner's
        # own deadline, not Kafka's, terminated this record.
        assert elapsed < 15.0
        assert consumer.assign_count == 1
        assert consumer.revoke_count == 0
        assert consumer.committed_offset() < start_offset + 1
    finally:
        consumer.close()


def test_real_kafka_processes_the_next_record_only_after_the_head_record_resolves(
    kafka_bootstrap_servers: str,
    session_factory: sessionmaker[Session],
) -> None:
    """A second already-published record waits for the head record's outcome.

    Publishes two records back to back, then makes the *first* record's
    processing require two real, sleeping retries before it succeeds. The
    second record's first (successful, first-attempt) inbox call must be
    timestamped strictly after the first record's *last* (successful) call
    -- proving ``run_once()`` for record two never began, let alone
    resolved, until record one's whole retry episode committed.
    """
    _seed_group_to_end(kafka_bootstrap_servers)
    research_job_id_1 = f"kafka-holb-1-{uuid4().hex}"
    research_job_id_2 = f"kafka-holb-2-{uuid4().hex}"
    event_1 = build_research_job_created(
        research_job_id=research_job_id_1, created_at=T0, event_id=uuid4()
    )
    event_2 = build_research_job_created(
        research_job_id=research_job_id_2, created_at=T0, event_id=uuid4()
    )
    _publish(kafka_bootstrap_servers, event_1)
    _publish(kafka_bootstrap_servers, event_2)

    consumer = _RebalanceTrackingConsumer(
        bootstrap_servers=kafka_bootstrap_servers,
        session_timeout_seconds=10.0,
        max_poll_interval_seconds=15.0,
    )
    inbox = _FailForEventThenDelegate(
        SqlAlchemyInboxRepository(),
        fail_for_event_id=event_1.event_id,
        fail_count=2,
    )
    try:
        runner = ConsumerRunner(
            consumer=consumer,
            session_factory=session_factory,
            inbox=inbox,
            projection=SqlAlchemyResearchJobProjectionRepository(),
            dead_letters=SqlAlchemyDeadLetterRepository(),
            consumer_id=_CONSUMER_ID,
            poll_timeout_seconds=2.0,
            max_poll_interval_seconds=15.0,
            timing_params=RetryTimingParameters(
                max_attempts=3,
                base_seconds=0.2,
                max_backoff_seconds=1.0,
                jitter_max_seconds=0.0,
                safety_margin_seconds=1.0,
                db_connect_timeout_seconds=0.01,
                db_pool_timeout_seconds=0.01,
                db_statement_timeout_seconds=0.01,
                processing_overhead_seconds=0.0,
                max_db_round_trips_per_attempt=8,
            ),
        )

        outcomes: list[ProcessOutcome] = []
        for _ in range(30):
            outcome = runner.run_once()
            if outcome is not ProcessOutcome.NO_MESSAGE:
                outcomes.append(outcome)
            if len(outcomes) >= 2:
                break
    finally:
        consumer.close()

    assert outcomes == [ProcessOutcome.APPLIED, ProcessOutcome.APPLIED]
    event_1_last_call = inbox.call_times[event_1.event_id][-1]
    event_2_first_call = inbox.call_times[event_2.event_id][0]
    assert len(inbox.call_times[event_1.event_id]) == 3
    assert len(inbox.call_times[event_2.event_id]) == 1
    assert event_2_first_call > event_1_last_call
