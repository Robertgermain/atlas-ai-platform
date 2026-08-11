"""Real-Kafka + real-PostgreSQL integration tests for the outbox relay
publishing through :class:`KafkaEventProducer` (Slice 13C1).

Requires a reachable Kafka broker with the reserved topic (the
``kafka_bootstrap_servers`` fixture ensures/verifies it) and the PostgreSQL
integration ``engine``/``session_factory`` fixtures.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from atlas.eventing import build_research_job_created
from atlas.outbox.clock import ControllableClock
from atlas.outbox.kafka_producer import KafkaEventProducer
from atlas.outbox.relay import OutboxRelay, RelayRunOutcome
from atlas.outbox.relay_lock import PostgresOutboxRelayLock
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.outbox import SqlAlchemyOutboxRepository
from tests.integration.kafka_support import (
    consume_from_offset,
    get_topic_end_offset,
    require_message_value,
)

T0 = datetime(2026, 8, 10, 15, 0, 0, tzinfo=UTC)

# The destructive restart test below is opt-in only (Slice 13C1 correction
# pass): it is never run as part of the ordinary integration suite or CI,
# which must stay free of Docker CLI mutations. Run it locally with:
#
#   ATLAS_ENABLE_KAFKA_RESTART_TESTS=1 uv run pytest \
#       tests/integration/test_outbox_relay_kafka.py \
#       -k test_broker_restart_retains_topic_and_data
_RESTART_TEST_ENV_VAR = "ATLAS_ENABLE_KAFKA_RESTART_TESTS"
_EXPECTED_RESTART_CONTAINER = "atlas-ai-platform-kafka-1"
_EXPECTED_RESTART_IMAGE = "apache/kafka:4.3.1"
_EXPECTED_RESTART_COMPOSE_SERVICE = "kafka"


class _ClockAdvancingKafkaProducer:
    """Wraps a real ``KafkaEventProducer``; advances a clock after each ack.

    Mirrors ``atlas.outbox.fakes.ClockAdvancingProducer`` but drives a real
    broker-confirmed publish, so the relay's clock (used for lease fencing)
    can be pushed past the lease deadline deterministically -- no sleeps.
    """

    def __init__(
        self,
        *,
        inner: KafkaEventProducer,
        clock: ControllableClock,
        advance_by: timedelta,
    ) -> None:
        self._inner = inner
        self._clock = clock
        self._advance_by = advance_by

    def publish(self, event: object) -> None:
        self._inner.publish(event)  # type: ignore[arg-type]
        self._clock.advance(self._advance_by)


@pytest.fixture
def kafka_producer(kafka_bootstrap_servers: str) -> Iterator[KafkaEventProducer]:
    producer = KafkaEventProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        delivery_timeout_seconds=10.0,
        socket_timeout_seconds=10.0,
    )
    yield producer
    producer.close()


def test_relay_publishes_to_kafka_in_ascending_outbox_position_order(
    engine: Engine,
    session_factory: sessionmaker[Session],
    kafka_bootstrap_servers: str,
    kafka_producer: KafkaEventProducer,
) -> None:
    repo = SqlAlchemyOutboxRepository()
    events = [
        build_research_job_created(research_job_id=f"kafka-ord-{i}", created_at=T0)
        for i in range(5)
    ]
    with session_scope(session_factory) as session:
        for event in events:
            repo.enqueue(session, event)

    start_offset = get_topic_end_offset(kafka_bootstrap_servers)
    lock = PostgresOutboxRelayLock(engine)
    lock.acquire()
    try:
        relay = OutboxRelay(
            session_factory=session_factory,
            repository=repo,
            producer=kafka_producer,
            lock=lock,
            batch_size=5,
            publish_lease_seconds=30.0,
        )
        result = relay.run_once()
    finally:
        lock.release()

    assert result.outcome == RelayRunOutcome.PUBLISHED
    assert result.published_count == 5

    messages = consume_from_offset(
        kafka_bootstrap_servers, start_offset=start_offset, count=5
    )
    assert len(messages) == 5
    received_ids = [
        json.loads(require_message_value(message))["event_id"] for message in messages
    ]
    assert received_ids == [str(event.event_id) for event in events]


def test_row_marked_published_only_after_broker_confirmed_delivery(
    engine: Engine,
    session_factory: sessionmaker[Session],
    kafka_bootstrap_servers: str,
) -> None:
    repo = SqlAlchemyOutboxRepository()
    event = build_research_job_created(
        research_job_id="kafka-ack-mark-1", created_at=T0
    )
    with session_scope(session_factory) as session:
        repo.enqueue(session, event)

    # Unreachable broker: publish must fail, so mark_published must never run.
    unreachable_producer = KafkaEventProducer(
        bootstrap_servers="127.0.0.1:9",
        delivery_timeout_seconds=2.0,
        socket_timeout_seconds=2.0,
    )
    lock = PostgresOutboxRelayLock(engine)
    lock.acquire()
    try:
        relay = OutboxRelay(
            session_factory=session_factory,
            repository=repo,
            producer=unreachable_producer,
            lock=lock,
            batch_size=1,
            publish_lease_seconds=30.0,
        )
        result = relay.run_once()
    finally:
        lock.release()
        unreachable_producer.close()

    assert result.outcome == RelayRunOutcome.RECOVERABLE_FAILURE
    with session_scope(session_factory) as session:
        row = repo.get_by_event_id(session, event.event_id)
        assert row is not None
        assert row.published_at is None

    # Reachable broker: publish succeeds and the message is truly present in
    # Kafka (broker-confirmed), only then is the row marked published.
    start_offset = get_topic_end_offset(kafka_bootstrap_servers)
    real_producer = KafkaEventProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        delivery_timeout_seconds=10.0,
        socket_timeout_seconds=10.0,
    )
    lock2 = PostgresOutboxRelayLock(engine)
    lock2.acquire()
    try:
        relay2 = OutboxRelay(
            session_factory=session_factory,
            repository=repo,
            producer=real_producer,
            lock=lock2,
            batch_size=1,
            publish_lease_seconds=30.0,
        )
        result2 = relay2.run_once()
    finally:
        lock2.release()
        real_producer.close()

    assert result2.outcome == RelayRunOutcome.PUBLISHED
    with session_scope(session_factory) as session:
        row = repo.get_by_event_id(session, event.event_id)
        assert row is not None
        assert row.published_at is not None

    messages = consume_from_offset(
        kafka_bootstrap_servers, start_offset=start_offset, count=1
    )
    assert len(messages) == 1
    assert json.loads(require_message_value(messages[0]))["event_id"] == str(
        event.event_id
    )


def test_ack_before_db_mark_crash_causes_duplicate_after_lease_reclaim(
    engine: Engine,
    session_factory: sessionmaker[Session],
    kafka_bootstrap_servers: str,
) -> None:
    """At-least-once gap at the real Kafka layer: same event_id published twice."""
    repo = SqlAlchemyOutboxRepository()
    event = build_research_job_created(research_job_id="kafka-dup-1", created_at=T0)
    with session_scope(session_factory) as session:
        repo.enqueue(session, event)

    clock = ControllableClock(T0)
    inner_producer = KafkaEventProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        delivery_timeout_seconds=10.0,
        socket_timeout_seconds=10.0,
    )
    crashing_producer = _ClockAdvancingKafkaProducer(
        inner=inner_producer, clock=clock, advance_by=timedelta(seconds=10)
    )

    start_offset = get_topic_end_offset(kafka_bootstrap_servers)

    lock = PostgresOutboxRelayLock(engine)
    lock.acquire()
    try:
        relay = OutboxRelay(
            session_factory=session_factory,
            repository=repo,
            producer=crashing_producer,
            lock=lock,
            batch_size=1,
            publish_lease_seconds=5.0,
            clock=clock,
        )
        result = relay.run_once()
    finally:
        lock.release()

    assert result.outcome == RelayRunOutcome.OWNERSHIP_LOST
    assert result.published_count == 0
    with session_scope(session_factory) as session:
        row = repo.get_by_event_id(session, event.event_id)
        assert row is not None
        assert row.published_at is None  # DB never recorded the ack

    # A later relay reclaims the row after lease expiry and republishes.
    clock.set(T0 + timedelta(seconds=10))
    fast_producer = KafkaEventProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        delivery_timeout_seconds=10.0,
        socket_timeout_seconds=10.0,
    )
    lock2 = PostgresOutboxRelayLock(engine)
    lock2.acquire()
    try:
        relay2 = OutboxRelay(
            session_factory=session_factory,
            repository=repo,
            producer=fast_producer,
            lock=lock2,
            batch_size=1,
            publish_lease_seconds=30.0,
            clock=clock,
        )
        result2 = relay2.run_once()
    finally:
        lock2.release()
        fast_producer.close()
        inner_producer.close()

    assert result2.outcome == RelayRunOutcome.PUBLISHED

    messages = consume_from_offset(
        kafka_bootstrap_servers, start_offset=start_offset, count=2
    )
    assert len(messages) == 2
    ids = [
        json.loads(require_message_value(message))["event_id"] for message in messages
    ]
    assert ids == [str(event.event_id), str(event.event_id)]


def test_broker_restart_retains_topic_and_data(kafka_bootstrap_servers: str) -> None:
    """Proves the named Compose volume actually persists Kafka log data.

    Destructive: issues ``docker restart`` against the local Compose Kafka
    container. Opt-in only, via ``ATLAS_ENABLE_KAFKA_RESTART_TESTS=1``, so
    the ordinary integration suite (including CI, which never runs Docker
    CLI mutations) skips only this one test. This is a distinct, documented
    opt-in skip -- separate from the five pre-existing opt-in live-provider
    skips (live model/tool/embedding tests).

    Requires the local `kafka` Compose container (skipped if it is not
    running). Before restarting, this test also validates the target
    container is actually the Atlas Compose Kafka container (expected image
    and Compose service label), refusing to restart anything else.
    """
    if os.environ.get(_RESTART_TEST_ENV_VAR) != "1":
        pytest.skip(
            "Destructive Kafka broker-restart test is opt-in; set "
            f"{_RESTART_TEST_ENV_VAR}=1 to run it against the local Compose "
            "Kafka container."
        )

    inspect = subprocess.run(
        [
            "docker",
            "inspect",
            "--format",
            '{{.Config.Image}}|{{index .Config.Labels "com.docker.compose.service"}}',
            _EXPECTED_RESTART_CONTAINER,
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    if inspect.returncode != 0:
        pytest.skip(
            "Local Compose Kafka container is not running; skipping restart test."
        )
    image, _, compose_service = inspect.stdout.strip().partition("|")
    assert image == _EXPECTED_RESTART_IMAGE, (
        f"Refusing to restart {_EXPECTED_RESTART_CONTAINER!r}: unexpected "
        f"image {image!r} (expected {_EXPECTED_RESTART_IMAGE!r})."
    )
    assert compose_service == _EXPECTED_RESTART_COMPOSE_SERVICE, (
        f"Refusing to restart {_EXPECTED_RESTART_CONTAINER!r}: unexpected "
        f"Compose service label {compose_service!r} (expected "
        f"{_EXPECTED_RESTART_COMPOSE_SERVICE!r})."
    )

    producer = KafkaEventProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        delivery_timeout_seconds=10.0,
        socket_timeout_seconds=10.0,
    )
    event = build_research_job_created(research_job_id="kafka-restart-1", created_at=T0)
    start_offset = get_topic_end_offset(kafka_bootstrap_servers)
    producer.publish(event)
    producer.close()

    restart = subprocess.run(
        ["docker", "restart", _EXPECTED_RESTART_CONTAINER],
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert restart.returncode == 0, restart.stderr.decode("utf-8", errors="replace")

    # Bounded wait for the broker to become reachable again (health-check
    # style poll, not an arbitrary sleep): the topic_admin verification call
    # itself is the readiness probe and is retried on failure.
    from atlas.outbox.errors import KafkaTopicVerificationError
    from atlas.outbox.topic_admin import verify_topic_partitioning

    deadline_attempts = 30
    for attempt in range(deadline_attempts):
        try:
            verify_topic_partitioning(
                bootstrap_servers=kafka_bootstrap_servers, timeout_seconds=2.0
            )
            break
        except KafkaTopicVerificationError:
            if attempt == deadline_attempts - 1:
                raise
            time.sleep(1.0)

    messages = consume_from_offset(
        kafka_bootstrap_servers,
        start_offset=start_offset,
        count=1,
        timeout_seconds=20.0,
    )
    assert len(messages) == 1
    assert json.loads(require_message_value(messages[0]))["event_id"] == str(
        event.event_id
    )
