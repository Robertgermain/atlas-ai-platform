"""Real-Kafka integration tests for ``KafkaEventProducer`` (Slice 13C1).

Requires a reachable Kafka broker with the reserved topic already created
(the ``kafka_bootstrap_servers`` fixture ensures/verifies it). Bootstrap
servers come from ``ATLAS_KAFKA_BOOTSTRAP_SERVERS`` (default matches local
Compose: ``127.0.0.1:9094``).
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest

from atlas.eventing import (
    DomainEvent,
    build_research_job_awaiting_review,
    build_research_job_completed,
    build_research_job_created,
    build_research_job_failed,
    build_research_job_retry_scheduled,
    parse_domain_event,
)
from atlas.outbox.errors import KafkaPublishError, KafkaPublishTimeoutError
from atlas.outbox.kafka_producer import KafkaEventProducer
from tests.integration.kafka_support import (
    consume_from_offset,
    get_topic_end_offset,
    require_message_value,
)

T0 = datetime(2026, 8, 10, 15, 0, 0, tzinfo=UTC)


@pytest.fixture
def kafka_producer(kafka_bootstrap_servers: str) -> Iterator[KafkaEventProducer]:
    producer = KafkaEventProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        delivery_timeout_seconds=10.0,
        socket_timeout_seconds=10.0,
    )
    yield producer
    producer.close()


def test_all_five_event_variants_round_trip_and_parse(
    kafka_bootstrap_servers: str, kafka_producer: KafkaEventProducer
) -> None:
    job_id = "kafka-rt-1"
    events: list[DomainEvent] = [
        build_research_job_created(research_job_id=job_id, created_at=T0),
        build_research_job_completed(research_job_id=job_id, completed_at=T0),
        build_research_job_failed(
            research_job_id=job_id, failed_at=T0, reason_class="BoomError"
        ),
        build_research_job_awaiting_review(
            research_job_id=job_id,
            workflow_execution_id="wf-1",
            entered_review_at=T0,
        ),
        build_research_job_retry_scheduled(
            research_job_id=job_id,
            abandoned_workflow_execution_id=None,
            job_retry_count=1,
            next_attempt_at=T0,
            occurred_at=T0,
        ),
    ]
    start_offset = get_topic_end_offset(kafka_bootstrap_servers)
    for event in events:
        kafka_producer.publish(event)

    messages = consume_from_offset(
        kafka_bootstrap_servers, start_offset=start_offset, count=len(events)
    )
    assert len(messages) == len(events)
    values = [require_message_value(message) for message in messages]
    parsed = [parse_domain_event(json.loads(value)) for value in values]
    assert [event.event_id for event in parsed] == [event.event_id for event in events]
    assert [event.event_type for event in parsed] == [
        event.event_type for event in events
    ]
    for message, event in zip(messages, events, strict=True):
        assert message.key() == str(event.event_id).encode("utf-8")
        headers = dict(message.headers() or [])
        assert headers["event_type"] == event.event_type.encode("utf-8")
        assert headers["event_version"] == b"1"
        assert headers["aggregate_type"] == b"research_job"


def test_publish_is_broker_confirmed_before_returning(
    kafka_bootstrap_servers: str, kafka_producer: KafkaEventProducer
) -> None:
    event = build_research_job_created(research_job_id="kafka-ack-1", created_at=T0)
    start_offset = get_topic_end_offset(kafka_bootstrap_servers)
    kafka_producer.publish(event)

    # The message must already be fetchable: publish() only returns after a
    # broker-confirmed delivery callback, not merely a local enqueue.
    messages = consume_from_offset(
        kafka_bootstrap_servers, start_offset=start_offset, count=1, timeout_seconds=5.0
    )
    assert len(messages) == 1
    assert messages[0].key() == str(event.event_id).encode("utf-8")


def test_unreachable_broker_publish_fails_within_its_bound() -> None:
    event = build_research_job_created(
        research_job_id="kafka-unreachable-1", created_at=T0
    )
    producer = KafkaEventProducer(
        bootstrap_servers="127.0.0.1:9",
        delivery_timeout_seconds=2.0,
        socket_timeout_seconds=2.0,
    )
    started = time.monotonic()
    try:
        with pytest.raises((KafkaPublishError, KafkaPublishTimeoutError)):
            producer.publish(event)
    finally:
        producer.close()
    elapsed = time.monotonic() - started
    assert elapsed < 10.0
