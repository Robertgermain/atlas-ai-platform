"""Integration-test-only Kafka connection, offset, and read-back helpers.

These helpers must never be imported by production application code. The
reserved topic is append-only and its Compose volume persists across local
test runs, so tests isolate their own messages by recording the topic's end
offset immediately before publishing and reading only from that offset
onward -- never a blind full-topic scan.
"""

from __future__ import annotations

import os
import time
from uuid import uuid4

from confluent_kafka import Consumer, Message, TopicPartition

from atlas.eventing.topic import RESEARCH_JOB_EVENTS_TOPIC_V1

DEFAULT_TEST_KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9094"


def test_kafka_bootstrap_servers() -> str:
    return os.environ.get(
        "ATLAS_KAFKA_BOOTSTRAP_SERVERS", DEFAULT_TEST_KAFKA_BOOTSTRAP_SERVERS
    )


def require_message_value(message: Message) -> bytes:
    """Return a message's value, asserting it is present.

    Every Atlas-published record always has a non-null value (canonical
    domain-event JSON); a ``None`` value here indicates a malformed test
    fixture or an unrelated record, so this fails loudly rather than
    silently narrowing ``bytes | None`` at each call site.
    """
    value = message.value()
    assert value is not None, "Kafka message value must not be None."
    return value


def get_topic_end_offset(
    bootstrap_servers: str,
    *,
    topic: str = RESEARCH_JOB_EVENTS_TOPIC_V1,
    timeout: float = 10.0,
) -> int:
    """Return the current high-watermark offset for partition 0."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"atlas-test-offset-{uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    try:
        _low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, 0), timeout=timeout
        )
        return int(high)
    finally:
        consumer.close()


def seed_consumer_group_offset(
    bootstrap_servers: str,
    *,
    group_id: str,
    offset: int,
    topic: str = RESEARCH_JOB_EVENTS_TOPIC_V1,
    assignment_timeout: float = 10.0,
) -> None:
    """Seed ``group_id``'s committed offset for partition 0 to ``offset``.

    Test-only isolation helper. The reserved topic is append-only and its
    Compose volume persists across local test runs, so a consumer using
    the real fixed production ``group_id`` (Slice 13C2A intentionally has
    no arbitrary-group escape hatch) would otherwise resume from whatever
    a previous local run last committed, or -- for a brand-new group --
    from the topic's entire history under ``auto.offset.reset=earliest``.
    Call this immediately before publishing a test's own events so the
    test's consumer only ever sees records the test itself produced.

    Uses a throwaway ``Consumer`` in the same group: subscribes, polls
    once to force partition assignment, then commits the target offset
    explicitly. Never constructs an ``atlas.consumer.kafka_consumer.
    KafkaEventConsumer`` (which enforces the group-id allowlist); this
    helper intentionally works with any group id a test supplies.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.subscribe([topic])
        deadline = time.monotonic() + assignment_timeout
        while not consumer.assignment() and time.monotonic() < deadline:
            consumer.poll(timeout=0.5)
        consumer.commit(offsets=[TopicPartition(topic, 0, offset)], asynchronous=False)
    finally:
        consumer.close()


def consume_from_offset(
    bootstrap_servers: str,
    *,
    start_offset: int,
    count: int,
    topic: str = RESEARCH_JOB_EVENTS_TOPIC_V1,
    timeout_seconds: float = 20.0,
) -> list[Message]:
    """Consume up to ``count`` messages starting at ``start_offset``.

    Bounded by ``timeout_seconds`` via a deadline poll loop (not an
    arbitrary sleep): returns whatever arrived within the bound, which may
    be fewer than ``count`` if the caller's assumption was wrong.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"atlas-test-consumer-{uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    messages: list[Message] = []
    try:
        consumer.assign([TopicPartition(topic, 0, start_offset)])
        deadline = time.monotonic() + timeout_seconds
        while len(messages) < count and time.monotonic() < deadline:
            message = consumer.poll(timeout=0.5)
            if message is None:
                continue
            if message.error() is not None:
                continue
            messages.append(message)
    finally:
        consumer.close()
    return messages
