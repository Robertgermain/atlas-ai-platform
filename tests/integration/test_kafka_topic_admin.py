"""Real-Kafka integration tests for topic administration (Slice 13C1).

Requires a reachable Kafka broker (Docker Compose `kafka` service or CI's
Kafka service). Bootstrap servers come from ``ATLAS_KAFKA_BOOTSTRAP_SERVERS``
(default matches local Compose: ``127.0.0.1:9094``). The reserved topic
itself is never mutated to an incorrect partition count here -- the
missing-topic / wrong-partition-count branches are exercised against
disposable throwaway topics via the module's private helper.
"""

from __future__ import annotations

import time
from uuid import uuid4

import pytest
from confluent_kafka.admin import AdminClient, NewTopic

from atlas.eventing.topic import RESEARCH_JOB_EVENTS_TOPIC_V1
from atlas.outbox.errors import KafkaTopicVerificationError
from atlas.outbox.topic_admin import (
    _verify_topic_partitioning,
    ensure_topic_exists,
    verify_broker_connectivity,
    verify_topic_partitioning,
)


def test_ensure_topic_exists_is_idempotent(kafka_bootstrap_servers: str) -> None:
    ensure_topic_exists(bootstrap_servers=kafka_bootstrap_servers, timeout_seconds=10.0)
    # Second call must not raise (already-exists is treated as success).
    ensure_topic_exists(bootstrap_servers=kafka_bootstrap_servers, timeout_seconds=10.0)


def test_reserved_topic_has_exactly_one_partition(kafka_bootstrap_servers: str) -> None:
    verify_topic_partitioning(
        bootstrap_servers=kafka_bootstrap_servers, timeout_seconds=10.0
    )


def test_unreachable_broker_fails_closed_within_its_bound() -> None:
    started = time.monotonic()
    with pytest.raises(KafkaTopicVerificationError):
        verify_broker_connectivity(bootstrap_servers="127.0.0.1:9", timeout_seconds=2.0)
    elapsed = time.monotonic() - started
    assert elapsed < 10.0


def test_missing_topic_fails_closed(kafka_bootstrap_servers: str) -> None:
    missing_topic = f"atlas.test-missing-topic.v1.{uuid4().hex}"
    with pytest.raises(KafkaTopicVerificationError):
        _verify_topic_partitioning(
            bootstrap_servers=kafka_bootstrap_servers,
            topic=missing_topic,
            timeout_seconds=10.0,
        )


def test_incorrectly_partitioned_topic_fails_closed(
    kafka_bootstrap_servers: str,
) -> None:
    throwaway_topic = f"atlas.test-wrong-partitions.v1.{uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap_servers})
    futures = admin.create_topics(
        [NewTopic(throwaway_topic, num_partitions=3, replication_factor=1)],
        request_timeout=10.0,
    )
    futures[throwaway_topic].result(timeout=10.0)
    try:
        with pytest.raises(KafkaTopicVerificationError):
            _verify_topic_partitioning(
                bootstrap_servers=kafka_bootstrap_servers,
                topic=throwaway_topic,
                timeout_seconds=10.0,
            )
    finally:
        admin.delete_topics([throwaway_topic], request_timeout=10.0)[
            throwaway_topic
        ].result(timeout=10.0)


def test_reserved_topic_name_matches_the_documented_constant() -> None:
    assert RESEARCH_JOB_EVENTS_TOPIC_V1 == "atlas.research-job-events.v1"
