"""Kafka AdminClient helper for the fixed reserved research-job topic.

This module never accepts an arbitrary runtime topic name: every function
operates on the constant ``RESEARCH_JOB_EVENTS_TOPIC_V1`` only. The broker
disables ``auto.create.topics.enable``, so this is the only supported way
Atlas creates or verifies the topic; nothing here silently relies on
broker-side auto-creation.
"""

from __future__ import annotations

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from atlas.eventing.topic import RESEARCH_JOB_EVENTS_TOPIC_V1
from atlas.outbox.errors import (
    KafkaProducerConfigurationError,
    KafkaTopicVerificationError,
)

REQUIRED_PARTITION_COUNT = 1
REQUIRED_REPLICATION_FACTOR = 1


def _build_admin_client(bootstrap_servers: str) -> AdminClient:
    if not bootstrap_servers.strip():
        raise KafkaProducerConfigurationError("EmptyBootstrapServers")
    try:
        return AdminClient({"bootstrap.servers": bootstrap_servers})
    except (KafkaException, ValueError):
        raise KafkaProducerConfigurationError("AdminClientConstructionFailed") from None


def ensure_topic_exists(
    *,
    bootstrap_servers: str,
    timeout_seconds: float = 10.0,
) -> None:
    """Idempotently create the fixed reserved topic if it does not exist.

    Safe to call repeatedly: an existing topic (with the expected partition
    count) is a no-op. An existing topic with the wrong partition count is
    left untouched here -- callers must separately call
    :func:`verify_topic_partitioning` to fail closed on that condition.
    """
    admin = _build_admin_client(bootstrap_servers)
    new_topic = NewTopic(
        RESEARCH_JOB_EVENTS_TOPIC_V1,
        num_partitions=REQUIRED_PARTITION_COUNT,
        replication_factor=REQUIRED_REPLICATION_FACTOR,
    )
    try:
        futures = admin.create_topics([new_topic], request_timeout=timeout_seconds)
    except (KafkaException, ValueError):
        raise KafkaTopicVerificationError("TopicCreateRequestFailed") from None

    future = futures[RESEARCH_JOB_EVENTS_TOPIC_V1]
    try:
        future.result(timeout=timeout_seconds)
    except KafkaException as exc:
        error = exc.args[0] if exc.args else None
        code = error.code() if isinstance(error, KafkaError) else None
        if code == KafkaError.TOPIC_ALREADY_EXISTS:
            return
        raise KafkaTopicVerificationError("TopicCreateFailed") from None
    except Exception:
        raise KafkaTopicVerificationError("TopicCreateFailed") from None


def verify_broker_connectivity(
    *,
    bootstrap_servers: str,
    timeout_seconds: float = 10.0,
) -> None:
    """Verify the broker responds to a metadata request within a bound.

    Distinct from :func:`verify_topic_partitioning` so relay startup can
    report broker-unreachable separately from a missing/misconfigured topic.
    """
    admin = _build_admin_client(bootstrap_servers)
    try:
        admin.list_topics(timeout=timeout_seconds)
    except KafkaException:
        raise KafkaTopicVerificationError("BrokerUnreachable") from None


def verify_topic_partitioning(
    *,
    bootstrap_servers: str,
    timeout_seconds: float = 10.0,
) -> None:
    """Verify the fixed reserved topic exists with exactly one partition.

    Raises :class:`KafkaTopicVerificationError` if the broker is
    unreachable within ``timeout_seconds``, the topic is missing, or the
    topic exists with an unexpected partition count (a controlled startup
    failure rather than silently proceeding).
    """
    _verify_topic_partitioning(
        bootstrap_servers=bootstrap_servers,
        topic=RESEARCH_JOB_EVENTS_TOPIC_V1,
        timeout_seconds=timeout_seconds,
    )


def _verify_topic_partitioning(
    *,
    bootstrap_servers: str,
    topic: str,
    timeout_seconds: float,
) -> None:
    """Shared verification body, parameterized by topic for test coverage only.

    Not exported: real callers always go through :func:`verify_topic_partitioning`,
    which fixes ``topic`` to the reserved constant. Tests use this to exercise
    the missing-topic / wrong-partition-count branches against a disposable
    throwaway topic without ever touching the shared reserved one.
    """
    admin = _build_admin_client(bootstrap_servers)
    try:
        cluster_metadata = admin.list_topics(topic=topic, timeout=timeout_seconds)
    except KafkaException:
        raise KafkaTopicVerificationError("BrokerUnreachable") from None

    topic_metadata = cluster_metadata.topics.get(topic)
    if topic_metadata is None or topic_metadata.error is not None:
        raise KafkaTopicVerificationError("TopicMissing")

    partition_count = len(topic_metadata.partitions)
    if partition_count != REQUIRED_PARTITION_COUNT:
        raise KafkaTopicVerificationError("UnexpectedPartitionCount")
