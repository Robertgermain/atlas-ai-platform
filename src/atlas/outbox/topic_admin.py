"""Kafka AdminClient helper for the fixed reserved research-job topic.

This module never accepts an arbitrary runtime topic name: every function
operates on the constant ``RESEARCH_JOB_EVENTS_TOPIC_V1`` only. The broker
disables ``auto.create.topics.enable``, so this is the only supported way
Atlas creates or verifies the topic; nothing here silently relies on
broker-side auto-creation.

Also executable directly as a one-shot administration job:
``python -m atlas.outbox.topic_admin`` (Milestone 14 Slice 14B). ``main()``
below is a thin startup-boundary wrapper -- it loads settings and calls the
three functions above in order; it does not duplicate any topic-creation or
verification logic.
"""

from __future__ import annotations

import logging
import sys

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from atlas.config import get_settings
from atlas.eventing.topic import RESEARCH_JOB_EVENTS_TOPIC_V1
from atlas.observability.events import Event
from atlas.observability.logging import (
    configure_logging,
    log_event,
    log_exception_boundary,
)
from atlas.outbox.errors import (
    KafkaProducerConfigurationError,
    KafkaTopicVerificationError,
    OutboxError,
)

logger = logging.getLogger(__name__)

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


def main() -> int:
    """One-shot Kafka topic administration: ``python -m atlas.outbox.topic_admin``.

    Startup order (fails closed, nonzero exit, on any step):

    1. Load and validate settings.
    2. Verify broker connectivity.
    3. Idempotently create the fixed reserved topic if it does not exist.
    4. Verify the fixed reserved topic exists with exactly one partition.

    This delegates to :func:`verify_broker_connectivity`,
    :func:`ensure_topic_exists`, and :func:`verify_topic_partitioning` only
    -- it does not implement any Kafka admin logic of its own. Intended as a
    Docker Compose one-shot job (Milestone 14 Slice 14B): other services can
    depend on this container's successful (zero) exit code rather than the
    broker's own auto-create behavior, which stays disabled.

    Logging discipline (Slice 15A1) matches ``python -m atlas.outbox``/
    ``atlas.consumer``: every log call goes through
    ``atlas.observability.logging.log_event``/``log_exception_boundary``,
    which only ever accept a fixed
    :class:`~atlas.observability.events.Event` name and the approved
    structured fields -- never a free-text message, ``str(exc)``,
    ``repr(exc)``, ``exc.args``, ``exc_info``, ``stack_info``, or any value
    derived from settings (Kafka broker addresses, configuration values).
    """
    configure_logging(service_role="kafka-topic-init")
    try:
        settings = get_settings()
    except Exception as exc:
        log_exception_boundary(logger, Event.STARTUP_FAILED, exc)
        return 1

    try:
        verify_broker_connectivity(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            timeout_seconds=settings.kafka_topic_verify_timeout_seconds,
        )
        ensure_topic_exists(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            timeout_seconds=settings.kafka_topic_verify_timeout_seconds,
        )
        verify_topic_partitioning(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            timeout_seconds=settings.kafka_topic_verify_timeout_seconds,
        )
    except OutboxError as exc:
        log_exception_boundary(logger, Event.STARTUP_VERIFICATION_FAILED, exc)
        return 1
    except Exception as exc:
        log_exception_boundary(logger, Event.STARTUP_VERIFICATION_FAILED, exc)
        return 1

    log_event(logger, Event.TOPIC_ADMIN_SUCCEEDED)
    return 0


if __name__ == "__main__":
    sys.exit(main())
