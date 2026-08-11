"""Kafka consumer adapter for the reserved research-job events topic (Slice 13C2A).

Hardcoded, non-configurable: ``enable.auto.commit=false`` (the caller
commits synchronously, once, only after its own PostgreSQL transaction
commits) and ``auto.offset.reset=earliest`` (a brand-new consumer group
must never silently skip events on first subscribe). ``group.id`` is
restricted to :data:`atlas.consumer.identity.ALLOWED_CONSUMER_GROUP_IDS`;
there is no way to construct this adapter with an arbitrary group. There is
also no public ``client_id`` parameter: the client id has no legitimate
production variability independent of the (already fixed) group id, so it
is looked up internally from
:data:`atlas.consumer.identity.CLIENT_ID_BY_CONSUMER_GROUP_ID`.

Exceptions raised here never include broker addresses, environment values,
configuration values, or raw librdkafka message/error text -- only fixed,
sanitized class-name-style strings, matching ``KafkaEventProducer``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition

from atlas.consumer.errors import ConsumerConfigurationError, ConsumerError
from atlas.consumer.identity import (
    ALLOWED_CONSUMER_GROUP_IDS,
    CLIENT_ID_BY_CONSUMER_GROUP_ID,
)
from atlas.eventing.topic import RESEARCH_JOB_EVENTS_TOPIC_V1


class _RdKafkaConsumerLike(Protocol):
    """The librdkafka ``Consumer`` surface this adapter depends on.

    A structural seam so network-free unit tests can inject a fake without
    ever constructing a real ``confluent_kafka.Consumer`` (which starts a
    background connection thread on construction).

    ``commit()`` is typed as returning plain ``object`` (not the narrower
    ``list[TopicPartition] | None`` the real client's own stub declares for
    ``asynchronous=False``) because this adapter deliberately does not trust
    that declared type at runtime -- see ``_commit_result_confirms_success``.
    """

    def subscribe(self, topics: list[str]) -> None: ...
    def poll(self, timeout: float) -> Message | None: ...
    def commit(
        self,
        message: Message | None = None,
        offsets: list[TopicPartition] | None = None,
        asynchronous: bool = True,
    ) -> object: ...
    def close(self) -> None: ...


def _commit_result_confirms_success(result: object) -> bool:
    """Verify a synchronous ``commit()`` result actually confirms success.

    Confluent's own documentation for ``asynchronous=False`` warns that a
    successful (non-raising) call still requires checking each returned
    ``TopicPartition``'s error field: "specific partitions may have failed
    and the .err field of each partition should be checked for success."
    A raised ``KafkaException`` is therefore not the only failure signal --
    this fails closed on ``None``, an empty result, any non-list result, or
    any element that is not an error-free ``TopicPartition``-shaped object.
    """
    if not isinstance(result, list) or len(result) == 0:
        return False
    for partition in result:
        # ``getattr(..., True)`` treats a missing ``.error`` attribute (an
        # unexpected result shape) the same as an explicit error: fail closed.
        if getattr(partition, "error", True) is not None:
            return False
    return True


class KafkaEventConsumer:
    """Manual-offset-commit Kafka consumer for the reserved research-job topic.

    Always subscribes to the fixed constant
    ``atlas.eventing.topic.RESEARCH_JOB_EVENTS_TOPIC_V1``; there is no
    constructor parameter to select a different topic at runtime.

    No PostgreSQL advisory lock is used here (unlike ``OutboxRelay``, which
    needs one): Kafka's own consumer-group protocol already guarantees at
    most one process actively owns the topic's single partition at a time.
    A second concurrently-running instance in the same group simply owns
    zero partitions until the first instance stops.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        group_id: str,
        session_timeout_seconds: float,
        max_poll_interval_seconds: float,
        _consumer_factory: Callable[[dict[str, object]], _RdKafkaConsumerLike]
        | None = None,
    ) -> None:
        """Construct the adapter.

        ``_consumer_factory`` is a private testing seam only: production
        code never passes it and always gets a real ``confluent_kafka.
        Consumer``. Unit tests inject a fake to stay network-free.
        """
        if not bootstrap_servers.strip():
            raise ConsumerConfigurationError("EmptyBootstrapServers")
        if group_id not in ALLOWED_CONSUMER_GROUP_IDS:
            raise ConsumerConfigurationError("DisallowedConsumerGroupId")
        if session_timeout_seconds <= 0 or max_poll_interval_seconds <= 0:
            raise ConsumerConfigurationError("NonPositiveTimeout")
        client_id = CLIENT_ID_BY_CONSUMER_GROUP_ID[group_id]
        self._closed = False
        factory = _consumer_factory or Consumer
        try:
            self._consumer = factory(
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
        except (KafkaException, ValueError):
            raise ConsumerConfigurationError("ConsumerConstructionFailed") from None

        try:
            self._consumer.subscribe([RESEARCH_JOB_EVENTS_TOPIC_V1])
        except KafkaException:
            self._closed = True
            # Best-effort cleanup: the underlying consumer was constructed
            # (and so may hold a background connection) but never
            # subscribed. A close failure here must never mask the
            # already-decided "SubscribeFailed" classification below.
            try:
                self._consumer.close()
            except Exception:
                pass
            raise ConsumerConfigurationError("SubscribeFailed") from None

    def poll(self, timeout_seconds: float) -> Message | None:
        """Return the next record within ``timeout_seconds``, or ``None``."""
        if self._closed:
            raise ConsumerError("PollAfterClose")
        try:
            return self._consumer.poll(timeout_seconds)
        except KafkaException:
            raise ConsumerError("PollFailed") from None

    def commit_message(self, message: Message) -> None:
        """Synchronously commit the offset for exactly one processed record.

        Fails closed unless the synchronous ``commit()`` call both avoids
        raising and returns a result that itself confirms success (see
        :func:`_commit_result_confirms_success`) -- a raised
        ``KafkaException`` is not the only failure signal this checks.
        """
        if self._closed:
            raise ConsumerError("CommitAfterClose")
        try:
            result = self._consumer.commit(message=message, asynchronous=False)
        except KafkaException:
            raise ConsumerError("CommitFailed") from None
        if not _commit_result_confirms_success(result):
            raise ConsumerError("CommitFailed")

    def close(self) -> None:
        """Best-effort shutdown (triggers a clean consumer-group leave). Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._consumer.close()
