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

Kafka failure classification (Slice 13C2B correction pass): this is the only
layer with access to the raw ``confluent_kafka.KafkaError`` metadata a
classification decision requires, so it is the only place that decides
transient-vs-fatal for a Kafka failure -- never the runner, and never by
inspecting an exception's string message or class name. It reuses (never
duplicates) the single centralized policy in
``atlas.outbox.kafka_errors.classify_kafka_error``, applied uniformly to an
exception raised by ``poll()``, a broker-error message returned by
``poll()`` (this adapter never returns one to its caller -- it always
raises instead), and a synchronous ``commit_message()`` failure (both a
raised exception and an unconfirmed-but-non-raising result). A
``KafkaErrorClass.RECOVERABLE`` classification raises
``atlas.consumer.errors.TransientKafkaError``; every other case (fatal,
unrecognized, or a malformed/unexpected result shape with no structured
error object to classify at all) raises the existing fatal ``ConsumerError``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import NoReturn, Protocol

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition

from atlas.consumer.errors import (
    ConsumerConfigurationError,
    ConsumerError,
    TransientKafkaError,
)
from atlas.consumer.identity import (
    ALLOWED_CONSUMER_GROUP_IDS,
    CLIENT_ID_BY_CONSUMER_GROUP_ID,
)
from atlas.eventing.topic import RESEARCH_JOB_EVENTS_TOPIC_V1
from atlas.outbox.kafka_errors import (
    KafkaErrorClass,
    classify_kafka_error,
    kafka_error_from_exception,
)


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


def _classify_unconfirmed_commit_result(result: object) -> KafkaErrorClass:
    """Classify a non-raising but unconfirmed ``commit()`` result.

    Only a well-shaped result (a nonempty list of ``TopicPartition``-like
    objects, each carrying its own structured ``KafkaError`` in ``.error``)
    has any metadata to classify at all. Any other shape (``None``, empty,
    non-list, or an element missing/mistyping ``.error``) has no structured
    signal and fails closed as fatal. When every offending partition's error
    is itself recoverable, the whole result is recoverable; any fatal
    partition error, or a mix including one, makes the whole result fatal.
    """
    if not isinstance(result, list) or len(result) == 0:
        return KafkaErrorClass.FATAL
    saw_any_error = False
    for partition in result:
        error = getattr(partition, "error", True)
        if error is None:
            continue
        saw_any_error = True
        if classify_kafka_error(error) is KafkaErrorClass.FATAL:
            return KafkaErrorClass.FATAL
    return KafkaErrorClass.RECOVERABLE if saw_any_error else KafkaErrorClass.FATAL


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
        """Return the next record within ``timeout_seconds``, or ``None``.

        Never returns an error-carrying ``Message`` to the caller: a
        broker-error result is classified and raised the same way as a
        raised ``poll()`` exception (see module docstring).
        """
        if self._closed:
            raise ConsumerError("PollAfterClose")
        try:
            message = self._consumer.poll(timeout_seconds)
        except KafkaException as exc:
            self._raise_classified(
                kafka_error_from_exception(exc), context="PollFailed"
            )
        if message is None:
            return None
        error = message.error()
        if error is not None:
            self._raise_classified(error, context="PollReturnedBrokerError")
        return message

    def commit_message(self, message: Message) -> None:
        """Synchronously commit the offset for exactly one processed record.

        Fails closed unless the synchronous ``commit()`` call both avoids
        raising and returns a result that itself confirms success (see
        :func:`_commit_result_confirms_success`) -- a raised
        ``KafkaException`` is not the only failure signal this checks. Both
        failure shapes are classified transient-vs-fatal (see module
        docstring) rather than always treated as fatal.
        """
        if self._closed:
            raise ConsumerError("CommitAfterClose")
        try:
            result = self._consumer.commit(message=message, asynchronous=False)
        except KafkaException as exc:
            self._raise_classified(
                kafka_error_from_exception(exc), context="CommitFailed"
            )
        if not _commit_result_confirms_success(result):
            classification = _classify_unconfirmed_commit_result(result)
            if classification is KafkaErrorClass.RECOVERABLE:
                raise TransientKafkaError("CommitFailed")
            raise ConsumerError("CommitFailed")

    def _raise_classified(self, error: object, *, context: str) -> NoReturn:
        """Raise ``TransientKafkaError`` if recoverable, else fatal ``ConsumerError``.

        See module docstring for the classification policy.
        """
        if classify_kafka_error(error) is KafkaErrorClass.RECOVERABLE:
            raise TransientKafkaError(context)
        raise ConsumerError(context)

    def close(self) -> None:
        """Best-effort shutdown (triggers a clean consumer-group leave). Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._consumer.close()
