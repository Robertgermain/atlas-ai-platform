"""Network-free unit tests for ``KafkaEventConsumer`` (Slice 13C2A).

A fake low-level consumer is injected via the private ``_consumer_factory``
seam so no real ``confluent_kafka.Consumer`` (which starts a real
connection thread on construction) is ever built here.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from confluent_kafka import KafkaError, KafkaException, Message

from atlas.consumer.errors import (
    ConsumerConfigurationError,
    ConsumerError,
    TransientKafkaError,
)
from atlas.consumer.identity import RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1
from atlas.consumer.kafka_consumer import KafkaEventConsumer
from atlas.eventing.topic import RESEARCH_JOB_EVENTS_TOPIC_V1

_GROUP = RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1
#: Sentinel distinguishing "caller passed no override" from "caller passed
#: ``None`` on purpose" (``None`` is itself one of the invalid commit shapes
#: under test).
_NO_OVERRIDE = object()


def _kafka_error(
    *, code: int = KafkaError._TIMED_OUT, fatal: bool = False, retriable: bool = False
) -> KafkaError:
    """A real ``KafkaError`` with the requested code/fatal/retriable flags.

    Mirrors ``tests/outbox/test_kafka_producer_unit.py``'s helper: exercises
    the same ``isinstance``/flag reads the production classifier uses,
    rather than a duck-typed stand-in.
    """
    return KafkaError(
        code,
        "synthetic-test-error-reason",
        fatal=fatal,
        retriable=retriable,
    )


@dataclass
class _FakeErrorMessage:
    """A ``confluent_kafka.Message``-shaped double carrying a broker error."""

    _error: object

    def error(self) -> object:
        return self._error


@dataclass
class _FakeCommittedPartition:
    """Duck-typed ``TopicPartition``-shaped result for a fake ``commit()``."""

    error: object | None


class _FakeRdKafkaConsumer:
    def __init__(
        self,
        *,
        raise_on_subscribe: Exception | None = None,
        raise_on_poll: Exception | None = None,
        raise_on_commit: Exception | None = None,
        raise_on_close: Exception | None = None,
        commit_result: object = _NO_OVERRIDE,
        poll_result: object = None,
    ) -> None:
        self.subscribed_topics: list[str] | None = None
        self.polled: list[float] = []
        self.committed: list[object] = []
        self.closed = False
        self._raise_on_subscribe = raise_on_subscribe
        self._raise_on_poll = raise_on_poll
        self._raise_on_commit = raise_on_commit
        self._raise_on_close = raise_on_close
        self._commit_result = commit_result
        self._poll_result = poll_result

    def subscribe(self, topics: list[str]) -> None:
        if self._raise_on_subscribe is not None:
            raise self._raise_on_subscribe
        self.subscribed_topics = topics

    def poll(self, timeout: float) -> Message | None:
        self.polled.append(timeout)
        if self._raise_on_poll is not None:
            raise self._raise_on_poll
        return self._poll_result  # type: ignore[return-value]

    def commit(
        self,
        message: Message | None = None,
        offsets: object | None = None,
        asynchronous: bool = True,
    ) -> object:
        del offsets
        if self._raise_on_commit is not None:
            raise self._raise_on_commit
        self.committed.append((message, asynchronous))
        if self._commit_result is _NO_OVERRIDE:
            return [_FakeCommittedPartition(error=None)]
        return self._commit_result

    def close(self) -> None:
        self.closed = True
        if self._raise_on_close is not None:
            raise self._raise_on_close


def _make(**overrides: object) -> tuple[KafkaEventConsumer, _FakeRdKafkaConsumer]:
    fake_holder: list[_FakeRdKafkaConsumer] = []

    def factory(_config: dict[str, object]) -> _FakeRdKafkaConsumer:
        fake = _FakeRdKafkaConsumer(**overrides)  # type: ignore[arg-type]
        fake_holder.append(fake)
        return fake

    consumer = KafkaEventConsumer(
        bootstrap_servers="unit-test-broker:9092",
        group_id=_GROUP,
        session_timeout_seconds=10.0,
        max_poll_interval_seconds=300.0,
        _consumer_factory=factory,
    )
    return consumer, fake_holder[0]


def test_subscribes_to_the_fixed_reserved_topic_only() -> None:
    _consumer, fake = _make()
    assert fake.subscribed_topics == [RESEARCH_JOB_EVENTS_TOPIC_V1]


def test_rejects_empty_bootstrap_servers() -> None:
    with pytest.raises(ConsumerConfigurationError, match="EmptyBootstrapServers"):
        KafkaEventConsumer(
            bootstrap_servers="   ",
            group_id=_GROUP,
            session_timeout_seconds=10.0,
            max_poll_interval_seconds=300.0,
            _consumer_factory=lambda _config: _FakeRdKafkaConsumer(),
        )


def test_rejects_a_disallowed_consumer_group_id() -> None:
    with pytest.raises(ConsumerConfigurationError, match="DisallowedConsumerGroupId"):
        KafkaEventConsumer(
            bootstrap_servers="unit-test-broker:9092",
            group_id="some-other-group",
            session_timeout_seconds=10.0,
            max_poll_interval_seconds=300.0,
            _consumer_factory=lambda _config: _FakeRdKafkaConsumer(),
        )


@pytest.mark.parametrize(
    ("session_timeout", "max_poll_interval"),
    [(0.0, 300.0), (-1.0, 300.0), (10.0, 0.0), (10.0, -5.0)],
)
def test_rejects_non_positive_timeouts(
    session_timeout: float, max_poll_interval: float
) -> None:
    with pytest.raises(ConsumerConfigurationError, match="NonPositiveTimeout"):
        KafkaEventConsumer(
            bootstrap_servers="unit-test-broker:9092",
            group_id=_GROUP,
            session_timeout_seconds=session_timeout,
            max_poll_interval_seconds=max_poll_interval,
            _consumer_factory=lambda _config: _FakeRdKafkaConsumer(),
        )


def test_construction_failure_is_wrapped_and_sanitized() -> None:
    def factory(_config: dict[str, object]) -> _FakeRdKafkaConsumer:
        raise KafkaException("synthetic-broker-failure-detail")

    with pytest.raises(
        ConsumerConfigurationError, match="ConsumerConstructionFailed"
    ) as excinfo:
        KafkaEventConsumer(
            bootstrap_servers="unit-test-broker:9092",
            group_id=_GROUP,
            session_timeout_seconds=10.0,
            max_poll_interval_seconds=300.0,
            _consumer_factory=factory,
        )
    assert "synthetic-broker-failure-detail" not in str(excinfo.value)


def test_subscribe_failure_closes_and_is_sanitized() -> None:
    fake_holder: list[_FakeRdKafkaConsumer] = []

    class _RaisingSubscribeConsumer(_FakeRdKafkaConsumer):
        def subscribe(self, topics: list[str]) -> None:
            raise KafkaException("synthetic-subscribe-detail")

    def factory(_config: dict[str, object]) -> _RaisingSubscribeConsumer:
        fake = _RaisingSubscribeConsumer()
        fake_holder.append(fake)
        return fake

    with pytest.raises(ConsumerConfigurationError, match="SubscribeFailed") as excinfo:
        KafkaEventConsumer(
            bootstrap_servers="unit-test-broker:9092",
            group_id=_GROUP,
            session_timeout_seconds=10.0,
            max_poll_interval_seconds=300.0,
            _consumer_factory=factory,
        )
    assert "synthetic-subscribe-detail" not in str(excinfo.value)
    assert fake_holder[0].closed is True


def test_subscribe_failure_with_close_failure_still_raises_subscribe_failed() -> None:
    """A close failure during subscribe-failure cleanup must never mask the
    original, already-decided ``SubscribeFailed`` classification."""

    fake_holder: list[_FakeRdKafkaConsumer] = []

    class _RaisingSubscribeAndCloseConsumer(_FakeRdKafkaConsumer):
        def subscribe(self, topics: list[str]) -> None:
            raise KafkaException("synthetic-subscribe-detail")

        def close(self) -> None:
            self.closed = True
            raise KafkaException("synthetic-close-detail")

    def factory(_config: dict[str, object]) -> _RaisingSubscribeAndCloseConsumer:
        fake = _RaisingSubscribeAndCloseConsumer()
        fake_holder.append(fake)
        return fake

    with pytest.raises(ConsumerConfigurationError, match="SubscribeFailed") as excinfo:
        KafkaEventConsumer(
            bootstrap_servers="unit-test-broker:9092",
            group_id=_GROUP,
            session_timeout_seconds=10.0,
            max_poll_interval_seconds=300.0,
            _consumer_factory=factory,
        )
    assert "synthetic-subscribe-detail" not in str(excinfo.value)
    assert "synthetic-close-detail" not in str(excinfo.value)
    assert fake_holder[0].closed is True


def test_poll_delegates_to_the_underlying_consumer() -> None:
    consumer, fake = _make()
    consumer.poll(1.5)
    assert fake.polled == [1.5]


def test_poll_failure_is_wrapped_and_sanitized() -> None:
    consumer, _fake = _make(raise_on_poll=KafkaException("synthetic-poll-detail"))
    with pytest.raises(ConsumerError, match="PollFailed") as excinfo:
        consumer.poll(1.0)
    assert "synthetic-poll-detail" not in str(excinfo.value)


def test_poll_exception_with_an_unrecognized_payload_is_fatal_not_transient() -> None:
    """A ``KafkaException`` with no ``KafkaError`` payload has no evidence to
    classify recoverable -- fails closed as the plain fatal ``ConsumerError``."""
    consumer, _fake = _make(raise_on_poll=KafkaException("synthetic-poll-detail"))
    with pytest.raises(ConsumerError, match="PollFailed") as excinfo:
        consumer.poll(1.0)
    assert not isinstance(excinfo.value, TransientKafkaError)


def test_poll_exception_with_a_retriable_kafka_error_is_transient() -> None:
    error = _kafka_error(retriable=True)
    consumer, _fake = _make(raise_on_poll=KafkaException(error))
    with pytest.raises(TransientKafkaError, match="PollFailed") as excinfo:
        consumer.poll(1.0)
    assert "synthetic-test-error-reason" not in str(excinfo.value)


def test_poll_exception_with_a_fatal_kafka_error_is_fatal_not_transient() -> None:
    error = _kafka_error(fatal=True)
    consumer, _fake = _make(raise_on_poll=KafkaException(error))
    with pytest.raises(ConsumerError, match="PollFailed") as excinfo:
        consumer.poll(1.0)
    assert not isinstance(excinfo.value, TransientKafkaError)


def test_poll_exception_with_a_neither_flag_kafka_error_is_fatal() -> None:
    error = _kafka_error(code=KafkaError._BAD_MSG, fatal=False, retriable=False)
    consumer, _fake = _make(raise_on_poll=KafkaException(error))
    with pytest.raises(ConsumerError, match="PollFailed") as excinfo:
        consumer.poll(1.0)
    assert not isinstance(excinfo.value, TransientKafkaError)


def test_poll_returned_broker_error_is_never_returned_to_the_caller() -> None:
    """A non-raising ``poll()`` result that itself carries a broker error
    must be classified and raised, never handed back as a normal message."""
    error = _kafka_error(fatal=True)
    consumer, _fake = _make(poll_result=_FakeErrorMessage(error))
    with pytest.raises(ConsumerError, match="PollReturnedBrokerError") as excinfo:
        consumer.poll(1.0)
    assert not isinstance(excinfo.value, TransientKafkaError)


def test_poll_returned_broker_error_retriable_is_transient() -> None:
    error = _kafka_error(retriable=True)
    consumer, _fake = _make(poll_result=_FakeErrorMessage(error))
    with pytest.raises(TransientKafkaError, match="PollReturnedBrokerError"):
        consumer.poll(1.0)


class _FakeMessage:
    def __init__(self) -> None:
        self.marker = "fake-message"


def test_commit_message_is_synchronous_for_exactly_the_given_message() -> None:
    consumer, fake = _make()
    message = _FakeMessage()
    consumer.commit_message(message)  # type: ignore[arg-type]
    assert fake.committed == [(message, False)]


def test_commit_succeeds_with_an_error_free_returned_partition() -> None:
    consumer, _fake = _make(
        commit_result=[_FakeCommittedPartition(error=None)],
    )
    consumer.commit_message(_FakeMessage())  # type: ignore[arg-type]


def test_commit_failure_is_wrapped_and_sanitized() -> None:
    consumer, _fake = _make(raise_on_commit=KafkaException("synthetic-commit-detail"))
    with pytest.raises(ConsumerError, match="CommitFailed") as excinfo:
        consumer.commit_message(_FakeMessage())  # type: ignore[arg-type]
    assert "synthetic-commit-detail" not in str(excinfo.value)


def test_commit_exception_with_an_unrecognized_payload_is_fatal_not_transient() -> None:
    consumer, _fake = _make(raise_on_commit=KafkaException("synthetic-commit-detail"))
    with pytest.raises(ConsumerError, match="CommitFailed") as excinfo:
        consumer.commit_message(_FakeMessage())  # type: ignore[arg-type]
    assert not isinstance(excinfo.value, TransientKafkaError)


def test_commit_exception_with_a_retriable_kafka_error_is_transient() -> None:
    error = _kafka_error(retriable=True)
    consumer, _fake = _make(raise_on_commit=KafkaException(error))
    with pytest.raises(TransientKafkaError, match="CommitFailed") as excinfo:
        consumer.commit_message(_FakeMessage())  # type: ignore[arg-type]
    assert "synthetic-test-error-reason" not in str(excinfo.value)


def test_commit_exception_with_a_fatal_kafka_error_is_fatal_not_transient() -> None:
    error = _kafka_error(fatal=True)
    consumer, _fake = _make(raise_on_commit=KafkaException(error))
    with pytest.raises(ConsumerError, match="CommitFailed") as excinfo:
        consumer.commit_message(_FakeMessage())  # type: ignore[arg-type]
    assert not isinstance(excinfo.value, TransientKafkaError)


def test_commit_with_partition_level_error_is_rejected_and_sanitized() -> None:
    """A synchronous ``commit()`` can return without raising yet still report
    a per-partition failure via ``.error`` -- this must not be treated as
    success just because no exception was raised. ``_MSG_TIMED_OUT`` is the
    one documented code classified recoverable despite neither flag being
    set (see ``atlas.outbox.kafka_errors``), so this is transient."""

    partition_error = KafkaError(
        KafkaError._MSG_TIMED_OUT, reason="synthetic-partition-error-detail"
    )
    consumer, _fake = _make(
        commit_result=[_FakeCommittedPartition(error=partition_error)],
    )
    with pytest.raises(TransientKafkaError, match="CommitFailed") as excinfo:
        consumer.commit_message(_FakeMessage())  # type: ignore[arg-type]
    assert "synthetic-partition-error-detail" not in str(excinfo.value)


def test_commit_with_fatal_partition_level_error_is_fatal_not_transient() -> None:
    partition_error = _kafka_error(fatal=True)
    consumer, _fake = _make(
        commit_result=[_FakeCommittedPartition(error=partition_error)],
    )
    with pytest.raises(ConsumerError, match="CommitFailed") as excinfo:
        consumer.commit_message(_FakeMessage())  # type: ignore[arg-type]
    assert not isinstance(excinfo.value, TransientKafkaError)


def test_commit_with_mixed_fatal_and_retriable_partition_errors_is_fatal() -> None:
    """One fatal partition error makes the whole unconfirmed result fatal,
    even if another partition's error is independently recoverable."""
    consumer, _fake = _make(
        commit_result=[
            _FakeCommittedPartition(error=_kafka_error(retriable=True)),
            _FakeCommittedPartition(error=_kafka_error(fatal=True)),
        ],
    )
    with pytest.raises(ConsumerError, match="CommitFailed") as excinfo:
        consumer.commit_message(_FakeMessage())  # type: ignore[arg-type]
    assert not isinstance(excinfo.value, TransientKafkaError)


def test_commit_with_none_result_is_rejected() -> None:
    consumer, _fake = _make(commit_result=None)
    with pytest.raises(ConsumerError, match="CommitFailed"):
        consumer.commit_message(_FakeMessage())  # type: ignore[arg-type]


def test_commit_with_empty_list_result_is_rejected() -> None:
    consumer, _fake = _make(commit_result=[])
    with pytest.raises(ConsumerError, match="CommitFailed"):
        consumer.commit_message(_FakeMessage())  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "unexpected_result",
    [
        "not-a-list-of-partitions",
        {"unexpected": "mapping-shape"},
        [object()],  # a list element with no ``.error`` attribute at all
    ],
)
def test_commit_with_unexpected_result_shape_is_rejected(
    unexpected_result: object,
) -> None:
    consumer, _fake = _make(commit_result=unexpected_result)
    with pytest.raises(ConsumerError, match="CommitFailed"):
        consumer.commit_message(_FakeMessage())  # type: ignore[arg-type]


def test_close_is_idempotent() -> None:
    consumer, fake = _make()
    consumer.close()
    consumer.close()
    assert fake.closed is True


def test_poll_after_close_is_rejected() -> None:
    consumer, _fake = _make()
    consumer.close()
    with pytest.raises(ConsumerError, match="PollAfterClose"):
        consumer.poll(1.0)


def test_commit_after_close_is_rejected() -> None:
    consumer, _fake = _make()
    consumer.close()
    with pytest.raises(ConsumerError, match="CommitAfterClose"):
        consumer.commit_message(_FakeMessage())  # type: ignore[arg-type]
