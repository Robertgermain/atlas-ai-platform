"""Network-free unit tests for ``KafkaEventProducer`` (Slice 13C1).

A fake low-level producer is injected via the private ``_producer_factory``
seam so no real ``confluent_kafka.Producer`` (which starts a background
connection thread on construction) is ever built here.

Kafka error classification tests use real ``confluent_kafka.KafkaError``
instances (constructed directly with explicit ``code``/``fatal``/``retriable``
values, which the library supports for exactly this purpose) rather than a
duck-typed stand-in, so the tests exercise the same ``isinstance`` check and
flag/code reads the production classifier uses. See
``atlas.outbox.kafka_errors`` for the full narrow, fail-closed policy this
exercises: fatal -> fatal; retriable -> recoverable; the single documented
``_MSG_TIMED_OUT`` exception (real evidence: ``fatal=False, retriable=False``
once librdkafka's own delivery-timeout budget is exhausted; treated as
recoverable at the Atlas durable-outbox level because delivery was not
confirmed within the bounded attempt, not because broker-side receipt was
ruled out -- republishing is compatible with Atlas's documented
at-least-once contract, and any resulting duplicate is handled by the stable
``event_id`` plus the consumer inbox/deduplication work planned for Slice
13C2) -> recoverable; every other neither-fatal-nor-retriable error (e.g.
``_BAD_MSG``) -> fatal; an unrecognized/non-``KafkaError`` payload -> fatal.
"""

from __future__ import annotations

import inspect
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from confluent_kafka import KafkaError, KafkaException

from atlas.eventing import build_research_job_created
from atlas.eventing.topic import RESEARCH_JOB_EVENTS_TOPIC_V1
from atlas.outbox.errors import (
    KafkaFatalProducerError,
    KafkaProducerConfigurationError,
    KafkaPublishError,
    KafkaPublishTimeoutError,
)
from atlas.outbox.kafka_producer import KafkaEventProducer

T0 = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)


def _kafka_error(
    *, code: int = KafkaError._TIMED_OUT, fatal: bool = False, retriable: bool = False
) -> KafkaError:
    """A real ``KafkaError`` with the requested code/fatal/retriable flags."""
    return KafkaError(
        code,
        "synthetic-test-error-reason",
        fatal=fatal,
        retriable=retriable,
    )


# Sentinel meaning "the delivery callback is never invoked at all" (distinct
# from a successful ``err=None`` delivery). Deliberately not a plain string:
# ``confluent_kafka.KafkaError`` raises from its own ``__eq__``/``__ne__``
# when compared against an unrelated type, so this must be compared with
# ``is``/``is not``, never ``==``/``!=``.
_NO_DELIVERY = object()


class _FakeRdKafkaProducer:
    """Fake librdkafka producer double controlling callback/flush behavior."""

    def __init__(
        self,
        *,
        deliver_error: object = _NO_DELIVERY,
        flush_remaining: int = 0,
        raise_on_produce: Exception | None = None,
        raise_on_flush: Exception | None = None,
    ) -> None:
        self.produced: list[tuple[str, bytes, bytes, list[tuple[str, bytes]]]] = []
        self._deliver_error = deliver_error
        self._flush_remaining = flush_remaining
        self._raise_on_produce = raise_on_produce
        self._raise_on_flush = raise_on_flush
        self._pending_callback: object | None = None

    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        headers: list[tuple[str, bytes]],
        on_delivery: object,
    ) -> None:
        if self._raise_on_produce is not None:
            raise self._raise_on_produce
        self.produced.append((topic, key, value, headers))
        self._pending_callback = on_delivery

    def flush(self, timeout: float) -> int:
        del timeout
        if self._raise_on_flush is not None:
            raise self._raise_on_flush
        if (
            self._pending_callback is not None
            and self._deliver_error is not _NO_DELIVERY
        ):
            self._pending_callback(self._deliver_error, object())  # type: ignore[operator]
        return self._flush_remaining


def _event() -> object:
    return build_research_job_created(
        research_job_id="job-kafka-1", created_at=T0, event_id=uuid4()
    )


def _producer(fake: _FakeRdKafkaProducer) -> KafkaEventProducer:
    return KafkaEventProducer(
        bootstrap_servers="unit-test-broker:9092",
        delivery_timeout_seconds=5.0,
        socket_timeout_seconds=5.0,
        _producer_factory=lambda _config: fake,
    )


def test_publish_uses_canonical_value_bytes() -> None:
    from atlas.eventing.serialization import serialize_domain_event

    event = _event()
    fake = _FakeRdKafkaProducer(deliver_error=None)
    producer = _producer(fake)
    producer.publish(event)  # type: ignore[arg-type]
    _, _, value, _ = fake.produced[0]
    assert value == serialize_domain_event(event).encode("utf-8")  # type: ignore[arg-type]


def test_publish_uses_event_id_as_key() -> None:
    event = _event()
    fake = _FakeRdKafkaProducer(deliver_error=None)
    producer = _producer(fake)
    producer.publish(event)  # type: ignore[arg-type]
    _, key, _, _ = fake.produced[0]
    assert key == str(event.event_id).encode("utf-8")  # type: ignore[attr-defined]


def test_publish_sets_exact_safe_headers() -> None:
    event = _event()
    fake = _FakeRdKafkaProducer(deliver_error=None)
    producer = _producer(fake)
    producer.publish(event)  # type: ignore[arg-type]
    _, _, _, headers = fake.produced[0]
    assert dict(headers) == {
        "event_type": b"research_job.created",
        "event_version": b"1",
        "aggregate_type": b"research_job",
    }


def test_publish_always_targets_the_reserved_topic() -> None:
    """``KafkaEventProducer`` has no way to publish to any other topic."""
    fake = _FakeRdKafkaProducer(deliver_error=None)
    producer = _producer(fake)
    producer.publish(_event())  # type: ignore[arg-type]
    topic, _, _, _ = fake.produced[0]
    assert topic == RESEARCH_JOB_EVENTS_TOPIC_V1
    # No constructor parameter exists to select a different topic at runtime.
    assert "topic" not in inspect.signature(KafkaEventProducer.__init__).parameters


def test_successful_delivery_callback_does_not_raise() -> None:
    fake = _FakeRdKafkaProducer(deliver_error=None, flush_remaining=0)
    producer = _producer(fake)
    producer.publish(_event())  # type: ignore[arg-type]


# --- Kafka error classification: delivery callback ----------------------


def test_callback_fatal_error_raises_fatal_producer_error_and_closes() -> None:
    fake = _FakeRdKafkaProducer(deliver_error=_kafka_error(fatal=True))
    producer = _producer(fake)
    with pytest.raises(KafkaFatalProducerError):
        producer.publish(_event())  # type: ignore[arg-type]
    # Producer must never be reused after a fatal error.
    with pytest.raises(KafkaFatalProducerError):
        producer.publish(_event())  # type: ignore[arg-type]
    assert len(fake.produced) == 1


def test_callback_retriable_error_raises_recoverable_publish_error() -> None:
    """``retriable() == True`` is Confluent's documented retry signal."""
    fake = _FakeRdKafkaProducer(deliver_error=_kafka_error(retriable=True))
    producer = _producer(fake)
    with pytest.raises(KafkaPublishError):
        producer.publish(_event())  # type: ignore[arg-type]


def test_callback_msg_timed_out_retriable_false_raises_recoverable() -> None:
    """The single narrow, documented exception: real librdkafka reports a
    per-message delivery timeout as ``fatal=False, retriable=False`` once its
    own internal ``delivery.timeout.ms`` budget is exhausted. This only proves
    Atlas did not receive broker-confirmed delivery within the configured
    deadline, not that the broker never received the record; Atlas treats it
    as recoverable at the durable-outbox level, consistent with its
    documented at-least-once contract -- a later retry may produce a
    duplicate on the topic, handled by the stable ``event_id`` plus the
    consumer inbox/deduplication work planned for Slice 13C2.
    """
    fake = _FakeRdKafkaProducer(
        deliver_error=_kafka_error(
            code=KafkaError._MSG_TIMED_OUT, fatal=False, retriable=False
        )
    )
    producer = _producer(fake)
    with pytest.raises(KafkaPublishError):
        producer.publish(_event())  # type: ignore[arg-type]


def test_callback_permanent_nonfatal_nonretriable_error_raises_fatal() -> None:
    """Neither fatal nor retriable, and not the one documented exception
    code: per Confluent's own guidance this is treated as non-retriable and
    must fail closed rather than be assumed safe to retry.
    """
    fake = _FakeRdKafkaProducer(
        deliver_error=_kafka_error(
            code=KafkaError._BAD_MSG, fatal=False, retriable=False
        )
    )
    producer = _producer(fake)
    with pytest.raises(KafkaFatalProducerError):
        producer.publish(_event())  # type: ignore[arg-type]


def test_missing_delivery_callback_raises_timeout_error() -> None:
    fake = _FakeRdKafkaProducer(deliver_error=_NO_DELIVERY, flush_remaining=0)
    producer = _producer(fake)
    with pytest.raises(KafkaPublishTimeoutError):
        producer.publish(_event())  # type: ignore[arg-type]


def test_nonzero_flush_result_after_success_raises_timeout_error() -> None:
    fake = _FakeRdKafkaProducer(deliver_error=None, flush_remaining=1)
    producer = _producer(fake)
    with pytest.raises(KafkaPublishTimeoutError):
        producer.publish(_event())  # type: ignore[arg-type]


# --- Kafka error classification: immediate produce() KafkaException -----


def test_buffer_error_on_produce_raises_recoverable_publish_error() -> None:
    fake = _FakeRdKafkaProducer(raise_on_produce=BufferError("Local: Queue full"))
    producer = _producer(fake)
    with pytest.raises(KafkaPublishError):
        producer.publish(_event())  # type: ignore[arg-type]


def test_produce_retriable_kafka_exception_raises_recoverable_publish_error() -> None:
    fake = _FakeRdKafkaProducer(
        raise_on_produce=KafkaException(_kafka_error(retriable=True))
    )
    producer = _producer(fake)
    with pytest.raises(KafkaPublishError):
        producer.publish(_event())  # type: ignore[arg-type]


def test_produce_msg_timed_out_retriable_false_raises_recoverable() -> None:
    fake = _FakeRdKafkaProducer(
        raise_on_produce=KafkaException(
            _kafka_error(code=KafkaError._MSG_TIMED_OUT, fatal=False, retriable=False)
        )
    )
    producer = _producer(fake)
    with pytest.raises(KafkaPublishError):
        producer.publish(_event())  # type: ignore[arg-type]


def test_produce_permanent_nonfatal_nonretriable_error_raises_fatal() -> None:
    fake = _FakeRdKafkaProducer(
        raise_on_produce=KafkaException(
            _kafka_error(code=KafkaError._BAD_MSG, fatal=False, retriable=False)
        )
    )
    producer = _producer(fake)
    with pytest.raises(KafkaFatalProducerError):
        producer.publish(_event())  # type: ignore[arg-type]


def test_produce_fatal_kafka_exception_raises_fatal_producer_error() -> None:
    fake = _FakeRdKafkaProducer(
        raise_on_produce=KafkaException(_kafka_error(fatal=True))
    )
    producer = _producer(fake)
    with pytest.raises(KafkaFatalProducerError):
        producer.publish(_event())  # type: ignore[arg-type]


def test_produce_malformed_kafka_exception_fails_closed_fatal() -> None:
    """A ``KafkaException`` without a recognizable ``KafkaError`` payload
    must never be assumed recoverable."""
    fake = _FakeRdKafkaProducer(raise_on_produce=KafkaException("not a KafkaError"))
    producer = _producer(fake)
    with pytest.raises(KafkaFatalProducerError):
        producer.publish(_event())  # type: ignore[arg-type]


def test_produce_empty_kafka_exception_fails_closed_fatal() -> None:
    fake = _FakeRdKafkaProducer(raise_on_produce=KafkaException())
    producer = _producer(fake)
    with pytest.raises(KafkaFatalProducerError):
        producer.publish(_event())  # type: ignore[arg-type]


# --- Kafka error classification: flush() KafkaException ------------------


def test_flush_retriable_kafka_exception_raises_recoverable_publish_error() -> None:
    fake = _FakeRdKafkaProducer(
        raise_on_flush=KafkaException(_kafka_error(retriable=True))
    )
    producer = _producer(fake)
    with pytest.raises(KafkaPublishError):
        producer.publish(_event())  # type: ignore[arg-type]


def test_flush_msg_timed_out_retriable_false_raises_recoverable() -> None:
    fake = _FakeRdKafkaProducer(
        raise_on_flush=KafkaException(
            _kafka_error(code=KafkaError._MSG_TIMED_OUT, fatal=False, retriable=False)
        )
    )
    producer = _producer(fake)
    with pytest.raises(KafkaPublishError):
        producer.publish(_event())  # type: ignore[arg-type]


def test_flush_permanent_nonfatal_nonretriable_error_raises_fatal() -> None:
    fake = _FakeRdKafkaProducer(
        raise_on_flush=KafkaException(
            _kafka_error(code=KafkaError._BAD_MSG, fatal=False, retriable=False)
        )
    )
    producer = _producer(fake)
    with pytest.raises(KafkaFatalProducerError):
        producer.publish(_event())  # type: ignore[arg-type]


def test_flush_fatal_kafka_exception_raises_fatal_producer_error() -> None:
    fake = _FakeRdKafkaProducer(raise_on_flush=KafkaException(_kafka_error(fatal=True)))
    producer = _producer(fake)
    with pytest.raises(KafkaFatalProducerError):
        producer.publish(_event())  # type: ignore[arg-type]


def test_flush_malformed_kafka_exception_fails_closed_fatal() -> None:
    fake = _FakeRdKafkaProducer(raise_on_flush=KafkaException("not a KafkaError"))
    producer = _producer(fake)
    with pytest.raises(KafkaFatalProducerError):
        producer.publish(_event())  # type: ignore[arg-type]


# --- Sanitization ----------------------------------------------------------


def test_errors_never_include_broker_or_raw_message_text() -> None:
    fake = _FakeRdKafkaProducer(deliver_error=_kafka_error(retriable=True))
    producer = _producer(fake)
    with pytest.raises(KafkaPublishError) as exc_info:
        producer.publish(_event())  # type: ignore[arg-type]
    message = str(exc_info.value)
    assert "unit-test-broker" not in message
    assert "synthetic-test-error-reason" not in message
    assert message == "RetriableDeliveryError"


def test_produce_and_flush_errors_never_include_raw_kafka_error_text() -> None:
    fake = _FakeRdKafkaProducer(
        raise_on_produce=KafkaException(_kafka_error(fatal=True))
    )
    producer = _producer(fake)
    with pytest.raises(KafkaFatalProducerError) as exc_info:
        producer.publish(_event())  # type: ignore[arg-type]
    message = str(exc_info.value)
    assert "synthetic-test-error-reason" not in message
    assert message == "FatalProduceCallFailed"

    fake2 = _FakeRdKafkaProducer(
        raise_on_flush=KafkaException(_kafka_error(retriable=True))
    )
    producer2 = _producer(fake2)
    with pytest.raises(KafkaPublishError) as exc_info2:
        producer2.publish(_event())  # type: ignore[arg-type]
    message2 = str(exc_info2.value)
    assert "synthetic-test-error-reason" not in message2
    assert message2 == "RetriableFlushCallFailed"


# --- Configuration ----------------------------------------------------------


def test_empty_bootstrap_servers_rejected() -> None:
    with pytest.raises(KafkaProducerConfigurationError):
        KafkaEventProducer(
            bootstrap_servers="   ",
            delivery_timeout_seconds=5.0,
            socket_timeout_seconds=5.0,
        )


def test_nonpositive_timeout_rejected() -> None:
    with pytest.raises(KafkaProducerConfigurationError):
        KafkaEventProducer(
            bootstrap_servers="broker:9092",
            delivery_timeout_seconds=0,
            socket_timeout_seconds=5.0,
        )


def test_publish_after_closed_producer_raises_fatal_without_touching_producer() -> None:
    fake = _FakeRdKafkaProducer(deliver_error=None)
    producer = _producer(fake)
    producer.close()
    with pytest.raises(KafkaFatalProducerError):
        producer.publish(_event())  # type: ignore[arg-type]
    assert fake.produced == []
