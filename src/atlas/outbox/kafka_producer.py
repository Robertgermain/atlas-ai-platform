"""Kafka producer adapter behind the typed ``EventProducer`` port (Slice 13C1).

Delivery success requires all three of:

1. the delivery callback was invoked;
2. the callback reported ``err is None``;
3. ``flush()`` returned zero outstanding messages.

Every Kafka error this adapter can observe (a delivery-callback error, an
immediate ``produce()`` failure, or a ``flush()`` failure) is classified by
the centralized :func:`atlas.outbox.kafka_errors.classify_kafka_error`
helper, which inspects only the error's own ``fatal()`` / ``retriable()``
flags -- never by stringifying the error or matching an exception class
name. A fatal or unclassifiable error means librdkafka's producer state (or
Atlas's ability to safely retry) is no longer trustworthy, so this adapter
marks itself closed and refuses further ``publish()`` calls.

Exceptions raised here never include broker addresses, environment values,
configuration values, or raw librdkafka message/error text -- only fixed,
sanitized class-name-style strings.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import NoReturn, Protocol

from confluent_kafka import KafkaException, Producer

from atlas.eventing.contracts import DomainEvent
from atlas.eventing.serialization import serialize_domain_event
from atlas.eventing.topic import RESEARCH_JOB_EVENTS_TOPIC_V1
from atlas.outbox.errors import (
    KafkaFatalProducerError,
    KafkaProducerConfigurationError,
    KafkaPublishError,
    KafkaPublishTimeoutError,
)
from atlas.outbox.kafka_errors import (
    KafkaErrorClass,
    classify_kafka_error,
    kafka_error_from_exception,
)

_HEADER_EVENT_TYPE = "event_type"
_HEADER_EVENT_VERSION = "event_version"
_HEADER_AGGREGATE_TYPE = "aggregate_type"
#: Optional (Slice 15A3): the relay's own ``outbox.publish`` span's
#: resulting W3C ``traceparent``, when tracing is bound. Never one of the
#: three required headers above -- a missing/malformed value on the
#: consumer side is always discarded as absent telemetry, never a
#: dead-letter condition (see ``atlas.consumer.deserialize``).
_HEADER_TRACEPARENT = "traceparent"

DeliveryCallback = Callable[[object, object], None]


class _RdKafkaProducerLike(Protocol):
    """The librdkafka ``Producer`` surface this adapter depends on.

    A structural seam so network-free unit tests can inject a fake without
    ever constructing a real ``confluent_kafka.Producer`` (which starts a
    background connection thread on construction).
    """

    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        headers: list[tuple[str, bytes]],
        on_delivery: DeliveryCallback,
    ) -> None: ...

    def flush(self, timeout: float) -> int: ...


class _DeliveryOutcome:
    """Mutable box populated by the librdkafka delivery callback."""

    __slots__ = ("error", "invoked")

    def __init__(self) -> None:
        self.invoked = False
        self.error: object | None = None


def _headers_for(
    event: DomainEvent, *, traceparent: str | None
) -> Sequence[tuple[str, bytes]]:
    headers = [
        (_HEADER_EVENT_TYPE, event.event_type.encode("utf-8")),
        (_HEADER_EVENT_VERSION, str(int(event.event_version)).encode("utf-8")),
        (_HEADER_AGGREGATE_TYPE, event.aggregate_type.encode("utf-8")),
    ]
    if traceparent is not None:
        headers.append((_HEADER_TRACEPARENT, traceparent.encode("utf-8")))
    return headers


class KafkaEventProducer:
    """Delivery-callback-confirmed Kafka producer for the reserved topic.

    Always publishes to the fixed constant
    ``atlas.eventing.topic.RESEARCH_JOB_EVENTS_TOPIC_V1``; there is no
    constructor parameter to select a different topic at runtime.

    ``acks=all`` and ``enable.idempotence=true`` are hardcoded, not
    configurable, so every publish is broker-acknowledged with
    idempotence-compatible ordering (``max.in.flight.requests.per.connection``
    is bounded to a value idempotence supports).
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        delivery_timeout_seconds: float,
        socket_timeout_seconds: float,
        _producer_factory: Callable[[dict[str, object]], _RdKafkaProducerLike]
        | None = None,
    ) -> None:
        """Construct the adapter.

        ``_producer_factory`` is a private testing seam only: production
        code never passes it and always gets a real ``confluent_kafka.
        Producer``. Unit tests inject a fake to stay network-free.
        """
        if not bootstrap_servers.strip():
            raise KafkaProducerConfigurationError("EmptyBootstrapServers")
        if delivery_timeout_seconds <= 0 or socket_timeout_seconds <= 0:
            raise KafkaProducerConfigurationError("NonPositiveTimeout")
        self._delivery_timeout_seconds = delivery_timeout_seconds
        self._closed = False
        factory = _producer_factory or Producer
        try:
            self._producer = factory(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "acks": "all",
                    "enable.idempotence": True,
                    "max.in.flight.requests.per.connection": 5,
                    "delivery.timeout.ms": int(delivery_timeout_seconds * 1000),
                    "socket.timeout.ms": int(socket_timeout_seconds * 1000),
                    "request.timeout.ms": int(socket_timeout_seconds * 1000),
                }
            )
        except (KafkaException, ValueError):
            raise KafkaProducerConfigurationError(
                "ProducerConstructionFailed"
            ) from None

    def publish(self, event: DomainEvent, *, traceparent: str | None = None) -> None:
        """Publish one envelope, raising only after broker-confirmed outcome.

        Raises ``KafkaPublishError``/``KafkaPublishTimeoutError`` for
        recoverable failures (safe to retry the same event later), or
        ``KafkaFatalProducerError`` when this producer instance must never
        be reused again. ``traceparent`` (Slice 15A3), when not ``None``, is
        injected as an additional optional header -- never one of the three
        required headers validated by ``atlas.consumer.deserialize``.
        """
        if self._closed:
            raise KafkaFatalProducerError("ProducerClosed")

        key = str(event.event_id).encode("utf-8")
        value = serialize_domain_event(event).encode("utf-8")
        outcome = _DeliveryOutcome()

        def _on_delivery(err: object, _msg: object) -> None:
            outcome.invoked = True
            outcome.error = err

        try:
            self._producer.produce(
                RESEARCH_JOB_EVENTS_TOPIC_V1,
                key=key,
                value=value,
                headers=list(_headers_for(event, traceparent=traceparent)),
                on_delivery=_on_delivery,
            )
        except BufferError:
            raise KafkaPublishError("LocalQueueFull") from None
        except KafkaException as exc:
            self._raise_for_kafka_error(
                kafka_error_from_exception(exc), context="ProduceCallFailed"
            )

        try:
            remaining = self._producer.flush(self._delivery_timeout_seconds)
        except KafkaException as exc:
            self._raise_for_kafka_error(
                kafka_error_from_exception(exc), context="FlushCallFailed"
            )

        if not outcome.invoked:
            raise KafkaPublishTimeoutError("DeliveryCallbackNotInvoked")
        error = outcome.error
        if error is not None:
            self._raise_for_kafka_error(error, context="DeliveryError")
        if remaining > 0:
            raise KafkaPublishTimeoutError("OutstandingMessagesAfterFlush")
        # Explicit success: callback invoked, no error, no outstanding messages.

    def _raise_for_kafka_error(self, error: object, *, context: str) -> NoReturn:
        """Classify and raise the typed error for any Kafka error observed."""
        if classify_kafka_error(error) is KafkaErrorClass.FATAL:
            # Permanently retires this producer instance: a fatal error
            # gives Atlas no safe basis to keep using it.
            self._closed = True
            raise KafkaFatalProducerError(f"Fatal{context}") from None
        raise KafkaPublishError(f"Retriable{context}") from None

    def close(self, *, timeout_seconds: float = 5.0) -> None:
        """Best-effort bounded flush and shutdown. Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._producer.flush(timeout_seconds)
