"""Transactional outbox errors (Milestone 13 Slice 13B)."""

from __future__ import annotations


class OutboxError(Exception):
    """Base class for outbox and relay failures."""


class OutboxEnqueueError(OutboxError):
    """Raised when a typed event cannot be durably inserted."""


class RelayOwnershipError(OutboxError):
    """Raised when the singleton outbox-relay advisory lock cannot be acquired."""


class RelayNotOwnerError(OutboxError):
    """Raised when relay work is attempted without holding the advisory lock."""


class EventPublishError(OutboxError):
    """Recoverable ``EventProducer.publish()`` failure. Safe to retry later.

    The relay releases the row's claim on this error; a later run may reclaim
    and republish the same ``event_id`` (at-least-once delivery).
    """


class FatalEventPublishError(OutboxError):
    """Unrecoverable ``EventProducer.publish()`` failure.

    Signals that the producer instance itself must never be reused. The
    relay still releases the row's claim, but the caller (the relay
    executable) must terminate nonzero rather than continue the poll loop
    with the same producer.
    """


class KafkaProducerConfigurationError(OutboxError):
    """Raised when Kafka producer/admin client configuration is invalid."""


class KafkaPublishError(EventPublishError):
    """Recoverable Kafka publish failure (e.g. local queue full, broker busy)."""


class KafkaPublishTimeoutError(EventPublishError):
    """Raised when a publish attempt does not confirm delivery within bound."""


class KafkaFatalProducerError(FatalEventPublishError):
    """Raised when librdkafka reports a fatal idempotent-producer error.

    Also raised, per ``atlas.outbox.kafka_errors.classify_kafka_error``'s
    narrow, fail-closed policy, for:

    - a Kafka error that could not be safely classified at all (e.g. an
      exception without a recognizable ``KafkaError`` payload); and
    - a Kafka error that is neither ``fatal()`` nor ``retriable()`` and is
      not the single narrow, documented ``_MSG_TIMED_OUT`` exception (e.g.
      a permanent, non-retriable error such as ``_BAD_MSG``).

    In both cases Atlas has no safe basis to assume a retry of the same
    event is safe, so this fails closed the same way a confirmed fatal
    idempotent-producer state does.
    """


class KafkaTopicVerificationError(OutboxError):
    """Raised when the fixed reserved topic cannot be verified as usable."""
