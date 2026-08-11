"""Centralized Kafka error classification (Slice 13C1 correction pass, v2).

A single helper decides whether a Kafka error is fatal or recoverable. This
module never inspects an error's string message or an exception's class name
to make that decision, and it is the only place in the codebase that
performs this classification -- ``kafka_producer.py`` applies it uniformly to
delivery-callback errors, immediate ``produce()`` failures, and ``flush()``
failures.

Policy (narrow and fail-closed, in order):

1. A payload that is not a recognizable ``confluent_kafka.KafkaError`` (e.g.
   an unrecognized ``KafkaException`` argument) -> fatal. There is no signal
   at all to classify it by.
2. ``error.fatal()`` -> fatal. Confluent's own fatal/abortable classification
   for the idempotent producer.
3. ``error.retriable()`` -> recoverable. This is Confluent's documented
   operation-retry signal.
4. ``error.code() == KafkaError._MSG_TIMED_OUT`` -> recoverable, as a single,
   narrow, documented exception specific to Atlas's durable outbox (not a
   general Confluent recommendation). Real-client evidence (Slice 13C1
   correction pass, integration test against an unreachable broker) shows
   librdkafka reports a per-message delivery timeout as ``fatal=False,
   retriable=False`` once its own internal ``delivery.timeout.ms`` budget is
   exhausted -- ``retriable()`` here reflects librdkafka's *own* exhausted
   retry budget, not whether a *later, independent* Atlas outbox-relay retry
   of the same durable event is safe once the broker is reachable again. This
   error only proves that Atlas did not receive broker-confirmed delivery
   within the configured deadline; it does not prove the broker never
   received the record. Treating it as recoverable is compatible with
   Atlas's documented at-least-once delivery contract: republishing the same
   ``event_id`` may produce a duplicate on the topic (never an exactly-once
   guarantee), and that duplicate is handled by the stable ``event_id`` plus
   the consumer inbox/deduplication work planned for Slice 13C2, not by this
   producer adapter.
5. Every other error that is neither fatal nor retriable -> fatal. Per
   Confluent's own guidance, an error with neither classification set is
   treated as non-retriable/permanent (e.g. ``_BAD_MSG``, a malformed
   record) and must fail closed rather than be assumed safe to retry.

Do not add further code exceptions to step 4 by guessing. Only add one after
a real integration test proves the exact code and documents why retrying the
durable outbox event is safe for that specific code.
"""

from __future__ import annotations

from enum import StrEnum

from confluent_kafka import KafkaError, KafkaException


class KafkaErrorClass(StrEnum):
    """Classification outcome for a Kafka error object."""

    FATAL = "fatal"
    RECOVERABLE = "recoverable"


# Kafka error codes that are recoverable at the Atlas durable-outbox level
# despite being neither `fatal()` nor `retriable()` per librdkafka's own
# flags. Each entry requires a real integration test proving the exact code
# is safe to retry (see module docstring, step 4) -- never a guess.
_RECOVERABLE_DESPITE_NEITHER_FLAG: frozenset[int] = frozenset(
    {KafkaError._MSG_TIMED_OUT}
)


def classify_kafka_error(error: object) -> KafkaErrorClass:
    """Classify a Kafka error per the narrow, fail-closed policy above.

    ``error`` is expected to be a ``confluent_kafka.KafkaError`` -- either
    the ``err`` argument passed to a delivery callback, or the value
    unwrapped from a ``KafkaException``'s first argument via
    :func:`kafka_error_from_exception`.
    """
    if not isinstance(error, KafkaError):
        return KafkaErrorClass.FATAL
    if error.fatal():
        return KafkaErrorClass.FATAL
    if error.retriable():
        return KafkaErrorClass.RECOVERABLE
    if error.code() in _RECOVERABLE_DESPITE_NEITHER_FLAG:
        return KafkaErrorClass.RECOVERABLE
    return KafkaErrorClass.FATAL


def kafka_error_from_exception(exc: KafkaException) -> object:
    """Extract the underlying error payload from a ``KafkaException``, if any."""
    return exc.args[0] if exc.args else None
