"""Typed errors and failure-code classification for the Kafka business consumer.

Slice 13C2A defined a flat set of string-parameterized errors with no retry
policy. Slice 13C2B replaces every poison-classification raise site with a
dedicated exception subclass carrying a fixed ``failure_code`` class
attribute, and adds explicit taxonomies for transient-infrastructure and
fatal failures:

- ``PoisonEventError`` subclasses: permanent, record-specific failures.
  Every one of these is persisted to the PostgreSQL dead-letter table (see
  ``atlas.persistence.repositories.consumer_dead_letter``) and the Kafka
  offset is committed afterward -- the poisoned record must never block
  the partition forever.
- ``TransientInfrastructureError`` subclasses: bounded, process-local retry
  (see ``atlas.consumer.runner``), then terminate nonzero with no offset
  commit and no dead-letter row on exhaustion.
- Everything else (``ConsumerConfigurationError``, ``ConsumerInboxConflictError``
  from the persistence layer, an unmapped/unrecognized error, or any
  non-``ConsumerError`` exception) is fatal: immediate termination, no
  retry, no dead-letter row, no offset commit.

``failure_code`` values are the only diagnostic text ever persisted to the
dead-letter table (see its CHECK constraint's fixed allowlist) -- never
``str(exc)``, ``repr(exc)``, ``exc.args``, raw Kafka error text, or any
payload-derived text. The mapping in ``failure_code_for`` is keyed by exact
type (not ``isinstance``), so an unmapped exception type fails closed
instead of silently inheriting a parent's code.
"""

from __future__ import annotations

from typing import ClassVar


class ConsumerError(Exception):
    """Base class for business-consumer failures."""


class ConsumerConfigurationError(ConsumerError):
    """Raised when Kafka consumer configuration is invalid. Fatal, never retried."""


class PoisonEventError(ConsumerError):
    """Base for every permanent, record-specific classification that dead-letters.

    Never raised directly -- only via one of the dedicated subclasses below,
    each of which fixes ``failure_code`` to a single allowlisted value.
    """

    failure_code: ClassVar[str]


class InvalidHeaderError(PoisonEventError):
    """Grouping base: Kafka record headers are missing, malformed, or inconsistent.

    Covers missing/duplicate/undecodable/unexpected header keys and a
    header value that disagrees with the decoded envelope's own
    ``event_type`` / ``event_version`` / ``aggregate_type``. Never raised
    directly -- see the dedicated subclasses below.
    """


class MissingHeadersError(InvalidHeaderError):
    """Kafka record has no headers at all."""

    failure_code = "missing_headers"


class UnexpectedHeadersShapeError(InvalidHeaderError):
    """Headers are neither a mapping nor a list of 2-tuples."""

    failure_code = "unexpected_headers_shape"


class UnexpectedHeaderKeyTypeError(InvalidHeaderError):
    """A header key is not a ``str``."""

    failure_code = "unexpected_header_key_type"


class DuplicateHeaderKeyError(InvalidHeaderError):
    """The same header key appears more than once."""

    failure_code = "duplicate_header_key"


class NullHeaderValueError(InvalidHeaderError):
    """A required header key is present with a ``None`` value."""

    failure_code = "null_header_value"


class UndecodableHeaderValueError(InvalidHeaderError):
    """A header value's bytes are not valid UTF-8."""

    failure_code = "undecodable_header_value"


class UnexpectedHeaderValueTypeError(InvalidHeaderError):
    """A header value is neither ``bytes`` nor ``str``."""

    failure_code = "unexpected_header_value_type"


class UnexpectedHeaderKeysError(InvalidHeaderError):
    """The header key set does not exactly match the expected fixed set."""

    failure_code = "unexpected_header_keys"


class EventTypeHeaderMismatchError(InvalidHeaderError):
    """The ``event_type`` header disagrees with the decoded envelope."""

    failure_code = "event_type_header_mismatch"


class EventVersionHeaderMismatchError(InvalidHeaderError):
    """The ``event_version`` header disagrees with the decoded envelope."""

    failure_code = "event_version_header_mismatch"


class AggregateTypeHeaderMismatchError(InvalidHeaderError):
    """The ``aggregate_type`` header disagrees with the decoded envelope."""

    failure_code = "aggregate_type_header_mismatch"


class MalformedEnvelopeError(PoisonEventError):
    """Grouping base: a Kafka record value cannot be decoded/validated.

    Covers an oversized or undecodable value, invalid JSON, a value that is
    not a JSON object, and a payload that fails the typed research-job
    event catalog's own validation (unsupported version, unknown event
    type, schema violation). Never raised directly -- see the dedicated
    subclasses below.
    """


class MissingValueError(MalformedEnvelopeError):
    """The Kafka record's value is ``None``."""

    failure_code = "missing_value"


class ValueTooLargeError(MalformedEnvelopeError):
    """The Kafka record's value exceeds the configured maximum size."""

    failure_code = "value_too_large"


class UndecodableValueError(MalformedEnvelopeError):
    """The Kafka record's value is not valid UTF-8."""

    failure_code = "undecodable_value"


class InvalidJsonError(MalformedEnvelopeError):
    """The Kafka record's value is not valid JSON."""

    failure_code = "invalid_json"


class ValueNotAnObjectError(MalformedEnvelopeError):
    """The Kafka record's decoded JSON value is not an object."""

    failure_code = "value_not_an_object"


class SchemaValidationFailedError(MalformedEnvelopeError):
    """The decoded JSON object fails the typed domain-event catalog's validation."""

    failure_code = "schema_validation_failed"


class LifecycleOrderViolationError(PoisonEventError):
    """A new event would be applied on top of a terminal projection row.

    The research-job lifecycle projection is keyed by ``research_job_id``
    and, once it records a terminal event (``research_job.completed`` or
    ``research_job.failed``), the domain guarantees no further event for
    that job should ever be produced. Combined with the reserved topic's
    single-partition global ordering, receiving a *different* event for an
    already-terminal projection row indicates an ordering invariant was
    violated somewhere upstream, so this fails closed instead of silently
    overwriting the recorded terminal state. An exact redelivery of the
    same ``event_id`` never reaches this check -- the inbox uniqueness
    boundary is consulted first and short-circuits it as a duplicate.

    Unlike every other ``PoisonEventError``, this is raised on a fully
    decoded, header-consistent, catalog-valid event (``decode_message``
    already returned successfully) -- so it is the only failure_code
    eligible for Tier-A dead-letter payload retention (see
    ``atlas.consumer.retention``).
    """

    failure_code = "lifecycle_order_violation"


class TransientInfrastructureError(ConsumerError):
    """Base for bounded, process-local retryable infrastructure failures.

    Never raised directly. Exhaustion of the retry budget always terminates
    the consumer nonzero with the Kafka offset uncommitted and no
    dead-letter row -- these are never a basis for dead-lettering a record.
    """


class TransientDatabaseError(TransientInfrastructureError):
    """A PostgreSQL/SQLAlchemy failure classified transient (see ``db_classify``)."""


class TransientKafkaError(TransientInfrastructureError):
    """A Kafka failure classified recoverable (see ``atlas.outbox.kafka_errors``).

    Raised only by ``KafkaEventConsumer`` (never by the runner directly),
    which is the only layer with access to the raw ``confluent_kafka.
    KafkaError`` metadata a classification decision requires. Covers a
    raised exception from ``poll()``, a broker-error message returned by
    ``poll()``, and a synchronous ``commit_message()`` failure -- see that
    module's docstring for the exact fatal-vs-transient policy (reused,
    never duplicated, from ``atlas.outbox.kafka_errors.classify_kafka_error``).
    ``ConsumerRunner`` retries a ``commit_message()``-site
    ``TransientKafkaError`` using the same bounded attempts/backoff/deadline
    machinery as a transient database error; a ``poll()``-site one (raised
    before any record is in hand, outside any retry episode) is instead
    handled by ``python -m atlas.consumer``'s poll loop, which backs off and
    polls again rather than terminating the whole process.
    """


class ProcessingDeadlineExceededError(ConsumerError):
    """The processing deadline could not accommodate another attempt/backoff.

    Terminates the consumer nonzero with the Kafka offset uncommitted.
    """


class ConsumerShutdownRequestedError(ConsumerError):
    """Shutdown was observed during a retry/backoff wait (see ``atlas.consumer.wait``).

    Not a failure: ``python -m atlas.consumer`` catches this specifically
    and exits cleanly (0) with the Kafka offset left uncommitted -- the
    record safely redelivers after restart via the inbox/dead-letter
    uniqueness boundaries. Never logged as an error.
    """


class RetryExhaustedError(ConsumerError):
    """The bounded transient-infrastructure retry budget was exhausted.

    Terminates the consumer nonzero with the Kafka offset uncommitted and
    no dead-letter row.
    """


class DeadLetterPersistenceExhaustedError(ConsumerError):
    """Dead-letter persistence itself exhausted its bounded transient retries.

    Terminates the consumer nonzero with the Kafka offset uncommitted.
    """


class OffsetCommitFailedAfterDeadLetterError(ConsumerError):
    """The Kafka offset commit failed after the dead-letter row was durably committed.

    Terminates the consumer nonzero. Redelivery of the same record after
    restart must reuse the existing dead-letter row (via the upsert
    uniqueness boundary), incrementing only ``dead_letter_delivery_count``.
    """


#: Exact-type allowlisted mapping from a ``PoisonEventError`` subclass to its
#: fixed, persisted ``failure_code``. Keyed by ``type(exc)`` (never
#: ``isinstance``): a future subclass that is not explicitly added here
#: fails closed in ``failure_code_for`` rather than silently inheriting a
#: parent grouping class's code. This is also the exact set backing the
#: PostgreSQL CHECK constraint on ``consumer_dead_letters.failure_code`` --
#: keep both in sync (see migration 20260809_0013).
_FAILURE_CODE_BY_EXACT_TYPE: dict[type[PoisonEventError], str] = {
    MissingHeadersError: MissingHeadersError.failure_code,
    UnexpectedHeadersShapeError: UnexpectedHeadersShapeError.failure_code,
    UnexpectedHeaderKeyTypeError: UnexpectedHeaderKeyTypeError.failure_code,
    DuplicateHeaderKeyError: DuplicateHeaderKeyError.failure_code,
    NullHeaderValueError: NullHeaderValueError.failure_code,
    UndecodableHeaderValueError: UndecodableHeaderValueError.failure_code,
    UnexpectedHeaderValueTypeError: UnexpectedHeaderValueTypeError.failure_code,
    UnexpectedHeaderKeysError: UnexpectedHeaderKeysError.failure_code,
    EventTypeHeaderMismatchError: EventTypeHeaderMismatchError.failure_code,
    EventVersionHeaderMismatchError: EventVersionHeaderMismatchError.failure_code,
    AggregateTypeHeaderMismatchError: AggregateTypeHeaderMismatchError.failure_code,
    MissingValueError: MissingValueError.failure_code,
    ValueTooLargeError: ValueTooLargeError.failure_code,
    UndecodableValueError: UndecodableValueError.failure_code,
    InvalidJsonError: InvalidJsonError.failure_code,
    ValueNotAnObjectError: ValueNotAnObjectError.failure_code,
    SchemaValidationFailedError: SchemaValidationFailedError.failure_code,
    LifecycleOrderViolationError: LifecycleOrderViolationError.failure_code,
}

#: The fixed allowlist backing the database CHECK constraint. Derived from
#: the mapping above so the two can never silently drift apart in Python.
ALLOWED_FAILURE_CODES: frozenset[str] = frozenset(_FAILURE_CODE_BY_EXACT_TYPE.values())


class UnmappedPoisonEventTypeError(ConsumerError):
    """Raised when a ``PoisonEventError`` subclass has no allowlisted failure_code.

    Fails closed rather than silently receiving a parent category's code --
    this should never happen in practice (every concrete subclass above is
    registered) but protects against a future subclass being added to the
    hierarchy without also being added to the mapping.
    """


def failure_code_for(exc: PoisonEventError) -> str:
    """Return the fixed, persisted failure_code for a poison-event exception."""
    code = _FAILURE_CODE_BY_EXACT_TYPE.get(type(exc))
    if code is None:
        raise UnmappedPoisonEventTypeError(type(exc).__name__)
    return code


#: Only ``LifecycleOrderViolationError`` is raised on a fully decoded,
#: header-consistent, catalog-valid event -- every other ``PoisonEventError``
#: is raised by ``atlas.consumer.deserialize.decode_message`` before it ever
#: returns a trusted ``DomainEvent``. This is the exact Tier-A/Tier-B
#: boundary used by ``atlas.consumer.retention``.
TIER_A_ELIGIBLE_FAILURE_CODES: frozenset[str] = frozenset(
    {LifecycleOrderViolationError.failure_code}
)
