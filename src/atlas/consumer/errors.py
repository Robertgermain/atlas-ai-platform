"""Typed errors for the Kafka business consumer (Slice 13C2A).

Slice 13C2A has no retry/classification policy yet (that is Slice 13C2B's
scope): every error defined here is treated identically by
``python -m atlas.consumer`` -- a sanitized log line naming only the error
class, then a nonzero process exit. Nothing here is safe to assume
recoverable without the bounded retry/DLQ work planned for 13C2B.
"""

from __future__ import annotations


class ConsumerError(Exception):
    """Base class for business-consumer failures."""


class ConsumerConfigurationError(ConsumerError):
    """Raised when Kafka consumer configuration is invalid."""


class MalformedEnvelopeError(ConsumerError):
    """Raised when a Kafka record value cannot be decoded/validated.

    Covers an oversized or undecodable value, invalid JSON, a value that is
    not a JSON object, and a payload that fails the typed research-job
    event catalog's own validation (unsupported version, unknown event
    type, schema violation).
    """


class InvalidHeaderError(ConsumerError):
    """Raised when Kafka record headers are missing, malformed, or inconsistent.

    Covers missing/duplicate/undecodable/unexpected header keys and a
    header value that disagrees with the decoded envelope's own
    ``event_type`` / ``event_version`` / ``aggregate_type``.
    """


class LifecycleOrderViolationError(ConsumerError):
    """Raised when a new event would be applied on top of a terminal projection row.

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
    """
