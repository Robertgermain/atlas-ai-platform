"""Dead-letter payload retention: Tier A (replayable) vs Tier B (untrusted).

Tier A applies only to ``LifecycleOrderViolationError`` -- the sole
``PoisonEventError`` raised on an already fully decoded, header-consistent,
catalog-valid ``DomainEvent`` (see ``atlas.consumer.errors.
TIER_A_ELIGIBLE_FAILURE_CODES``). Every other poison classification is
raised by ``decode_message`` before it ever returns a trusted event, so the
original bytes are treated as fully untrusted: only a SHA-256 hash and byte
length are retained, never the raw value or raw headers.

``payload_byte_length`` (below) is the *raw Kafka record value*'s length in
bytes, bounded by ``atlas.consumer.deserialize.MAX_MESSAGE_VALUE_BYTES`` (32
KiB) before a Tier-A poison classification can even be reached.
``retained_canonical_value`` is a *different* value -- the event
re-serialized through ``atlas.eventing.serialization.serialize_domain_event``
-- which is not the same bytes and is not guaranteed to be the same length
(canonical key ordering/escaping can differ from whatever encoding produced
the original record). ``MAX_RETAINED_CANONICAL_VALUE_BYTES`` bounds that
*re-serialized* value and is kept numerically equal to
``MAX_MESSAGE_VALUE_BYTES`` (and to migration ``20260809_0013``'s
``ck_consumer_dead_letters_retained_value_bound`` CHECK -- keep all three in
sync) purely because they happen to share a sensible bound today, not
because they are the same measurement.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from atlas.consumer.deserialize import MAX_MESSAGE_VALUE_BYTES
from atlas.consumer.errors import TIER_A_ELIGIBLE_FAILURE_CODES
from atlas.eventing.contracts import DomainEvent
from atlas.eventing.serialization import serialize_domain_event

#: Bound on the *canonical re-serialized* value persisted to
#: ``consumer_dead_letters.retained_canonical_value`` -- see the module
#: docstring for why this is a distinct measurement from the raw Kafka
#: record's byte length, kept numerically aligned to
#: ``MAX_MESSAGE_VALUE_BYTES`` and to the migration's CHECK constraint.
MAX_RETAINED_CANONICAL_VALUE_BYTES = MAX_MESSAGE_VALUE_BYTES


@dataclass(frozen=True, slots=True)
class DeadLetterRetention:
    """The exact fields ``ConsumerDeadLetterModel`` persists for one poison event."""

    event_id: object | None
    event_type: str | None
    event_version: int | None
    aggregate_type: str | None
    aggregate_id: str | None
    payload_sha256: str
    payload_byte_length: int
    retained_canonical_value: str | None
    retained_header_event_type: str | None
    retained_header_event_version: str | None
    retained_header_aggregate_type: str | None
    replay_eligible: bool


def build_retention(
    *,
    failure_code: str,
    raw_value: bytes | None,
    decoded_event: DomainEvent | None,
) -> DeadLetterRetention:
    """Build the retention record for one poison classification.

    ``decoded_event`` must be provided if and only if ``failure_code`` is
    Tier-A eligible (currently only ``lifecycle_order_violation``) -- every
    other failure_code is raised before a trusted event exists.
    """
    tier_a = failure_code in TIER_A_ELIGIBLE_FAILURE_CODES
    if tier_a != (decoded_event is not None):
        raise ValueError(
            "decoded_event must be provided if and only if failure_code is "
            "Tier-A eligible."
        )

    raw_bytes = raw_value or b""
    payload_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    payload_byte_length = len(raw_bytes)

    if decoded_event is None:
        return DeadLetterRetention(
            event_id=None,
            event_type=None,
            event_version=None,
            aggregate_type=None,
            aggregate_id=None,
            payload_sha256=payload_sha256,
            payload_byte_length=payload_byte_length,
            retained_canonical_value=None,
            retained_header_event_type=None,
            retained_header_event_version=None,
            retained_header_aggregate_type=None,
            replay_eligible=False,
        )

    canonical_value = serialize_domain_event(decoded_event)
    if len(canonical_value.encode("utf-8")) > MAX_RETAINED_CANONICAL_VALUE_BYTES:
        # Extremely unlikely (canonical re-serialization is not guaranteed
        # to be the same size as the already-bounded raw record -- see the
        # module docstring) but must never make DLQ persistence itself
        # fail: degrade to Tier B, still recording identifying metadata for
        # operators, never the oversized bytes and never replay-eligible.
        return DeadLetterRetention(
            event_id=decoded_event.event_id,
            event_type=decoded_event.event_type,
            event_version=int(decoded_event.event_version),
            aggregate_type=decoded_event.aggregate_type,
            aggregate_id=decoded_event.aggregate_id,
            payload_sha256=payload_sha256,
            payload_byte_length=payload_byte_length,
            retained_canonical_value=None,
            retained_header_event_type=None,
            retained_header_event_version=None,
            retained_header_aggregate_type=None,
            replay_eligible=False,
        )

    return DeadLetterRetention(
        event_id=decoded_event.event_id,
        event_type=decoded_event.event_type,
        event_version=int(decoded_event.event_version),
        aggregate_type=decoded_event.aggregate_type,
        aggregate_id=decoded_event.aggregate_id,
        payload_sha256=payload_sha256,
        payload_byte_length=payload_byte_length,
        retained_canonical_value=canonical_value,
        retained_header_event_type=decoded_event.event_type,
        retained_header_event_version=str(int(decoded_event.event_version)),
        retained_header_aggregate_type=decoded_event.aggregate_type,
        replay_eligible=True,
    )
