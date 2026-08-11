"""Strict decode/validate boundary from a raw Kafka record to a typed ``DomainEvent``.

Never logs or embeds raw header/value bytes in any exception message --
every failure here raises one of ``atlas.consumer.errors``'s fixed,
sanitized error classes.
"""

from __future__ import annotations

import json
from typing import Protocol

from atlas.consumer.errors import InvalidHeaderError, MalformedEnvelopeError
from atlas.eventing.contracts import DomainEvent, parse_domain_event
from atlas.eventing.errors import DomainEventError

# Twice the outbox producer's 16 KiB payload cap (atlas.eventing.serialization.
# MAX_PAYLOAD_JSON_BYTES) plus envelope overhead -- defense in depth against a
# corrupted or foreign record, not a tight production bound.
MAX_MESSAGE_VALUE_BYTES = 32 * 1024

_EXPECTED_HEADER_KEYS = frozenset({"event_type", "event_version", "aggregate_type"})


class _KafkaMessageLike(Protocol):
    """The ``confluent_kafka.Message`` surface this module depends on.

    ``headers()`` is typed as plain ``object`` (not ``confluent_kafka.
    _types.HeadersType``, a private module) because real messages, per
    confluent-kafka's own stub, may structurally return either a mapping
    or a list of pairs with ``str | bytes | None`` values; this module
    narrows that itself with explicit ``isinstance`` checks below rather
    than trusting the wider declared type.
    """

    def value(self) -> bytes | None: ...
    def headers(self) -> object: ...


def _headers_as_mapping(message: _KafkaMessageLike) -> dict[str, str]:
    raw_headers = message.headers()
    if raw_headers is None:
        raise InvalidHeaderError("MissingHeaders")

    pairs: list[tuple[object, object]] = []
    if isinstance(raw_headers, dict):
        pairs = list(raw_headers.items())
    elif isinstance(raw_headers, list):
        for item in raw_headers:
            if not (isinstance(item, tuple) and len(item) == 2):
                raise InvalidHeaderError("UnexpectedHeadersShape")
            pairs.append(item)
    else:
        raise InvalidHeaderError("UnexpectedHeadersShape")

    mapping: dict[str, str] = {}
    for key, value in pairs:
        if not isinstance(key, str):
            raise InvalidHeaderError("UnexpectedHeaderKeyType")
        if key in mapping:
            raise InvalidHeaderError("DuplicateHeaderKey")
        if value is None:
            raise InvalidHeaderError("NullHeaderValue")
        if isinstance(value, bytes):
            try:
                mapping[key] = value.decode("utf-8")
            except UnicodeDecodeError:
                raise InvalidHeaderError("UndecodableHeaderValue") from None
        elif isinstance(value, str):
            mapping[key] = value
        else:
            raise InvalidHeaderError("UnexpectedHeaderValueType")
    if set(mapping) != _EXPECTED_HEADER_KEYS:
        raise InvalidHeaderError("UnexpectedHeaderKeys")
    return mapping


def decode_message(message: _KafkaMessageLike) -> DomainEvent:
    """Strictly decode and validate one Kafka record into a typed ``DomainEvent``.

    Fails closed (raises) on: missing/duplicate/undecodable/unexpected
    headers, a missing/oversized/undecodable value, malformed JSON, a
    value that is not a JSON object, a payload that fails the typed
    research-job event catalog's own validation (unsupported version,
    unknown event type, schema violation), or a header that disagrees with
    the decoded envelope.
    """
    headers = _headers_as_mapping(message)

    value = message.value()
    if value is None:
        raise MalformedEnvelopeError("MissingValue")
    if len(value) > MAX_MESSAGE_VALUE_BYTES:
        raise MalformedEnvelopeError("ValueTooLarge")
    try:
        decoded_text = value.decode("utf-8")
    except UnicodeDecodeError:
        raise MalformedEnvelopeError("UndecodableValue") from None
    try:
        data = json.loads(decoded_text)
    except json.JSONDecodeError:
        raise MalformedEnvelopeError("InvalidJson") from None
    if not isinstance(data, dict):
        raise MalformedEnvelopeError("ValueNotAnObject")

    try:
        event = parse_domain_event(data)
    except DomainEventError:
        raise MalformedEnvelopeError("SchemaValidationFailed") from None

    if headers["event_type"] != event.event_type:
        raise InvalidHeaderError("EventTypeHeaderMismatch")
    if headers["event_version"] != str(int(event.event_version)):
        raise InvalidHeaderError("EventVersionHeaderMismatch")
    if headers["aggregate_type"] != event.aggregate_type:
        raise InvalidHeaderError("AggregateTypeHeaderMismatch")

    return event
