"""Canonical deterministic JSON serialization for domain-event envelopes."""

from __future__ import annotations

import json
from typing import Any

from atlas.eventing.contracts import DomainEvent
from atlas.eventing.errors import DomainEventSerializationError

# Soft application bound matching the PostgreSQL 16 KiB CHECK on payload JSONB.
MAX_PAYLOAD_JSON_BYTES = 16 * 1024


def canonical_json_dumps(data: dict[str, Any]) -> str:
    """Return deterministic compact JSON with sorted object keys."""
    return json.dumps(
        data,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def domain_event_to_canonical_dict(event: DomainEvent) -> dict[str, Any]:
    """Dump a typed envelope to a JSON-ready mapping (mode='json')."""
    return event.model_dump(mode="json")


def serialize_domain_event(event: DomainEvent) -> str:
    """Serialize a typed envelope to canonical JSON text."""
    try:
        return canonical_json_dumps(domain_event_to_canonical_dict(event))
    except Exception as exc:
        raise DomainEventSerializationError(
            f"Failed to serialize domain event ({exc.__class__.__name__})."
        ) from None


def serialize_payload(event: DomainEvent) -> dict[str, Any]:
    """Return the JSON-ready payload mapping for durable outbox storage."""
    payload = event.payload.model_dump(mode="json")
    encoded = canonical_json_dumps(payload).encode("utf-8")
    if len(encoded) > MAX_PAYLOAD_JSON_BYTES:
        raise DomainEventSerializationError(
            "Domain event payload exceeds the 16 KiB outbox limit."
        )
    return payload
