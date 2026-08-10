"""Typed research-job domain-event contracts (Milestone 13 Slice 13B)."""

from atlas.eventing.builders import (
    build_research_job_awaiting_review,
    build_research_job_completed,
    build_research_job_created,
    build_research_job_failed,
    build_research_job_retry_scheduled,
)
from atlas.eventing.contracts import (
    SUPPORTED_EVENT_TYPES,
    DomainEvent,
    ResearchJobAwaitingReviewEvent,
    ResearchJobAwaitingReviewPayload,
    ResearchJobCompletedEvent,
    ResearchJobCompletedPayload,
    ResearchJobCreatedEvent,
    ResearchJobCreatedPayload,
    ResearchJobFailedEvent,
    ResearchJobFailedPayload,
    ResearchJobRetryScheduledEvent,
    ResearchJobRetryScheduledPayload,
    parse_domain_event,
)
from atlas.eventing.errors import (
    DomainEventError,
    DomainEventSerializationError,
    DomainEventValidationError,
)
from atlas.eventing.serialization import (
    MAX_PAYLOAD_JSON_BYTES,
    canonical_json_dumps,
    domain_event_to_canonical_dict,
    serialize_domain_event,
    serialize_payload,
)
from atlas.eventing.topic import RESEARCH_JOB_EVENTS_TOPIC_V1

__all__ = [
    "SUPPORTED_EVENT_TYPES",
    "DomainEvent",
    "DomainEventError",
    "DomainEventSerializationError",
    "DomainEventValidationError",
    "MAX_PAYLOAD_JSON_BYTES",
    "RESEARCH_JOB_EVENTS_TOPIC_V1",
    "ResearchJobAwaitingReviewEvent",
    "ResearchJobAwaitingReviewPayload",
    "ResearchJobCompletedEvent",
    "ResearchJobCompletedPayload",
    "ResearchJobCreatedEvent",
    "ResearchJobCreatedPayload",
    "ResearchJobFailedEvent",
    "ResearchJobFailedPayload",
    "ResearchJobRetryScheduledEvent",
    "ResearchJobRetryScheduledPayload",
    "build_research_job_awaiting_review",
    "build_research_job_completed",
    "build_research_job_created",
    "build_research_job_failed",
    "build_research_job_retry_scheduled",
    "canonical_json_dumps",
    "domain_event_to_canonical_dict",
    "parse_domain_event",
    "serialize_domain_event",
    "serialize_payload",
]
