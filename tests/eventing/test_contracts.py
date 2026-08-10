"""Unit tests for typed research-job domain-event contracts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from atlas.eventing import (
    DomainEventValidationError,
    ResearchJobCreatedEvent,
    ResearchJobCreatedPayload,
    ResearchJobFailedEvent,
    ResearchJobFailedPayload,
    build_research_job_completed,
    build_research_job_created,
    build_research_job_failed,
    canonical_json_dumps,
    domain_event_to_canonical_dict,
    parse_domain_event,
    serialize_domain_event,
)

T0 = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)


def test_created_event_canonical_serialization_is_deterministic() -> None:
    event_id = uuid4()
    event = build_research_job_created(
        research_job_id="job-1",
        created_at=T0,
        event_id=event_id,
    )
    first = serialize_domain_event(event)
    second = serialize_domain_event(event)
    assert first == second
    assert '"event_type":"research_job.created"' in first
    assert first == canonical_json_dumps(domain_event_to_canonical_dict(event))


def test_naive_timestamp_rejected() -> None:
    with pytest.raises((DomainEventValidationError, ValidationError)):
        ResearchJobCreatedPayload(
            research_job_id="job-1",
            created_at=datetime(2026, 8, 10, 12, 0, 0),
        )


def test_aware_timestamp_normalized_to_utc() -> None:
    eastern = timezone(timedelta(hours=-4))
    local = datetime(2026, 8, 10, 8, 0, 0, tzinfo=eastern)
    event = build_research_job_created(research_job_id="job-1", created_at=local)
    assert event.occurred_at == datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)
    assert event.payload.created_at == event.occurred_at


def test_unknown_event_type_rejected() -> None:
    with pytest.raises(DomainEventValidationError):
        parse_domain_event(
            {
                "event_id": str(uuid4()),
                "event_version": 1,
                "event_type": "research_job.unknown",
                "aggregate_type": "research_job",
                "aggregate_id": "job-1",
                "occurred_at": T0.isoformat(),
                "payload": {"research_job_id": "job-1", "created_at": T0.isoformat()},
            }
        )


def test_unknown_event_version_rejected() -> None:
    with pytest.raises(DomainEventValidationError):
        parse_domain_event(
            {
                "event_id": str(uuid4()),
                "event_version": 2,
                "event_type": "research_job.created",
                "aggregate_type": "research_job",
                "aggregate_id": "job-1",
                "occurred_at": T0.isoformat(),
                "payload": {"research_job_id": "job-1", "created_at": T0.isoformat()},
            }
        )


def test_payload_envelope_mismatch_rejected() -> None:
    with pytest.raises((DomainEventValidationError, ValidationError)):
        ResearchJobCreatedEvent(
            event_id=uuid4(),
            aggregate_id="job-1",
            occurred_at=T0,
            payload=ResearchJobCreatedPayload(
                research_job_id="job-other",
                created_at=T0,
            ),
        )


def test_occurred_at_must_match_payload_timestamp() -> None:
    with pytest.raises((DomainEventValidationError, ValidationError)):
        ResearchJobCreatedEvent(
            event_id=uuid4(),
            aggregate_id="job-1",
            occurred_at=T0,
            payload=ResearchJobCreatedPayload(
                research_job_id="job-1",
                created_at=T0 + timedelta(seconds=1),
            ),
        )


def test_failed_payload_rejects_whitespace_reason_class() -> None:
    with pytest.raises((DomainEventValidationError, ValidationError)):
        ResearchJobFailedPayload(
            research_job_id="job-1",
            failed_at=T0,
            reason_class="bad reason",
        )


def test_failed_event_builder_accepts_sanitized_class() -> None:
    event = build_research_job_failed(
        research_job_id="job-1",
        failed_at=T0,
        reason_class="ProcessingTimeout",
    )
    assert isinstance(event, ResearchJobFailedEvent)
    assert event.payload.reason_class == "ProcessingTimeout"


def test_completed_builder_aligns_timestamps() -> None:
    event = build_research_job_completed(
        research_job_id="job-1",
        completed_at=T0,
    )
    assert event.occurred_at == T0
    assert event.payload.completed_at == T0
