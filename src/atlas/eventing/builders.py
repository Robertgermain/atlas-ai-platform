"""Builders for research-job domain events using mutation timestamps."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from atlas.eventing.contracts import (
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
)


def build_research_job_created(
    *,
    research_job_id: str,
    created_at: datetime,
    event_id: UUID | None = None,
) -> ResearchJobCreatedEvent:
    """Build a created event; ``occurred_at`` equals ``created_at``."""
    return ResearchJobCreatedEvent(
        event_id=event_id or uuid4(),
        aggregate_id=research_job_id,
        occurred_at=created_at,
        payload=ResearchJobCreatedPayload(
            research_job_id=research_job_id,
            created_at=created_at,
        ),
    )


def build_research_job_completed(
    *,
    research_job_id: str,
    completed_at: datetime,
    event_id: UUID | None = None,
) -> ResearchJobCompletedEvent:
    """Build a completed event; ``occurred_at`` equals ``completed_at``."""
    return ResearchJobCompletedEvent(
        event_id=event_id or uuid4(),
        aggregate_id=research_job_id,
        occurred_at=completed_at,
        payload=ResearchJobCompletedPayload(
            research_job_id=research_job_id,
            completed_at=completed_at,
        ),
    )


def build_research_job_failed(
    *,
    research_job_id: str,
    failed_at: datetime,
    reason_class: str,
    event_id: UUID | None = None,
) -> ResearchJobFailedEvent:
    """Build a failed event with a sanitized reason class only."""
    return ResearchJobFailedEvent(
        event_id=event_id or uuid4(),
        aggregate_id=research_job_id,
        occurred_at=failed_at,
        payload=ResearchJobFailedPayload(
            research_job_id=research_job_id,
            failed_at=failed_at,
            reason_class=reason_class,
        ),
    )


def build_research_job_awaiting_review(
    *,
    research_job_id: str,
    workflow_execution_id: str,
    entered_review_at: datetime,
    event_id: UUID | None = None,
) -> ResearchJobAwaitingReviewEvent:
    """Build an awaiting-review event; ``occurred_at`` equals ``entered_review_at``."""
    return ResearchJobAwaitingReviewEvent(
        event_id=event_id or uuid4(),
        aggregate_id=research_job_id,
        occurred_at=entered_review_at,
        payload=ResearchJobAwaitingReviewPayload(
            research_job_id=research_job_id,
            workflow_execution_id=workflow_execution_id,
            entered_review_at=entered_review_at,
        ),
    )


def build_research_job_retry_scheduled(
    *,
    research_job_id: str,
    abandoned_workflow_execution_id: str | None,
    job_retry_count: int,
    next_attempt_at: datetime,
    occurred_at: datetime,
    event_id: UUID | None = None,
) -> ResearchJobRetryScheduledEvent:
    """Build a retry-scheduled event using the domain mutation timestamp."""
    return ResearchJobRetryScheduledEvent(
        event_id=event_id or uuid4(),
        aggregate_id=research_job_id,
        occurred_at=occurred_at,
        payload=ResearchJobRetryScheduledPayload(
            research_job_id=research_job_id,
            abandoned_workflow_execution_id=abandoned_workflow_execution_id,
            job_retry_count=job_retry_count,
            next_attempt_at=next_attempt_at,
        ),
    )
