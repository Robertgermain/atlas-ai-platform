"""Typed, frozen research-job domain-event envelopes (Slice 13B).

Public event boundaries are Pydantic models only — never ``dict[str, Any]``.
Payloads exclude secrets, raw exception text, claim tokens, idempotency keys,
prompts, evidence/report bodies, and credential-bearing URLs.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)

from atlas.eventing.errors import DomainEventValidationError

EventVersion = Literal[1]
AggregateType = Literal["research_job"]

ResearchJobCreatedType = Literal["research_job.created"]
ResearchJobCompletedType = Literal["research_job.completed"]
ResearchJobFailedType = Literal["research_job.failed"]
ResearchJobAwaitingReviewType = Literal["research_job.awaiting_review"]
ResearchJobRetryScheduledType = Literal["research_job.retry_scheduled"]

EventType = (
    ResearchJobCreatedType
    | ResearchJobCompletedType
    | ResearchJobFailedType
    | ResearchJobAwaitingReviewType
    | ResearchJobRetryScheduledType
)

SUPPORTED_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "research_job.created",
        "research_job.completed",
        "research_job.failed",
        "research_job.awaiting_review",
        "research_job.retry_scheduled",
    }
)


def normalize_utc(value: datetime) -> datetime:
    """Reject naive datetimes and normalize aware values to UTC."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise DomainEventValidationError("Timestamps must be timezone-aware.")
    return value.astimezone(UTC)


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ResearchJobCreatedPayload(_FrozenModel):
    """Payload for ``research_job.created``."""

    research_job_id: str = Field(min_length=1)
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return normalize_utc(value)


class ResearchJobCompletedPayload(_FrozenModel):
    """Payload for ``research_job.completed``."""

    research_job_id: str = Field(min_length=1)
    completed_at: datetime

    @field_validator("completed_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return normalize_utc(value)


class ResearchJobFailedPayload(_FrozenModel):
    """Payload for ``research_job.failed`` (sanitized reason class only)."""

    research_job_id: str = Field(min_length=1)
    failed_at: datetime
    reason_class: str = Field(min_length=1, max_length=128)

    @field_validator("failed_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return normalize_utc(value)

    @field_validator("reason_class")
    @classmethod
    def _sanitize_reason_class(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise DomainEventValidationError("reason_class must be non-empty.")
        if any(ch.isspace() for ch in cleaned):
            raise DomainEventValidationError(
                "reason_class must not contain whitespace."
            )
        if len(cleaned) > 128:
            raise DomainEventValidationError("reason_class exceeds max length.")
        return cleaned


class ResearchJobAwaitingReviewPayload(_FrozenModel):
    """Payload for ``research_job.awaiting_review``."""

    research_job_id: str = Field(min_length=1)
    workflow_execution_id: str = Field(min_length=1)
    entered_review_at: datetime

    @field_validator("entered_review_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return normalize_utc(value)


class ResearchJobRetryScheduledPayload(_FrozenModel):
    """Payload for ``research_job.retry_scheduled``."""

    research_job_id: str = Field(min_length=1)
    abandoned_workflow_execution_id: str | None
    job_retry_count: int = Field(ge=1, le=2)
    next_attempt_at: datetime

    @field_validator("next_attempt_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return normalize_utc(value)

    @field_validator("abandoned_workflow_execution_id")
    @classmethod
    def _optional_execution_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            raise DomainEventValidationError(
                "abandoned_workflow_execution_id must be non-empty when set."
            )
        return cleaned


class _EventBase(_FrozenModel):
    event_id: UUID
    event_version: EventVersion = 1
    aggregate_type: AggregateType = "research_job"
    aggregate_id: str = Field(min_length=1)
    occurred_at: datetime

    @field_validator("occurred_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return normalize_utc(value)


class ResearchJobCreatedEvent(_EventBase):
    """Envelope for ``research_job.created``."""

    event_type: ResearchJobCreatedType = "research_job.created"
    payload: ResearchJobCreatedPayload

    @model_validator(mode="after")
    def _align_payload(self) -> Self:
        if self.aggregate_id != self.payload.research_job_id:
            raise DomainEventValidationError(
                "aggregate_id must match payload.research_job_id."
            )
        if self.occurred_at != self.payload.created_at:
            raise DomainEventValidationError(
                "occurred_at must match payload.created_at."
            )
        return self


class ResearchJobCompletedEvent(_EventBase):
    """Envelope for ``research_job.completed``."""

    event_type: ResearchJobCompletedType = "research_job.completed"
    payload: ResearchJobCompletedPayload

    @model_validator(mode="after")
    def _align_payload(self) -> Self:
        if self.aggregate_id != self.payload.research_job_id:
            raise DomainEventValidationError(
                "aggregate_id must match payload.research_job_id."
            )
        if self.occurred_at != self.payload.completed_at:
            raise DomainEventValidationError(
                "occurred_at must match payload.completed_at."
            )
        return self


class ResearchJobFailedEvent(_EventBase):
    """Envelope for ``research_job.failed``."""

    event_type: ResearchJobFailedType = "research_job.failed"
    payload: ResearchJobFailedPayload

    @model_validator(mode="after")
    def _align_payload(self) -> Self:
        if self.aggregate_id != self.payload.research_job_id:
            raise DomainEventValidationError(
                "aggregate_id must match payload.research_job_id."
            )
        if self.occurred_at != self.payload.failed_at:
            raise DomainEventValidationError(
                "occurred_at must match payload.failed_at."
            )
        return self


class ResearchJobAwaitingReviewEvent(_EventBase):
    """Envelope for ``research_job.awaiting_review``."""

    event_type: ResearchJobAwaitingReviewType = "research_job.awaiting_review"
    payload: ResearchJobAwaitingReviewPayload

    @model_validator(mode="after")
    def _align_payload(self) -> Self:
        if self.aggregate_id != self.payload.research_job_id:
            raise DomainEventValidationError(
                "aggregate_id must match payload.research_job_id."
            )
        if self.occurred_at != self.payload.entered_review_at:
            raise DomainEventValidationError(
                "occurred_at must match payload.entered_review_at."
            )
        return self


class ResearchJobRetryScheduledEvent(_EventBase):
    """Envelope for ``research_job.retry_scheduled``."""

    event_type: ResearchJobRetryScheduledType = "research_job.retry_scheduled"
    payload: ResearchJobRetryScheduledPayload

    @model_validator(mode="after")
    def _align_payload(self) -> Self:
        if self.aggregate_id != self.payload.research_job_id:
            raise DomainEventValidationError(
                "aggregate_id must match payload.research_job_id."
            )
        return self


DomainEvent = Annotated[
    ResearchJobCreatedEvent
    | ResearchJobCompletedEvent
    | ResearchJobFailedEvent
    | ResearchJobAwaitingReviewEvent
    | ResearchJobRetryScheduledEvent,
    Field(discriminator="event_type"),
]

_DOMAIN_EVENT_ADAPTER: TypeAdapter[DomainEvent] = TypeAdapter(DomainEvent)


def parse_domain_event(data: object) -> DomainEvent:
    """Parse and validate a domain event from a structured mapping."""
    try:
        return _DOMAIN_EVENT_ADAPTER.validate_python(data)
    except DomainEventValidationError:
        raise
    except Exception as exc:
        raise DomainEventValidationError(
            f"Invalid domain event ({exc.__class__.__name__})."
        ) from None
