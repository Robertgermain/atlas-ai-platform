"""Research job domain model and lifecycle transitions."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from atlas.domain.exceptions import InvalidResearchJobError, InvalidTransitionError


class ResearchJobStatus(StrEnum):
    """Lifecycle states for a research job."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


def _require_non_empty(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise InvalidResearchJobError(f"{field_name} must be a string.")
    stripped = value.strip()
    if not stripped:
        raise InvalidResearchJobError(f"{field_name} must be a non-empty string.")
    return stripped


def _resolve_timestamp(at: datetime | None) -> datetime:
    if at is None:
        return datetime.now(UTC)
    if at.tzinfo is None or at.utcoffset() is None:
        raise InvalidResearchJobError("Timestamps must be timezone-aware.")
    return at.astimezone(UTC)


def _ensure_not_before(at: datetime, previous: datetime) -> None:
    if at < previous:
        raise InvalidResearchJobError(
            "Timestamp must not be earlier than the job's updated_at."
        )


class ResearchJob:
    """Slotted research-job entity with enforced lifecycle transitions."""

    __slots__ = (
        "_id",
        "_question",
        "_status",
        "_created_at",
        "_updated_at",
        "_started_at",
        "_finished_at",
        "_result",
        "_failure_reason",
    )

    def __init__(self, id: str, question: str, *, at: datetime | None = None) -> None:
        cleaned_id = _require_non_empty(id, "id")
        cleaned_question = _require_non_empty(question, "question")
        timestamp = _resolve_timestamp(at)
        self._id = cleaned_id
        self._question = cleaned_question
        self._status = ResearchJobStatus.PENDING
        self._created_at = timestamp
        self._updated_at = timestamp
        self._started_at: datetime | None = None
        self._finished_at: datetime | None = None
        self._result: str | None = None
        self._failure_reason: str | None = None

    @classmethod
    def create(
        cls,
        id: str,
        question: str,
        *,
        at: datetime | None = None,
    ) -> ResearchJob:
        """Create a PENDING research job."""
        return cls(id, question, at=at)

    @property
    def id(self) -> str:
        return self._id

    @property
    def question(self) -> str:
        return self._question

    @property
    def status(self) -> ResearchJobStatus:
        return self._status

    @property
    def created_at(self) -> datetime:
        return self._created_at

    @property
    def updated_at(self) -> datetime:
        return self._updated_at

    @property
    def started_at(self) -> datetime | None:
        return self._started_at

    @property
    def finished_at(self) -> datetime | None:
        return self._finished_at

    @property
    def result(self) -> str | None:
        return self._result

    @property
    def failure_reason(self) -> str | None:
        return self._failure_reason

    def start(self, *, at: datetime | None = None) -> None:
        """Transition PENDING → RUNNING."""
        self._require_status(ResearchJobStatus.PENDING, ResearchJobStatus.RUNNING)
        timestamp = _resolve_timestamp(at)
        _ensure_not_before(timestamp, self._updated_at)
        self._status = ResearchJobStatus.RUNNING
        self._started_at = timestamp
        self._updated_at = timestamp

    def complete(self, result: str, *, at: datetime | None = None) -> None:
        """Transition RUNNING → COMPLETED."""
        self._require_status(ResearchJobStatus.RUNNING, ResearchJobStatus.COMPLETED)
        cleaned_result = _require_non_empty(result, "result")
        timestamp = _resolve_timestamp(at)
        _ensure_not_before(timestamp, self._updated_at)
        self._status = ResearchJobStatus.COMPLETED
        self._result = cleaned_result
        self._failure_reason = None
        self._finished_at = timestamp
        self._updated_at = timestamp

    def fail(self, reason: str, *, at: datetime | None = None) -> None:
        """Transition RUNNING → FAILED."""
        self._require_status(ResearchJobStatus.RUNNING, ResearchJobStatus.FAILED)
        cleaned_reason = _require_non_empty(reason, "failure_reason")
        timestamp = _resolve_timestamp(at)
        _ensure_not_before(timestamp, self._updated_at)
        self._status = ResearchJobStatus.FAILED
        self._failure_reason = cleaned_reason
        self._result = None
        self._finished_at = timestamp
        self._updated_at = timestamp

    def _require_status(
        self,
        expected: ResearchJobStatus,
        attempted: ResearchJobStatus,
    ) -> None:
        if self._status != expected:
            raise InvalidTransitionError(
                current=self._status.value,
                attempted=attempted.value,
            )
