"""Research job domain model and lifecycle transitions."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from atlas.domain.exceptions import InvalidResearchJobError, InvalidTransitionError

MAX_RESEARCH_JOB_ID_LENGTH = 128


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


def _require_job_id(value: object) -> str:
    cleaned = _require_non_empty(value, "id")
    if len(cleaned) > MAX_RESEARCH_JOB_ID_LENGTH:
        raise InvalidResearchJobError(
            f"id must be at most {MAX_RESEARCH_JOB_ID_LENGTH} characters."
        )
    return cleaned


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


def _ensure_order(
    earlier: datetime,
    later: datetime,
    *,
    earlier_name: str,
    later_name: str,
) -> None:
    if later < earlier:
        raise InvalidResearchJobError(
            f"{later_name} must not be earlier than {earlier_name}."
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
        cleaned_id = _require_job_id(id)
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

    @classmethod
    def reconstitute(
        cls,
        *,
        id: str,
        question: str,
        status: ResearchJobStatus,
        created_at: datetime,
        updated_at: datetime,
        started_at: datetime | None,
        finished_at: datetime | None,
        result: str | None,
        failure_reason: str | None,
    ) -> ResearchJob:
        """Rebuild a research job from durable state without applying transitions."""
        if not isinstance(status, ResearchJobStatus):
            raise InvalidResearchJobError("status must be a ResearchJobStatus.")

        cleaned_id = _require_job_id(id)
        cleaned_question = _require_non_empty(question, "question")
        created = _resolve_timestamp(created_at)
        updated = _resolve_timestamp(updated_at)
        started = None if started_at is None else _resolve_timestamp(started_at)
        finished = None if finished_at is None else _resolve_timestamp(finished_at)
        cleaned_result = (
            None if result is None else _require_non_empty(result, "result")
        )
        cleaned_failure = (
            None
            if failure_reason is None
            else _require_non_empty(failure_reason, "failure_reason")
        )

        _ensure_order(
            created,
            updated,
            earlier_name="created_at",
            later_name="updated_at",
        )

        if status is ResearchJobStatus.PENDING:
            if started is not None or finished is not None:
                raise InvalidResearchJobError(
                    "PENDING jobs cannot have started_at or finished_at."
                )
            if cleaned_result is not None or cleaned_failure is not None:
                raise InvalidResearchJobError(
                    "PENDING jobs cannot have result or failure_reason."
                )
        elif status is ResearchJobStatus.RUNNING:
            if started is None or finished is not None:
                raise InvalidResearchJobError(
                    "RUNNING jobs require started_at and no finished_at."
                )
            if cleaned_result is not None or cleaned_failure is not None:
                raise InvalidResearchJobError(
                    "RUNNING jobs cannot have result or failure_reason."
                )
            _ensure_order(
                created,
                started,
                earlier_name="created_at",
                later_name="started_at",
            )
            _ensure_order(
                started,
                updated,
                earlier_name="started_at",
                later_name="updated_at",
            )
        elif status is ResearchJobStatus.COMPLETED:
            if started is None or finished is None or cleaned_result is None:
                raise InvalidResearchJobError(
                    "COMPLETED jobs require started_at, finished_at, and result."
                )
            if cleaned_failure is not None:
                raise InvalidResearchJobError(
                    "COMPLETED jobs cannot have failure_reason."
                )
            _ensure_order(
                created,
                started,
                earlier_name="created_at",
                later_name="started_at",
            )
            _ensure_order(
                started,
                finished,
                earlier_name="started_at",
                later_name="finished_at",
            )
            _ensure_order(
                finished,
                updated,
                earlier_name="finished_at",
                later_name="updated_at",
            )
        elif status is ResearchJobStatus.FAILED:
            if started is None or finished is None or cleaned_failure is None:
                raise InvalidResearchJobError(
                    "FAILED jobs require started_at, finished_at, and failure_reason."
                )
            if cleaned_result is not None:
                raise InvalidResearchJobError("FAILED jobs cannot have result.")
            _ensure_order(
                created,
                started,
                earlier_name="created_at",
                later_name="started_at",
            )
            _ensure_order(
                started,
                finished,
                earlier_name="started_at",
                later_name="finished_at",
            )
            _ensure_order(
                finished,
                updated,
                earlier_name="finished_at",
                later_name="updated_at",
            )
        else:
            raise InvalidResearchJobError(f"Unsupported status: {status}.")

        job = object.__new__(cls)
        job._id = cleaned_id
        job._question = cleaned_question
        job._status = status
        job._created_at = created
        job._updated_at = updated
        job._started_at = started
        job._finished_at = finished
        job._result = cleaned_result
        job._failure_reason = cleaned_failure
        return job

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
