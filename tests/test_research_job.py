"""Unit tests for the ResearchJob domain lifecycle."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from atlas.domain import (
    InvalidResearchJobError,
    InvalidTransitionError,
    ResearchJob,
    ResearchJobStatus,
)

T0 = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)
T1 = T0 + timedelta(minutes=1)
T2 = T0 + timedelta(minutes=2)
T_BEFORE = T0 - timedelta(seconds=1)
T_NAIVE = datetime(2026, 8, 8, 12, 0, 0)
OFFSET_MINUS_FIVE = timezone(timedelta(hours=-5))
T0_OFFSET = datetime(2026, 8, 8, 7, 0, 0, tzinfo=OFFSET_MINUS_FIVE)
T1_OFFSET = datetime(2026, 8, 8, 7, 1, 0, tzinfo=OFFSET_MINUS_FIVE)
T2_OFFSET = datetime(2026, 8, 8, 7, 2, 0, tzinfo=OFFSET_MINUS_FIVE)


def test_create_sets_pending_with_deterministic_timestamps() -> None:
    job = ResearchJob.create("job-1", "  What is Atlas?  ", at=T0)

    assert job.id == "job-1"
    assert job.question == "What is Atlas?"
    assert job.status is ResearchJobStatus.PENDING
    assert job.created_at == T0
    assert job.updated_at == T0
    assert job.started_at is None
    assert job.finished_at is None
    assert job.result is None
    assert job.failure_reason is None


def test_init_also_creates_pending_job() -> None:
    job = ResearchJob("job-1", "question", at=T0)

    assert job.status is ResearchJobStatus.PENDING
    assert job.created_at == T0


def test_start_transitions_pending_to_running() -> None:
    job = ResearchJob.create("job-1", "question", at=T0)

    job.start(at=T1)

    assert job.status is ResearchJobStatus.RUNNING
    assert job.started_at == T1
    assert job.updated_at == T1
    assert job.finished_at is None


def test_complete_transitions_running_to_completed() -> None:
    job = ResearchJob.create("job-1", "question", at=T0)
    job.start(at=T1)

    job.complete("  Final report  ", at=T2)

    assert job.status is ResearchJobStatus.COMPLETED
    assert job.result == "Final report"
    assert job.failure_reason is None
    assert job.finished_at == T2
    assert job.updated_at == T2


def test_fail_transitions_running_to_failed() -> None:
    job = ResearchJob.create("job-1", "question", at=T0)
    job.start(at=T1)

    job.fail("  timed out  ", at=T2)

    assert job.status is ResearchJobStatus.FAILED
    assert job.failure_reason == "timed out"
    assert job.result is None
    assert job.finished_at == T2
    assert job.updated_at == T2


@pytest.mark.parametrize(
    ("value",),
    [
        ("",),
        ("   ",),
        ("\t\n",),
    ],
)
def test_create_rejects_empty_or_whitespace_id(value: str) -> None:
    with pytest.raises(InvalidResearchJobError, match="id"):
        ResearchJob.create(value, "question", at=T0)


@pytest.mark.parametrize(
    ("value",),
    [
        ("",),
        ("   ",),
        ("\t\n",),
    ],
)
def test_create_rejects_empty_or_whitespace_question(value: str) -> None:
    with pytest.raises(InvalidResearchJobError, match="question"):
        ResearchJob.create("job-1", value, at=T0)


def test_create_rejects_timezone_naive_timestamp() -> None:
    with pytest.raises(InvalidResearchJobError, match="timezone-aware"):
        ResearchJob.create("job-1", "question", at=T_NAIVE)


def test_start_rejects_timezone_naive_timestamp() -> None:
    job = ResearchJob.create("job-1", "question", at=T0)

    with pytest.raises(InvalidResearchJobError, match="timezone-aware"):
        job.start(at=T_NAIVE)


def test_complete_rejects_timezone_naive_timestamp() -> None:
    job = ResearchJob.create("job-1", "question", at=T0)
    job.start(at=T1)

    with pytest.raises(InvalidResearchJobError, match="timezone-aware"):
        job.complete("result", at=T_NAIVE)


def test_fail_rejects_timezone_naive_timestamp() -> None:
    job = ResearchJob.create("job-1", "question", at=T0)
    job.start(at=T1)

    with pytest.raises(InvalidResearchJobError, match="timezone-aware"):
        job.fail("reason", at=T_NAIVE)


def test_start_rejects_backward_timestamp() -> None:
    job = ResearchJob.create("job-1", "question", at=T0)

    with pytest.raises(InvalidResearchJobError, match="updated_at"):
        job.start(at=T_BEFORE)


def test_complete_rejects_backward_timestamp() -> None:
    job = ResearchJob.create("job-1", "question", at=T0)
    job.start(at=T1)

    with pytest.raises(InvalidResearchJobError, match="updated_at"):
        job.complete("result", at=T0)


def test_fail_rejects_backward_timestamp() -> None:
    job = ResearchJob.create("job-1", "question", at=T0)
    job.start(at=T1)

    with pytest.raises(InvalidResearchJobError, match="updated_at"):
        job.fail("reason", at=T0)


def test_complete_rejects_empty_or_whitespace_result() -> None:
    job = ResearchJob.create("job-1", "question", at=T0)
    job.start(at=T1)

    with pytest.raises(InvalidResearchJobError, match="result"):
        job.complete("   ", at=T2)


def test_fail_rejects_empty_or_whitespace_reason() -> None:
    job = ResearchJob.create("job-1", "question", at=T0)
    job.start(at=T1)

    with pytest.raises(InvalidResearchJobError, match="failure_reason"):
        job.fail("\n\t", at=T2)


def test_complete_from_pending_is_invalid() -> None:
    job = ResearchJob.create("job-1", "question", at=T0)

    with pytest.raises(InvalidTransitionError) as exc_info:
        job.complete("result", at=T1)

    assert exc_info.value.current == ResearchJobStatus.PENDING.value
    assert exc_info.value.attempted == ResearchJobStatus.COMPLETED.value
    assert job.status is ResearchJobStatus.PENDING


def test_fail_from_pending_is_invalid() -> None:
    job = ResearchJob.create("job-1", "question", at=T0)

    with pytest.raises(InvalidTransitionError) as exc_info:
        job.fail("reason", at=T1)

    assert exc_info.value.current == ResearchJobStatus.PENDING.value
    assert exc_info.value.attempted == ResearchJobStatus.FAILED.value


def test_start_from_running_is_invalid() -> None:
    job = ResearchJob.create("job-1", "question", at=T0)
    job.start(at=T1)

    with pytest.raises(InvalidTransitionError) as exc_info:
        job.start(at=T2)

    assert exc_info.value.current == ResearchJobStatus.RUNNING.value
    assert exc_info.value.attempted == ResearchJobStatus.RUNNING.value


@pytest.mark.parametrize(
    ("method_name", "args"),
    [
        ("start", ()),
        ("complete", ("result",)),
        ("fail", ("reason",)),
    ],
)
def test_no_transitions_after_completed(
    method_name: str,
    args: tuple[str, ...],
) -> None:
    job = ResearchJob.create("job-1", "question", at=T0)
    job.start(at=T1)
    job.complete("result", at=T2)
    method = getattr(job, method_name)

    with pytest.raises(InvalidTransitionError) as exc_info:
        method(*args, at=T2 + timedelta(minutes=1))

    assert exc_info.value.current == ResearchJobStatus.COMPLETED.value
    assert job.status is ResearchJobStatus.COMPLETED
    assert job.result == "result"


@pytest.mark.parametrize(
    ("method_name", "args"),
    [
        ("start", ()),
        ("complete", ("result",)),
        ("fail", ("reason",)),
    ],
)
def test_no_transitions_after_failed(
    method_name: str,
    args: tuple[str, ...],
) -> None:
    job = ResearchJob.create("job-1", "question", at=T0)
    job.start(at=T1)
    job.fail("reason", at=T2)
    method = getattr(job, method_name)

    with pytest.raises(InvalidTransitionError) as exc_info:
        method(*args, at=T2 + timedelta(minutes=1))

    assert exc_info.value.current == ResearchJobStatus.FAILED.value
    assert job.status is ResearchJobStatus.FAILED
    assert job.failure_reason == "reason"


def test_equal_updated_at_timestamp_is_allowed() -> None:
    job = ResearchJob.create("job-1", "question", at=T0)

    job.start(at=T0)

    assert job.started_at == T0
    assert job.updated_at == T0


@pytest.mark.parametrize(
    ("bad_value",),
    [
        (123,),
        (None,),
        (["job-1"],),
        ({"id": "job-1"},),
    ],
)
def test_create_rejects_non_string_id(bad_value: object) -> None:
    with pytest.raises(
        InvalidResearchJobError, match="id must be a string"
    ) as exc_info:
        ResearchJob.create(bad_value, "question", at=T0)  # type: ignore[arg-type]

    assert not isinstance(exc_info.value, AttributeError)


@pytest.mark.parametrize(
    ("bad_value",),
    [
        (123,),
        (None,),
        (["question"],),
        (True,),
    ],
)
def test_create_rejects_non_string_question(bad_value: object) -> None:
    with pytest.raises(
        InvalidResearchJobError,
        match="question must be a string",
    ) as exc_info:
        ResearchJob.create("job-1", bad_value, at=T0)  # type: ignore[arg-type]

    assert not isinstance(exc_info.value, AttributeError)


@pytest.mark.parametrize(
    ("bad_value",),
    [
        (123,),
        (None,),
        (["result"],),
        (b"result",),
    ],
)
def test_complete_rejects_non_string_result(bad_value: object) -> None:
    job = ResearchJob.create("job-1", "question", at=T0)
    job.start(at=T1)

    with pytest.raises(
        InvalidResearchJobError,
        match="result must be a string",
    ) as exc_info:
        job.complete(bad_value, at=T2)  # type: ignore[arg-type]

    assert not isinstance(exc_info.value, AttributeError)
    assert job.status is ResearchJobStatus.RUNNING
    assert job.result is None
    assert job.updated_at == T1


@pytest.mark.parametrize(
    ("bad_value",),
    [
        (123,),
        (None,),
        (["reason"],),
        (0,),
    ],
)
def test_fail_rejects_non_string_reason(bad_value: object) -> None:
    job = ResearchJob.create("job-1", "question", at=T0)
    job.start(at=T1)

    with pytest.raises(
        InvalidResearchJobError,
        match="failure_reason must be a string",
    ) as exc_info:
        job.fail(bad_value, at=T2)  # type: ignore[arg-type]

    assert not isinstance(exc_info.value, AttributeError)
    assert job.status is ResearchJobStatus.RUNNING
    assert job.failure_reason is None
    assert job.updated_at == T1


def test_create_normalizes_non_utc_timestamp_to_utc() -> None:
    job = ResearchJob.create("job-1", "question", at=T0_OFFSET)

    assert job.created_at == T0
    assert job.updated_at == T0
    assert job.created_at.utcoffset() == timedelta(0)
    assert job.updated_at.utcoffset() == timedelta(0)


def test_transitions_normalize_non_utc_timestamps_consistently() -> None:
    job = ResearchJob.create("job-1", "question", at=T0_OFFSET)

    job.start(at=T1_OFFSET)
    assert job.started_at == T1
    assert job.updated_at == T1
    assert job.started_at is not None
    assert job.started_at.utcoffset() == timedelta(0)

    job.complete("report", at=T2_OFFSET)
    assert job.finished_at == T2
    assert job.updated_at == T2
    assert job.finished_at is not None
    assert job.finished_at.utcoffset() == timedelta(0)


def test_fail_normalizes_non_utc_timestamp_to_utc() -> None:
    job = ResearchJob.create("job-1", "question", at=T0_OFFSET)
    job.start(at=T1_OFFSET)

    job.fail("boom", at=T2_OFFSET)

    assert job.finished_at == T2
    assert job.updated_at == T2
    assert job.finished_at is not None
    assert job.finished_at.utcoffset() == timedelta(0)
