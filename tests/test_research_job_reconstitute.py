"""Unit tests for ResearchJob.reconstitute."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from atlas.domain import (
    MAX_RESEARCH_JOB_ID_LENGTH,
    InvalidResearchJobError,
    ResearchJob,
    ResearchJobStatus,
)

T0 = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)
T1 = T0 + timedelta(minutes=1)
T2 = T0 + timedelta(minutes=2)
T3 = T0 + timedelta(minutes=3)


def test_reconstitute_pending() -> None:
    job = ResearchJob.reconstitute(
        id="job-1",
        question="question",
        status=ResearchJobStatus.PENDING,
        created_at=T0,
        updated_at=T0,
        started_at=None,
        finished_at=None,
        result=None,
        failure_reason=None,
    )
    assert job.status is ResearchJobStatus.PENDING


def test_reconstitute_completed() -> None:
    job = ResearchJob.reconstitute(
        id="job-1",
        question="question",
        status=ResearchJobStatus.COMPLETED,
        created_at=T0,
        updated_at=T2,
        started_at=T1,
        finished_at=T2,
        result="report",
        failure_reason=None,
    )
    assert job.status is ResearchJobStatus.COMPLETED
    assert job.result == "report"


def test_reconstitute_rejects_inconsistent_completed() -> None:
    with pytest.raises(InvalidResearchJobError):
        ResearchJob.reconstitute(
            id="job-1",
            question="question",
            status=ResearchJobStatus.COMPLETED,
            created_at=T0,
            updated_at=T2,
            started_at=T1,
            finished_at=T2,
            result=None,
            failure_reason=None,
        )


def test_reconstitute_running_rejects_started_before_created() -> None:
    with pytest.raises(InvalidResearchJobError, match="started_at"):
        ResearchJob.reconstitute(
            id="job-1",
            question="question",
            status=ResearchJobStatus.RUNNING,
            created_at=T1,
            updated_at=T2,
            started_at=T0,
            finished_at=None,
            result=None,
            failure_reason=None,
        )


def test_reconstitute_running_rejects_updated_before_started() -> None:
    with pytest.raises(InvalidResearchJobError, match="updated_at"):
        ResearchJob.reconstitute(
            id="job-1",
            question="question",
            status=ResearchJobStatus.RUNNING,
            created_at=T0,
            updated_at=T0,
            started_at=T1,
            finished_at=None,
            result=None,
            failure_reason=None,
        )


def test_reconstitute_completed_rejects_started_before_created() -> None:
    with pytest.raises(InvalidResearchJobError, match="started_at"):
        ResearchJob.reconstitute(
            id="job-1",
            question="question",
            status=ResearchJobStatus.COMPLETED,
            created_at=T1,
            updated_at=T3,
            started_at=T0,
            finished_at=T2,
            result="report",
            failure_reason=None,
        )


def test_reconstitute_completed_rejects_finished_before_started() -> None:
    with pytest.raises(InvalidResearchJobError, match="finished_at"):
        ResearchJob.reconstitute(
            id="job-1",
            question="question",
            status=ResearchJobStatus.COMPLETED,
            created_at=T0,
            updated_at=T3,
            started_at=T2,
            finished_at=T1,
            result="report",
            failure_reason=None,
        )


def test_reconstitute_completed_rejects_updated_before_finished() -> None:
    with pytest.raises(InvalidResearchJobError, match="updated_at"):
        ResearchJob.reconstitute(
            id="job-1",
            question="question",
            status=ResearchJobStatus.COMPLETED,
            created_at=T0,
            updated_at=T1,
            started_at=T1,
            finished_at=T2,
            result="report",
            failure_reason=None,
        )


def test_reconstitute_failed_rejects_started_before_created() -> None:
    with pytest.raises(InvalidResearchJobError, match="started_at"):
        ResearchJob.reconstitute(
            id="job-1",
            question="question",
            status=ResearchJobStatus.FAILED,
            created_at=T1,
            updated_at=T3,
            started_at=T0,
            finished_at=T2,
            result=None,
            failure_reason="boom",
        )


def test_reconstitute_failed_rejects_finished_before_started() -> None:
    with pytest.raises(InvalidResearchJobError, match="finished_at"):
        ResearchJob.reconstitute(
            id="job-1",
            question="question",
            status=ResearchJobStatus.FAILED,
            created_at=T0,
            updated_at=T3,
            started_at=T2,
            finished_at=T1,
            result=None,
            failure_reason="boom",
        )


def test_reconstitute_failed_rejects_updated_before_finished() -> None:
    with pytest.raises(InvalidResearchJobError, match="updated_at"):
        ResearchJob.reconstitute(
            id="job-1",
            question="question",
            status=ResearchJobStatus.FAILED,
            created_at=T0,
            updated_at=T1,
            started_at=T1,
            finished_at=T2,
            result=None,
            failure_reason="boom",
        )


def test_create_accepts_id_at_max_length() -> None:
    job_id = "a" * MAX_RESEARCH_JOB_ID_LENGTH
    job = ResearchJob.create(job_id, "question", at=T0)
    assert job.id == job_id


def test_create_rejects_id_over_max_length() -> None:
    with pytest.raises(InvalidResearchJobError, match="at most 128"):
        ResearchJob.create("a" * (MAX_RESEARCH_JOB_ID_LENGTH + 1), "question", at=T0)


def test_reconstitute_accepts_id_at_max_length() -> None:
    job_id = "b" * MAX_RESEARCH_JOB_ID_LENGTH
    job = ResearchJob.reconstitute(
        id=job_id,
        question="question",
        status=ResearchJobStatus.PENDING,
        created_at=T0,
        updated_at=T0,
        started_at=None,
        finished_at=None,
        result=None,
        failure_reason=None,
    )
    assert job.id == job_id


def test_reconstitute_rejects_id_over_max_length() -> None:
    with pytest.raises(InvalidResearchJobError, match="at most 128"):
        ResearchJob.reconstitute(
            id="c" * (MAX_RESEARCH_JOB_ID_LENGTH + 1),
            question="question",
            status=ResearchJobStatus.PENDING,
            created_at=T0,
            updated_at=T0,
            started_at=None,
            finished_at=None,
            result=None,
            failure_reason=None,
        )
