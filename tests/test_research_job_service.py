"""Unit tests for ResearchJobService."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy.orm import Session

from atlas.application.exceptions import (
    IdempotencyConflictError,
    ResearchJobLookupError,
)
from atlas.application.ports import ResearchJobIdempotencyRecord
from atlas.application.research_jobs import ResearchJobService
from atlas.domain import ResearchJob
from atlas.persistence.exceptions import IdempotencyKeyConflictError

T0 = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)


class _FakeSession:
    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


class _FakeSessionFactory:
    def __call__(self) -> _FakeSession:
        return _FakeSession()


class _FakeRepository:
    def __init__(self) -> None:
        self.jobs_by_id: dict[str, ResearchJob] = {}
        self.records_by_key: dict[str, ResearchJobIdempotencyRecord] = {}
        self.add_calls = 0
        self.fail_idempotency_on_add = False

    def add(
        self,
        session: Session,
        job: ResearchJob,
        *,
        idempotency_key: str,
        request_fingerprint: str,
    ) -> None:
        del session
        self.add_calls += 1
        if self.fail_idempotency_on_add or idempotency_key in self.records_by_key:
            raise IdempotencyKeyConflictError()
        self.jobs_by_id[job.id] = job
        self.records_by_key[idempotency_key] = ResearchJobIdempotencyRecord(
            job=job,
            request_fingerprint=request_fingerprint,
        )

    def get(self, session: Session, job_id: str) -> ResearchJob | None:
        del session
        return self.jobs_by_id.get(job_id)

    def get_by_idempotency_key(
        self,
        session: Session,
        idempotency_key: str,
    ) -> ResearchJobIdempotencyRecord | None:
        del session
        return self.records_by_key.get(idempotency_key)

    def save(self, session: Session, job: ResearchJob) -> None:
        del session, job
        raise NotImplementedError


def test_submit_creates_pending_job() -> None:
    repo = _FakeRepository()
    service = ResearchJobService(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        id_factory=lambda: "job-fixed",
    )

    job = service.submit("What is Atlas?", idempotency_key="key-1")

    assert job.id == "job-fixed"
    assert job.question == "What is Atlas?"
    assert job.status.value == "PENDING"
    assert repo.add_calls == 1


def test_submit_replays_matching_idempotency_key() -> None:
    repo = _FakeRepository()
    service = ResearchJobService(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        id_factory=lambda: "job-1",
    )
    original = service.submit("same question", idempotency_key="key-1")
    service_again = ResearchJobService(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        id_factory=lambda: "job-2",
    )

    replayed = service_again.submit("same question", idempotency_key="key-1")

    assert replayed.id == original.id
    assert repo.add_calls == 2


def test_submit_conflicts_when_payload_differs() -> None:
    repo = _FakeRepository()
    service = ResearchJobService(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        id_factory=lambda: "job-1",
    )
    service.submit("question a", idempotency_key="key-1")

    with pytest.raises(IdempotencyConflictError):
        service.submit("question b", idempotency_key="key-1")


def test_get_returns_job() -> None:
    repo = _FakeRepository()
    job = ResearchJob.create("job-1", "question", at=T0)
    repo.jobs_by_id[job.id] = job
    service = ResearchJobService(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
    )

    loaded = service.get("job-1")
    assert loaded.id == "job-1"


def test_get_missing_raises() -> None:
    service = ResearchJobService(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=_FakeRepository(),
    )

    with pytest.raises(ResearchJobLookupError) as exc_info:
        service.get("missing")

    assert exc_info.value.job_id == "missing"
