"""Integration tests for SqlAlchemyResearchJobRepository."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from atlas.domain import ResearchJob
from atlas.persistence.db import session_scope
from atlas.persistence.exceptions import ResearchJobAlreadyExistsError
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository

T0 = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)
T1 = T0 + timedelta(minutes=1)
T2 = T0 + timedelta(minutes=2)


def test_add_and_get_across_separate_sessions(
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyResearchJobRepository()
    job = ResearchJob.create("job-1", "What is Atlas?", at=T0)

    with session_scope(session_factory) as session:
        repo.add(session, job)

    with session_scope(session_factory) as session:
        loaded = repo.get(session, "job-1")

    assert loaded is not None
    assert loaded.id == "job-1"
    assert loaded.question == "What is Atlas?"
    assert loaded.status.value == "PENDING"
    assert loaded.created_at == T0
    assert loaded.updated_at == T0
    assert loaded.created_at.utcoffset() == timedelta(0)


def test_save_persists_lifecycle_updates(
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyResearchJobRepository()
    job = ResearchJob.create("job-2", "question", at=T0)
    job.start(at=T1)
    job.complete("report", at=T2)

    with session_scope(session_factory) as session:
        pending = ResearchJob.create("job-2", "question", at=T0)
        repo.add(session, pending)

    with session_scope(session_factory) as session:
        repo.save(session, job)

    with session_scope(session_factory) as session:
        loaded = repo.get(session, "job-2")

    assert loaded is not None
    assert loaded.status.value == "COMPLETED"
    assert loaded.result == "report"
    assert loaded.started_at == T1
    assert loaded.finished_at == T2
    assert loaded.finished_at is not None
    assert loaded.finished_at.utcoffset() == timedelta(0)


def test_duplicate_id_preserves_integrity_error_cause(
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyResearchJobRepository()
    job = ResearchJob.create("job-dup", "question", at=T0)

    with session_scope(session_factory) as session:
        repo.add(session, job)

    with pytest.raises(ResearchJobAlreadyExistsError) as exc_info:
        with session_scope(session_factory) as session:
            repo.add(session, ResearchJob.create("job-dup", "other", at=T0))

    assert isinstance(exc_info.value.__cause__, IntegrityError)

    with session_scope(session_factory) as session:
        loaded = repo.get(session, "job-dup")
    assert loaded is not None
    assert loaded.question == "question"


def test_failed_transaction_does_not_persist_partial_row(
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyResearchJobRepository()

    with pytest.raises(RuntimeError, match="boom"):
        with session_scope(session_factory) as session:
            repo.add(session, ResearchJob.create("job-rollback", "question", at=T0))
            raise RuntimeError("boom")

    with session_scope(session_factory) as session:
        assert repo.get(session, "job-rollback") is None
