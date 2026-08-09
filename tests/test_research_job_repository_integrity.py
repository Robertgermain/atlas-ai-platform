"""Unit tests for repository integrity-error translation."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import IntegrityError

from atlas.domain import ResearchJob
from atlas.persistence.exceptions import (
    IdempotencyKeyConflictError,
    ResearchJobAlreadyExistsError,
)
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository

T0 = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)


class _FakeDiag:
    def __init__(self, constraint_name: str | None) -> None:
        self.constraint_name = constraint_name


class _FakeDbApiError(Exception):
    def __init__(self, sqlstate: str, constraint_name: str | None = None) -> None:
        super().__init__(sqlstate)
        self.sqlstate = sqlstate
        self.diag = _FakeDiag(constraint_name)


def _add(repo: SqlAlchemyResearchJobRepository, session: MagicMock) -> None:
    job = ResearchJob.create("job-1", "question", at=T0)
    repo.add(
        session,
        job,
        idempotency_key="key-1",
        request_fingerprint="a" * 64,
    )


def test_add_translates_primary_key_unique_violation() -> None:
    session = MagicMock()
    integrity_error = IntegrityError(
        "INSERT",
        {},
        _FakeDbApiError("23505", "research_jobs_pkey"),
    )
    session.flush.side_effect = integrity_error
    repo = SqlAlchemyResearchJobRepository()

    with pytest.raises(ResearchJobAlreadyExistsError) as exc_info:
        _add(repo, session)

    assert exc_info.value.__cause__ is integrity_error
    session.rollback.assert_not_called()


def test_add_translates_idempotency_unique_violation() -> None:
    session = MagicMock()
    integrity_error = IntegrityError(
        "INSERT",
        {},
        _FakeDbApiError("23505", "uq_research_jobs_idempotency_key"),
    )
    session.flush.side_effect = integrity_error
    repo = SqlAlchemyResearchJobRepository()

    with pytest.raises(IdempotencyKeyConflictError) as exc_info:
        _add(repo, session)

    assert exc_info.value.__cause__ is integrity_error
    session.rollback.assert_not_called()


def test_add_rethrows_unrecognized_unique_violation() -> None:
    session = MagicMock()
    integrity_error = IntegrityError(
        "INSERT",
        {},
        _FakeDbApiError("23505", "uq_research_jobs_unexpected"),
    )
    session.flush.side_effect = integrity_error
    repo = SqlAlchemyResearchJobRepository()

    with pytest.raises(IntegrityError) as exc_info:
        _add(repo, session)

    assert exc_info.value is integrity_error
    session.rollback.assert_not_called()


def test_add_rethrows_unique_violation_without_constraint_name() -> None:
    session = MagicMock()
    integrity_error = IntegrityError(
        "INSERT",
        {},
        _FakeDbApiError("23505", None),
    )
    session.flush.side_effect = integrity_error
    repo = SqlAlchemyResearchJobRepository()

    with pytest.raises(IntegrityError) as exc_info:
        _add(repo, session)

    assert exc_info.value is integrity_error
    session.rollback.assert_not_called()


def test_add_rethrows_unrelated_integrity_error() -> None:
    session = MagicMock()
    integrity_error = IntegrityError(
        "INSERT",
        {},
        _FakeDbApiError("23514"),
    )
    session.flush.side_effect = integrity_error
    repo = SqlAlchemyResearchJobRepository()

    with pytest.raises(IntegrityError) as exc_info:
        _add(repo, session)

    assert exc_info.value is integrity_error
    session.rollback.assert_not_called()
