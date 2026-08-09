"""Unit tests for repository integrity-error translation."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import IntegrityError

from atlas.domain import ResearchJob
from atlas.persistence.exceptions import ResearchJobAlreadyExistsError
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository

T0 = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)


class _FakeDbApiError(Exception):
    def __init__(self, sqlstate: str) -> None:
        super().__init__(sqlstate)
        self.sqlstate = sqlstate


def test_add_translates_unique_violation() -> None:
    session = MagicMock()
    integrity_error = IntegrityError(
        "INSERT",
        {},
        _FakeDbApiError("23505"),
    )
    session.flush.side_effect = integrity_error
    repo = SqlAlchemyResearchJobRepository()
    job = ResearchJob.create("job-1", "question", at=T0)

    with pytest.raises(ResearchJobAlreadyExistsError) as exc_info:
        repo.add(session, job)

    assert exc_info.value.__cause__ is integrity_error
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
    job = ResearchJob.create("job-1", "question", at=T0)

    with pytest.raises(IntegrityError) as exc_info:
        repo.add(session, job)

    assert exc_info.value is integrity_error
    session.rollback.assert_not_called()
