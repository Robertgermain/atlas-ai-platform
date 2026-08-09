"""FastAPI dependency providers for the research-job API."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends
from sqlalchemy.orm import Session, sessionmaker

from atlas.application.research_jobs import ResearchJobService
from atlas.persistence.db import get_session_factory
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository


def provide_session_factory() -> sessionmaker[Session]:
    """Return the lazy SQLAlchemy session factory."""
    return get_session_factory()


def provide_research_job_repository() -> SqlAlchemyResearchJobRepository:
    """Return the concrete research-job repository."""
    return SqlAlchemyResearchJobRepository()


def provide_research_job_service(
    session_factory: Annotated[
        sessionmaker[Session],
        Depends(provide_session_factory),
    ],
    repository: Annotated[
        SqlAlchemyResearchJobRepository,
        Depends(provide_research_job_repository),
    ],
) -> ResearchJobService:
    """Wire the research-job application service."""
    return ResearchJobService(
        session_factory=session_factory,
        repository=repository,
    )
