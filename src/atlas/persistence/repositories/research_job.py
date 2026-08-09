"""SQLAlchemy repository for research jobs."""

from __future__ import annotations

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from atlas.domain import ResearchJob
from atlas.persistence.exceptions import (
    ResearchJobAlreadyExistsError,
    ResearchJobNotFoundError,
)
from atlas.persistence.mappers.research_job import (
    apply_domain_to_orm,
    to_domain,
    to_orm,
)
from atlas.persistence.models import ResearchJobModel

_UNIQUE_VIOLATION_SQLSTATE = "23505"


def _is_unique_violation(error: IntegrityError) -> bool:
    """Return True when the integrity failure is a PostgreSQL unique/PK violation."""
    orig = error.orig
    if orig is None:
        return False
    sqlstate = getattr(orig, "sqlstate", None)
    if sqlstate == _UNIQUE_VIOLATION_SQLSTATE:
        return True
    return getattr(orig, "pgcode", None) == _UNIQUE_VIOLATION_SQLSTATE


class SqlAlchemyResearchJobRepository:
    """Persist and load ResearchJob aggregates with an explicit session."""

    def add(self, session: Session, job: ResearchJob) -> None:
        """Insert a new research job."""
        session.add(to_orm(job))
        try:
            session.flush()
        except IntegrityError as err:
            if _is_unique_violation(err):
                raise ResearchJobAlreadyExistsError(job.id) from err
            raise

    def get(self, session: Session, job_id: str) -> ResearchJob | None:
        """Load a research job by id, or None if missing."""
        model = session.get(ResearchJobModel, job_id)
        if model is None:
            return None
        return to_domain(model)

    def save(self, session: Session, job: ResearchJob) -> None:
        """Update an existing research job."""
        model = session.get(ResearchJobModel, job.id)
        if model is None:
            raise ResearchJobNotFoundError(job.id)
        apply_domain_to_orm(job, model)
        session.flush()
