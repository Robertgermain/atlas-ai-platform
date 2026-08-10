"""FastAPI dependency providers for the research-job API."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.orm import Session, sessionmaker

from atlas.application.research_jobs import ResearchJobService
from atlas.application.review import ReviewService
from atlas.config.settings import Settings, get_settings
from atlas.coordination.composition import build_rate_limiter
from atlas.coordination.contracts import RateLimiter
from atlas.coordination.errors import RateLimitExceededError
from atlas.embeddings.composition import build_text_embedder
from atlas.evaluation.service import EvaluationService
from atlas.evidence.retrieve import EvidenceEmbeddingService
from atlas.evidence.service import EvidenceIngestService, ReportArtifactService
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


def provide_evidence_ingest_service(
    session_factory: Annotated[
        sessionmaker[Session],
        Depends(provide_session_factory),
    ],
) -> EvidenceIngestService:
    settings = get_settings()
    embedder = build_text_embedder(settings)
    embedding_service = EvidenceEmbeddingService(
        session_factory=session_factory,
        embedder=embedder,
        embedding_profile=settings.embedding_profile,
    )
    return EvidenceIngestService(
        session_factory=session_factory,
        embedding_service=embedding_service,
    )


def provide_report_artifact_service(
    session_factory: Annotated[
        sessionmaker[Session],
        Depends(provide_session_factory),
    ],
) -> ReportArtifactService:
    return ReportArtifactService(session_factory=session_factory)


def provide_evaluation_service(
    session_factory: Annotated[
        sessionmaker[Session],
        Depends(provide_session_factory),
    ],
) -> EvaluationService:
    """Wire the durable evaluation service."""
    return EvaluationService(session_factory=session_factory)


def provide_settings() -> Settings:
    """Return the current application settings."""
    return get_settings()


def provide_review_service(
    session_factory: Annotated[
        sessionmaker[Session],
        Depends(provide_session_factory),
    ],
    settings: Annotated[Settings, Depends(provide_settings)],
) -> ReviewService:
    """Wire the operator review service."""
    return ReviewService(
        session_factory=session_factory,
        database_url=settings.database_url,
    )


def provide_rate_limiter(
    settings: Annotated[Settings, Depends(provide_settings)],
) -> RateLimiter:
    """Wire the configured rate limiter (no-op unless Redis is enabled)."""
    return build_rate_limiter(settings)


def enforce_create_job_rate_limit(
    request: Request,
    rate_limiter: Annotated[RateLimiter, Depends(provide_rate_limiter)],
) -> None:
    """Rate-limit ``POST /v1/research-jobs`` by direct peer IP.

    Idempotent replays count toward the limit: this check runs before
    idempotency-key resolution.
    """
    identity = request.client.host if request.client is not None else "unknown"
    decision = rate_limiter.check(identity=identity)
    if not decision.allowed:
        raise RateLimitExceededError(decision.retry_after_seconds)
