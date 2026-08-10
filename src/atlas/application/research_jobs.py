"""Research-job application service."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable

from sqlalchemy.orm import Session, sessionmaker

from atlas.application.exceptions import (
    IdempotencyConflictError,
    ResearchJobLookupError,
)
from atlas.application.ports import ResearchJobRepository
from atlas.domain import ResearchJob
from atlas.eventing.builders import build_research_job_created
from atlas.outbox.ports import OutboxEnqueuer
from atlas.persistence.db import session_scope
from atlas.persistence.exceptions import IdempotencyKeyConflictError
from atlas.persistence.repositories.outbox import SqlAlchemyOutboxRepository


def _fingerprint_create_request(question: str) -> str:
    """Hash the deterministic canonical representation of a create request."""
    canonical = json.dumps(
        {"question": question},
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class ResearchJobService:
    """Create and retrieve research jobs with durable idempotent submission."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        repository: ResearchJobRepository,
        outbox: OutboxEnqueuer | None = None,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository
        self._outbox = outbox or SqlAlchemyOutboxRepository()
        self._id_factory = id_factory or (lambda: str(uuid.uuid4()))

    def submit(self, question: str, *, idempotency_key: str) -> ResearchJob:
        """Persist a PENDING research job or replay a matching idempotent request.

        Enqueues ``research_job.created`` in the same transaction as the insert.
        Idempotent replay does not enqueue another created event. Outbox failure
        rolls back the job insert.
        """
        job_id = self._id_factory()
        job = ResearchJob.create(job_id, question)
        fingerprint = _fingerprint_create_request(job.question)

        try:
            with session_scope(self._session_factory) as session:
                self._repository.add(
                    session,
                    job,
                    idempotency_key=idempotency_key,
                    request_fingerprint=fingerprint,
                )
                self._outbox.enqueue(
                    session,
                    build_research_job_created(
                        research_job_id=job.id,
                        created_at=job.created_at,
                    ),
                )
        except IdempotencyKeyConflictError:
            return self._replay_or_conflict(
                idempotency_key=idempotency_key,
                request_fingerprint=fingerprint,
            )

        return job

    def get(self, job_id: str) -> ResearchJob:
        """Return an existing research job or raise when missing."""
        with session_scope(self._session_factory) as session:
            job = self._repository.get(session, job_id)
        if job is None:
            raise ResearchJobLookupError(job_id)
        return job

    def _replay_or_conflict(
        self,
        *,
        idempotency_key: str,
        request_fingerprint: str,
    ) -> ResearchJob:
        with session_scope(self._session_factory) as session:
            record = self._repository.get_by_idempotency_key(
                session,
                idempotency_key,
            )
        if record is None:
            raise IdempotencyConflictError()
        if record.request_fingerprint != request_fingerprint:
            raise IdempotencyConflictError()
        return record.job
