"""Claim-fenced report persistence ownership integration tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from atlas.domain import ResearchJob
from atlas.evidence.contracts import ClaimStructured, IngestDocumentRequest, MediaType
from atlas.evidence.errors import EvidenceOwnershipError
from atlas.evidence.service import EvidenceIngestService, ReportArtifactService
from atlas.persistence.db import session_scope
from atlas.persistence.models import ResearchJobModel
from atlas.persistence.models.evidence import ReportArtifactModel
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository
from tests.integration.research_job_fixtures import bind_profile_and_start_claimed_job

CLAIM_A = "a" * 64
CLAIM_B = "b" * 64


def _create_job_and_execution(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
) -> str:
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, "Claim-fenced report question"),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="c" * 64,
        )
        return workflow_repo.create_execution(
            session,
            research_job_id=job_id,
            thread_id=job_id,
            at=at,
        )


def _claim_job(
    session_factory: sessionmaker[Session],
    job_id: str,
    claim_token: str,
) -> None:
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        model = session.get(ResearchJobModel, job_id)
        assert model is not None
        bind_profile_and_start_claimed_job(
            model,
            at=at,
            claim_token=claim_token,
            lease_expires_at=at + timedelta(hours=1),
        )


def test_stale_claim_rejected_and_no_artifact_persisted(
    session_factory: sessionmaker[Session],
) -> None:
    """Worker A loses claim to B; A's persist_final with stale token fails."""
    job_id = "report-fence-stale"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    ingest = EvidenceIngestService(session_factory=session_factory)
    reports = ReportArtifactService(session_factory=session_factory)
    doc = ingest.ingest_document(
        IngestDocumentRequest(
            corpus_key="fence-corpus",
            text="Evidence for claim-fenced report persistence.",
            media_type=MediaType.TEXT_PLAIN,
        )
    )
    evidence_id = doc.evidence_item_ids[0]
    ingest.link_evidence_to_job(
        research_job_id=job_id,
        evidence_item_ids=[evidence_id],
        workflow_execution_id=execution_id,
    )
    claims = [
        ClaimStructured(
            text="Grounded claim",
            evidence_item_ids=[evidence_id],
        )
    ]

    _claim_job(session_factory, job_id, CLAIM_A)
    _claim_job(session_factory, job_id, CLAIM_B)

    with pytest.raises(EvidenceOwnershipError):
        reports.persist_final(
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            body_text="Stale worker report body",
            claims=claims,
            claim_token=CLAIM_A,
        )

    with session_factory() as session:
        report_count = session.execute(
            select(func.count()).select_from(ReportArtifactModel)
        ).scalar_one()
    assert report_count == 0

    _claim_job(session_factory, job_id, CLAIM_B)
    result = reports.persist_final(
        research_job_id=job_id,
        workflow_execution_id=execution_id,
        body_text="Winner worker report body",
        claims=claims,
        claim_token=CLAIM_B,
    )
    assert result.created is True
