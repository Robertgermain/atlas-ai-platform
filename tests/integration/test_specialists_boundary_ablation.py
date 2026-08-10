"""Database-backed specialist boundary/ablation evidence (Slice 11B)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from atlas.api.deps import provide_session_factory
from atlas.domain import ResearchJob
from atlas.evidence.contracts import ClaimStructured, IngestDocumentRequest, MediaType
from atlas.evidence.errors import CitationIntegrityError
from atlas.evidence.service import (
    CitationValidator,
    EvidenceIngestService,
    ReportArtifactService,
)
from atlas.main import app
from atlas.models.fakes import DeterministicResearchDrafter
from atlas.persistence.db import reset_engine_cache, session_scope
from atlas.persistence.models import ResearchJobModel
from atlas.persistence.models.evidence import (
    CitationModel,
    ClaimModel,
    EvidenceJobLinkModel,
    ReportArtifactModel,
)
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository
from atlas.specialists.citation_verifier import DurableCitationVerifier
from atlas.specialists.contracts import CitationVerifierInput, SynthesizerInput
from atlas.specialists.errors import SpecialistCitationError
from atlas.specialists.synthesizer import BoundedReportSynthesizer

CLAIM = "a" * 64


def _claim_job(
    session_factory: sessionmaker[Session],
    job_id: str,
    claim_token: str = CLAIM,
) -> None:
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        model = session.get(ResearchJobModel, job_id)
        assert model is not None
        if model.status == "PENDING":
            from atlas.persistence.mappers.research_job import (
                apply_domain_to_orm,
                to_domain,
            )

            job = to_domain(model)
            job.start(at=at)
            apply_domain_to_orm(job, model)
        model.claim_token = claim_token
        model.lease_expires_at = at + timedelta(hours=1)
        session.flush()


def test_ablation_verifier_catches_unlinked_citation(
    session_factory: sessionmaker[Session],
) -> None:
    """Citation verifier catches job-unlinked citations before complete."""
    ingest = EvidenceIngestService(session_factory=session_factory)
    verifier = DurableCitationVerifier(
        citation_validator=CitationValidator(session_factory=session_factory),
        evidence_ingest=ingest,
    )
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    job_id = "ablation-unlinked"
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, "Ablation unlinked"),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="a" * 64,
        )
        workflow_repo.create_execution(
            session,
            research_job_id=job_id,
            thread_id=job_id,
            at=at,
        )
    doc = ingest.ingest_document(
        IngestDocumentRequest(
            corpus_key="ablation-unlinked-doc",
            text="Evidence exists but is not linked to the research job.",
            media_type=MediaType.TEXT_PLAIN,
        )
    )
    with pytest.raises(SpecialistCitationError):
        verifier.run(
            CitationVerifierInput(
                research_job_id=job_id,
                claims=[
                    ClaimStructured(
                        text="Unlinked",
                        evidence_item_ids=[doc.evidence_item_ids[0]],
                    )
                ],
            )
        )


def test_ablation_persist_rejects_when_verifier_bypassed(
    session_factory: sessionmaker[Session],
) -> None:
    """Defense-in-depth: persist_final still rejects invalid citations."""
    job_a = "ablation-bypass-a"
    job_b = "ablation-bypass-b"
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        for job_id in (job_a, job_b):
            job_repo.add(
                session,
                ResearchJob.create(job_id, "Bypass ablation"),
                idempotency_key=f"key-{job_id}",
                request_fingerprint="b" * 64,
            )
        execution_a = workflow_repo.create_execution(
            session, research_job_id=job_a, thread_id=job_a, at=at
        )
        execution_b = workflow_repo.create_execution(
            session, research_job_id=job_b, thread_id=job_b, at=at
        )
    ingest = EvidenceIngestService(session_factory=session_factory)
    reports = ReportArtifactService(session_factory=session_factory)
    doc = ingest.ingest_document(
        IngestDocumentRequest(
            corpus_key="ablation-bypass-doc",
            text="Linked only to job A.",
            media_type=MediaType.TEXT_PLAIN,
        )
    )
    evidence_id = doc.evidence_item_ids[0]
    ingest.link_evidence_to_job(
        research_job_id=job_a,
        evidence_item_ids=[evidence_id],
        workflow_execution_id=execution_a,
    )
    _claim_job(session_factory, job_b)
    with pytest.raises(CitationIntegrityError):
        reports.persist_final(
            research_job_id=job_b,
            workflow_execution_id=execution_b,
            body_text="Bypassed verifier report",
            claims=[
                ClaimStructured(
                    text="Should fail",
                    evidence_item_ids=[evidence_id],
                )
            ],
            claim_token=CLAIM,
        )


def test_ablation_empty_pack_yields_no_claims_or_citations(
    session_factory: sessionmaker[Session],
) -> None:
    """Empty evidence pack → no claims/citations through synth, verify, persist."""
    job_id = "ablation-empty-pack"
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, "Empty pack"),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="c" * 64,
        )
        execution_id = workflow_repo.create_execution(
            session, research_job_id=job_id, thread_id=job_id, at=at
        )
    ingest = EvidenceIngestService(session_factory=session_factory)
    synthesizer = BoundedReportSynthesizer(
        drafter=DeterministicResearchDrafter(),
        evidence_ingest=ingest,
    )
    synth = synthesizer.run(
        SynthesizerInput(
            job_id=job_id,
            question="Empty pack question",
            plan=["a", "b", "c"],
            findings=["finding without durable evidence ids"],
            evidence_item_ids=[],
            prompt_version="draft.v2",
        )
    )
    assert synth.evidence_pack == []
    assert synth.claims == []

    verifier = DurableCitationVerifier(
        citation_validator=CitationValidator(session_factory=session_factory),
        evidence_ingest=ingest,
    )
    verified = verifier.run(
        CitationVerifierInput(research_job_id=job_id, claims=list(synth.claims))
    )
    assert verified.claims == []

    reports = ReportArtifactService(session_factory=session_factory)
    _claim_job(session_factory, job_id)
    reports.persist_final(
        research_job_id=job_id,
        workflow_execution_id=execution_id,
        body_text="Report with no citations",
        claims=[],
        claim_token=CLAIM,
    )
    with session_factory() as session:
        claim_count = session.execute(
            select(func.count()).select_from(ClaimModel)
        ).scalar_one()
        cite_count = session.execute(
            select(func.count()).select_from(CitationModel)
        ).scalar_one()
        report_count = session.execute(
            select(func.count()).select_from(ReportArtifactModel)
        ).scalar_one()
    assert report_count == 1
    assert claim_count == 0
    assert cite_count == 0


def test_ablation_valid_linked_evidence_persists_and_citations_api(
    session_factory: sessionmaker[Session],
) -> None:
    """Valid linked evidence passes synth → verify → persist → citations API."""
    job_id = "ablation-valid-linked"
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, "Valid linked ablation"),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="d" * 64,
        )
        execution_id = workflow_repo.create_execution(
            session, research_job_id=job_id, thread_id=job_id, at=at
        )
    ingest = EvidenceIngestService(session_factory=session_factory)
    doc = ingest.ingest_document(
        IngestDocumentRequest(
            corpus_key="ablation-valid-doc",
            text="Operator corpus evidence for a grounded claim.",
            media_type=MediaType.TEXT_PLAIN,
        )
    )
    evidence_id = doc.evidence_item_ids[0]
    ingest.link_evidence_to_job(
        research_job_id=job_id,
        evidence_item_ids=[evidence_id],
        workflow_execution_id=execution_id,
    )
    synthesizer = BoundedReportSynthesizer(
        drafter=DeterministicResearchDrafter(),
        evidence_ingest=ingest,
    )
    synth = synthesizer.run(
        SynthesizerInput(
            job_id=job_id,
            question="Valid linked",
            plan=["a", "b", "c"],
            findings=["grounded finding"],
            evidence_item_ids=[evidence_id],
            prompt_version="draft.v2",
        )
    )
    assert synth.claims
    assert all(evidence_id in claim.evidence_item_ids for claim in synth.claims)
    verifier = DurableCitationVerifier(
        citation_validator=CitationValidator(session_factory=session_factory),
        evidence_ingest=ingest,
    )
    verified = verifier.run(
        CitationVerifierInput(research_job_id=job_id, claims=list(synth.claims))
    )
    reports = ReportArtifactService(session_factory=session_factory)
    _claim_job(session_factory, job_id)
    reports.persist_final(
        research_job_id=job_id,
        workflow_execution_id=execution_id,
        body_text="Cited report",
        claims=list(verified.claims),
        claim_token=CLAIM,
    )

    reset_engine_cache()
    app.dependency_overrides[provide_session_factory] = lambda: session_factory
    try:
        client = TestClient(app)
        response = client.get(f"/v1/research-jobs/{job_id}/citations")
        assert response.status_code == 200
        body = response.json()
        assert body["research_job_id"] == job_id
        assert body["report_artifact_id"] is not None
        assert len(body["citations"]) >= 1
        cite = body["citations"][0]
        assert cite["evidence_item_id"] == evidence_id
        assert cite["document_id"]
        assert cite["source_id"]
        assert cite["source_canonical_uri"]
    finally:
        app.dependency_overrides.clear()
        reset_engine_cache()

    with session_factory() as session:
        links = session.scalars(
            select(EvidenceJobLinkModel).where(
                EvidenceJobLinkModel.research_job_id == job_id
            )
        ).all()
    assert {link.evidence_item_id for link in links} == {evidence_id}
