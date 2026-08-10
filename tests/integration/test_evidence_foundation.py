"""Integration tests for evidence ingest, citations, and report idempotency."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from atlas.domain import ResearchJob
from atlas.evidence.contracts import ClaimStructured, IngestDocumentRequest, MediaType
from atlas.evidence.errors import (
    CitationIntegrityError,
    ReportArtifactConflictError,
)
from atlas.evidence.hash import document_content_sha256, evidence_item_content_sha256
from atlas.evidence.service import (
    EvidenceIngestService,
    ReportArtifactService,
)
from atlas.main import app
from atlas.persistence.db import session_scope
from atlas.persistence.models import ResearchJobModel
from atlas.persistence.models.evidence import (
    CitationModel,
    ClaimModel,
    ReportArtifactModel,
)
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository

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


@pytest.fixture
def session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)


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
            ResearchJob.create(job_id, "What is evidence provenance?"),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="a" * 64,
        )
        return workflow_repo.create_execution(
            session,
            research_job_id=job_id,
            thread_id=job_id,
            at=at,
        )


def test_corpus_ingest_idempotent_and_api(
    session_factory: sessionmaker[Session],
    test_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ATLAS_DATABASE_URL", test_database_url)
    from atlas.persistence.db import reset_engine_cache

    reset_engine_cache()
    service = EvidenceIngestService(session_factory=session_factory)
    request = IngestDocumentRequest(
        corpus_key="handbook",
        title="Handbook",
        media_type=MediaType.TEXT_MARKDOWN,
        text="# Title\n\nHello   world",
    )
    first = service.ingest_document(request)
    second = service.ingest_document(request)
    assert first.created is True
    assert second.created is False
    assert first.document_id == second.document_id
    assert first.evidence_item_ids == second.evidence_item_ids

    raw_bytes = b"# Title\n\nHello   world"
    assert first.content_sha256 == document_content_sha256(raw_bytes)

    client = TestClient(app)
    response = client.post(
        "/v1/evidence/documents",
        json={
            "corpus_key": "handbook",
            "media_type": "text/markdown",
            "text": "# Title\n\nHello   world",
        },
    )
    assert response.status_code == 200
    assert response.json()["created"] is False
    item = client.get(f"/v1/evidence/items/{first.evidence_item_ids[0]}")
    assert item.status_code == 200
    body = item.json()
    assert body["document_content_sha256"] == first.content_sha256
    assert body["content_sha256"] == evidence_item_content_sha256(body["text"])


def test_citation_integrity_application_layer(
    session_factory: sessionmaker[Session],
) -> None:
    job_a = "job-cite-a"
    job_b = "job-cite-b"
    execution_a = _create_job_and_execution(session_factory, job_id=job_a)
    execution_b = _create_job_and_execution(session_factory, job_id=job_b)
    ingest = EvidenceIngestService(session_factory=session_factory)
    reports = ReportArtifactService(session_factory=session_factory)
    corpus = ingest.ingest_document(
        IngestDocumentRequest(
            corpus_key="shared-cite",
            text="Shared corpus evidence for Atlas.",
            media_type=MediaType.TEXT_PLAIN,
        )
    )
    evidence_id = corpus.evidence_item_ids[0]
    ingest.link_evidence_to_job(
        research_job_id=job_a,
        evidence_item_ids=[evidence_id],
        workflow_execution_id=execution_a,
    )
    claim = ClaimStructured(
        text="A grounded claim",
        evidence_item_ids=[evidence_id],
    )
    _claim_job(session_factory, job_b)
    with pytest.raises(CitationIntegrityError):
        reports.persist_final(
            research_job_id=job_b,
            workflow_execution_id=execution_b,
            body_text="Report body",
            claims=[claim],
            claim_token=CLAIM,
        )


def test_citation_integrity_database_fk(
    session_factory: sessionmaker[Session],
) -> None:
    job_a = "job-fk-a"
    job_b = "job-fk-b"
    execution_a = _create_job_and_execution(session_factory, job_id=job_a)
    execution_b = _create_job_and_execution(session_factory, job_id=job_b)
    ingest = EvidenceIngestService(session_factory=session_factory)
    corpus = ingest.ingest_document(
        IngestDocumentRequest(
            corpus_key="fk-corpus",
            text="FK corpus evidence.",
            media_type=MediaType.TEXT_PLAIN,
        )
    )
    evidence_id = corpus.evidence_item_ids[0]
    ingest.link_evidence_to_job(
        research_job_id=job_a,
        evidence_item_ids=[evidence_id],
        workflow_execution_id=execution_a,
    )

    at = datetime.now(UTC)
    with pytest.raises(IntegrityError):
        with session_scope(session_factory) as session:
            artifact_id = str(uuid.uuid4())
            session.add(
                ReportArtifactModel(
                    id=artifact_id,
                    research_job_id=job_b,
                    workflow_execution_id=execution_b,
                    artifact_kind="final",
                    body_text="x",
                    content_sha256="b" * 64,
                    created_at=at,
                )
            )
            claim_id = str(uuid.uuid4())
            session.add(
                ClaimModel(
                    id=claim_id,
                    report_artifact_id=artifact_id,
                    research_job_id=job_b,
                    ordinal=0,
                    text="bad",
                )
            )
            session.add(
                CitationModel(
                    id=str(uuid.uuid4()),
                    claim_id=claim_id,
                    research_job_id=job_b,
                    evidence_item_id=evidence_id,
                    ordinal=0,
                )
            )
            session.flush()


def test_report_persist_idempotent_replay(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "job-report-1"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    ingest = EvidenceIngestService(session_factory=session_factory)
    reports = ReportArtifactService(session_factory=session_factory)
    doc = ingest.ingest_document(
        IngestDocumentRequest(
            corpus_key="report-corpus",
            text="Evidence used in the report.",
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
            text="Claim one",
            evidence_item_ids=[evidence_id],
        )
    ]
    body = "Final rendered report body"
    _claim_job(session_factory, job_id)
    first = reports.persist_final(
        research_job_id=job_id,
        workflow_execution_id=execution_id,
        body_text=body,
        claims=claims,
        claim_token=CLAIM,
    )
    assert first.created is True
    second = reports.persist_final(
        research_job_id=job_id,
        workflow_execution_id=execution_id,
        body_text=body,
        claims=claims,
        claim_token=CLAIM,
    )
    assert second.created is False
    assert second.id == first.id

    with pytest.raises(ReportArtifactConflictError):
        reports.persist_final(
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            body_text="Different body",
            claims=claims,
            claim_token=CLAIM,
        )

    with session_scope(session_factory) as session:
        claim_count = session.execute(text("SELECT count(*) FROM claims")).scalar_one()
        citation_count = session.execute(
            text("SELECT count(*) FROM citations")
        ).scalar_one()
    assert claim_count == 1
    assert citation_count == 1

    client_payload = reports.get_job_citations(job_id)
    assert client_payload.report_artifact_id == first.id
    assert len(client_payload.citations) == 1
    chain = client_payload.citations[0]
    assert chain.evidence_item_id == evidence_id
    assert chain.document_id
    assert chain.source_id


def test_search_hit_ingest_links_tool_and_job(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "job-search-1"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    with session_scope(session_factory) as session:
        session.execute(
            text(
                """
                INSERT INTO tool_invocations (
                    id, invocation_key, origin, research_job_id,
                    workflow_execution_id, node_name, tool_id, tool_version,
                    provider, tool_policy_version, input_fingerprint, status,
                    output_summary_json, started_at, finished_at
                ) VALUES (
                    :id, :key, 'WORKFLOW', :job, :exec, 'research',
                    'web_search', 'tools.v1', 'fake', '2026-08-09.tools.v1',
                    :fp, 'SUCCEEDED', '{"output":{},"finding_text":"x"}'::jsonb,
                    :at, :at
                )
                """
            ),
            {
                "id": "11111111-1111-4111-8111-111111111111",
                "key": "c" * 64,
                "job": job_id,
                "exec": execution_id,
                "fp": "d" * 64,
                "at": datetime.now(UTC),
            },
        )

    ingest = EvidenceIngestService(session_factory=session_factory)
    evidence_id = ingest.ingest_search_hit(
        research_job_id=job_id,
        workflow_execution_id=execution_id,
        tool_invocation_id="11111111-1111-4111-8111-111111111111",
        title="Example",
        url="HTTPS://Example.COM/page#frag",
        snippet="A snippet",
    )
    view = ingest.get_item(evidence_id)
    assert view.canonical_uri == "https://example.com/page"
    assert view.strength.value == "search_snippet"
    assert view.trust_label == "[untrusted_source]"
