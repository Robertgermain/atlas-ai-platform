"""Integration tests for pgvector embeddings, retrieval, and HNSW."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, func, inspect, select, text
from sqlalchemy.orm import Session, sessionmaker

from atlas.api.deps import provide_evidence_ingest_service
from atlas.domain import ResearchJob
from atlas.embeddings.bounds import EMBEDDING_PROFILE_V1
from atlas.embeddings.contracts import (
    EmbedTextsRequest,
    EmbedTextsResult,
)
from atlas.embeddings.errors import EmbeddingProviderError
from atlas.embeddings.fakes import DeterministicFakeEmbedder
from atlas.evidence.contracts import (
    EvidenceStrength,
    IngestDocumentRequest,
    IngestDocumentResult,
    MediaType,
    SourceKind,
)
from atlas.evidence.retrieve import EvidenceEmbeddingService, EvidenceRetriever
from atlas.evidence.service import EvidenceIngestService
from atlas.main import app
from atlas.persistence.db import session_scope
from atlas.persistence.models.embedding import EvidenceEmbeddingModel
from atlas.persistence.models.evidence import DocumentModel, EvidenceItemModel
from atlas.persistence.repositories.embedding import SqlAlchemyEmbeddingRepository
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository


@pytest.fixture
def session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)


class _FailingEmbedder:
    def embed_texts(self, request: EmbedTextsRequest) -> EmbedTextsResult:
        raise EmbeddingProviderError("forced embedding failure")


def _ingest_with_embedder(
    session_factory: sessionmaker[Session],
    *,
    corpus_key: str,
    text_body: str,
    embedder: DeterministicFakeEmbedder | None = None,
) -> IngestDocumentResult:
    active = embedder or DeterministicFakeEmbedder()
    embedding_service = EvidenceEmbeddingService(
        session_factory=session_factory,
        embedder=active,
    )
    ingest = EvidenceIngestService(
        session_factory=session_factory,
        embedding_service=embedding_service,
    )
    return ingest.ingest_document(
        IngestDocumentRequest(
            corpus_key=corpus_key,
            text=text_body,
            media_type=MediaType.TEXT_PLAIN,
        )
    )


def test_migration_has_pgvector_and_hnsw(engine: Engine) -> None:
    inspector = inspect(engine)
    assert inspector.has_table("evidence_embeddings")
    with engine.connect() as connection:
        version = connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one()
        ext = connection.execute(
            text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
        ).scalar_one_or_none()
        index = connection.execute(
            text(
                """
                SELECT 1 FROM pg_indexes
                WHERE indexname = 'ix_evidence_embeddings_hnsw_cosine'
                """
            )
        ).scalar_one_or_none()
        assert version == "20260809_0012"
    assert ext == 1
    assert index == 1


def test_embed_idempotent_and_backfill(session_factory: sessionmaker[Session]) -> None:
    embedder = DeterministicFakeEmbedder()
    embedding_service = EvidenceEmbeddingService(
        session_factory=session_factory,
        embedder=embedder,
    )
    ingest = EvidenceIngestService(
        session_factory=session_factory,
        embedding_service=embedding_service,
    )
    request = IngestDocumentRequest(
        corpus_key="embed-doc",
        text="Vector retrieval uses cosine distance over embeddings.",
        media_type=MediaType.TEXT_PLAIN,
    )
    first = ingest.ingest_document(request)
    second = ingest.ingest_document(request)
    assert first.created is True
    assert second.created is False
    assert first.evidence_item_ids == second.evidence_item_ids
    inserted = embedding_service.embed_evidence_items(first.evidence_item_ids)
    assert inserted == 0


def test_partial_embedding_failure_leaves_evidence(
    session_factory: sessionmaker[Session],
) -> None:
    failing = EvidenceIngestService(
        session_factory=session_factory,
        embedding_service=EvidenceEmbeddingService(
            session_factory=session_factory,
            embedder=_FailingEmbedder(),
        ),
    )
    with pytest.raises(EmbeddingProviderError):
        failing.ingest_document(
            IngestDocumentRequest(
                corpus_key="partial-fail",
                text="Evidence without embedding after provider failure.",
                media_type=MediaType.TEXT_PLAIN,
            )
        )
    working = EvidenceEmbeddingService(
        session_factory=session_factory,
        embedder=DeterministicFakeEmbedder(),
    )
    bare = EvidenceIngestService(session_factory=session_factory)
    result = bare.ingest_document(
        IngestDocumentRequest(
            corpus_key="partial-fail",
            text="Evidence without embedding after provider failure.",
            media_type=MediaType.TEXT_PLAIN,
        )
    )
    assert result.created is False
    assert working.embed_evidence_items(result.evidence_item_ids) >= 1


def test_exact_retrieval_ranks_quantum_ahead_of_baking(
    session_factory: sessionmaker[Session],
) -> None:
    embedder = DeterministicFakeEmbedder()
    quantum = _ingest_with_embedder(
        session_factory,
        corpus_key="quantum-doc",
        text_body="Quantum cryptography and photon polarization key exchange.",
        embedder=embedder,
    )
    baking = _ingest_with_embedder(
        session_factory,
        corpus_key="baking-doc",
        text_body="Sourdough bread baking fermentation and crusty loaves.",
        embedder=embedder,
    )
    retriever = EvidenceRetriever(
        session_factory=session_factory,
        embedder=embedder,
        use_hnsw=False,
    )
    exact = retriever.retrieve(
        query="quantum cryptography photon polarization",
        k=5,
        source_kinds=[SourceKind.CORPUS_TEXT],
        mode="exact",
    )
    assert exact
    assert exact[0].evidence.id == quantum.evidence_item_ids[0]
    assert exact[0].evidence.document_id == quantum.document_id
    assert "quantum" in exact[0].evidence.text.lower()
    ranked_ids = [hit.evidence.id for hit in exact]
    assert quantum.evidence_item_ids[0] in ranked_ids
    assert ranked_ids.index(quantum.evidence_item_ids[0]) < ranked_ids.index(
        baking.evidence_item_ids[0]
    )


def test_metadata_filters_include_and_exclude(
    session_factory: sessionmaker[Session],
) -> None:
    embedder = DeterministicFakeEmbedder()
    embedding_service = EvidenceEmbeddingService(
        session_factory=session_factory,
        embedder=embedder,
    )
    ingest = EvidenceIngestService(
        session_factory=session_factory,
        embedding_service=embedding_service,
    )
    corpus = ingest.ingest_document(
        IngestDocumentRequest(
            corpus_key="filter-corpus",
            text="Operator corpus notes on quantum cryptography photon keys.",
            media_type=MediaType.TEXT_PLAIN,
        )
    )
    job_id = "job-filter-meta"
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, "Filter metadata retrieval"),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="f" * 64,
        )
        execution_id = workflow_repo.create_execution(
            session,
            research_job_id=job_id,
            thread_id=job_id,
            at=at,
        )
    search_id = ingest.ingest_search_hit(
        research_job_id=job_id,
        workflow_execution_id=execution_id,
        tool_invocation_id=None,
        title="Web hit",
        url="https://example.com/quantum",
        snippet="Quantum cryptography and photon polarization from the web.",
    )
    retriever = EvidenceRetriever(
        session_factory=session_factory,
        embedder=embedder,
        use_hnsw=False,
    )
    corpus_only = retriever.retrieve(
        query="quantum cryptography photon",
        k=5,
        source_kinds=[SourceKind.CORPUS_TEXT],
        mode="exact",
    )
    corpus_ids = {hit.evidence.id for hit in corpus_only}
    assert corpus.evidence_item_ids[0] in corpus_ids
    assert search_id not in corpus_ids
    assert all(
        hit.evidence.source_kind is SourceKind.CORPUS_TEXT for hit in corpus_only
    )

    web_only = retriever.retrieve(
        query="quantum cryptography photon",
        k=5,
        source_kinds=[SourceKind.WEB_SEARCH],
        mode="exact",
    )
    web_ids = {hit.evidence.id for hit in web_only}
    assert search_id in web_ids
    assert corpus.evidence_item_ids[0] not in web_ids
    assert all(hit.evidence.source_kind is SourceKind.WEB_SEARCH for hit in web_only)

    chunks = retriever.retrieve(
        query="quantum cryptography photon",
        k=5,
        strengths=[EvidenceStrength.DOCUMENT_CHUNK],
        mode="exact",
    )
    chunk_ids = {hit.evidence.id for hit in chunks}
    assert corpus.evidence_item_ids[0] in chunk_ids
    assert search_id not in chunk_ids

    snippets = retriever.retrieve(
        query="quantum cryptography photon",
        k=5,
        strengths=[EvidenceStrength.SEARCH_SNIPPET],
        mode="exact",
    )
    snippet_ids = {hit.evidence.id for hit in snippets}
    assert search_id in snippet_ids
    assert corpus.evidence_item_ids[0] not in snippet_ids


def test_hnsw_explain_uses_index_and_filters_remain_effective(
    session_factory: sessionmaker[Session],
) -> None:
    embedder = DeterministicFakeEmbedder()
    quantum = _ingest_with_embedder(
        session_factory,
        corpus_key="hnsw-quantum",
        text_body="Quantum cryptography and photon polarization key exchange.",
        embedder=embedder,
    )
    baking = _ingest_with_embedder(
        session_factory,
        corpus_key="hnsw-baking",
        text_body="Sourdough bread baking fermentation and crusty loaves.",
        embedder=embedder,
    )
    repo = SqlAlchemyEmbeddingRepository()
    with session_scope(session_factory) as session:
        assert repo.hnsw_index_exists(session) is True
        query_vector = embedder.embed_texts(
            EmbedTextsRequest(texts=["quantum cryptography photon polarization"])
        ).embeddings[0]
        plan = repo.explain_hnsw_candidate_plan(
            session,
            query_vector=query_vector,
            embedding_profile=EMBEDDING_PROFILE_V1,
            limit=5,
        )
        assert "ix_evidence_embeddings_hnsw_cosine" in plan

    retriever = EvidenceRetriever(
        session_factory=session_factory,
        embedder=embedder,
        use_hnsw=True,
    )
    hits = retriever.retrieve(
        query="quantum cryptography photon polarization",
        k=5,
        source_kinds=[SourceKind.CORPUS_TEXT],
        mode="hnsw",
    )
    assert hits
    assert hits[0].evidence.id == quantum.evidence_item_ids[0]
    assert hits[0].evidence.document_id == quantum.document_id
    assert hits[0].evidence.source_id
    assert hits[0].evidence.source_kind is SourceKind.CORPUS_TEXT
    assert "quantum" in hits[0].evidence.text.lower()
    ranked_ids = [hit.evidence.id for hit in hits]
    assert ranked_ids.index(quantum.evidence_item_ids[0]) < ranked_ids.index(
        baking.evidence_item_ids[0]
    )

    baking_only = retriever.retrieve(
        query="sourdough bread baking fermentation",
        k=5,
        source_kinds=[SourceKind.WEB_SEARCH],
        mode="hnsw",
    )
    assert baking_only == []


def test_retrieved_corpus_can_be_linked_and_cited(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "job-retrieve-1"
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, "What is quantum cryptography?"),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="e" * 64,
        )
        execution_id = workflow_repo.create_execution(
            session,
            research_job_id=job_id,
            thread_id=job_id,
            at=at,
        )
    embedder = DeterministicFakeEmbedder()
    embedding_service = EvidenceEmbeddingService(
        session_factory=session_factory,
        embedder=embedder,
    )
    ingest = EvidenceIngestService(
        session_factory=session_factory,
        embedding_service=embedding_service,
    )
    doc = ingest.ingest_document(
        IngestDocumentRequest(
            corpus_key="cite-quantum",
            text="Quantum key distribution and entanglement for secure channels.",
            media_type=MediaType.TEXT_PLAIN,
        )
    )
    retriever = EvidenceRetriever(
        session_factory=session_factory,
        embedder=embedder,
    )
    hits = retriever.retrieve(
        query="quantum key distribution entanglement",
        k=3,
        mode="exact",
    )
    assert hits
    evidence_id = hits[0].evidence.id
    assert evidence_id in doc.evidence_item_ids
    assert hits[0].evidence.document_id == doc.document_id
    ingest.link_evidence_to_job(
        research_job_id=job_id,
        evidence_item_ids=[evidence_id],
        workflow_execution_id=execution_id,
    )


def test_api_partial_embedding_failure_structured_503_and_backfill(
    session_factory: sessionmaker[Session],
) -> None:
    failing_service = EvidenceIngestService(
        session_factory=session_factory,
        embedding_service=EvidenceEmbeddingService(
            session_factory=session_factory,
            embedder=_FailingEmbedder(),
        ),
    )
    app.dependency_overrides[provide_evidence_ingest_service] = lambda: failing_service
    client = TestClient(app)
    payload = {
        "corpus_key": "api-partial-embed",
        "media_type": "text/plain",
        "text": "Evidence remains after embedding provider failure via API.",
    }
    try:
        failed = client.post("/v1/evidence/documents", json=payload)
        assert failed.status_code == 503
        body = failed.json()
        assert body["error"]["code"] == "embedding_provider_failed"
        assert body["error"]["message"] == "Embedding provider failed."
        assert "forced embedding failure" not in failed.text
    finally:
        app.dependency_overrides.clear()

    with session_scope(session_factory) as session:
        documents = session.execute(
            select(func.count()).select_from(DocumentModel)
        ).scalar_one()
        evidence_items = session.execute(
            select(func.count()).select_from(EvidenceItemModel)
        ).scalar_one()
        embeddings = session.execute(
            select(func.count()).select_from(EvidenceEmbeddingModel)
        ).scalar_one()
    assert documents == 1
    assert evidence_items == 1
    assert embeddings == 0

    working_service = EvidenceIngestService(
        session_factory=session_factory,
        embedding_service=EvidenceEmbeddingService(
            session_factory=session_factory,
            embedder=DeterministicFakeEmbedder(),
        ),
    )
    app.dependency_overrides[provide_evidence_ingest_service] = lambda: working_service
    try:
        replay = client.post("/v1/evidence/documents", json=payload)
        assert replay.status_code == 200
        replay_body = replay.json()
        assert replay_body["created"] is False
        assert len(replay_body["evidence_item_ids"]) == 1
    finally:
        app.dependency_overrides.clear()

    with session_scope(session_factory) as session:
        documents = session.execute(
            select(func.count()).select_from(DocumentModel)
        ).scalar_one()
        evidence_items = session.execute(
            select(func.count()).select_from(EvidenceItemModel)
        ).scalar_one()
        embeddings = session.execute(
            select(func.count()).select_from(EvidenceEmbeddingModel)
        ).scalar_one()
    assert documents == 1
    assert evidence_items == 1
    assert embeddings == 1
