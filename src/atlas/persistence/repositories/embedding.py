"""Repository for evidence embeddings and semantic retrieval."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import Select, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from atlas.embeddings.bounds import (
    EMBEDDING_DIMENSIONS_V1,
    EMBEDDING_PROFILE_V1,
    HNSW_CANDIDATE_MULTIPLIER,
    MAX_HNSW_CANDIDATES,
)
from atlas.embeddings.errors import EmbeddingConflictError
from atlas.evidence.contracts import (
    EvidenceItemView,
    EvidenceStrength,
    MediaType,
    SourceKind,
    TrustClass,
)
from atlas.persistence.models.embedding import EvidenceEmbeddingModel
from atlas.persistence.models.evidence import (
    DocumentModel,
    EvidenceItemModel,
    EvidenceJobLinkModel,
    SourceModel,
)


@dataclass(frozen=True, slots=True)
class RetrievedEvidenceRow:
    evidence: EvidenceItemView
    distance: float


@dataclass(frozen=True, slots=True)
class _CandidateDistance:
    evidence_item_id: str
    distance: float


class SqlAlchemyEmbeddingRepository:
    def list_missing_embedding_ids(
        self,
        session: Session,
        *,
        evidence_item_ids: list[str],
        embedding_profile: str = EMBEDDING_PROFILE_V1,
    ) -> list[str]:
        if not evidence_item_ids:
            return []
        existing = set(
            session.execute(
                select(EvidenceEmbeddingModel.evidence_item_id).where(
                    EvidenceEmbeddingModel.embedding_profile == embedding_profile,
                    EvidenceEmbeddingModel.evidence_item_id.in_(evidence_item_ids),
                )
            ).scalars()
        )
        return [item_id for item_id in evidence_item_ids if item_id not in existing]

    def insert_embeddings(
        self,
        session: Session,
        *,
        rows: list[tuple[str, list[float]]],
        embedding_profile: str,
        at: datetime,
        overwrite: bool = False,
    ) -> int:
        """Insert embeddings. Returns number of rows inserted.

        When ``overwrite`` is false, conflicts are skipped (idempotent). When
        true is requested in the future, callers must pass overwrite explicitly;
        Milestone 10B never silently overwrites.
        """
        if overwrite:
            raise EmbeddingConflictError("silent embedding overwrite is not allowed")
        before = set(
            self.list_missing_embedding_ids(
                session,
                evidence_item_ids=[evidence_item_id for evidence_item_id, _ in rows],
                embedding_profile=embedding_profile,
            )
        )
        for evidence_item_id, vector in rows:
            if len(vector) != EMBEDDING_DIMENSIONS_V1:
                raise EmbeddingConflictError("embedding dimensions mismatch")
            stmt = (
                pg_insert(EvidenceEmbeddingModel)
                .values(
                    evidence_item_id=evidence_item_id,
                    embedding_profile=embedding_profile,
                    dimensions=EMBEDDING_DIMENSIONS_V1,
                    embedding=vector,
                    embedded_at=at,
                )
                .on_conflict_do_nothing(
                    index_elements=["evidence_item_id", "embedding_profile"]
                )
            )
            session.execute(stmt)
        after = set(
            self.list_missing_embedding_ids(
                session,
                evidence_item_ids=[evidence_item_id for evidence_item_id, _ in rows],
                embedding_profile=embedding_profile,
            )
        )
        return len(before) - len(after)

    def retrieve_exact(
        self,
        session: Session,
        *,
        query_vector: list[float],
        embedding_profile: str,
        k: int,
        source_kinds: list[str] | None = None,
        strengths: list[str] | None = None,
        research_job_id: str | None = None,
        include_operator_corpus: bool = True,
    ) -> list[RetrievedEvidenceRow]:
        """Exact cosine retrieval with approximate indexes disabled locally.

        Uses transaction-local ``SET LOCAL enable_indexscan/bitmapscan = off`` so
        the planner cannot use HNSW (or other indexes) for this query. Settings
        do not leak beyond the current transaction / pooled connection checkout.
        """
        session.execute(text("SET LOCAL enable_indexscan = off"))
        session.execute(text("SET LOCAL enable_bitmapscan = off"))
        return self._retrieve_joined(
            session,
            query_vector=query_vector,
            embedding_profile=embedding_profile,
            k=k,
            source_kinds=source_kinds,
            strengths=strengths,
            research_job_id=research_job_id,
            include_operator_corpus=include_operator_corpus,
            candidate_ids=None,
        )

    def retrieve_hnsw(
        self,
        session: Session,
        *,
        query_vector: list[float],
        embedding_profile: str,
        k: int,
        source_kinds: list[str] | None = None,
        strengths: list[str] | None = None,
        research_job_id: str | None = None,
        include_operator_corpus: bool = True,
    ) -> list[RetrievedEvidenceRow]:
        """HNSW-eligible candidate fetch, then filter and deterministic final order.

        Inner query orders solely by ``embedding <=> query`` with ``LIMIT`` so
        PostgreSQL can use ``ix_evidence_embeddings_hnsw_cosine``. Metadata
        filters apply after the candidate fetch. Final ordering of surviving
        candidates is ``(distance, evidence_item_id)`` capped to ``k``.

        Atlas does not globally force index use; the planner still chooses plans
        unless a caller deliberately applies transaction-local settings (as in
        the EXPLAIN integration test).
        """
        has_filters = bool(
            source_kinds
            or strengths
            or research_job_id is not None
            or not include_operator_corpus
        )
        multiplier = HNSW_CANDIDATE_MULTIPLIER if has_filters else 1
        candidate_limit = min(max(k * multiplier, k), MAX_HNSW_CANDIDATES)
        candidates = self.fetch_hnsw_candidates(
            session,
            query_vector=query_vector,
            embedding_profile=embedding_profile,
            limit=candidate_limit,
        )
        if not candidates:
            return []
        return self._retrieve_joined(
            session,
            query_vector=query_vector,
            embedding_profile=embedding_profile,
            k=k,
            source_kinds=source_kinds,
            strengths=strengths,
            research_job_id=research_job_id,
            include_operator_corpus=include_operator_corpus,
            candidate_ids=[item.evidence_item_id for item in candidates],
            candidate_distances={
                item.evidence_item_id: item.distance for item in candidates
            },
        )

    def fetch_hnsw_candidates(
        self,
        session: Session,
        *,
        query_vector: list[float],
        embedding_profile: str,
        limit: int,
    ) -> list[_CandidateDistance]:
        """Index-eligible ANN candidate query (no secondary ORDER BY key)."""
        vector_literal = _vector_literal(query_vector)
        rows = session.execute(
            text(
                """
                SELECT evidence_item_id,
                       (embedding <=> CAST(:query_vector AS vector)) AS distance
                FROM evidence_embeddings
                WHERE embedding_profile = :profile
                ORDER BY embedding <=> CAST(:query_vector AS vector)
                LIMIT :lim
                """
            ),
            {
                "query_vector": vector_literal,
                "profile": embedding_profile,
                "lim": limit,
            },
        ).all()
        return [
            _CandidateDistance(
                evidence_item_id=str(row.evidence_item_id),
                distance=float(row.distance),
            )
            for row in rows
        ]

    def explain_hnsw_candidate_plan(
        self,
        session: Session,
        *,
        query_vector: list[float],
        embedding_profile: str,
        limit: int,
    ) -> str:
        """Return EXPLAIN text for the HNSW-eligible candidate query shape.

        Applies test-oriented transaction-local planner settings so the plan is
        deterministic for asserting index eligibility. Production retrieval does
        not set these GUCs.
        """
        session.execute(text("SET LOCAL enable_seqscan = off"))
        vector_literal = _vector_literal(query_vector)
        lines = (
            session.execute(
                text(
                    """
                EXPLAIN (FORMAT TEXT)
                SELECT evidence_item_id,
                       (embedding <=> CAST(:query_vector AS vector)) AS distance
                FROM evidence_embeddings
                WHERE embedding_profile = :profile
                ORDER BY embedding <=> CAST(:query_vector AS vector)
                LIMIT :lim
                """
                ),
                {
                    "query_vector": vector_literal,
                    "profile": embedding_profile,
                    "lim": limit,
                },
            )
            .scalars()
            .all()
        )
        return "\n".join(str(line) for line in lines)

    def _retrieve_joined(
        self,
        session: Session,
        *,
        query_vector: list[float],
        embedding_profile: str,
        k: int,
        source_kinds: list[str] | None,
        strengths: list[str] | None,
        research_job_id: str | None,
        include_operator_corpus: bool,
        candidate_ids: list[str] | None,
        candidate_distances: dict[str, float] | None = None,
    ) -> list[RetrievedEvidenceRow]:
        distance = EvidenceEmbeddingModel.embedding.cosine_distance(query_vector).label(
            "distance"
        )
        stmt: Select[tuple[EvidenceItemModel, DocumentModel, SourceModel, float]] = (
            select(EvidenceItemModel, DocumentModel, SourceModel, distance)
            .join(
                EvidenceEmbeddingModel,
                EvidenceEmbeddingModel.evidence_item_id == EvidenceItemModel.id,
            )
            .join(DocumentModel, DocumentModel.id == EvidenceItemModel.document_id)
            .join(SourceModel, SourceModel.id == DocumentModel.source_id)
            .where(EvidenceEmbeddingModel.embedding_profile == embedding_profile)
        )
        if candidate_ids is not None:
            if not candidate_ids:
                return []
            stmt = stmt.where(EvidenceItemModel.id.in_(candidate_ids))
        if source_kinds:
            stmt = stmt.where(SourceModel.source_kind.in_(source_kinds))
        if strengths:
            stmt = stmt.where(EvidenceItemModel.strength.in_(strengths))
        if research_job_id is not None:
            linked = select(EvidenceJobLinkModel.evidence_item_id).where(
                EvidenceJobLinkModel.research_job_id == research_job_id
            )
            if include_operator_corpus:
                stmt = stmt.where(
                    (EvidenceItemModel.id.in_(linked))
                    | (SourceModel.trust_class == TrustClass.OPERATOR_CORPUS.value)
                )
            else:
                stmt = stmt.where(EvidenceItemModel.id.in_(linked))
        elif not include_operator_corpus:
            stmt = stmt.where(
                SourceModel.trust_class != TrustClass.OPERATOR_CORPUS.value
            )

        # Deterministic tie-break: distance, then evidence_item_id.
        stmt = stmt.order_by(distance, EvidenceItemModel.id).limit(k)

        rows = session.execute(stmt).all()
        results: list[RetrievedEvidenceRow] = []
        for item, document, source, dist in rows:
            # Prefer ANN candidate distance when present (same cosine metric).
            resolved = (
                candidate_distances.get(item.id, float(dist))
                if candidate_distances is not None
                else float(dist)
            )
            results.append(
                RetrievedEvidenceRow(
                    evidence=EvidenceItemView(
                        id=item.id,
                        document_id=document.id,
                        source_id=source.id,
                        source_kind=SourceKind(source.source_kind),
                        canonical_uri=source.canonical_uri,
                        display_uri=source.display_uri,
                        trust_class=TrustClass(source.trust_class),
                        ordinal=item.ordinal,
                        text=item.text,
                        content_sha256=item.content_sha256,
                        char_start=item.char_start,
                        char_end=item.char_end,
                        strength=EvidenceStrength(item.strength),
                        trust_label=item.trust_label,
                        document_content_sha256=document.content_sha256,
                        parser_version=document.parser_version,
                        media_type=MediaType(document.media_type),
                    ),
                    distance=resolved,
                )
            )
        return results

    def hnsw_index_exists(self, session: Session) -> bool:
        value = session.execute(
            text(
                """
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname = 'ix_evidence_embeddings_hnsw_cosine'
                """
            )
        ).scalar_one_or_none()
        return value is not None


def _vector_literal(values: list[float]) -> str:
    return "[" + ",".join(repr(float(value)) for value in values) + "]"
