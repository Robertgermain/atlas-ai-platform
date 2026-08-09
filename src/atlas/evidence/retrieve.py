"""Embedding persistence and provenance-preserving retrieval services."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.orm import Session, sessionmaker

from atlas.embeddings.bounds import (
    EMBEDDING_PROFILE_V1,
    MAX_EMBED_BATCH_SIZE,
    MAX_EMBED_ITEMS_PER_CALL,
)
from atlas.embeddings.contracts import EmbedTextsRequest
from atlas.embeddings.ports import TextEmbedder
from atlas.evidence.contracts import (
    EvidenceItemView,
    EvidenceStrength,
    SourceKind,
)
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.embedding import SqlAlchemyEmbeddingRepository


@dataclass(frozen=True, slots=True)
class RetrievedEvidence:
    evidence: EvidenceItemView
    distance: float


class EvidenceEmbeddingService:
    """Embed evidence texts outside long DB transactions; persist idempotently."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        embedder: TextEmbedder,
        repository: SqlAlchemyEmbeddingRepository | None = None,
        embedding_profile: str = EMBEDDING_PROFILE_V1,
    ) -> None:
        self._session_factory = session_factory
        self._embedder = embedder
        self._repository = repository or SqlAlchemyEmbeddingRepository()
        self._embedding_profile = embedding_profile

    def embed_evidence_items(
        self,
        evidence_item_ids: list[str],
    ) -> int:
        """Embed missing items only. Returns number of newly stored embeddings.

        Evidence rows must already exist. Provider calls happen outside the
        persistence transaction. Existing ``(id, profile)`` rows are never
        overwritten.
        """
        if not evidence_item_ids:
            return 0
        capped = evidence_item_ids[:MAX_EMBED_ITEMS_PER_CALL]
        with session_scope(self._session_factory) as session:
            missing = self._repository.list_missing_embedding_ids(
                session,
                evidence_item_ids=capped,
                embedding_profile=self._embedding_profile,
            )
            views = _load_texts(session, missing)

        if not views:
            return 0

        inserted_total = 0
        for start in range(0, len(views), MAX_EMBED_BATCH_SIZE):
            batch = views[start : start + MAX_EMBED_BATCH_SIZE]
            batch_ids = [view.id for view in batch]
            # Provider call outside DB transaction.
            result = self._embedder.embed_texts(
                EmbedTextsRequest(
                    texts=[view.text for view in batch],
                    embedding_profile=self._embedding_profile,
                )
            )
            rows = list(zip(batch_ids, result.embeddings, strict=True))
            at = datetime.now(UTC)
            with session_scope(self._session_factory) as session:
                still_missing = set(
                    self._repository.list_missing_embedding_ids(
                        session,
                        evidence_item_ids=batch_ids,
                        embedding_profile=self._embedding_profile,
                    )
                )
                self._repository.insert_embeddings(
                    session,
                    rows=rows,
                    embedding_profile=self._embedding_profile,
                    at=at,
                )
                remaining = set(
                    self._repository.list_missing_embedding_ids(
                        session,
                        evidence_item_ids=batch_ids,
                        embedding_profile=self._embedding_profile,
                    )
                )
                inserted_total += len(still_missing) - len(remaining)
        return inserted_total


class EvidenceRetriever:
    """Typed retrieval boundary preserving evidence → document → source identity."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        embedder: TextEmbedder,
        repository: SqlAlchemyEmbeddingRepository | None = None,
        embedding_profile: str = EMBEDDING_PROFILE_V1,
        use_hnsw: bool = False,
    ) -> None:
        self._session_factory = session_factory
        self._embedder = embedder
        self._repository = repository or SqlAlchemyEmbeddingRepository()
        self._embedding_profile = embedding_profile
        self._use_hnsw = use_hnsw

    def retrieve(
        self,
        *,
        query: str,
        k: int = 5,
        source_kinds: list[SourceKind] | None = None,
        strengths: list[EvidenceStrength] | None = None,
        research_job_id: str | None = None,
        include_operator_corpus: bool = True,
        mode: str | None = None,
    ) -> list[RetrievedEvidence]:
        cleaned = query.strip()
        if not cleaned:
            return []
        hard_k = min(max(k, 1), 8)
        embedded = self._embedder.embed_texts(
            EmbedTextsRequest(
                texts=[cleaned],
                embedding_profile=self._embedding_profile,
            )
        )
        query_vector = embedded.embeddings[0]
        kinds = [kind.value for kind in source_kinds] if source_kinds else None
        strength_values = (
            [strength.value for strength in strengths] if strengths else None
        )
        use_hnsw = self._use_hnsw if mode is None else mode == "hnsw"
        with session_scope(self._session_factory) as session:
            if use_hnsw:
                rows = self._repository.retrieve_hnsw(
                    session,
                    query_vector=query_vector,
                    embedding_profile=self._embedding_profile,
                    k=hard_k,
                    source_kinds=kinds,
                    strengths=strength_values,
                    research_job_id=research_job_id,
                    include_operator_corpus=include_operator_corpus,
                )
            else:
                rows = self._repository.retrieve_exact(
                    session,
                    query_vector=query_vector,
                    embedding_profile=self._embedding_profile,
                    k=hard_k,
                    source_kinds=kinds,
                    strengths=strength_values,
                    research_job_id=research_job_id,
                    include_operator_corpus=include_operator_corpus,
                )
        return [
            RetrievedEvidence(evidence=row.evidence, distance=row.distance)
            for row in rows
        ]


def _load_texts(
    session: Session,
    evidence_item_ids: list[str],
) -> list[EvidenceItemView]:
    if not evidence_item_ids:
        return []
    from atlas.persistence.repositories.evidence import SqlAlchemyEvidenceRepository

    repo = SqlAlchemyEvidenceRepository()
    return repo.list_evidence_context(session, evidence_item_ids=evidence_item_ids)
