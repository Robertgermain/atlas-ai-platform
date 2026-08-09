"""Offline fake-embedding retrieval evaluation.

Pipeline geometry checks only — not real semantic quality.

These scores validate deterministic embeddings, exact cosine retrieval, filters,
provenance, and metric calculation against a crafted fixture. They do **not**
claim real-world semantic retrieval quality.
"""

from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

from atlas.embeddings.fakes import DeterministicFakeEmbedder
from atlas.evidence.contracts import IngestDocumentRequest, MediaType
from atlas.evidence.metrics import mean, mrr_at_k, recall_at_k
from atlas.evidence.retrieve import EvidenceEmbeddingService, EvidenceRetriever
from atlas.evidence.service import EvidenceIngestService

DATASET_PATH = Path(__file__).with_name("retrieval_dataset.json")
RECALL_THRESHOLD = 0.80
MRR_THRESHOLD = 0.70


def test_offline_retrieval_eval_meets_thresholds(
    session_factory: sessionmaker[Session],
) -> None:
    payload = json.loads(DATASET_PATH.read_text(encoding="utf-8"))
    embedder = DeterministicFakeEmbedder()
    embedding_service = EvidenceEmbeddingService(
        session_factory=session_factory,
        embedder=embedder,
    )
    ingest = EvidenceIngestService(
        session_factory=session_factory,
        embedding_service=embedding_service,
    )
    key_to_evidence: dict[str, str] = {}
    for doc in payload["corpus"]:
        result = ingest.ingest_document(
            IngestDocumentRequest(
                corpus_key=doc["corpus_key"],
                title=doc["title"],
                media_type=MediaType.TEXT_PLAIN,
                text=doc["text"],
            )
        )
        key_to_evidence[doc["corpus_key"]] = result.evidence_item_ids[0]

    retriever = EvidenceRetriever(
        session_factory=session_factory,
        embedder=embedder,
        use_hnsw=False,
    )
    recalls: list[float] = []
    mrrs: list[float] = []
    for query in payload["queries"]:
        hits = retriever.retrieve(query=query["query"], k=5, mode="exact")
        ranked_keys: list[str] = []
        for hit in hits:
            for corpus_key, evidence_id in key_to_evidence.items():
                if evidence_id == hit.evidence.id:
                    ranked_keys.append(corpus_key)
                    break
            assert hit.evidence.source_id
            assert hit.evidence.document_id
            assert hit.evidence.id
        relevant = list(query["relevant_corpus_keys"])
        recalls.append(recall_at_k(relevant, ranked_keys, k=5))
        mrrs.append(mrr_at_k(relevant, ranked_keys, k=5))

    recall = mean(recalls)
    mrr = mean(mrrs)
    assert recall >= RECALL_THRESHOLD, f"Recall@5={recall:.3f} below {RECALL_THRESHOLD}"
    assert mrr >= MRR_THRESHOLD, f"MRR@5={mrr:.3f} below {MRR_THRESHOLD}"
