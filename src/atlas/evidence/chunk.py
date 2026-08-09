"""Deterministic Unicode code-point chunking."""

from __future__ import annotations

from dataclasses import dataclass

from atlas.evidence.bounds import (
    CHUNK_OVERLAP_CODE_POINTS,
    CHUNK_SIZE_CODE_POINTS,
    MAX_CHUNKS_PER_DOCUMENT,
)
from atlas.evidence.errors import EvidenceTooLargeError, EvidenceValidationError
from atlas.evidence.hash import evidence_item_content_sha256


@dataclass(frozen=True, slots=True)
class TextChunk:
    ordinal: int
    text: str
    content_sha256: str
    char_start: int
    char_end: int


def chunk_normalized_text(
    normalized: str,
    *,
    chunk_size: int = CHUNK_SIZE_CODE_POINTS,
    overlap: int = CHUNK_OVERLAP_CODE_POINTS,
    max_chunks: int = MAX_CHUNKS_PER_DOCUMENT,
) -> list[TextChunk]:
    """Split normalized text into overlapping Unicode code-point windows."""
    if chunk_size < 1:
        raise EvidenceValidationError("chunk_size must be positive")
    if overlap < 0 or overlap >= chunk_size:
        raise EvidenceValidationError("overlap must satisfy 0 <= overlap < chunk_size")
    if not normalized:
        raise EvidenceValidationError("normalized text must be non-empty")

    length = len(normalized)
    if length <= chunk_size:
        return [
            TextChunk(
                ordinal=0,
                text=normalized,
                content_sha256=evidence_item_content_sha256(normalized),
                char_start=0,
                char_end=length,
            )
        ]

    step = chunk_size - overlap
    chunks: list[TextChunk] = []
    start = 0
    ordinal = 0
    while start < length:
        if ordinal >= max_chunks:
            raise EvidenceTooLargeError("document exceeds maximum chunk count")
        end = min(start + chunk_size, length)
        text = normalized[start:end]
        chunks.append(
            TextChunk(
                ordinal=ordinal,
                text=text,
                content_sha256=evidence_item_content_sha256(text),
                char_start=start,
                char_end=end,
            )
        )
        if end >= length:
            break
        start += step
        ordinal += 1
    return chunks
