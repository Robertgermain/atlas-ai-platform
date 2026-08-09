"""Pydantic contracts for evidence, claims, citations, and report artifacts."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

from atlas.evidence.bounds import (
    MAX_CLAIMS_PER_DRAFT,
    MAX_CORPUS_KEY_CHARS,
    MAX_EVIDENCE_IDS_PER_CLAIM,
    MAX_SOURCE_URI_CHARS,
    MAX_TITLE_CHARS,
)


class SourceKind(StrEnum):
    WEB_SEARCH = "web_search"
    CORPUS_TEXT = "corpus_text"


class TrustClass(StrEnum):
    UNTRUSTED_EXTERNAL = "untrusted_external"
    OPERATOR_CORPUS = "operator_corpus"


class EvidenceStrength(StrEnum):
    SEARCH_SNIPPET = "search_snippet"
    DOCUMENT_CHUNK = "document_chunk"


class MediaType(StrEnum):
    TEXT_PLAIN = "text/plain"
    TEXT_MARKDOWN = "text/markdown"


class EvidenceContextItem(BaseModel):
    """Evidence pack entry passed to the research drafter."""

    evidence_item_id: str
    text: Annotated[str, Field(min_length=1)]
    source_display_uri: Annotated[str, Field(max_length=MAX_SOURCE_URI_CHARS)]
    strength: EvidenceStrength
    trust_label: str


class ClaimStructured(BaseModel):
    """Provider-facing structured claim with evidence references."""

    text: Annotated[str, Field(min_length=1, max_length=4000)]
    evidence_item_ids: Annotated[
        list[str],
        Field(min_length=1, max_length=MAX_EVIDENCE_IDS_PER_CLAIM),
    ]

    @field_validator("text")
    @classmethod
    def strip_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("claim text must be non-empty")
        return cleaned

    @field_validator("evidence_item_ids")
    @classmethod
    def validate_ids(cls, value: list[str]) -> list[str]:
        cleaned = [item.strip() for item in value]
        if not cleaned or any(not item for item in cleaned):
            raise ValueError("evidence_item_ids must be non-empty strings")
        if len(set(cleaned)) != len(cleaned):
            raise ValueError("evidence_item_ids must be unique within a claim")
        return cleaned


class IngestDocumentRequest(BaseModel):
    corpus_key: Annotated[str, Field(min_length=1, max_length=MAX_CORPUS_KEY_CHARS)]
    title: Annotated[str | None, Field(default=None, max_length=MAX_TITLE_CHARS)] = None
    media_type: MediaType = MediaType.TEXT_MARKDOWN
    text: Annotated[str, Field(min_length=1)]

    @field_validator("corpus_key")
    @classmethod
    def validate_corpus_key(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("corpus_key must be non-empty")
        allowed = set(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-"
        )
        if any(ch not in allowed for ch in cleaned):
            raise ValueError("corpus_key contains invalid characters")
        return cleaned


class IngestDocumentResult(BaseModel):
    source_id: str
    document_id: str
    content_sha256: str
    parser_version: str
    evidence_item_ids: list[str]
    created: bool


class EvidenceItemView(BaseModel):
    id: str
    document_id: str
    source_id: str
    source_kind: SourceKind
    canonical_uri: str
    display_uri: str
    trust_class: TrustClass
    ordinal: int
    text: str
    content_sha256: str
    char_start: int | None
    char_end: int | None
    strength: EvidenceStrength
    trust_label: str
    document_content_sha256: str
    parser_version: str
    media_type: MediaType


class CitationChainItem(BaseModel):
    claim_id: str
    claim_ordinal: int
    claim_text: str
    citation_ordinal: int
    evidence_item_id: str
    evidence_text: str
    evidence_strength: EvidenceStrength
    evidence_trust_label: str
    document_id: str
    document_content_sha256: str
    source_id: str
    source_kind: SourceKind
    source_canonical_uri: str
    source_display_uri: str
    source_trust_class: TrustClass


class JobCitationsResponse(BaseModel):
    research_job_id: str
    report_artifact_id: str | None
    workflow_execution_id: str | None
    citations: list[CitationChainItem]


class PersistFinalReportRequest(BaseModel):
    research_job_id: str
    workflow_execution_id: str
    body_text: str
    claims: Annotated[list[ClaimStructured], Field(max_length=MAX_CLAIMS_PER_DRAFT)]


class ReportArtifactView(BaseModel):
    id: str
    research_job_id: str
    workflow_execution_id: str
    artifact_kind: Literal["final"] = "final"
    body_text: str
    content_sha256: str
    created_at: datetime
    created: bool
