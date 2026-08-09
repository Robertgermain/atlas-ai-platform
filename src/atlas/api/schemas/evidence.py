"""Pydantic contracts for evidence HTTP APIs."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints, field_validator

from atlas.evidence.bounds import (
    MAX_CORPUS_KEY_CHARS,
    MAX_TITLE_CHARS,
)
from atlas.evidence.contracts import (
    CitationChainItem,
    EvidenceItemView,
    IngestDocumentResult,
    JobCitationsResponse,
    MediaType,
)

NormalizedCorpusKey = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=MAX_CORPUS_KEY_CHARS,
        pattern=r"^[a-zA-Z0-9._:-]+$",
    ),
]


class CreateEvidenceDocumentRequest(BaseModel):
    corpus_key: NormalizedCorpusKey
    title: Annotated[str | None, Field(default=None, max_length=MAX_TITLE_CHARS)] = None
    media_type: MediaType = MediaType.TEXT_MARKDOWN
    text: Annotated[str, Field(min_length=1)]

    @field_validator("title")
    @classmethod
    def strip_title(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None


class EvidenceDocumentResponse(BaseModel):
    source_id: str
    document_id: str
    content_sha256: str
    parser_version: str
    evidence_item_ids: list[str]
    created: bool

    @classmethod
    def from_result(cls, result: IngestDocumentResult) -> EvidenceDocumentResponse:
        return cls(
            source_id=result.source_id,
            document_id=result.document_id,
            content_sha256=result.content_sha256,
            parser_version=result.parser_version,
            evidence_item_ids=list(result.evidence_item_ids),
            created=result.created,
        )


class EvidenceItemResponse(BaseModel):
    id: str
    document_id: str
    source_id: str
    source_kind: str
    canonical_uri: str
    display_uri: str
    trust_class: str
    ordinal: int
    text: str
    content_sha256: str
    char_start: int | None
    char_end: int | None
    strength: str
    trust_label: str
    document_content_sha256: str
    parser_version: str
    media_type: str

    @classmethod
    def from_view(cls, view: EvidenceItemView) -> EvidenceItemResponse:
        return cls(
            id=view.id,
            document_id=view.document_id,
            source_id=view.source_id,
            source_kind=view.source_kind.value,
            canonical_uri=view.canonical_uri,
            display_uri=view.display_uri,
            trust_class=view.trust_class.value,
            ordinal=view.ordinal,
            text=view.text,
            content_sha256=view.content_sha256,
            char_start=view.char_start,
            char_end=view.char_end,
            strength=view.strength.value,
            trust_label=view.trust_label,
            document_content_sha256=view.document_content_sha256,
            parser_version=view.parser_version,
            media_type=view.media_type.value,
        )


class CitationChainResponseItem(BaseModel):
    claim_id: str
    claim_ordinal: int
    claim_text: str
    citation_ordinal: int
    evidence_item_id: str
    evidence_text: str
    evidence_strength: str
    evidence_trust_label: str
    document_id: str
    document_content_sha256: str
    source_id: str
    source_kind: str
    source_canonical_uri: str
    source_display_uri: str
    source_trust_class: str

    @classmethod
    def from_item(cls, item: CitationChainItem) -> CitationChainResponseItem:
        return cls(
            claim_id=item.claim_id,
            claim_ordinal=item.claim_ordinal,
            claim_text=item.claim_text,
            citation_ordinal=item.citation_ordinal,
            evidence_item_id=item.evidence_item_id,
            evidence_text=item.evidence_text,
            evidence_strength=item.evidence_strength.value,
            evidence_trust_label=item.evidence_trust_label,
            document_id=item.document_id,
            document_content_sha256=item.document_content_sha256,
            source_id=item.source_id,
            source_kind=item.source_kind.value,
            source_canonical_uri=item.source_canonical_uri,
            source_display_uri=item.source_display_uri,
            source_trust_class=item.source_trust_class.value,
        )


class JobCitationsHttpResponse(BaseModel):
    research_job_id: str
    report_artifact_id: str | None
    workflow_execution_id: str | None
    citations: list[CitationChainResponseItem]

    @classmethod
    def from_domain(cls, payload: JobCitationsResponse) -> JobCitationsHttpResponse:
        return cls(
            research_job_id=payload.research_job_id,
            report_artifact_id=payload.report_artifact_id,
            workflow_execution_id=payload.workflow_execution_id,
            citations=[
                CitationChainResponseItem.from_item(item) for item in payload.citations
            ],
        )
