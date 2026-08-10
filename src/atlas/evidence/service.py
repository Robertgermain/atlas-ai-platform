"""Application services for evidence ingestion, citation validation, and reports."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session, sessionmaker

from atlas.evidence.bounds import (
    MAX_DOCUMENT_REQUEST_BYTES,
    TRUST_OPERATOR_CORPUS,
    TRUST_UNTRUSTED,
)
from atlas.evidence.chunk import TextChunk, chunk_normalized_text
from atlas.evidence.contracts import (
    ClaimStructured,
    EvidenceContextItem,
    EvidenceItemView,
    EvidenceStrength,
    IngestDocumentRequest,
    IngestDocumentResult,
    JobCitationsResponse,
    ReportArtifactView,
)
from atlas.evidence.errors import (
    CitationIntegrityError,
    ClaimEvidenceRequiredError,
    EvidenceTooLargeError,
    EvidenceValidationError,
)
from atlas.evidence.hash import document_content_sha256, evidence_item_content_sha256
from atlas.evidence.normalize import (
    accept_raw_text,
    media_type_for_parser,
    normalize_for_media_type,
    normalize_search_snippet,
)
from atlas.evidence.pack import build_evidence_context_pack
from atlas.evidence.url import canonicalize_http_url
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.evidence import (
    ClaimCitationSpec,
    SqlAlchemyEvidenceRepository,
    canonical_citation_mapping,
)

if TYPE_CHECKING:
    from atlas.evidence.retrieve import EvidenceEmbeddingService


class EvidenceIngestService:
    """Ingest corpus text and search snippets into durable evidence rows."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        repository: SqlAlchemyEvidenceRepository | None = None,
        embedding_service: EvidenceEmbeddingService | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository or SqlAlchemyEvidenceRepository()
        self._embedding_service = embedding_service

    def ingest_document(self, request: IngestDocumentRequest) -> IngestDocumentResult:
        raw = accept_raw_text(request.text)
        if len(raw) > MAX_DOCUMENT_REQUEST_BYTES:
            raise EvidenceTooLargeError("document request exceeds maximum size")
        normalized, parser_version = normalize_for_media_type(
            raw,
            request.media_type.value,
        )
        content_hash = document_content_sha256(raw)
        chunks = chunk_normalized_text(normalized)
        at = datetime.now(UTC)
        with session_scope(self._session_factory) as session:
            source = self._repository.upsert_corpus_source(
                session,
                corpus_key=request.corpus_key,
                title=request.title,
                at=at,
            )
            persisted = self._repository.get_or_create_document_with_chunks(
                session,
                source_id=source.id,
                content_sha256=content_hash,
                media_type=request.media_type.value,
                raw_text=raw.decode("utf-8"),
                normalized_text=normalized,
                byte_length=len(raw),
                parser_version=parser_version,
                chunks=chunks,
                strength=EvidenceStrength.DOCUMENT_CHUNK.value,
                trust_label=TRUST_OPERATOR_CORPUS,
                metadata={"corpus_key": request.corpus_key},
                at=at,
            )
        # Embedding runs after the evidence transaction commits. If embedding
        # fails, evidence rows remain and missing vectors can be backfilled.
        self._maybe_embed(list(persisted.evidence_item_ids))
        return IngestDocumentResult(
            source_id=persisted.source_id,
            document_id=persisted.document_id,
            content_sha256=persisted.content_sha256,
            parser_version=persisted.parser_version,
            evidence_item_ids=list(persisted.evidence_item_ids),
            created=persisted.created,
        )

    def ingest_search_hit(
        self,
        *,
        research_job_id: str,
        workflow_execution_id: str | None,
        tool_invocation_id: str | None,
        title: str,
        url: str,
        snippet: str,
    ) -> str:
        """Persist one search hit and link it to the research job.

        Returns the evidence_item_id.
        """
        canonical_uri, display_uri = canonicalize_http_url(url)
        normalized, parser_version, raw = normalize_search_snippet(
            title=title,
            snippet=snippet,
        )
        content_hash = document_content_sha256(raw)
        chunk = TextChunk(
            ordinal=0,
            text=normalized,
            content_sha256=evidence_item_content_sha256(normalized),
            char_start=0,
            char_end=len(normalized),
        )
        at = datetime.now(UTC)
        with session_scope(self._session_factory) as session:
            source = self._repository.upsert_web_source(
                session,
                canonical_uri=canonical_uri,
                display_uri=display_uri,
                title=title.strip() or None,
                at=at,
            )
            persisted = self._repository.get_or_create_document_with_chunks(
                session,
                source_id=source.id,
                content_sha256=content_hash,
                media_type=media_type_for_parser(parser_version),
                raw_text=raw.decode("utf-8"),
                normalized_text=normalized,
                byte_length=len(raw),
                parser_version=parser_version,
                chunks=[chunk],
                strength=EvidenceStrength.SEARCH_SNIPPET.value,
                trust_label=TRUST_UNTRUSTED,
                metadata={"title": title, "snippet": snippet},
                at=at,
            )
            evidence_item_id = persisted.evidence_item_ids[0]
            self._repository.ensure_job_link(
                session,
                research_job_id=research_job_id,
                evidence_item_id=evidence_item_id,
                workflow_execution_id=workflow_execution_id,
                tool_invocation_id=tool_invocation_id,
                at=at,
            )
        self._maybe_embed([evidence_item_id])
        return evidence_item_id

    def get_item(self, evidence_item_id: str) -> EvidenceItemView:
        with session_scope(self._session_factory) as session:
            return self._repository.get_evidence_item_view(session, evidence_item_id)

    def link_evidence_to_job(
        self,
        *,
        research_job_id: str,
        evidence_item_ids: list[str],
        workflow_execution_id: str | None = None,
    ) -> None:
        at = datetime.now(UTC)
        with session_scope(self._session_factory) as session:
            for evidence_item_id in evidence_item_ids:
                # Ensure item exists.
                self._repository.get_evidence_item_view(session, evidence_item_id)
                self._repository.ensure_job_link(
                    session,
                    research_job_id=research_job_id,
                    evidence_item_id=evidence_item_id,
                    workflow_execution_id=workflow_execution_id,
                    tool_invocation_id=None,
                    at=at,
                )

    def build_drafter_evidence_pack(
        self,
        evidence_item_ids: list[str],
    ) -> list[EvidenceContextItem]:
        """Build a ranked evidence pack under item, character, and byte caps.

        The ``MAX_DRAFT_EVIDENCE_CONTEXT_BYTES`` limit applies to the sum of
        UTF-8 bytes of evidence display ``text`` fields only (after per-item
        truncation). IDs, URIs, labels, and LangChain prompt framing are
        outside this budget.
        """
        with session_scope(self._session_factory) as session:
            views = self._repository.list_evidence_context(
                session,
                evidence_item_ids=evidence_item_ids,
            )
        return build_evidence_context_pack(views)

    def _maybe_embed(self, evidence_item_ids: list[str]) -> None:
        if self._embedding_service is None or not evidence_item_ids:
            return
        self._embedding_service.embed_evidence_items(evidence_item_ids)


class CitationValidator:
    """Fail-closed validation of claim → job-linked evidence references."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        repository: SqlAlchemyEvidenceRepository | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository or SqlAlchemyEvidenceRepository()

    def validate(
        self,
        *,
        research_job_id: str,
        claims: list[ClaimStructured],
    ) -> list[ClaimCitationSpec]:
        specs = [
            ClaimCitationSpec(
                text=claim.text,
                evidence_item_ids=list(claim.evidence_item_ids),
            )
            for claim in claims
        ]
        if not specs:
            return []
        with session_scope(self._session_factory) as session:
            allowed = self._repository.list_linked_evidence_ids(
                session,
                research_job_id=research_job_id,
            )
        for spec in specs:
            if not spec.evidence_item_ids:
                raise ClaimEvidenceRequiredError(
                    "every claim must cite at least one evidence item"
                )
            for evidence_item_id in spec.evidence_item_ids:
                if evidence_item_id not in allowed:
                    raise CitationIntegrityError(
                        "claim cites evidence not linked to the research job"
                    )
        return specs


class ReportArtifactService:
    """Idempotent final report persistence by workflow_execution_id."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        repository: SqlAlchemyEvidenceRepository | None = None,
        citation_validator: CitationValidator | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository or SqlAlchemyEvidenceRepository()
        self._citation_validator = citation_validator or CitationValidator(
            session_factory=session_factory,
            repository=self._repository,
        )

    def persist_final(
        self,
        *,
        research_job_id: str,
        workflow_execution_id: str,
        body_text: str,
        claims: list[ClaimStructured],
        claim_token: str,
        at: datetime | None = None,
    ) -> ReportArtifactView:
        cleaned_body = body_text.strip()
        if not cleaned_body:
            raise EvidenceValidationError("report body must be non-empty")
        specs = self._citation_validator.validate(
            research_job_id=research_job_id,
            claims=claims,
        )
        _ = canonical_citation_mapping(specs)
        now = at if at is not None else datetime.now(UTC)
        with session_scope(self._session_factory) as session:
            if not claim_token.strip():
                from atlas.evidence.errors import EvidenceOwnershipError

                raise EvidenceOwnershipError(
                    "Claim token is required for report persistence."
                )
            from atlas.evidence.errors import EvidenceOwnershipError
            from atlas.persistence.models import ResearchJobModel

            job_model = session.get(
                ResearchJobModel, research_job_id, with_for_update=True
            )
            if (
                job_model is None
                or job_model.status != "RUNNING"
                or job_model.claim_token != claim_token
                or job_model.lease_expires_at is None
                or job_model.lease_expires_at <= now
            ):
                raise EvidenceOwnershipError(
                    "Claim-fenced report persistence failed ownership check."
                )

            artifact, created = self._repository.persist_final_report(
                session,
                research_job_id=research_job_id,
                workflow_execution_id=workflow_execution_id,
                body_text=cleaned_body,
                claims=specs,
                at=now,
            )
            return ReportArtifactView(
                id=artifact.id,
                research_job_id=artifact.research_job_id,
                workflow_execution_id=artifact.workflow_execution_id,
                body_text=artifact.body_text,
                content_sha256=artifact.content_sha256,
                created_at=artifact.created_at,
                created=created,
            )

    def get_job_citations(self, research_job_id: str) -> JobCitationsResponse:
        with session_scope(self._session_factory) as session:
            artifact, items = self._repository.list_job_citations(
                session,
                research_job_id=research_job_id,
            )
        return JobCitationsResponse(
            research_job_id=research_job_id,
            report_artifact_id=artifact.id if artifact else None,
            workflow_execution_id=(
                artifact.workflow_execution_id if artifact else None
            ),
            citations=items,
        )
