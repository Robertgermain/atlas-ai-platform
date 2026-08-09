"""SQLAlchemy repository for evidence provenance and report artifacts."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from atlas.evidence.bounds import TRUST_OPERATOR_CORPUS, TRUST_UNTRUSTED
from atlas.evidence.chunk import TextChunk
from atlas.evidence.contracts import (
    CitationChainItem,
    EvidenceItemView,
    EvidenceStrength,
    MediaType,
    SourceKind,
    TrustClass,
)
from atlas.evidence.errors import (
    CitationIntegrityError,
    ClaimEvidenceRequiredError,
    EvidenceNotFoundError,
    ReportArtifactConflictError,
)
from atlas.evidence.hash import report_artifact_content_sha256
from atlas.persistence.models.evidence import (
    CitationModel,
    ClaimModel,
    DocumentModel,
    EvidenceItemModel,
    EvidenceJobLinkModel,
    ReportArtifactModel,
    SourceModel,
)


@dataclass(frozen=True, slots=True)
class PersistedDocument:
    source_id: str
    document_id: str
    content_sha256: str
    parser_version: str
    evidence_item_ids: list[str]
    created: bool


@dataclass(frozen=True, slots=True)
class ClaimCitationSpec:
    text: str
    evidence_item_ids: list[str]


def canonical_citation_mapping(claims: list[ClaimCitationSpec]) -> str:
    """Deterministic JSON representation of claims and citation order."""
    payload = [
        {
            "ordinal": index,
            "text": claim.text,
            "evidence_item_ids": list(claim.evidence_item_ids),
        }
        for index, claim in enumerate(claims)
    ]
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


class SqlAlchemyEvidenceRepository:
    """Persistence helpers for sources through citations."""

    def upsert_corpus_source(
        self,
        session: Session,
        *,
        corpus_key: str,
        title: str | None,
        at: datetime,
    ) -> SourceModel:
        uri = f"corpus:{corpus_key}"
        return self._upsert_source(
            session,
            source_kind=SourceKind.CORPUS_TEXT.value,
            canonical_uri=uri,
            display_uri=uri,
            title=title,
            trust_class=TrustClass.OPERATOR_CORPUS.value,
            at=at,
        )

    def upsert_web_source(
        self,
        session: Session,
        *,
        canonical_uri: str,
        display_uri: str,
        title: str | None,
        at: datetime,
    ) -> SourceModel:
        return self._upsert_source(
            session,
            source_kind=SourceKind.WEB_SEARCH.value,
            canonical_uri=canonical_uri,
            display_uri=display_uri,
            title=title,
            trust_class=TrustClass.UNTRUSTED_EXTERNAL.value,
            at=at,
        )

    def get_or_create_document_with_chunks(
        self,
        session: Session,
        *,
        source_id: str,
        content_sha256: str,
        media_type: str,
        raw_text: str,
        normalized_text: str,
        byte_length: int,
        parser_version: str,
        chunks: list[TextChunk],
        strength: str,
        trust_label: str,
        metadata: dict[str, object] | None,
        at: datetime,
    ) -> PersistedDocument:
        existing = session.execute(
            select(DocumentModel).where(
                DocumentModel.source_id == source_id,
                DocumentModel.content_sha256 == content_sha256,
                DocumentModel.parser_version == parser_version,
            )
        ).scalar_one_or_none()
        if existing is not None:
            item_ids = self._list_evidence_ids_for_document(session, existing.id)
            return PersistedDocument(
                source_id=source_id,
                document_id=existing.id,
                content_sha256=existing.content_sha256,
                parser_version=existing.parser_version,
                evidence_item_ids=item_ids,
                created=False,
            )

        document_id = str(uuid.uuid4())
        created_item_ids: list[str] = []
        try:
            with session.begin_nested():
                session.add(
                    DocumentModel(
                        id=document_id,
                        source_id=source_id,
                        content_sha256=content_sha256,
                        media_type=media_type,
                        raw_text=raw_text,
                        normalized_text=normalized_text,
                        byte_length=byte_length,
                        parser_version=parser_version,
                        metadata_json=dict(metadata or {}),
                        created_at=at,
                    )
                )
                for chunk in chunks:
                    item_id = str(uuid.uuid4())
                    created_item_ids.append(item_id)
                    session.add(
                        EvidenceItemModel(
                            id=item_id,
                            document_id=document_id,
                            ordinal=chunk.ordinal,
                            text=chunk.text,
                            content_sha256=chunk.content_sha256,
                            char_start=chunk.char_start,
                            char_end=chunk.char_end,
                            strength=strength,
                            trust_label=trust_label,
                        )
                    )
                session.flush()
        except IntegrityError:
            # Concurrent insert of the same document identity: replay.
            existing = session.execute(
                select(DocumentModel).where(
                    DocumentModel.source_id == source_id,
                    DocumentModel.content_sha256 == content_sha256,
                    DocumentModel.parser_version == parser_version,
                )
            ).scalar_one()
            item_ids = self._list_evidence_ids_for_document(session, existing.id)
            return PersistedDocument(
                source_id=source_id,
                document_id=existing.id,
                content_sha256=existing.content_sha256,
                parser_version=existing.parser_version,
                evidence_item_ids=item_ids,
                created=False,
            )
        return PersistedDocument(
            source_id=source_id,
            document_id=document_id,
            content_sha256=content_sha256,
            parser_version=parser_version,
            evidence_item_ids=created_item_ids,
            created=True,
        )

    def ensure_job_link(
        self,
        session: Session,
        *,
        research_job_id: str,
        evidence_item_id: str,
        workflow_execution_id: str | None,
        tool_invocation_id: str | None,
        at: datetime,
    ) -> None:
        stmt = (
            pg_insert(EvidenceJobLinkModel)
            .values(
                research_job_id=research_job_id,
                evidence_item_id=evidence_item_id,
                workflow_execution_id=workflow_execution_id,
                tool_invocation_id=tool_invocation_id,
                linked_at=at,
            )
            .on_conflict_do_nothing(
                index_elements=["research_job_id", "evidence_item_id"]
            )
        )
        session.execute(stmt)

    def list_linked_evidence_ids(
        self,
        session: Session,
        *,
        research_job_id: str,
    ) -> set[str]:
        rows = session.execute(
            select(EvidenceJobLinkModel.evidence_item_id).where(
                EvidenceJobLinkModel.research_job_id == research_job_id
            )
        ).scalars()
        return set(rows)

    def get_evidence_item_view(
        self,
        session: Session,
        evidence_item_id: str,
    ) -> EvidenceItemView:
        row = session.execute(
            select(EvidenceItemModel, DocumentModel, SourceModel)
            .join(DocumentModel, EvidenceItemModel.document_id == DocumentModel.id)
            .join(SourceModel, DocumentModel.source_id == SourceModel.id)
            .where(EvidenceItemModel.id == evidence_item_id)
        ).one_or_none()
        if row is None:
            raise EvidenceNotFoundError(evidence_item_id)
        item, document, source = row
        return EvidenceItemView(
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
        )

    def list_evidence_context(
        self,
        session: Session,
        *,
        evidence_item_ids: list[str],
    ) -> list[EvidenceItemView]:
        if not evidence_item_ids:
            return []
        rows = session.execute(
            select(EvidenceItemModel, DocumentModel, SourceModel)
            .join(DocumentModel, EvidenceItemModel.document_id == DocumentModel.id)
            .join(SourceModel, DocumentModel.source_id == SourceModel.id)
            .where(EvidenceItemModel.id.in_(evidence_item_ids))
        ).all()
        by_id = {item.id: (item, document, source) for item, document, source in rows}
        views: list[EvidenceItemView] = []
        for item_id in evidence_item_ids:
            packed = by_id.get(item_id)
            if packed is None:
                continue
            item, document, source = packed
            views.append(
                EvidenceItemView(
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
                )
            )
        return views

    def persist_final_report(
        self,
        session: Session,
        *,
        research_job_id: str,
        workflow_execution_id: str,
        body_text: str,
        claims: list[ClaimCitationSpec],
        at: datetime,
    ) -> tuple[ReportArtifactModel, bool]:
        """Persist or idempotently replay a final report for an execution.

        Canonical comparison uses:
        - ``content_sha256`` of the exact rendered body UTF-8 bytes
        - ``canonical_citation_mapping(claims)`` for claim text + evidence id order
        """
        content_hash = report_artifact_content_sha256(body_text)
        requested_mapping = canonical_citation_mapping(claims)

        existing = session.execute(
            select(ReportArtifactModel).where(
                ReportArtifactModel.workflow_execution_id == workflow_execution_id
            )
        ).scalar_one_or_none()
        if existing is not None:
            self._assert_replay_matches(
                session,
                artifact=existing,
                research_job_id=research_job_id,
                content_hash=content_hash,
                requested_mapping=requested_mapping,
            )
            return existing, False

        self._validate_claims_for_job(
            session,
            research_job_id=research_job_id,
            claims=claims,
        )

        artifact_id = str(uuid.uuid4())
        artifact = ReportArtifactModel(
            id=artifact_id,
            research_job_id=research_job_id,
            workflow_execution_id=workflow_execution_id,
            artifact_kind="final",
            body_text=body_text,
            content_sha256=content_hash,
            created_at=at,
        )
        try:
            with session.begin_nested():
                session.add(artifact)
                session.flush()
                claim_rows: list[tuple[str, ClaimCitationSpec]] = []
                for claim_ordinal, claim in enumerate(claims):
                    claim_id = str(uuid.uuid4())
                    claim_rows.append((claim_id, claim))
                    session.add(
                        ClaimModel(
                            id=claim_id,
                            report_artifact_id=artifact_id,
                            research_job_id=research_job_id,
                            ordinal=claim_ordinal,
                            text=claim.text,
                        )
                    )
                session.flush()
                for claim_id, claim in claim_rows:
                    for citation_ordinal, evidence_item_id in enumerate(
                        claim.evidence_item_ids
                    ):
                        session.add(
                            CitationModel(
                                id=str(uuid.uuid4()),
                                claim_id=claim_id,
                                research_job_id=research_job_id,
                                evidence_item_id=evidence_item_id,
                                ordinal=citation_ordinal,
                            )
                        )
                session.flush()
        except IntegrityError as exc:
            # Likely concurrent insert on workflow_execution_id uniqueness.
            existing = session.execute(
                select(ReportArtifactModel).where(
                    ReportArtifactModel.workflow_execution_id == workflow_execution_id
                )
            ).scalar_one_or_none()
            if existing is None:
                raise ReportArtifactConflictError(
                    "report artifact persistence conflict"
                ) from exc
            self._assert_replay_matches(
                session,
                artifact=existing,
                research_job_id=research_job_id,
                content_hash=content_hash,
                requested_mapping=requested_mapping,
            )
            return existing, False
        return artifact, True

    def list_job_citations(
        self,
        session: Session,
        *,
        research_job_id: str,
    ) -> tuple[ReportArtifactModel | None, list[CitationChainItem]]:
        artifact = session.execute(
            select(ReportArtifactModel)
            .where(ReportArtifactModel.research_job_id == research_job_id)
            .order_by(ReportArtifactModel.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        if artifact is None:
            return None, []

        rows = session.execute(
            select(
                ClaimModel,
                CitationModel,
                EvidenceItemModel,
                DocumentModel,
                SourceModel,
            )
            .join(CitationModel, CitationModel.claim_id == ClaimModel.id)
            .join(
                EvidenceItemModel,
                EvidenceItemModel.id == CitationModel.evidence_item_id,
            )
            .join(DocumentModel, DocumentModel.id == EvidenceItemModel.document_id)
            .join(SourceModel, SourceModel.id == DocumentModel.source_id)
            .where(ClaimModel.report_artifact_id == artifact.id)
            .order_by(ClaimModel.ordinal, CitationModel.ordinal)
        ).all()
        items: list[CitationChainItem] = []
        for claim, citation, evidence, document, source in rows:
            items.append(
                CitationChainItem(
                    claim_id=claim.id,
                    claim_ordinal=claim.ordinal,
                    claim_text=claim.text,
                    citation_ordinal=citation.ordinal,
                    evidence_item_id=evidence.id,
                    evidence_text=evidence.text,
                    evidence_strength=EvidenceStrength(evidence.strength),
                    evidence_trust_label=evidence.trust_label,
                    document_id=document.id,
                    document_content_sha256=document.content_sha256,
                    source_id=source.id,
                    source_kind=SourceKind(source.source_kind),
                    source_canonical_uri=source.canonical_uri,
                    source_display_uri=source.display_uri,
                    source_trust_class=TrustClass(source.trust_class),
                )
            )
        return artifact, items

    def _assert_replay_matches(
        self,
        session: Session,
        *,
        artifact: ReportArtifactModel,
        research_job_id: str,
        content_hash: str,
        requested_mapping: str,
    ) -> None:
        if artifact.research_job_id != research_job_id:
            raise ReportArtifactConflictError(
                "report artifact exists for a different research job"
            )
        if artifact.content_sha256 != content_hash:
            raise ReportArtifactConflictError(
                "report artifact content hash mismatch for workflow execution"
            )
        existing_claims = self._load_claim_specs(session, artifact.id)
        existing_mapping = canonical_citation_mapping(existing_claims)
        if existing_mapping != requested_mapping:
            raise ReportArtifactConflictError(
                "report artifact citation mapping mismatch for workflow execution"
            )

    def _load_claim_specs(
        self,
        session: Session,
        report_artifact_id: str,
    ) -> list[ClaimCitationSpec]:
        claims = (
            session.execute(
                select(ClaimModel)
                .where(ClaimModel.report_artifact_id == report_artifact_id)
                .order_by(ClaimModel.ordinal)
            )
            .scalars()
            .all()
        )
        specs: list[ClaimCitationSpec] = []
        for claim in claims:
            evidence_ids = list(
                session.execute(
                    select(CitationModel.evidence_item_id)
                    .where(CitationModel.claim_id == claim.id)
                    .order_by(CitationModel.ordinal)
                ).scalars()
            )
            specs.append(
                ClaimCitationSpec(text=claim.text, evidence_item_ids=evidence_ids)
            )
        return specs

    def _validate_claims_for_job(
        self,
        session: Session,
        *,
        research_job_id: str,
        claims: list[ClaimCitationSpec],
    ) -> None:
        allowed = self.list_linked_evidence_ids(
            session,
            research_job_id=research_job_id,
        )
        for claim in claims:
            if not claim.evidence_item_ids:
                raise ClaimEvidenceRequiredError(
                    "every claim must cite at least one evidence item"
                )
            for evidence_item_id in claim.evidence_item_ids:
                if evidence_item_id not in allowed:
                    raise CitationIntegrityError(
                        "claim cites evidence not linked to the research job"
                    )

    def _list_evidence_ids_for_document(
        self,
        session: Session,
        document_id: str,
    ) -> list[str]:
        return list(
            session.execute(
                select(EvidenceItemModel.id)
                .where(EvidenceItemModel.document_id == document_id)
                .order_by(EvidenceItemModel.ordinal)
            ).scalars()
        )

    def _upsert_source(
        self,
        session: Session,
        *,
        source_kind: str,
        canonical_uri: str,
        display_uri: str,
        title: str | None,
        trust_class: str,
        at: datetime,
    ) -> SourceModel:
        existing = session.execute(
            select(SourceModel).where(
                SourceModel.source_kind == source_kind,
                SourceModel.canonical_uri == canonical_uri,
            )
        ).scalar_one_or_none()
        if existing is not None:
            if title and existing.title != title:
                existing.title = title
                existing.updated_at = at
            if existing.display_uri != display_uri:
                existing.display_uri = display_uri
                existing.updated_at = at
            return existing

        source = SourceModel(
            id=str(uuid.uuid4()),
            source_kind=source_kind,
            canonical_uri=canonical_uri,
            display_uri=display_uri,
            title=title,
            trust_class=trust_class,
            created_at=at,
            updated_at=at,
        )
        try:
            with session.begin_nested():
                session.add(source)
                session.flush()
        except IntegrityError:
            return session.execute(
                select(SourceModel).where(
                    SourceModel.source_kind == source_kind,
                    SourceModel.canonical_uri == canonical_uri,
                )
            ).scalar_one()
        return source


def default_trust_label(trust_class: str) -> str:
    if trust_class == TrustClass.OPERATOR_CORPUS.value:
        return TRUST_OPERATOR_CORPUS
    return TRUST_UNTRUSTED
