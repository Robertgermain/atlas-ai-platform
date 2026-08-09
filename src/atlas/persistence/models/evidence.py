"""ORM mappings for evidence, claims, citations, and report artifacts."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from atlas.persistence.models.base import Base


class SourceModel(Base):
    __tablename__ = "sources"
    __table_args__ = (
        UniqueConstraint(
            "source_kind",
            "canonical_uri",
            name="uq_sources_kind_canonical_uri",
        ),
        CheckConstraint(
            "source_kind IN ('web_search', 'corpus_text')",
            name="ck_sources_kind",
        ),
        CheckConstraint(
            "trust_class IN ('untrusted_external', 'operator_corpus')",
            name="ck_sources_trust_class",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    source_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    canonical_uri: Mapped[str] = mapped_column(Text, nullable=False)
    display_uri: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    trust_class: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class DocumentModel(Base):
    __tablename__ = "documents"
    __table_args__ = (
        UniqueConstraint(
            "source_id",
            "content_sha256",
            "parser_version",
            name="uq_documents_source_hash_parser",
        ),
        CheckConstraint(
            "media_type IN ('text/plain', 'text/markdown')",
            name="ck_documents_media_type",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    source_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("sources.id", name="fk_documents_source_id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    media_type: Mapped[str] = mapped_column(String(32), nullable=False)
    raw_text: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_text: Mapped[str] = mapped_column(Text, nullable=False)
    byte_length: Mapped[int] = mapped_column(Integer, nullable=False)
    parser_version: Mapped[str] = mapped_column(String(64), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB(none_as_null=True),
        nullable=False,
        default=dict,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class EvidenceItemModel(Base):
    __tablename__ = "evidence_items"
    __table_args__ = (
        UniqueConstraint(
            "document_id",
            "ordinal",
            name="uq_evidence_items_document_ordinal",
        ),
        CheckConstraint(
            "strength IN ('search_snippet', 'document_chunk')",
            name="ck_evidence_items_strength",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    document_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey(
            "documents.id",
            name="fk_evidence_items_document_id",
            ondelete="CASCADE",
        ),
        nullable=False,
        index=True,
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    char_start: Mapped[int | None] = mapped_column(Integer, nullable=True)
    char_end: Mapped[int | None] = mapped_column(Integer, nullable=True)
    strength: Mapped[str] = mapped_column(String(32), nullable=False)
    trust_label: Mapped[str] = mapped_column(String(64), nullable=False)


class EvidenceJobLinkModel(Base):
    __tablename__ = "evidence_job_links"
    __table_args__ = (
        PrimaryKeyConstraint(
            "research_job_id",
            "evidence_item_id",
            name="pk_evidence_job_links",
        ),
    )

    research_job_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey(
            "research_jobs.id",
            name="fk_evidence_job_links_research_job_id",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    evidence_item_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey(
            "evidence_items.id",
            name="fk_evidence_job_links_evidence_item_id",
            ondelete="CASCADE",
        ),
        nullable=False,
        index=True,
    )
    workflow_execution_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey(
            "workflow_executions.id",
            name="fk_evidence_job_links_workflow_execution_id",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    tool_invocation_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey(
            "tool_invocations.id",
            name="fk_evidence_job_links_tool_invocation_id",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    linked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ReportArtifactModel(Base):
    __tablename__ = "report_artifacts"
    __table_args__ = (
        UniqueConstraint(
            "workflow_execution_id",
            name="uq_report_artifacts_workflow_execution_id",
        ),
        UniqueConstraint(
            "id",
            "research_job_id",
            name="uq_report_artifacts_id_job",
        ),
        CheckConstraint(
            "artifact_kind = 'final'",
            name="ck_report_artifacts_kind",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    research_job_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey(
            "research_jobs.id",
            name="fk_report_artifacts_research_job_id",
            ondelete="CASCADE",
        ),
        nullable=False,
        index=True,
    )
    workflow_execution_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey(
            "workflow_executions.id",
            name="fk_report_artifacts_workflow_execution_id",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    artifact_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    body_text: Mapped[str] = mapped_column(Text, nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class ClaimModel(Base):
    __tablename__ = "claims"
    __table_args__ = (
        ForeignKeyConstraint(
            ["report_artifact_id", "research_job_id"],
            ["report_artifacts.id", "report_artifacts.research_job_id"],
            name="fk_claims_artifact_job",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "report_artifact_id",
            "ordinal",
            name="uq_claims_artifact_ordinal",
        ),
        UniqueConstraint(
            "id",
            "research_job_id",
            name="uq_claims_id_job",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    report_artifact_id: Mapped[str] = mapped_column(
        String(36), nullable=False, index=True
    )
    research_job_id: Mapped[str] = mapped_column(String(128), nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)


class CitationModel(Base):
    __tablename__ = "citations"
    __table_args__ = (
        ForeignKeyConstraint(
            ["claim_id", "research_job_id"],
            ["claims.id", "claims.research_job_id"],
            name="fk_citations_claim_job",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["research_job_id", "evidence_item_id"],
            [
                "evidence_job_links.research_job_id",
                "evidence_job_links.evidence_item_id",
            ],
            name="fk_citations_job_evidence_link",
        ),
        UniqueConstraint(
            "claim_id",
            "evidence_item_id",
            "ordinal",
            name="uq_citations_claim_evidence_ordinal",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    claim_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    research_job_id: Mapped[str] = mapped_column(String(128), nullable=False)
    evidence_item_id: Mapped[str] = mapped_column(
        String(36), nullable=False, index=True
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
