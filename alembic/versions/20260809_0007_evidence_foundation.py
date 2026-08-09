"""Add evidence, provenance, claims, citations, and report artifact tables.

Revision ID: 20260809_0007
Revises: 20260809_0006
Create Date: 2026-08-09 18:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260809_0007"
down_revision: str | None = "20260809_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "sources",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("source_kind", sa.String(length=32), nullable=False),
        sa.Column("canonical_uri", sa.Text(), nullable=False),
        sa.Column("display_uri", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("trust_class", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "source_kind",
            "canonical_uri",
            name="uq_sources_kind_canonical_uri",
        ),
        sa.CheckConstraint(
            "source_kind IN ('web_search', 'corpus_text')",
            name="ck_sources_kind",
        ),
        sa.CheckConstraint(
            "trust_class IN ('untrusted_external', 'operator_corpus')",
            name="ck_sources_trust_class",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_sources_id_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(canonical_uri)) > 0",
            name="ck_sources_canonical_uri_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(display_uri)) > 0",
            name="ck_sources_display_uri_nonempty",
        ),
        sa.CheckConstraint(
            "updated_at >= created_at",
            name="ck_sources_updated_after_created",
        ),
    )

    op.create_table(
        "documents",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("source_id", sa.String(length=36), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("media_type", sa.String(length=32), nullable=False),
        sa.Column("raw_text", sa.Text(), nullable=False),
        sa.Column("normalized_text", sa.Text(), nullable=False),
        sa.Column("byte_length", sa.Integer(), nullable=False),
        sa.Column("parser_version", sa.String(length=64), nullable=False),
        sa.Column(
            "metadata_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name="fk_documents_source_id",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "source_id",
            "content_sha256",
            "parser_version",
            name="uq_documents_source_hash_parser",
        ),
        sa.CheckConstraint(
            "media_type IN ('text/plain', 'text/markdown')",
            name="ck_documents_media_type",
        ),
        sa.CheckConstraint(
            "byte_length >= 1",
            name="ck_documents_byte_length_positive",
        ),
        sa.CheckConstraint(
            "length(content_sha256) = 64",
            name="ck_documents_content_sha256_len",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_documents_id_nonempty",
        ),
    )
    op.create_index("ix_documents_source_id", "documents", ["source_id"])

    op.create_table(
        "evidence_items",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("document_id", sa.String(length=36), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("char_start", sa.Integer(), nullable=True),
        sa.Column("char_end", sa.Integer(), nullable=True),
        sa.Column("strength", sa.String(length=32), nullable=False),
        sa.Column("trust_label", sa.String(length=64), nullable=False),
        sa.ForeignKeyConstraint(
            ["document_id"],
            ["documents.id"],
            name="fk_evidence_items_document_id",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "document_id",
            "ordinal",
            name="uq_evidence_items_document_ordinal",
        ),
        sa.CheckConstraint(
            "ordinal >= 0",
            name="ck_evidence_items_ordinal_nonnegative",
        ),
        sa.CheckConstraint(
            "strength IN ('search_snippet', 'document_chunk')",
            name="ck_evidence_items_strength",
        ),
        sa.CheckConstraint(
            "length(content_sha256) = 64",
            name="ck_evidence_items_content_sha256_len",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_evidence_items_id_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(text)) > 0",
            name="ck_evidence_items_text_nonempty",
        ),
    )
    op.create_index("ix_evidence_items_document_id", "evidence_items", ["document_id"])

    op.create_table(
        "evidence_job_links",
        sa.Column("research_job_id", sa.String(length=128), nullable=False),
        sa.Column("evidence_item_id", sa.String(length=36), nullable=False),
        sa.Column("workflow_execution_id", sa.String(length=36), nullable=True),
        sa.Column("tool_invocation_id", sa.String(length=36), nullable=True),
        sa.Column("linked_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "research_job_id",
            "evidence_item_id",
            name="pk_evidence_job_links",
        ),
        sa.ForeignKeyConstraint(
            ["research_job_id"],
            ["research_jobs.id"],
            name="fk_evidence_job_links_research_job_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["evidence_item_id"],
            ["evidence_items.id"],
            name="fk_evidence_job_links_evidence_item_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_execution_id"],
            ["workflow_executions.id"],
            name="fk_evidence_job_links_workflow_execution_id",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["tool_invocation_id"],
            ["tool_invocations.id"],
            name="fk_evidence_job_links_tool_invocation_id",
            ondelete="SET NULL",
        ),
    )
    op.create_index(
        "ix_evidence_job_links_evidence_item_id",
        "evidence_job_links",
        ["evidence_item_id"],
    )

    op.create_table(
        "report_artifacts",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("research_job_id", sa.String(length=128), nullable=False),
        sa.Column("workflow_execution_id", sa.String(length=36), nullable=False),
        sa.Column("artifact_kind", sa.String(length=16), nullable=False),
        sa.Column("body_text", sa.Text(), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["research_job_id"],
            ["research_jobs.id"],
            name="fk_report_artifacts_research_job_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_execution_id"],
            ["workflow_executions.id"],
            name="fk_report_artifacts_workflow_execution_id",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "workflow_execution_id",
            name="uq_report_artifacts_workflow_execution_id",
        ),
        sa.UniqueConstraint(
            "id",
            "research_job_id",
            name="uq_report_artifacts_id_job",
        ),
        sa.CheckConstraint(
            "artifact_kind = 'final'",
            name="ck_report_artifacts_kind",
        ),
        sa.CheckConstraint(
            "length(content_sha256) = 64",
            name="ck_report_artifacts_content_sha256_len",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_report_artifacts_id_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(body_text)) > 0",
            name="ck_report_artifacts_body_nonempty",
        ),
    )
    op.create_index(
        "ix_report_artifacts_research_job_id",
        "report_artifacts",
        ["research_job_id"],
    )

    op.create_table(
        "claims",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("report_artifact_id", sa.String(length=36), nullable=False),
        sa.Column("research_job_id", sa.String(length=128), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["report_artifact_id", "research_job_id"],
            ["report_artifacts.id", "report_artifacts.research_job_id"],
            name="fk_claims_artifact_job",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "report_artifact_id",
            "ordinal",
            name="uq_claims_artifact_ordinal",
        ),
        sa.UniqueConstraint(
            "id",
            "research_job_id",
            name="uq_claims_id_job",
        ),
        sa.CheckConstraint(
            "ordinal >= 0",
            name="ck_claims_ordinal_nonnegative",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_claims_id_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(text)) > 0",
            name="ck_claims_text_nonempty",
        ),
    )
    op.create_index("ix_claims_report_artifact_id", "claims", ["report_artifact_id"])

    op.create_table(
        "citations",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("claim_id", sa.String(length=36), nullable=False),
        sa.Column("research_job_id", sa.String(length=128), nullable=False),
        sa.Column("evidence_item_id", sa.String(length=36), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["claim_id", "research_job_id"],
            ["claims.id", "claims.research_job_id"],
            name="fk_citations_claim_job",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["research_job_id", "evidence_item_id"],
            [
                "evidence_job_links.research_job_id",
                "evidence_job_links.evidence_item_id",
            ],
            name="fk_citations_job_evidence_link",
        ),
        sa.UniqueConstraint(
            "claim_id",
            "evidence_item_id",
            "ordinal",
            name="uq_citations_claim_evidence_ordinal",
        ),
        sa.CheckConstraint(
            "ordinal >= 0",
            name="ck_citations_ordinal_nonnegative",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_citations_id_nonempty",
        ),
    )
    op.create_index("ix_citations_claim_id", "citations", ["claim_id"])
    op.create_index(
        "ix_citations_evidence_item_id",
        "citations",
        ["evidence_item_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_citations_evidence_item_id", table_name="citations")
    op.drop_index("ix_citations_claim_id", table_name="citations")
    op.drop_table("citations")
    op.drop_index("ix_claims_report_artifact_id", table_name="claims")
    op.drop_table("claims")
    op.drop_index("ix_report_artifacts_research_job_id", table_name="report_artifacts")
    op.drop_table("report_artifacts")
    op.drop_index(
        "ix_evidence_job_links_evidence_item_id",
        table_name="evidence_job_links",
    )
    op.drop_table("evidence_job_links")
    op.drop_index("ix_evidence_items_document_id", table_name="evidence_items")
    op.drop_table("evidence_items")
    op.drop_index("ix_documents_source_id", table_name="documents")
    op.drop_table("documents")
    op.drop_table("sources")
