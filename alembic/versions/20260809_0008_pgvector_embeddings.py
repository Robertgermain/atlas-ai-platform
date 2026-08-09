"""Enable pgvector and store versioned evidence embeddings.

Revision ID: 20260809_0008
Revises: 20260809_0007
Create Date: 2026-08-09 20:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import Vector

revision: str = "20260809_0008"
down_revision: str | None = "20260809_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EMBEDDING_DIMENSIONS = 1536


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.create_table(
        "evidence_embeddings",
        sa.Column("evidence_item_id", sa.String(length=36), nullable=False),
        sa.Column("embedding_profile", sa.String(length=64), nullable=False),
        sa.Column("dimensions", sa.Integer(), nullable=False),
        sa.Column("embedding", Vector(EMBEDDING_DIMENSIONS), nullable=False),
        sa.Column("embedded_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "evidence_item_id",
            "embedding_profile",
            name="pk_evidence_embeddings",
        ),
        sa.ForeignKeyConstraint(
            ["evidence_item_id"],
            ["evidence_items.id"],
            name="fk_evidence_embeddings_evidence_item_id",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            f"dimensions = {EMBEDDING_DIMENSIONS}",
            name="ck_evidence_embeddings_dimensions_v1",
        ),
        sa.CheckConstraint(
            "length(trim(embedding_profile)) > 0",
            name="ck_evidence_embeddings_profile_nonempty",
        ),
    )
    op.create_index(
        "ix_evidence_embeddings_hnsw_cosine",
        "evidence_embeddings",
        ["embedding"],
        postgresql_using="hnsw",
        postgresql_with={"m": 16, "ef_construction": 64},
        postgresql_ops={"embedding": "vector_cosine_ops"},
        postgresql_where=sa.text("embedding_profile = 'embeddings.v1'"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_evidence_embeddings_hnsw_cosine",
        table_name="evidence_embeddings",
        postgresql_using="hnsw",
    )
    op.drop_table("evidence_embeddings")
    # Leave the vector extension installed; dropping it can fail when other
    # objects depend on it and is unnecessary for local rollback of this slice.
