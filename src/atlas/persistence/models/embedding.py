"""ORM mapping for evidence embeddings."""

from __future__ import annotations

from datetime import datetime

from pgvector.sqlalchemy import Vector
from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from atlas.embeddings.bounds import EMBEDDING_DIMENSIONS_V1
from atlas.persistence.models.base import Base


class EvidenceEmbeddingModel(Base):
    __tablename__ = "evidence_embeddings"
    __table_args__ = (
        CheckConstraint(
            f"dimensions = {EMBEDDING_DIMENSIONS_V1}",
            name="ck_evidence_embeddings_dimensions_v1",
        ),
    )

    evidence_item_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey(
            "evidence_items.id",
            name="fk_evidence_embeddings_evidence_item_id",
            ondelete="CASCADE",
        ),
        primary_key=True,
    )
    embedding_profile: Mapped[str] = mapped_column(String(64), primary_key=True)
    dimensions: Mapped[int] = mapped_column(Integer, nullable=False)
    embedding: Mapped[list[float]] = mapped_column(
        Vector(EMBEDDING_DIMENSIONS_V1),
        nullable=False,
    )
    embedded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
