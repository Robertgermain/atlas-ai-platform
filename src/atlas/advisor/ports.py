"""Read-only ports for the advisory analyst. No mutation methods exist."""

from __future__ import annotations

from typing import Protocol

from sqlalchemy.orm import Session

from atlas.advisor.contracts import AdvisoryAnalysis, AdvisoryIncidentFacts
from atlas.advisor.snapshot import SnapshotLoad


class AdvisorySnapshotPort(Protocol):
    """Load approved columns for one research job. SELECT only."""

    def load(self, session: Session, research_job_id: str) -> SnapshotLoad:
        """Return raw approved rows for assembly. Never mutates."""


class AdvisoryAnalystPort(Protocol):
    """Produce a structured analysis from deterministic facts."""

    def analyze(
        self,
        facts: AdvisoryIncidentFacts,
        *,
        analysis_id: str | None = None,
    ) -> AdvisoryAnalysis:
        """Return validated structured output. Never opens a database session."""
