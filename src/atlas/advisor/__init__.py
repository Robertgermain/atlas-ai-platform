"""Bounded, read-only advisory operations analyst (Milestone 15 Slice 15C2).

Human-invoked CLI only: ``python -m atlas.advisor --research-job-id <id>``.
"""

from __future__ import annotations

from atlas.advisor.contracts import AdvisoryAnalysis, AdvisoryIncidentFacts
from atlas.advisor.errors import AdvisoryError

__all__ = [
    "AdvisoryAnalysis",
    "AdvisoryError",
    "AdvisoryIncidentFacts",
]
