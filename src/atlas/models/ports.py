"""Atlas capability ports for research planning and drafting."""

from __future__ import annotations

from typing import Protocol

from atlas.models.contracts import DraftRequest, DraftResult, PlanRequest, PlanResult


class ResearchPlanner(Protocol):
    def plan(self, request: PlanRequest) -> PlanResult: ...


class ResearchDrafter(Protocol):
    def draft(self, request: DraftRequest) -> DraftResult: ...
