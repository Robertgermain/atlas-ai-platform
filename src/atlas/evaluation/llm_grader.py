"""Semantic groundedness grader boundary (Slice 12A).

Live LangChain-backed semantic evaluation is **deferred** until later in
Milestone 12. Slice 12A ships:

- :class:`~atlas.evaluation.graders.FakeSemanticGroundednessGrader` for offline
  deterministic tests
- :class:`DeferredSemanticGroundednessPort` as an explicit non-implemented
  scaffold (not a working live adapter)

Default composition skips the semantic dimension entirely.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field

from atlas.evaluation.contracts import DimensionResult, EvaluationCandidateInput
from atlas.evaluation.errors import EvaluationTerminalError
from atlas.evaluation.graders import FakeSemanticGroundednessGrader

__all__ = [
    "DeferredSemanticGroundednessPort",
    "FakeSemanticGroundednessGrader",
    "LiveSemanticGroundednessGrader",
    "SemanticGroundednessOutput",
]


class SemanticClaimSupport(BaseModel):
    """Per-claim structured support label from a future LLM grader."""

    claim_ordinal: Annotated[int, Field(ge=1)]
    support: Literal["supported", "unsupported", "unclear"]
    score: Annotated[float, Field(ge=0.0, le=1.0)]


class SemanticGroundednessOutput(BaseModel):
    """Structured LLM output contract reserved for a future live adapter."""

    claims: list[SemanticClaimSupport] = Field(default_factory=list)
    aggregate_score: Annotated[float, Field(ge=0.0, le=1.0)]


class DeferredSemanticGroundednessPort:
    """Explicit deferred scaffold — not a working live semantic grader.

    Calling this class always fails closed. It exists so Milestone 12 can keep
    a typed port shape without claiming opt-in live evaluation works.
    """

    version: str = "deferred.unimplemented"

    def grade(
        self,
        candidate: EvaluationCandidateInput,
        *,
        linked_ids: set[str] | None = None,
    ) -> DimensionResult:
        del candidate, linked_ids
        raise EvaluationTerminalError(
            "Live semantic groundedness evaluation is deferred until later "
            "in Milestone 12."
        )


# Backward-compatible alias; prefer DeferredSemanticGroundednessPort.
LiveSemanticGroundednessGrader = DeferredSemanticGroundednessPort
