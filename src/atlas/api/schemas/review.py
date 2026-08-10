"""Pydantic contracts for the operator review API."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, StringConstraints


class ReviewDecisionRequest(BaseModel):
    """POST body for a human review decision."""

    decision: Literal["approve", "reject"]
    actor_id: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=128),
    ] = "operator"
    evaluation_run_id: (
        Annotated[
            str,
            StringConstraints(strip_whitespace=True, min_length=1, max_length=36),
        ]
        | None
    ) = None


class ReviewDecisionResponse(BaseModel):
    """Response body for a review decision."""

    id: str
    research_job_id: str
    decision: str
    actor_id: str
    status: Annotated[str, Field(description="created | replayed")]
