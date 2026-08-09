"""Planner specialist: bounded typed plan from the research question."""

from __future__ import annotations

from atlas.models.contracts import PlanRequest
from atlas.models.ports import ResearchPlanner
from atlas.specialists.contracts import PlannerInput, PlannerOutput
from atlas.specialists.errors import SpecialistValidationError


class BoundedPlannerSpecialist:
    """Owns the planner specialist boundary.

    Wraps the model ``ResearchPlanner`` port and independently enforces the
    established exactly-three-task contract before returning a typed handoff.
    Does not call research tools or retrieval.
    """

    def __init__(self, planner: ResearchPlanner) -> None:
        self._planner = planner

    def run(self, request: PlannerInput) -> PlannerOutput:
        job_id = request.job_id.strip()
        question = request.question.strip()
        if not job_id or not question:
            raise SpecialistValidationError("planner input is invalid")
        result = self._planner.plan(
            PlanRequest(
                job_id=job_id,
                question=question,
                prompt_version=request.prompt_version,
            )
        )
        try:
            return PlannerOutput(
                tasks=list(result.tasks),
                prompt_version=request.prompt_version,
            )
        except Exception as exc:
            raise SpecialistValidationError("planner output is invalid") from exc
