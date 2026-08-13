"""Thin traced adapters for Atlas ports that LangChain does not wrap natively."""

from __future__ import annotations

from atlas.models.contracts import DraftRequest, DraftResult, PlanRequest, PlanResult
from atlas.models.ports import ResearchDrafter, ResearchPlanner
from atlas.observability.langsmith.tracing import RunType, trace_ai


def _model_run_type(*, native_llm: bool) -> RunType:
    """Fake ports emit an LLM run; LangChain-backed ports emit a chain parent."""
    return "chain" if native_llm else "llm"


class TracedResearchPlanner:
    """Wrap a ``ResearchPlanner`` with an explicit ``model.plan`` LangSmith run.

    Fake deterministic planners have no native LangChain LLM child, so the
    Atlas run is ``run_type="llm"``. LangChain-backed planners already emit a
    provider LLM run; the Atlas wrap is then a logical ``chain`` parent so the
    same call is never an LLM run nested directly inside another LLM run.
    """

    def __init__(self, inner: ResearchPlanner, *, native_llm: bool = False) -> None:
        self._inner = inner
        self._run_type = _model_run_type(native_llm=native_llm)

    def plan(self, request: PlanRequest) -> PlanResult:
        return trace_ai(
            name="model.plan",
            run_type=self._run_type,
            metadata={
                "atlas.research_job_id": request.job_id,
                "atlas.node_name": "plan",
                "atlas.prompt_version": request.prompt_version,
            },
            fn=lambda: self._inner.plan(request),
        )


class TracedResearchDrafter:
    """Wrap a ``ResearchDrafter`` with an explicit ``model.draft`` LangSmith run.

    See :class:`TracedResearchPlanner` for the fake-versus-live run-type policy.
    """

    def __init__(self, inner: ResearchDrafter, *, native_llm: bool = False) -> None:
        self._inner = inner
        self._run_type = _model_run_type(native_llm=native_llm)

    def draft(self, request: DraftRequest) -> DraftResult:
        return trace_ai(
            name="model.draft",
            run_type=self._run_type,
            metadata={
                "atlas.research_job_id": request.job_id,
                "atlas.node_name": "draft",
                "atlas.prompt_version": request.prompt_version,
            },
            fn=lambda: self._inner.draft(request),
        )
