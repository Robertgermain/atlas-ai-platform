"""Typed LangGraph research workflow (deterministic nodes only)."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextvars import ContextVar
from typing import Any, TypedDict, cast

from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph

from atlas.workflow.fakes import (
    build_draft,
    build_research_plan,
    format_research_report,
    run_fake_research,
)

NODE_NAMES: tuple[str, ...] = (
    "validate",
    "plan",
    "research",
    "draft",
    "complete",
)


class ResearchGraphState(TypedDict):
    """Typed channels for the deterministic research graph."""

    job_id: str
    question: str
    plan: list[str]
    findings: list[str]
    draft: str
    result: str


class NodeAuditHooks:
    """Optional hooks invoked around each graph node (audit / test spies)."""

    def begin(self, node_name: str) -> int:
        """Record node start; return attempt number."""
        raise NotImplementedError

    def complete(self, node_name: str, attempt: int) -> None:
        """Record successful node completion."""
        raise NotImplementedError

    def fail(self, node_name: str, attempt: int, error: Exception) -> None:
        """Record node failure with a sanitized error."""
        raise NotImplementedError


_node_hooks: ContextVar[NodeAuditHooks | None] = ContextVar(
    "atlas_workflow_node_hooks",
    default=None,
)
_node_counters: ContextVar[dict[str, int] | None] = ContextVar(
    "atlas_workflow_node_counters",
    default=None,
)


def set_node_hooks(hooks: NodeAuditHooks | None) -> None:
    """Bind audit hooks for the current worker processing attempt."""
    _node_hooks.set(hooks)


def set_node_counters(counters: dict[str, int] | None) -> None:
    """Bind optional per-node execution counters (tests)."""
    _node_counters.set(counters)


def initial_graph_state(*, job_id: str, question: str) -> ResearchGraphState:
    """Build the initial state for a new workflow thread."""
    return {
        "job_id": job_id,
        "question": question,
        "plan": [],
        "findings": [],
        "draft": "",
        "result": "",
    }


def validate_node(state: ResearchGraphState) -> dict[str, Any]:
    """Reject empty job_id or question."""
    job_id = state["job_id"].strip()
    question = state["question"].strip()
    if not job_id:
        raise ValueError("job_id must be non-empty")
    if not question:
        raise ValueError("question must be non-empty")
    return {"job_id": job_id, "question": question}


def plan_node(state: ResearchGraphState) -> dict[str, Any]:
    """Produce a bounded deterministic plan."""
    return {"plan": build_research_plan(state["question"])}


def research_node(state: ResearchGraphState) -> dict[str, Any]:
    """Run the fake research tool for each plan task."""
    plan = state["plan"]
    if len(plan) != 3:
        raise ValueError("plan must contain exactly three tasks")
    return {"findings": [run_fake_research(task) for task in plan]}


def draft_node(state: ResearchGraphState) -> dict[str, Any]:
    """Draft a deterministic intermediate report."""
    plan = state["plan"]
    findings = state["findings"]
    if len(findings) != len(plan):
        raise ValueError("findings must align with plan tasks")
    return {
        "draft": build_draft(
            question=state["question"],
            plan=plan,
            findings=findings,
        )
    }


def complete_node(state: ResearchGraphState) -> dict[str, Any]:
    """Format the final stable research report."""
    draft = state["draft"]
    if not draft.strip():
        raise ValueError("draft must be non-empty before complete")
    return {
        "result": format_research_report(
            question=state["question"],
            plan=state["plan"],
            findings=state["findings"],
            draft=draft,
        )
    }


def _wrap_node(
    node_name: str,
    node_fn: Callable[[ResearchGraphState], Mapping[str, Any]],
) -> Callable[[ResearchGraphState], dict[str, Any]]:
    def wrapped(state: ResearchGraphState) -> dict[str, Any]:
        counters = _node_counters.get()
        if counters is not None:
            counters[node_name] = counters.get(node_name, 0) + 1

        hooks = _node_hooks.get()
        attempt: int | None = None
        if hooks is not None:
            attempt = hooks.begin(node_name)
        try:
            result = dict(node_fn(state))
        except Exception as exc:
            if hooks is not None and attempt is not None:
                hooks.fail(node_name, attempt, exc)
            raise
        if hooks is not None and attempt is not None:
            hooks.complete(node_name, attempt)
        return result

    return wrapped


def build_research_graph(
    *,
    checkpointer: object,
    interrupt_after: Sequence[str] | None = None,
) -> CompiledStateGraph[
    ResearchGraphState, None, ResearchGraphState, ResearchGraphState
]:
    """Compile the five-node research graph with the given checkpointer."""
    graph = StateGraph(ResearchGraphState)
    # LangGraph NodeCallable typing rejects our wrapped callables without cast.
    graph.add_node("validate", cast(Any, _wrap_node("validate", validate_node)))
    graph.add_node("plan", cast(Any, _wrap_node("plan", plan_node)))
    graph.add_node("research", cast(Any, _wrap_node("research", research_node)))
    graph.add_node("draft", cast(Any, _wrap_node("draft", draft_node)))
    graph.add_node("complete", cast(Any, _wrap_node("complete", complete_node)))
    graph.add_edge(START, "validate")
    graph.add_edge("validate", "plan")
    graph.add_edge("plan", "research")
    graph.add_edge("research", "draft")
    graph.add_edge("draft", "complete")
    graph.add_edge("complete", END)
    return graph.compile(
        checkpointer=cast(Any, checkpointer),
        interrupt_after=list(interrupt_after) if interrupt_after else None,
    )
