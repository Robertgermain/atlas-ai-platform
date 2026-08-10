"""Blocking spike: prove LangGraph interrupt_after await_review → complete.

This test must pass against the installed LangGraph version before Slice 12B
production changes or migration 0010. It uses a minimal graph that mirrors the
approved review topology without changing production wiring yet.
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict, cast

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph


class _SpikeState(TypedDict):
    route: Literal["pass", "review"]
    result: str
    report_persisted: bool
    nodes: list[str]


def _record(state: _SpikeState, name: str) -> dict[str, Any]:
    return {"nodes": [*state.get("nodes", []), name]}


def _policy(state: _SpikeState) -> dict[str, Any]:
    return _record(state, "policy")


def _route_after_policy(
    state: _SpikeState,
) -> Literal["complete", "await_review"]:
    if state["route"] == "pass":
        return "complete"
    return "await_review"


def _await_review(state: _SpikeState) -> dict[str, Any]:
    return _record(state, "await_review")


def _complete(state: _SpikeState) -> dict[str, Any]:
    updates = _record(state, "complete")
    updates["result"] = "accepted-report"
    updates["report_persisted"] = True
    return updates


def _compile_spike_graph(*, checkpointer: object) -> Any:
    graph: StateGraph[_SpikeState] = StateGraph(_SpikeState)
    graph.add_node("policy", _policy)
    graph.add_node("await_review", _await_review)
    graph.add_node("complete", _complete)
    graph.add_edge(START, "policy")
    graph.add_conditional_edges(
        "policy",
        _route_after_policy,
        {
            "complete": "complete",
            "await_review": "await_review",
        },
    )
    graph.add_edge("await_review", "complete")
    graph.add_edge("complete", END)
    return graph.compile(
        checkpointer=cast(Any, checkpointer),
        interrupt_after=["await_review"],
    )


def test_passing_route_is_not_interrupted() -> None:
    """Ordinary policy → complete must run to END without pausing."""
    saver = InMemorySaver()
    graph = _compile_spike_graph(checkpointer=saver)
    config: RunnableConfig = {"configurable": {"thread_id": "spike-pass"}}
    final = graph.invoke(
        {
            "route": "pass",
            "result": "",
            "report_persisted": False,
            "nodes": [],
        },
        config,
    )
    snapshot = graph.get_state(config)
    assert snapshot.next == ()
    assert final["result"] == "accepted-report"
    assert final["report_persisted"] is True
    assert final["nodes"] == ["policy", "complete"]
    assert "await_review" not in final["nodes"]


def test_review_interrupt_resume_idempotent_across_graph_instances() -> None:
    """Prove await_review pause/resume semantics required by Slice 12B."""
    shared_saver = InMemorySaver()
    thread = "spike-review-exec-1"
    config: RunnableConfig = {"configurable": {"thread_id": thread}}

    graph_a = _compile_spike_graph(checkpointer=shared_saver)
    paused = graph_a.invoke(
        {
            "route": "review",
            "result": "",
            "report_persisted": False,
            "nodes": [],
        },
        config,
    )
    snapshot_a = graph_a.get_state(config)

    # 2–3: paused immediately before complete; graph has not ended.
    assert snapshot_a.next == ("complete",)
    assert paused["nodes"] == ["policy", "await_review"]
    assert paused["result"] == ""
    assert paused["report_persisted"] is False
    assert "complete" not in paused["nodes"]

    # 5: new compiled-graph instance resumes the same durable checkpoint.
    del graph_a
    graph_b = _compile_spike_graph(checkpointer=shared_saver)
    resumed = graph_b.invoke(None, config)
    snapshot_b = graph_b.get_state(config)

    # 4: resume runs only complete.
    assert resumed["nodes"] == ["policy", "await_review", "complete"]
    assert resumed["result"] == "accepted-report"
    assert resumed["report_persisted"] is True
    assert snapshot_b.next == ()

    # 6: repeated invocation returns durable result without re-running complete.
    complete_count_before = resumed["nodes"].count("complete")
    again = graph_b.invoke(None, config)
    assert again["result"] == "accepted-report"
    assert again["nodes"].count("complete") == complete_count_before
    assert graph_b.get_state(config).next == ()


def test_no_report_before_review_continuation() -> None:
    """Gate: report persistence flag must stay false until resume of complete."""
    saver = InMemorySaver()
    graph = _compile_spike_graph(checkpointer=saver)
    config: RunnableConfig = {"configurable": {"thread_id": "spike-no-report"}}
    paused = graph.invoke(
        {
            "route": "review",
            "result": "",
            "report_persisted": False,
            "nodes": [],
        },
        config,
    )
    assert paused["report_persisted"] is False
    assert paused.get("result", "") == ""
    assert graph.get_state(config).next == ("complete",)
