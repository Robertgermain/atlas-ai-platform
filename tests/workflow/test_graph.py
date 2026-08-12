"""Unit tests for deterministic research report formatting and graph nodes."""

from __future__ import annotations

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from prometheus_client import CollectorRegistry

from atlas.observability.metrics.catalog import AtlasMetrics
from atlas.workflow.fakes import (
    build_draft,
    build_research_plan,
    format_research_report,
    run_fake_research,
)
from atlas.workflow.graph import (
    build_research_graph,
    default_fake_runtime_context,
    initial_graph_state,
)


def _sample_count(metrics: AtlasMetrics, metric_name: str, **labels: str) -> float:
    total = 0.0
    for family in metrics.registry.collect():
        for sample in family.samples:
            if sample.name != metric_name:
                continue
            if all(sample.labels.get(k) == v for k, v in labels.items()):
                total += sample.value
    return total


def _assert_report_structure(report: str, question: str) -> None:
    assert "Question:" in report
    assert question in report
    assert "Plan:" in report
    assert "Findings:" in report
    assert "Draft:" in report


def test_fakes_are_deterministic() -> None:
    question = "What is Atlas?"
    plan_a = build_research_plan(question)
    plan_b = build_research_plan(question)
    assert plan_a == plan_b
    assert len(plan_a) == 3
    findings = [run_fake_research(task) for task in plan_a]
    draft = build_draft(question=question, plan=plan_a, findings=findings)
    report_a = format_research_report(
        question=question,
        plan=plan_a,
        findings=findings,
        draft=draft,
    )
    report_b = format_research_report(
        question=question,
        plan=plan_a,
        findings=findings,
        draft=draft,
    )
    assert report_a == report_b
    _assert_report_structure(report_a, question)


def test_graph_completes_deterministically_with_memory_checkpointer() -> None:
    question = "Explain reliability"
    counters: dict[str, int] = {}
    context = default_fake_runtime_context(node_counters=counters)
    graph = build_research_graph(checkpointer=InMemorySaver())
    config: RunnableConfig = {"configurable": {"thread_id": "unit-job-1"}}
    first = graph.invoke(
        initial_graph_state(job_id="unit-job-1", question=question),
        config,
        context=context,
    )
    second_config: RunnableConfig = {"configurable": {"thread_id": "unit-job-2"}}
    second = graph.invoke(
        initial_graph_state(job_id="unit-job-1", question=question),
        second_config,
        context=context,
    )

    assert first["result"] == second["result"]
    _assert_report_structure(first["result"], question)
    assert counters["validate"] == 2
    assert counters["plan"] == 2
    assert counters["research"] == 2
    assert counters["draft"] == 2
    assert counters["verify_citations"] == 2
    assert counters["evaluate"] == 2
    assert counters["policy"] == 2
    assert counters["complete"] == 2
    assert "terminal" not in counters


def test_graph_node_execution_observes_workflow_node_metrics() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    context = default_fake_runtime_context(metrics=metrics)
    graph = build_research_graph(checkpointer=InMemorySaver())
    config: RunnableConfig = {"configurable": {"thread_id": "unit-job-metrics"}}

    graph.invoke(
        initial_graph_state(job_id="unit-job-metrics", question="Explain reliability"),
        config,
        context=context,
    )

    for node_name in ("validate", "plan", "research", "draft", "verify_citations"):
        assert (
            _sample_count(
                metrics,
                "atlas_workflow_node_executions_total",
                node_name=node_name,
                outcome="completed",
            )
            == 1
        ), node_name
    assert (
        _sample_count(
            metrics,
            "atlas_workflow_node_executions_total",
            node_name="complete",
            outcome="completed",
        )
        == 1
    )


def test_graph_node_failure_observes_failed_workflow_node_metric() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    context = default_fake_runtime_context(metrics=metrics)
    graph = build_research_graph(checkpointer=InMemorySaver())
    config: RunnableConfig = {"configurable": {"thread_id": "unit-job-metrics-fail"}}

    try:
        graph.invoke(
            initial_graph_state(job_id="", question="Explain reliability"),
            config,
            context=context,
        )
    except ValueError:
        pass

    assert (
        _sample_count(
            metrics,
            "atlas_workflow_node_executions_total",
            node_name="validate",
            outcome="failed",
        )
        == 1
    )
