"""PostgreSQL restart-recovery test for LangGraph checkpoints."""

from __future__ import annotations

from langchain_core.runnables import RunnableConfig
from sqlalchemy.orm import Session, sessionmaker

from atlas.workflow.graph import (
    build_research_graph,
    default_fake_runtime_context,
    initial_graph_state,
)
from atlas.workflow.processor import (
    create_checkpoint_runtime,
)


def test_interrupt_after_plan_survives_full_runtime_disposal(
    test_database_url: str,
    session_factory: sessionmaker[Session],
) -> None:
    """Prove true restart recovery via interrupt_after and fresh B instances.

    session_factory is unused for SQLAlchemy work here but keeps the suite on the
    migrated Postgres fixture path.
    """
    del session_factory
    job_id = "resume-job-1"
    question = "How does Atlas recover?"
    config: RunnableConfig = {"configurable": {"thread_id": job_id}}
    counters: dict[str, int] = {}
    context = default_fake_runtime_context(node_counters=counters)

    runtime_a = create_checkpoint_runtime(test_database_url)
    try:
        graph_a = build_research_graph(
            checkpointer=runtime_a.checkpointer,
            interrupt_after=["plan"],
        )
        interrupted = graph_a.invoke(
            initial_graph_state(job_id=job_id, question=question),
            config,
            context=context,
        )
        snapshot_a = graph_a.get_state(config)
    finally:
        runtime_a.close()
        del graph_a
        del runtime_a

    assert interrupted["plan"]
    assert len(interrupted["plan"]) == 3
    assert interrupted["result"] == ""
    assert snapshot_a.next == ("research",)
    assert counters == {"validate": 1, "plan": 1}

    runtime_b = create_checkpoint_runtime(test_database_url)
    try:
        graph_b = build_research_graph(checkpointer=runtime_b.checkpointer)
        completed = graph_b.invoke(None, config, context=context)
        snapshot_b = graph_b.get_state(config)
    finally:
        runtime_b.close()

    assert snapshot_b.next == ()
    assert "Question:" in completed["result"]
    assert "Plan:" in completed["result"]
    assert "Findings:" in completed["result"]
    assert "Draft:" in completed["result"]
    assert question in completed["result"]
    assert counters["validate"] == 1
    assert counters["plan"] == 1
    assert counters["research"] == 1
    assert counters["draft"] == 1
    assert counters["complete"] == 1
