"""Opt-in live LangSmith trace verification (never enabled in CI).

Requires ``ATLAS_ENABLE_LIVE_LANGSMITH_TESTS=1`` and ``ATLAS_LANGSMITH_API_KEY``.
Uses fake Atlas model/tool/embedding providers only. Unique traces are
retained in the configured LangSmith project for manual cleanup.

Network-free unit tests in this module cover bounded polling until a
trace is complete enough to validate; they do not call hosted LangSmith.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, Literal
from uuid import uuid4

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver

from atlas.config.settings import Settings
from atlas.models.fakes import (
    DeterministicResearchDrafter,
    DeterministicResearchPlanner,
)
from atlas.observability.langsmith import (
    FLUSH_BOUND_SECONDS,
    TracedResearchDrafter,
    TracedResearchPlanner,
    attach_run_metadata,
    configure_langsmith,
    reset_langsmith_for_tests,
    trace_research_job,
)
from atlas.specialists.planner import BoundedPlannerSpecialist
from atlas.specialists.synthesizer import BoundedReportSynthesizer
from atlas.workflow.graph import (
    build_research_graph,
    default_fake_runtime_context,
    initial_graph_state,
)

_LIVE_UNARMED = (
    os.environ.get("ATLAS_ENABLE_LIVE_LANGSMITH_TESTS") != "1"
    or not (os.environ.get("ATLAS_LANGSMITH_API_KEY") or "").strip()
)
_LIVE_SKIP_REASON = (
    "Live LangSmith tests require ATLAS_ENABLE_LIVE_LANGSMITH_TESTS=1 "
    "and ATLAS_LANGSMITH_API_KEY"
)

_LIVE_QUERY_DEADLINE_SECONDS = 30.0
_LIVE_QUERY_INTERVAL_SECONDS = 1.0
_RUN_SELECT = (
    "id",
    "name",
    "run_type",
    "trace_id",
    "parent_run_id",
    "inputs",
    "outputs",
    "extra",
)
_NATIVE_GRAPH_NODES = (
    "validate",
    "plan",
    "research",
    "draft",
    "verify_citations",
    "evaluate",
    "policy",
    "complete",
)
_EXPLICIT_BOUNDARIES = ("model.plan", "model.draft")
_REQUIRED_RUN_NAMES = frozenset(
    {
        "atlas.research_job",
        "atlas.research_graph",
        *_NATIVE_GRAPH_NODES,
        *_EXPLICIT_BOUNDARIES,
    }
)
_EXPECTED_PARENT_NAME = {
    "atlas.research_graph": "atlas.research_job",
    **{name: "atlas.research_graph" for name in _NATIVE_GRAPH_NODES},
    "model.plan": "plan",
    "model.draft": "draft",
}
_CANARIES = (
    "sk-",
    "lsv2_",
    "hunter2",
    "ATLAS_LANGSMITH_API_KEY",
)
_MSG_TIMEOUT = "timed out waiting for a complete live trace"
_MSG_DUPLICATE = "duplicate run name in the live trace"
_MSG_OUT_OF_TRACE_PARENT = "child run parent is outside this trace"
_MSG_INVALID_HIERARCHY = "live trace hierarchy is invalid"

TraceReadiness = Literal["complete", "incomplete", "invalid"]


def _run_metadata(run: object) -> dict[str, object]:
    extra = getattr(run, "extra", None) or {}
    merged: dict[str, object] = {}
    if isinstance(extra, Mapping):
        nested = extra.get("metadata")
        if isinstance(nested, Mapping):
            merged.update(dict(nested))
    direct = getattr(run, "metadata", None)
    if isinstance(direct, Mapping):
        merged.update(dict(direct))
    return merged


def _walk_strings(value: object) -> Iterator[str]:
    if value is None:
        return
    if isinstance(value, str):
        yield value
        return
    if isinstance(value, (bool, int, float)):
        yield str(value)
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            yield str(key)
            yield from _walk_strings(item)
        return
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        for item in value:
            yield from _walk_strings(item)
        return
    dumped = getattr(value, "model_dump", None)
    if callable(dumped):
        yield from _walk_strings(dumped())
        return
    raw = getattr(value, "__dict__", None)
    if isinstance(raw, dict) and not isinstance(value, type):
        yield from _walk_strings(raw)


def _exported_payload(run: object) -> dict[str, object]:
    return {
        "inputs": getattr(run, "inputs", None),
        "outputs": getattr(run, "outputs", None),
        "metadata": _run_metadata(run),
    }


def _payload_contains(payload: object, needle: str) -> bool:
    for text in _walk_strings(payload):
        if needle in text:
            return True
    return False


def _ensure_live_project(client: Any, project_name: str) -> None:
    """Create the live project when missing. Other SDK failures propagate."""
    from langsmith.utils import LangSmithConflictError, LangSmithNotFoundError

    try:
        client.read_project(project_name=project_name)
    except LangSmithNotFoundError:
        try:
            client.create_project(project_name=project_name)
        except LangSmithConflictError:
            client.read_project(project_name=project_name)


def _wait_for_unique_root(
    client: Any,
    *,
    project_name: str,
    live_test_id: str,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    deadline_seconds: float = _LIVE_QUERY_DEADLINE_SECONDS,
    interval_seconds: float = _LIVE_QUERY_INTERVAL_SECONDS,
) -> Any:
    from langsmith.utils import LangSmithNotFoundError

    list_runs = client.list_runs
    metadata_filter = f'has(metadata, \'{{"atlas.live_test_id": "{live_test_id}"}}\')'
    deadline = monotonic() + deadline_seconds
    while True:
        try:
            roots = list(
                list_runs(
                    project_name=project_name,
                    is_root=True,
                    filter=metadata_filter,
                    select=list(_RUN_SELECT),
                    limit=20,
                )
            )
        except LangSmithNotFoundError:
            roots = []
        matched = [
            run
            for run in roots
            if _run_metadata(run).get("atlas.live_test_id") == live_test_id
        ]
        if len(matched) == 1:
            return matched[0]
        if len(matched) > 1:
            pytest.fail("expected exactly one live root for this invocation")
        if monotonic() >= deadline:
            pytest.fail("timed out waiting for the unique live root")
        sleep(interval_seconds)


def _trace_readiness(
    runs: Sequence[Any], *, trace_id: object
) -> tuple[TraceReadiness, str]:
    """Classify a list_runs snapshot without printing payloads or ids."""
    by_id: dict[str, Any] = {}
    by_name: dict[str, Any] = {}
    for run in runs:
        run_id = str(getattr(run, "id", ""))
        by_id[run_id] = run
        if str(getattr(run, "trace_id", None)) != str(trace_id):
            continue
        name = getattr(run, "name", None)
        if not isinstance(name, str):
            continue
        if name in by_name:
            return "invalid", _MSG_DUPLICATE
        by_name[name] = run

    if not _REQUIRED_RUN_NAMES <= set(by_name):
        return "incomplete", ""

    for run in runs:
        if str(getattr(run, "trace_id", None)) != str(trace_id):
            continue
        name = getattr(run, "name", None)
        parent_id = getattr(run, "parent_run_id", None)
        if name == "atlas.research_job":
            if parent_id is not None:
                return "invalid", _MSG_INVALID_HIERARCHY
            continue
        if parent_id is None:
            if isinstance(name, str) and name in _REQUIRED_RUN_NAMES:
                return "incomplete", ""
            continue
        parent = by_id.get(str(parent_id))
        if parent is None:
            return "incomplete", ""
        if str(getattr(parent, "trace_id", None)) != str(trace_id):
            return "invalid", _MSG_OUT_OF_TRACE_PARENT
        expected = _EXPECTED_PARENT_NAME.get(name) if isinstance(name, str) else None
        if expected is not None and getattr(parent, "name", None) != expected:
            return "invalid", _MSG_INVALID_HIERARCHY
    return "complete", ""


def _runs_for_trace(
    client: Any,
    *,
    project_name: str,
    trace_id: object,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    deadline_seconds: float = _LIVE_QUERY_DEADLINE_SECONDS,
    interval_seconds: float = _LIVE_QUERY_INTERVAL_SECONDS,
) -> list[Any]:
    """Poll ``list_runs(trace_id=...)`` until the hierarchy is complete enough."""
    list_runs = client.list_runs
    deadline = monotonic() + deadline_seconds
    while True:
        runs = list(
            list_runs(
                project_name=project_name,
                trace_id=trace_id,
                select=list(_RUN_SELECT),
                limit=100,
            )
        )
        status, message = _trace_readiness(runs, trace_id=trace_id)
        if status == "complete":
            return runs
        if status == "invalid":
            pytest.fail(message)
        if monotonic() >= deadline:
            pytest.fail(_MSG_TIMEOUT)
        sleep(interval_seconds)


def _assert_expected_hierarchy(runs: Sequence[Any], *, trace_id: object) -> None:
    status, message = _trace_readiness(runs, trace_id=trace_id)
    if status != "complete":
        pytest.fail(message or _MSG_INVALID_HIERARCHY)


@pytest.mark.skipif(_LIVE_UNARMED, reason=_LIVE_SKIP_REASON)
def test_live_fake_graph_trace_appears_with_allowlisted_metadata_only() -> None:
    reset_langsmith_for_tests()
    live_test_id = f"live-{uuid4().hex}"
    settings = Settings()
    handle = None
    question = f"Explain reliability hunter2 {live_test_id}"
    try:
        handle = configure_langsmith(settings)
        if not handle.enabled or handle.client is None:
            pytest.fail("LangSmith client did not initialize")
        client = handle.client
        _ensure_live_project(client, settings.langsmith_project)
        context = default_fake_runtime_context()
        context = replace(
            context,
            planner_specialist=BoundedPlannerSpecialist(
                TracedResearchPlanner(DeterministicResearchPlanner())
            ),
            synthesizer=BoundedReportSynthesizer(
                drafter=TracedResearchDrafter(DeterministicResearchDrafter()),
            ),
        )
        graph = build_research_graph(checkpointer=InMemorySaver())
        config: RunnableConfig = {
            "configurable": {"thread_id": live_test_id},
            "run_name": "atlas.research_graph",
            "tags": ["atlas", "research-job", "live-test"],
            "metadata": {"atlas.live_test_id": live_test_id},
        }

        def _invoke() -> object:
            attach_run_metadata({"atlas.live_test_id": live_test_id})
            return graph.invoke(
                initial_graph_state(job_id=live_test_id, question=question),
                config,
                context=context,
            )

        trace_research_job(
            job_id=live_test_id,
            workflow_execution_id=live_test_id,
            fn=_invoke,
        )
        assert FLUSH_BOUND_SECONDS == 5.0

        root = _wait_for_unique_root(
            client,
            project_name=settings.langsmith_project,
            live_test_id=live_test_id,
        )
        assert root.name == "atlas.research_job"
        trace_id = root.trace_id
        runs = _runs_for_trace(
            client,
            project_name=settings.langsmith_project,
            trace_id=trace_id,
        )
        _assert_expected_hierarchy(runs, trace_id=trace_id)
        for run in runs:
            payload = _exported_payload(run)
            for canary in _CANARIES:
                assert not _payload_contains(payload.get("inputs"), canary)
                assert not _payload_contains(payload.get("outputs"), canary)
            assert not _payload_contains(payload.get("inputs"), question)
            assert not _payload_contains(payload.get("outputs"), question)
            metadata = payload["metadata"]
            assert isinstance(metadata, dict)
            assert metadata.get("atlas.live_test_id") == live_test_id
            for canary in _CANARIES:
                assert not _payload_contains(metadata, canary)
            assert not _payload_contains(metadata, question)
    finally:
        if handle is not None:
            handle.close()
        reset_langsmith_for_tests()


@dataclass
class _FakeRun:
    id: str
    name: str
    trace_id: str
    parent_run_id: str | None = None
    extra: dict[str, object] | None = None


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps = 0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps += 1
        self.now += seconds


class _ScriptedClient:
    def __init__(self, batches: Sequence[Sequence[_FakeRun]]) -> None:
        self._batches = [list(batch) for batch in batches]
        self.calls = 0

    def list_runs(self, **_kwargs: object) -> Iterator[_FakeRun]:
        index = min(self.calls, len(self._batches) - 1)
        self.calls += 1
        return iter(self._batches[index])


def _complete_trace_runs(trace_id: str = "trace-1") -> list[_FakeRun]:
    root = _FakeRun(id="root", name="atlas.research_job", trace_id=trace_id)
    graph = _FakeRun(
        id="graph",
        name="atlas.research_graph",
        trace_id=trace_id,
        parent_run_id="root",
    )
    nodes = [
        _FakeRun(
            id=f"node-{name}",
            name=name,
            trace_id=trace_id,
            parent_run_id="graph",
        )
        for name in _NATIVE_GRAPH_NODES
    ]
    plan_model = _FakeRun(
        id="model-plan",
        name="model.plan",
        trace_id=trace_id,
        parent_run_id="node-plan",
    )
    draft_model = _FakeRun(
        id="model-draft",
        name="model.draft",
        trace_id=trace_id,
        parent_run_id="node-draft",
    )
    return [root, graph, *nodes, plan_model, draft_model]


def test_runs_for_trace_polls_until_incomplete_snapshot_becomes_complete() -> None:
    complete = _complete_trace_runs()
    client = _ScriptedClient([[complete[0]], complete])
    clock = _Clock()
    runs = _runs_for_trace(
        client,
        project_name="atlas-local",
        trace_id="trace-1",
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        deadline_seconds=5.0,
        interval_seconds=1.0,
    )
    assert client.calls == 2
    assert clock.sleeps == 1
    assert {run.name for run in runs} >= _REQUIRED_RUN_NAMES


def test_runs_for_trace_times_out_on_permanently_incomplete_trace() -> None:
    client = _ScriptedClient([[_complete_trace_runs()[0]]])
    clock = _Clock()
    with pytest.raises(pytest.fail.Exception, match=_MSG_TIMEOUT):
        _runs_for_trace(
            client,
            project_name="atlas-local",
            trace_id="trace-1",
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            deadline_seconds=2.0,
            interval_seconds=1.0,
        )
    assert clock.sleeps >= 1
    assert client.calls >= 2


def test_runs_for_trace_fails_closed_on_out_of_trace_parent() -> None:
    runs = _complete_trace_runs()
    outsider = _FakeRun(
        id="outsider",
        name="other-trace-run",
        trace_id="other-trace",
        parent_run_id=None,
    )
    plan = next(run for run in runs if run.name == "model.plan")
    plan.parent_run_id = "outsider"
    client = _ScriptedClient([[*runs, outsider]])
    clock = _Clock()
    with pytest.raises(pytest.fail.Exception, match=_MSG_OUT_OF_TRACE_PARENT):
        _runs_for_trace(
            client,
            project_name="atlas-local",
            trace_id="trace-1",
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            deadline_seconds=5.0,
            interval_seconds=1.0,
        )
    assert clock.sleeps == 0
    assert client.calls == 1


class _NotFoundThenRootClient:
    def __init__(self, root: _FakeRun) -> None:
        self._root = root
        self.calls = 0

    def list_runs(self, **_kwargs: object) -> Iterator[_FakeRun]:
        from langsmith.utils import LangSmithNotFoundError

        self.calls += 1
        if self.calls == 1:
            raise LangSmithNotFoundError("Project missing")
        return iter([self._root])


def test_wait_for_unique_root_polls_through_missing_project() -> None:
    live_test_id = "live-unit-1"
    root = _FakeRun(
        id="root",
        name="atlas.research_job",
        trace_id="trace-1",
        extra={"metadata": {"atlas.live_test_id": live_test_id}},
    )
    client = _NotFoundThenRootClient(root)
    clock = _Clock()
    found = _wait_for_unique_root(
        client,
        project_name="atlas-local",
        live_test_id=live_test_id,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        deadline_seconds=5.0,
        interval_seconds=1.0,
    )
    assert found is root
    assert client.calls == 2
    assert clock.sleeps == 1


def test_wait_for_unique_root_does_not_swallow_non_not_found_errors() -> None:
    class _Boom:
        def list_runs(self, **_kwargs: object) -> Iterator[_FakeRun]:
            raise RuntimeError("auth-failed")

    clock = _Clock()
    with pytest.raises(RuntimeError, match="auth-failed"):
        _wait_for_unique_root(
            _Boom(),
            project_name="atlas-local",
            live_test_id="live-unit-2",
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            deadline_seconds=5.0,
            interval_seconds=1.0,
        )
    assert clock.sleeps == 0
