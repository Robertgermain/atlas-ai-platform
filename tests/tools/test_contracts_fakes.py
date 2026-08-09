"""Unit tests for tool contracts, fakes, budgets, and permissions."""

from __future__ import annotations

import pytest

from atlas.tools.contracts import (
    MAX_FINDING_BYTES,
    UNTRUSTED_SOURCE_LABEL,
    ToolCallContext,
    ToolId,
    ToolOrigin,
)
from atlas.tools.errors import (
    ToolBudgetExhaustedError,
    ToolPermissionDeniedError,
    ToolSsrfBlockedError,
    sanitize_tool_error,
)
from atlas.tools.fakes import FakeFetchUrlTool, FakeWebSearchTool, project_finding_text
from atlas.tools.registry import default_permission_policy
from atlas.tools.service import ToolBudgets


def _workflow_context() -> ToolCallContext:
    return ToolCallContext(
        origin=ToolOrigin.WORKFLOW,
        research_job_id="job-1",
        workflow_execution_id="exec-1",
        node_name="research",
    )


def test_fake_web_search_is_deterministic() -> None:
    tool = FakeWebSearchTool()
    ctx = _workflow_context()
    first = tool.invoke({"query": "Atlas reliability", "max_results": 2}, context=ctx)
    second = tool.invoke({"query": "Atlas reliability", "max_results": 2}, context=ctx)
    assert first.output == second.output
    assert first.finding_text.startswith(UNTRUSTED_SOURCE_LABEL)
    assert first.meta.tool_id is ToolId.WEB_SEARCH


def test_fake_fetch_rejects_bad_scheme() -> None:
    tool = FakeFetchUrlTool()
    with pytest.raises(ToolSsrfBlockedError):
        tool.invoke({"url": "file:///etc/passwd"}, context=_workflow_context())


def test_project_finding_respects_byte_cap() -> None:
    body = "x" * (MAX_FINDING_BYTES + 100)
    projected = project_finding_text(body)
    assert len(projected.encode("utf-8")) <= MAX_FINDING_BYTES
    assert projected.startswith(UNTRUSTED_SOURCE_LABEL)


def test_permission_policy_denies_plan_node_tools() -> None:
    policy = default_permission_policy()
    with pytest.raises(ToolPermissionDeniedError):
        policy.assert_allowed(
            origin=ToolOrigin.WORKFLOW,
            tool_id=ToolId.WEB_SEARCH,
            node_name="plan",
        )


def test_budget_exhausted_raises_controlled_error() -> None:
    budgets = ToolBudgets(
        max_logical_calls=1,
        max_attempts_per_call=2,
        attempt_timeout_seconds=8.0,
        node_deadline_seconds=45.0,
    )
    budgets.assert_can_start_logical_call()
    budgets.record_logical_call()
    with pytest.raises(ToolBudgetExhaustedError):
        budgets.assert_can_start_logical_call()


def test_sanitize_tool_error_is_class_only() -> None:
    err = ToolBudgetExhaustedError("secret query and key=sk-abc")
    text = sanitize_tool_error(err)
    assert "sk-abc" not in text
    assert "secret query" not in text
    assert "ToolBudgetExhaustedError" in text
