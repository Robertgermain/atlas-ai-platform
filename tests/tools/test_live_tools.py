"""Opt-in live tool tests (skipped unless explicitly enabled)."""

from __future__ import annotations

import os

import pytest

from atlas.config.settings import Settings
from atlas.tools.composition import build_live_registry
from atlas.tools.contracts import ToolCallContext, ToolId, ToolOrigin

pytestmark = pytest.mark.skipif(
    os.environ.get("ATLAS_ENABLE_LIVE_TOOL_TESTS") != "1",
    reason="Live tool tests require ATLAS_ENABLE_LIVE_TOOL_TESTS=1",
)


def test_live_tavily_search() -> None:
    settings = Settings()
    if (
        settings.tavily_api_key is None
        or not settings.tavily_api_key.get_secret_value()
    ):
        pytest.skip("ATLAS_TAVILY_API_KEY not configured")
    registry = build_live_registry(
        Settings(
            tool_provider="tavily",
            tool_fetch_enabled=False,
            tavily_api_key=settings.tavily_api_key,
        )
    )
    tool = registry.get(ToolId.WEB_SEARCH)
    result = tool.invoke(
        {"query": "OpenAI", "max_results": 1},
        context=ToolCallContext(
            origin=ToolOrigin.WORKFLOW,
            research_job_id="live-job",
            workflow_execution_id="live-exec",
            node_name="research",
        ),
    )
    assert result.output.get("hits")
