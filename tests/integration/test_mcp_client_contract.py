"""In-memory FastMCP Client contract test (actual MCP list/call path)."""

from __future__ import annotations

import asyncio

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from atlas.config.settings import Settings
from atlas.mcp.server import MCP_ACTOR_ID, configure_mcp_service, mcp
from atlas.persistence.db import session_scope
from atlas.tools.composition import build_live_registry, build_mcp_tool_service
from atlas.tools.contracts import ToolId, ToolProviderId
from atlas.tools.registry import default_permission_policy
from atlas.tools.service import ToolInvocationService


def test_mcp_list_and_call_web_search(
    session_factory: sessionmaker[Session],
) -> None:
    settings = Settings(tool_provider="fake", tool_fetch_enabled=False)
    service = build_mcp_tool_service(settings, session_factory=session_factory)
    configure_mcp_service(service)

    async def _run() -> None:
        from fastmcp import Client

        async with Client(mcp) as client:
            tools = await client.list_tools()
            names = {tool.name for tool in tools}
            assert "web_search" in names
            assert "fetch_url" in names

            result = await client.call_tool(
                "web_search",
                {"query": "Atlas MCP", "max_results": 2},
            )
            assert result is not None

    asyncio.run(_run())

    with session_scope(session_factory) as session:
        row = (
            session.execute(
                text(
                    """
                    SELECT origin, research_job_id, workflow_execution_id,
                           actor_id, status
                    FROM tool_invocations
                    WHERE origin = 'MCP'
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                )
            )
            .mappings()
            .one()
        )
    assert row["origin"] == "MCP"
    assert row["research_job_id"] is None
    assert row["workflow_execution_id"] is None
    assert row["actor_id"] == MCP_ACTOR_ID
    assert row["status"] == "SUCCEEDED"


def test_mcp_tool_schema_excludes_workflow_attribution(
    session_factory: sessionmaker[Session],
) -> None:
    settings = Settings(tool_provider="fake")
    configure_mcp_service(
        build_mcp_tool_service(settings, session_factory=session_factory)
    )

    async def _run() -> None:
        from fastmcp import Client

        async with Client(mcp) as client:
            tools = await client.list_tools()
            web = next(t for t in tools if t.name == "web_search")
            props = (web.inputSchema or {}).get("properties", {})
            assert "research_job_id" not in props
            assert "workflow_execution_id" not in props
            assert "actor_id" not in props
            assert "job_id" not in props

    asyncio.run(_run())


def test_mcp_disabled_fetch_under_live_config_is_protocol_error(
    session_factory: sessionmaker[Session],
) -> None:
    """Live tavily config must not return fake fetch content via MCP."""
    registry = build_live_registry(
        Settings(
            tool_provider="tavily",
            tool_fetch_enabled=False,
            tavily_api_key=SecretStr("test-key"),
        )
    )
    assert ToolId.FETCH_URL not in registry.list_ids()
    service = ToolInvocationService(
        session_factory=session_factory,
        registry=registry,
        policy=default_permission_policy(),
        provider_by_tool={ToolId.WEB_SEARCH: ToolProviderId.TAVILY},
        budgets=None,
    )
    configure_mcp_service(service)

    async def _run() -> None:
        from fastmcp import Client
        from fastmcp.exceptions import ToolError as McpToolError

        async with Client(mcp) as client:
            with pytest.raises(McpToolError) as exc_info:
                await client.call_tool(
                    "fetch_url",
                    {"url": "https://example.com/"},
                )
            message = str(exc_info.value)
            assert "ToolPermissionDeniedError" in message
            assert "example.com" not in message
            assert "sk-" not in message

    asyncio.run(_run())
