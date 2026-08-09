"""FastMCP stdio server exposing Atlas governed tools."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError as McpToolError

from atlas.config.settings import Settings, get_settings
from atlas.persistence.db import get_engine, get_session_factory
from atlas.tools.composition import build_mcp_tool_service
from atlas.tools.contracts import (
    TOOL_POLICY_VERSION,
    TOOL_VERSION,
    ToolCallContext,
    ToolId,
    ToolOrigin,
)
from atlas.tools.errors import ToolError, sanitize_tool_error
from atlas.tools.service import ToolInvocationService

# Per-process actor identity — never accepted from tool arguments.
MCP_ACTOR_ID = str(uuid4())

# mask_error_details=True: only explicit McpToolError messages reach clients.
mcp = FastMCP("atlas-tools", mask_error_details=True)


def _service_from_settings(settings: Settings | None = None) -> ToolInvocationService:
    cfg = settings or get_settings()
    engine = get_engine(cfg.database_url)
    session_factory = get_session_factory(engine)
    return build_mcp_tool_service(cfg, session_factory=session_factory)


_SERVICE: ToolInvocationService | None = None


def get_mcp_service() -> ToolInvocationService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = _service_from_settings()
    return _SERVICE


def configure_mcp_service(service: ToolInvocationService) -> None:
    """Test helper to inject a fake/ledger service without touching env secrets."""
    global _SERVICE
    _SERVICE = service


def _mcp_context() -> ToolCallContext:
    return ToolCallContext(
        origin=ToolOrigin.MCP,
        tool_version=TOOL_VERSION,
        tool_policy_version=TOOL_POLICY_VERSION,
        actor_id=MCP_ACTOR_ID,
    )


def _raise_mcp_error(exc: ToolError) -> None:
    """Surface a controlled Atlas failure as an MCP protocol tool error."""
    raise McpToolError(sanitize_tool_error(exc)) from None


@mcp.tool(name="web_search")
def web_search(query: str, max_results: int = 3) -> dict[str, Any]:
    """Search the web through Atlas-governed policy (MCP origin)."""
    try:
        result = get_mcp_service().invoke(
            tool_id=ToolId.WEB_SEARCH,
            raw_input={"query": query, "max_results": max_results},
            context=_mcp_context(),
        )
    except ToolError as exc:
        _raise_mcp_error(exc)
    return {
        "output": result.output,
        "finding_text": result.finding_text,
        "meta": result.meta.model_dump(mode="json"),
    }


@mcp.tool(name="fetch_url")
def fetch_url(url: str) -> dict[str, Any]:
    """Fetch a URL through Atlas-governed policy (MCP origin).

    Under live ``tavily`` configuration without a safe fetch transport, the
    tool remains listed but fails as an MCP error (never returns fake content).
    """
    try:
        result = get_mcp_service().invoke(
            tool_id=ToolId.FETCH_URL,
            raw_input={"url": url},
            context=_mcp_context(),
        )
    except ToolError as exc:
        _raise_mcp_error(exc)
    return {
        "output": result.output,
        "finding_text": result.finding_text,
        "meta": result.meta.model_dump(mode="json"),
    }


def run_stdio() -> None:
    """Run the FastMCP server over stdio."""
    mcp.run(transport="stdio")
