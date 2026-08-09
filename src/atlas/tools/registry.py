"""Tool registry and permission policies."""

from __future__ import annotations

from atlas.tools.contracts import ToolId, ToolOrigin
from atlas.tools.errors import ToolPermissionDeniedError
from atlas.tools.ports import ResearchTool


class ToolRegistry:
    """Maps approved tool ids to ResearchTool implementations."""

    def __init__(self, tools: dict[ToolId, ResearchTool]) -> None:
        self._tools = dict(tools)

    def get(self, tool_id: ToolId) -> ResearchTool:
        try:
            return self._tools[tool_id]
        except KeyError as exc:
            raise ToolPermissionDeniedError("tool is not registered") from exc

    def list_ids(self) -> list[ToolId]:
        return sorted(self._tools.keys(), key=lambda item: item.value)


class NodePermissionPolicy:
    """Per-origin / per-node allowlists for governed tools."""

    def __init__(
        self,
        *,
        workflow_node_tools: dict[str, frozenset[ToolId]],
        mcp_tools: frozenset[ToolId],
    ) -> None:
        self._workflow_node_tools = dict(workflow_node_tools)
        self._mcp_tools = frozenset(mcp_tools)

    def assert_allowed(
        self,
        *,
        origin: ToolOrigin,
        tool_id: ToolId,
        node_name: str | None,
    ) -> None:
        if origin is ToolOrigin.MCP:
            if tool_id not in self._mcp_tools:
                raise ToolPermissionDeniedError("tool not allowed for MCP origin")
            return
        if origin is ToolOrigin.WORKFLOW:
            if node_name is None:
                raise ToolPermissionDeniedError("workflow tool calls require node_name")
            allowed = self._workflow_node_tools.get(node_name, frozenset())
            if tool_id not in allowed:
                raise ToolPermissionDeniedError(
                    "tool not allowed for this workflow node"
                )
            return
        raise ToolPermissionDeniedError("unknown tool origin")


def default_permission_policy() -> NodePermissionPolicy:
    research_tools = frozenset({ToolId.WEB_SEARCH, ToolId.FETCH_URL})
    return NodePermissionPolicy(
        workflow_node_tools={
            "research": research_tools,
            "validate": frozenset(),
            "plan": frozenset(),
            "draft": frozenset(),
            "complete": frozenset(),
        },
        mcp_tools=research_tools,
    )
