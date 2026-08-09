"""Atlas capability ports for research tools."""

from __future__ import annotations

from typing import Protocol

from atlas.tools.contracts import ToolCallContext, ToolId, ToolInvocationResult


class ResearchTool(Protocol):
    @property
    def tool_id(self) -> ToolId: ...

    def invoke(
        self,
        raw_input: dict[str, object],
        *,
        context: ToolCallContext,
    ) -> ToolInvocationResult: ...
