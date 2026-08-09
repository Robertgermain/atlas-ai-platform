"""Typed contracts for Atlas governed research tools."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, field_validator

TOOL_POLICY_VERSION = "2026-08-09.tools.v1"
TOOL_VERSION = "tools.v1"
MAX_FINDING_BYTES = 4096
UNTRUSTED_SOURCE_LABEL = "[untrusted_source]"


class ToolId(StrEnum):
    WEB_SEARCH = "web_search"
    FETCH_URL = "fetch_url"


class ToolOrigin(StrEnum):
    WORKFLOW = "WORKFLOW"
    MCP = "MCP"


class ToolProviderId(StrEnum):
    FAKE = "fake"
    TAVILY = "tavily"
    HTTPX = "httpx"


class ToolRetryClass(StrEnum):
    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"
    TEMPORARY = "temporary"
    AUTH_CONFIG = "auth_config"
    INVALID_REQUEST = "invalid_request"
    PERMISSION_DENIED = "permission_denied"
    SSRF_BLOCKED = "ssrf_blocked"
    CONTENT_REJECTED = "content_rejected"
    BUDGET_EXHAUSTED = "budget_exhausted"
    UNKNOWN = "unknown"
    NONE = "none"


class WebSearchInput(BaseModel):
    query: Annotated[str, Field(min_length=1, max_length=500)]
    max_results: Annotated[int, Field(default=3, ge=1, le=5)] = 3

    @field_validator("query")
    @classmethod
    def strip_query(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("query must be non-empty")
        return cleaned


class WebSearchHit(BaseModel):
    title: Annotated[str, Field(max_length=300)]
    url: Annotated[str, Field(max_length=2000)]
    snippet: Annotated[str, Field(max_length=500)]


class WebSearchOutput(BaseModel):
    hits: Annotated[list[WebSearchHit], Field(max_length=5)]


class FetchUrlInput(BaseModel):
    url: Annotated[str, Field(min_length=1, max_length=2000)]

    @field_validator("url")
    @classmethod
    def strip_url(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("url must be non-empty")
        return cleaned


class FetchUrlOutput(BaseModel):
    final_url: Annotated[str, Field(max_length=2000)]
    content_type: Annotated[str, Field(max_length=128)]
    text: Annotated[str, Field(max_length=50_000)]
    byte_length: Annotated[int, Field(ge=0)]
    truncated: bool = False


class ToolResultMeta(BaseModel):
    tool_id: ToolId
    provider: ToolProviderId
    tool_version: str
    tool_policy_version: str
    latency_ms: int
    status: Literal["succeeded", "failed"] = "succeeded"
    retry_class: ToolRetryClass = ToolRetryClass.NONE
    content_digest: str | None = None
    byte_length: int | None = None


class ToolCallContext(BaseModel):
    """Trusted attribution stamped by composition — never taken from MCP args."""

    origin: ToolOrigin
    tool_version: str = TOOL_VERSION
    tool_policy_version: str = TOOL_POLICY_VERSION
    actor_id: str | None = None
    research_job_id: str | None = None
    workflow_execution_id: str | None = None
    node_name: str | None = None
    workflow_node_attempt: int | None = None


class ToolInvocationResult(BaseModel):
    output: dict[str, Any]
    meta: ToolResultMeta
    finding_text: str
    invocation_id: str | None = None
