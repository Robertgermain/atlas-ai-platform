"""Mocked Tavily httpx adapter contract tests (streaming bounds)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import httpx
import pytest

from atlas.tools.contracts import ToolCallContext, ToolOrigin
from atlas.tools.errors import (
    ToolAuthConfigError,
    ToolContentRejectedError,
    ToolRateLimitedError,
    ToolTimeoutError,
)
from atlas.tools.search_tavily import MAX_TAVILY_RESPONSE_BYTES, TavilyWebSearchTool


def _ctx() -> ToolCallContext:
    return ToolCallContext(
        origin=ToolOrigin.WORKFLOW,
        research_job_id="job",
        workflow_execution_id="exec",
        node_name="research",
    )


def _stream_client(
    *,
    status: int = 200,
    headers: dict[str, str] | None = None,
    chunks: list[bytes] | None = None,
    side_effect: Exception | None = None,
) -> MagicMock:
    client = MagicMock()
    if side_effect is not None:
        client.stream.side_effect = side_effect
        return client

    response = MagicMock()
    response.status_code = status
    response.headers = (
        headers if headers is not None else {"content-type": "application/json"}
    )
    response.iter_bytes.return_value = iter(chunks or [b"{}"])
    response.__enter__ = MagicMock(return_value=response)
    response.__exit__ = MagicMock(return_value=False)
    client.stream.return_value = response
    return client


def test_tavily_maps_results() -> None:
    payload = {
        "results": [
            {
                "title": "One",
                "url": "https://example.com/1",
                "content": "snippet one",
            }
        ]
    }
    body = json.dumps(payload).encode("utf-8")
    client = _stream_client(chunks=[body])
    tool = TavilyWebSearchTool(api_key="test-key", timeout_seconds=8.0, client=client)
    result = tool.invoke({"query": "atlas", "max_results": 1}, context=_ctx())
    assert result.output["hits"][0]["url"] == "https://example.com/1"
    assert client.stream.call_args.kwargs["headers"]["Authorization"].startswith(
        "Bearer "
    )
    assert "test-key" not in str(result.meta)


def test_tavily_rate_limit() -> None:
    client = _stream_client(status=429, chunks=[b"{}"])
    tool = TavilyWebSearchTool(api_key="test-key", timeout_seconds=8.0, client=client)
    with pytest.raises(ToolRateLimitedError):
        tool.invoke({"query": "atlas"}, context=_ctx())


def test_tavily_timeout() -> None:
    client = _stream_client(side_effect=httpx.TimeoutException("timeout"))
    tool = TavilyWebSearchTool(api_key="test-key", timeout_seconds=8.0, client=client)
    with pytest.raises(ToolTimeoutError):
        tool.invoke({"query": "atlas"}, context=_ctx())


def test_tavily_requires_key() -> None:
    with pytest.raises(ToolAuthConfigError):
        TavilyWebSearchTool(api_key="   ", timeout_seconds=8.0)


def test_tavily_rejects_oversized_streamed_body() -> None:
    client = _stream_client(chunks=[b"x" * 60_000, b"y" * MAX_TAVILY_RESPONSE_BYTES])
    tool = TavilyWebSearchTool(api_key="test-key", timeout_seconds=8.0, client=client)
    with pytest.raises(ToolContentRejectedError):
        tool.invoke({"query": "atlas"}, context=_ctx())


def test_tavily_rejects_oversized_content_length() -> None:
    client = _stream_client(
        headers={
            "content-type": "application/json",
            "content-length": str(MAX_TAVILY_RESPONSE_BYTES + 1),
        },
        chunks=[b"{}"],
    )
    tool = TavilyWebSearchTool(api_key="test-key", timeout_seconds=8.0, client=client)
    with pytest.raises(ToolContentRejectedError):
        tool.invoke({"query": "atlas"}, context=_ctx())


def test_tavily_rejects_missing_content_type() -> None:
    client = _stream_client(headers={}, chunks=[b'{"results":[]}'])
    tool = TavilyWebSearchTool(api_key="test-key", timeout_seconds=8.0, client=client)
    with pytest.raises(ToolContentRejectedError):
        tool.invoke({"query": "atlas"}, context=_ctx())


def test_tavily_rejects_non_json_content_type() -> None:
    client = _stream_client(
        headers={"content-type": "text/html"},
        chunks=[b"<html></html>"],
    )
    tool = TavilyWebSearchTool(api_key="test-key", timeout_seconds=8.0, client=client)
    with pytest.raises(ToolContentRejectedError):
        tool.invoke({"query": "atlas"}, context=_ctx())


def test_tavily_valid_json_within_limit_succeeds() -> None:
    payload = {
        "results": [{"title": "Ok", "url": "https://example.com/ok", "content": "c"}]
    }
    body = json.dumps(payload).encode("utf-8")
    client = _stream_client(
        headers={
            "content-type": "application/json",
            "content-length": str(len(body)),
        },
        chunks=[body[:10], body[10:]],
    )
    tool = TavilyWebSearchTool(api_key="test-key", timeout_seconds=8.0, client=client)
    result = tool.invoke({"query": "atlas", "max_results": 1}, context=_ctx())
    assert result.output["hits"][0]["title"] == "Ok"
