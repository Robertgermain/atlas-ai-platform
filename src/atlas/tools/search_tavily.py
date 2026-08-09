"""Tavily web search via direct httpx (no tavily-python SDK)."""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any

import httpx

from atlas.tools.contracts import (
    TOOL_POLICY_VERSION,
    TOOL_VERSION,
    ToolCallContext,
    ToolId,
    ToolInvocationResult,
    ToolProviderId,
    ToolResultMeta,
    ToolRetryClass,
    WebSearchHit,
    WebSearchInput,
    WebSearchOutput,
)
from atlas.tools.errors import (
    ToolAuthConfigError,
    ToolContentRejectedError,
    ToolInvalidRequestError,
    ToolRateLimitedError,
    ToolTemporaryError,
    ToolTimeoutError,
    ToolUnknownError,
)
from atlas.tools.fakes import project_finding_text

TAVILY_SEARCH_URL = "https://api.tavily.com/search"
MAX_TAVILY_RESPONSE_BYTES = 200_000
_ACCEPTED_JSON_TYPES = frozenset({"application/json", "text/json"})


def _require_json_content_type(headers: httpx.Headers) -> None:
    raw = headers.get("content-type")
    if raw is None or not str(raw).strip():
        raise ToolContentRejectedError("Tavily response content-type missing")
    media = str(raw).split(";", 1)[0].strip().lower()
    if media not in _ACCEPTED_JSON_TYPES:
        raise ToolContentRejectedError("Tavily response content-type rejected")


def _reject_oversized_content_length(headers: httpx.Headers) -> None:
    raw = headers.get("content-length")
    if raw is None:
        return
    try:
        declared = int(str(raw).strip())
    except ValueError as exc:
        raise ToolContentRejectedError("Tavily Content-Length invalid") from exc
    if declared > MAX_TAVILY_RESPONSE_BYTES:
        raise ToolContentRejectedError("Tavily Content-Length exceeded size limit")


def _stream_body(response: httpx.Response, *, max_bytes: int) -> bytes:
    """Read streamed bytes, stopping as soon as ``max_bytes`` is exceeded."""
    chunks: list[bytes] = []
    total = 0
    for chunk in response.iter_bytes():
        if not chunk:
            continue
        total += len(chunk)
        if total > max_bytes:
            raise ToolContentRejectedError("Tavily response exceeded size limit")
        chunks.append(chunk)
    return b"".join(chunks)


def _translate_status(status_code: int) -> None:
    if status_code in {401, 403}:
        raise ToolAuthConfigError("Tavily authentication failed")
    if status_code == 429:
        raise ToolRateLimitedError("Tavily rate limited")
    if status_code >= 500:
        raise ToolTemporaryError("Tavily upstream error")
    if status_code >= 400:
        raise ToolInvalidRequestError("Tavily rejected request")


class TavilyWebSearchTool:
    """Live web_search backed by Tavily's documented HTTP search API."""

    def __init__(
        self,
        *,
        api_key: str,
        timeout_seconds: float,
        client: httpx.Client | None = None,
    ) -> None:
        if not api_key.strip():
            raise ToolAuthConfigError("Tavily API key is required")
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds
        self._client = client

    @property
    def tool_id(self) -> ToolId:
        return ToolId.WEB_SEARCH

    def invoke(
        self,
        raw_input: dict[str, object],
        *,
        context: ToolCallContext,
    ) -> ToolInvocationResult:
        del context
        started = time.perf_counter()
        try:
            parsed = WebSearchInput.model_validate(raw_input)
        except Exception as exc:
            raise ToolInvalidRequestError("invalid web_search input") from exc

        payload = {
            "query": parsed.query,
            "max_results": parsed.max_results,
            "search_depth": "basic",
            "include_answer": False,
        }
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        try:
            body = self._post_bounded(payload=payload, headers=headers)
        except (
            ToolAuthConfigError,
            ToolContentRejectedError,
            ToolInvalidRequestError,
            ToolRateLimitedError,
            ToolTemporaryError,
            ToolTimeoutError,
            ToolUnknownError,
        ):
            raise
        except httpx.TimeoutException as exc:
            raise ToolTimeoutError("Tavily search timed out") from exc
        except httpx.HTTPError as exc:
            raise ToolTemporaryError("Tavily HTTP error") from exc

        try:
            data_obj: Any = json.loads(body.decode("utf-8"))
        except Exception as exc:
            raise ToolUnknownError("Tavily response was not JSON") from exc
        if not isinstance(data_obj, dict):
            raise ToolUnknownError("Tavily response was not a JSON object")
        data: dict[str, Any] = data_obj

        raw_results = data.get("results")
        if not isinstance(raw_results, list):
            raise ToolUnknownError("Tavily response missing results")

        hits: list[WebSearchHit] = []
        for item in raw_results[: parsed.max_results]:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "")[:300]
            url = str(item.get("url") or "")[:2000]
            snippet = str(item.get("content") or item.get("snippet") or "")[:500]
            if not url:
                continue
            hits.append(WebSearchHit(title=title or url, url=url, snippet=snippet))

        output = WebSearchOutput(hits=hits)
        finding = project_finding_text(
            f"search:{parsed.query} | "
            + " ; ".join(f"{h.title} ({h.url}) — {h.snippet}" for h in hits)
        )
        latency_ms = int((time.perf_counter() - started) * 1000)
        return ToolInvocationResult(
            output=output.model_dump(mode="json"),
            meta=ToolResultMeta(
                tool_id=ToolId.WEB_SEARCH,
                provider=ToolProviderId.TAVILY,
                tool_version=TOOL_VERSION,
                tool_policy_version=TOOL_POLICY_VERSION,
                latency_ms=latency_ms,
                status="succeeded",
                retry_class=ToolRetryClass.NONE,
                content_digest=hashlib.sha256(finding.encode("utf-8")).hexdigest(),
                byte_length=len(finding.encode("utf-8")),
            ),
            finding_text=finding,
        )

    def _post_bounded(
        self,
        *,
        payload: dict[str, Any],
        headers: dict[str, str],
    ) -> bytes:
        if self._client is not None:
            with self._client.stream(
                "POST",
                TAVILY_SEARCH_URL,
                json=payload,
                headers=headers,
                timeout=self._timeout_seconds,
            ) as response:
                return self._consume_response(response)

        with httpx.Client(
            timeout=self._timeout_seconds,
            trust_env=False,
        ) as client:
            with client.stream(
                "POST",
                TAVILY_SEARCH_URL,
                json=payload,
                headers=headers,
            ) as response:
                return self._consume_response(response)

    def _consume_response(self, response: httpx.Response) -> bytes:
        _translate_status(response.status_code)
        _require_json_content_type(response.headers)
        _reject_oversized_content_length(response.headers)
        return _stream_body(response, max_bytes=MAX_TAVILY_RESPONSE_BYTES)
