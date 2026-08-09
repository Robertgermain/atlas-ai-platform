"""Deterministic fake research tools (default / test path)."""

from __future__ import annotations

import hashlib
import time

from atlas.tools.contracts import (
    TOOL_POLICY_VERSION,
    TOOL_VERSION,
    UNTRUSTED_SOURCE_LABEL,
    FetchUrlInput,
    FetchUrlOutput,
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
from atlas.tools.errors import ToolInvalidRequestError
from atlas.tools.security import parse_and_validate_url


def project_finding_text(body: str) -> str:
    """Label untrusted content and bound to MAX_FINDING_BYTES."""
    from atlas.tools.contracts import MAX_FINDING_BYTES

    labeled = f"{UNTRUSTED_SOURCE_LABEL} {body.strip()}"
    encoded = labeled.encode("utf-8")
    if len(encoded) <= MAX_FINDING_BYTES:
        return labeled
    # Truncate on UTF-8 byte boundary without raising.
    truncated = encoded[:MAX_FINDING_BYTES]
    while truncated:
        try:
            return truncated.decode("utf-8")
        except UnicodeDecodeError:
            truncated = truncated[:-1]
    return UNTRUSTED_SOURCE_LABEL


class FakeWebSearchTool:
    """Deterministic search tool with stable hit URLs."""

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
        hits: list[WebSearchHit] = []
        for index in range(parsed.max_results):
            digest = hashlib.sha256(f"{parsed.query}:{index}".encode()).hexdigest()[:12]
            hits.append(
                WebSearchHit(
                    title=f"Fake result {index + 1} for {parsed.query}",
                    url=f"https://example.com/fake/{digest}",
                    snippet=f"Deterministic snippet {index + 1} about {parsed.query}",
                )
            )
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
                provider=ToolProviderId.FAKE,
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


class FakeFetchUrlTool:
    """Deterministic fetch tool (still validates URL policy)."""

    @property
    def tool_id(self) -> ToolId:
        return ToolId.FETCH_URL

    def invoke(
        self,
        raw_input: dict[str, object],
        *,
        context: ToolCallContext,
    ) -> ToolInvocationResult:
        del context
        started = time.perf_counter()
        try:
            parsed = FetchUrlInput.model_validate(raw_input)
        except Exception as exc:
            raise ToolInvalidRequestError("invalid fetch_url input") from exc
        # Fake path enforces scheme/port/userinfo without live DNS.
        normalized, _hostname, _port, _scheme = parse_and_validate_url(parsed.url)
        body = (
            f"Fake fetched content for {normalized}. "
            "This is deterministic untrusted source text for tests."
        )
        output = FetchUrlOutput(
            final_url=normalized,
            content_type="text/plain",
            text=body,
            byte_length=len(body.encode("utf-8")),
            truncated=False,
        )
        finding = project_finding_text(f"fetch:{normalized} | {body}")
        latency_ms = int((time.perf_counter() - started) * 1000)
        return ToolInvocationResult(
            output=output.model_dump(mode="json"),
            meta=ToolResultMeta(
                tool_id=ToolId.FETCH_URL,
                provider=ToolProviderId.FAKE,
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
