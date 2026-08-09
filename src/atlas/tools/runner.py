"""Governed research tool runner: plan → findings via search/fetch."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from atlas.evidence.service import EvidenceIngestService
from atlas.tools.contracts import (
    TOOL_POLICY_VERSION,
    TOOL_VERSION,
    ToolCallContext,
    ToolId,
    ToolInvocationResult,
    ToolOrigin,
)
from atlas.tools.errors import ToolBudgetExhaustedError
from atlas.tools.service import ToolBudgets, ToolInvocationService


@dataclass(slots=True)
class ResearchNodeOutcome:
    """Findings plus durable evidence item ids produced during research."""

    findings: list[str]
    evidence_item_ids: list[str] = field(default_factory=list)


class ResearchPlanExecutor(Protocol):
    """Produce findings for a validated three-task plan."""

    def research(
        self,
        *,
        plan: list[str],
        context: ToolCallContext,
    ) -> ResearchNodeOutcome: ...


class _InvokableTool(Protocol):
    def invoke(
        self,
        raw_input: dict[str, object],
        *,
        context: ToolCallContext,
    ) -> ToolInvocationResult: ...


def _persist_search_hits(
    *,
    evidence_ingest: EvidenceIngestService | None,
    context: ToolCallContext,
    search_result: ToolInvocationResult,
) -> list[str]:
    if evidence_ingest is None:
        return []
    if not context.research_job_id:
        return []
    hits = search_result.output.get("hits") or []
    evidence_ids: list[str] = []
    for hit in hits:
        if not isinstance(hit, dict):
            continue
        title = str(hit.get("title") or "")
        url = str(hit.get("url") or "")
        snippet = str(hit.get("snippet") or "")
        if not url.strip():
            continue
        evidence_ids.append(
            evidence_ingest.ingest_search_hit(
                research_job_id=context.research_job_id,
                workflow_execution_id=context.workflow_execution_id,
                tool_invocation_id=search_result.invocation_id,
                title=title,
                url=url,
                snippet=snippet,
            )
        )
    return evidence_ids


class SimpleResearchExecutor:
    """Direct registry invocation without ledger (deterministic unit path)."""

    def __init__(
        self,
        *,
        search_tool: _InvokableTool,
        fetch_tool: _InvokableTool | None,
        fetch_enabled: bool,
        budgets: ToolBudgets,
        policy_assert: Callable[..., None],
        evidence_ingest: EvidenceIngestService | None = None,
    ) -> None:
        self._search_tool = search_tool
        self._fetch_tool = fetch_tool
        self._fetch_enabled = fetch_enabled
        self._budgets = budgets
        self._policy_assert = policy_assert
        self._evidence_ingest = evidence_ingest

    def research(
        self,
        *,
        plan: list[str],
        context: ToolCallContext,
    ) -> ResearchNodeOutcome:
        if len(plan) != 3:
            raise ValueError("plan must contain exactly three tasks")
        # Fresh node budget for each research-node entry.
        self._budgets.node_started_at = time.monotonic()
        self._budgets.logical_calls_used = 0
        findings: list[str] = []
        evidence_item_ids: list[str] = []
        for task in plan:
            self._budgets.assert_can_start_logical_call()
            self._policy_assert(
                origin=context.origin,
                tool_id=ToolId.WEB_SEARCH,
                node_name=context.node_name,
            )
            self._budgets.record_logical_call()
            search_result = self._search_tool.invoke(
                {"query": task, "max_results": 3},
                context=context,
            )
            evidence_item_ids.extend(
                _persist_search_hits(
                    evidence_ingest=self._evidence_ingest,
                    context=context,
                    search_result=search_result,
                )
            )
            finding = search_result.finding_text
            if self._fetch_enabled and self._fetch_tool is not None:
                hits = search_result.output.get("hits") or []
                if hits:
                    self._budgets.assert_can_start_logical_call()
                    self._policy_assert(
                        origin=context.origin,
                        tool_id=ToolId.FETCH_URL,
                        node_name=context.node_name,
                    )
                    self._budgets.record_logical_call()
                    top_url = str(hits[0].get("url") or "")
                    fetch_result = self._fetch_tool.invoke(
                        {"url": top_url},
                        context=context,
                    )
                    finding = fetch_result.finding_text
            findings.append(finding)
        # Preserve order while deduplicating.
        deduped: list[str] = []
        seen: set[str] = set()
        for item_id in evidence_item_ids:
            if item_id not in seen:
                seen.add(item_id)
                deduped.append(item_id)
        return ResearchNodeOutcome(findings=findings, evidence_item_ids=deduped)


class LedgerBackedResearchExecutor:
    """Research-node executor that records every logical tool call in the ledger."""

    def __init__(
        self,
        *,
        service: ToolInvocationService,
        fetch_enabled: bool,
        evidence_ingest: EvidenceIngestService | None = None,
    ) -> None:
        self._service = service
        self._fetch_enabled = fetch_enabled
        self._evidence_ingest = evidence_ingest

    def research(
        self,
        *,
        plan: list[str],
        context: ToolCallContext,
    ) -> ResearchNodeOutcome:
        if len(plan) != 3:
            raise ValueError("plan must contain exactly three tasks")
        budgets = self._service._budgets
        if budgets is not None:
            budgets.node_started_at = time.monotonic()
            budgets.logical_calls_used = 0
        findings: list[str] = []
        evidence_item_ids: list[str] = []
        for task in plan:
            try:
                search_result = self._service.invoke(
                    tool_id=ToolId.WEB_SEARCH,
                    raw_input={"query": task, "max_results": 3},
                    context=context,
                )
            except ToolBudgetExhaustedError:
                raise
            evidence_item_ids.extend(
                _persist_search_hits(
                    evidence_ingest=self._evidence_ingest,
                    context=context,
                    search_result=search_result,
                )
            )
            finding = search_result.finding_text
            if self._fetch_enabled:
                hits = search_result.output.get("hits") or []
                if not hits:
                    findings.append(finding)
                    continue
                top_url = str(hits[0].get("url") or "")
                try:
                    fetch_result = self._service.invoke(
                        tool_id=ToolId.FETCH_URL,
                        raw_input={"url": top_url},
                        context=context,
                    )
                except ToolBudgetExhaustedError:
                    raise
                finding = fetch_result.finding_text
            findings.append(finding)
        deduped: list[str] = []
        seen: set[str] = set()
        for item_id in evidence_item_ids:
            if item_id not in seen:
                seen.add(item_id)
                deduped.append(item_id)
        return ResearchNodeOutcome(findings=findings, evidence_item_ids=deduped)


def default_tool_call_context(
    *,
    research_job_id: str,
    workflow_execution_id: str | None,
    workflow_node_attempt: int | None = None,
) -> ToolCallContext:
    return ToolCallContext(
        origin=ToolOrigin.WORKFLOW,
        tool_version=TOOL_VERSION,
        tool_policy_version=TOOL_POLICY_VERSION,
        research_job_id=research_job_id,
        workflow_execution_id=workflow_execution_id,
        node_name="research",
        workflow_node_attempt=workflow_node_attempt,
    )
