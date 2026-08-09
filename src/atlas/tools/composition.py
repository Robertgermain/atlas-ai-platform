"""Compose research tools from settings."""

from __future__ import annotations

from typing import TYPE_CHECKING

from atlas.tools.contracts import ToolId, ToolProviderId
from atlas.tools.errors import ToolAuthConfigError
from atlas.tools.fakes import FakeFetchUrlTool, FakeWebSearchTool
from atlas.tools.registry import ToolRegistry, default_permission_policy
from atlas.tools.runner import (
    LedgerBackedResearchExecutor,
    ResearchPlanExecutor,
    SimpleResearchExecutor,
)
from atlas.tools.search_tavily import TavilyWebSearchTool
from atlas.tools.service import ToolBudgets, ToolInvocationService

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker

    from atlas.config.settings import Settings
    from atlas.evidence.service import EvidenceIngestService


def build_tool_budgets(settings: Settings) -> ToolBudgets:
    return ToolBudgets(
        max_logical_calls=settings.tool_max_logical_calls_per_research_node,
        max_attempts_per_call=settings.tool_max_attempts_per_call,
        attempt_timeout_seconds=settings.tool_attempt_timeout_seconds,
        node_deadline_seconds=settings.research_node_tool_deadline_seconds,
    )


def build_fake_registry() -> ToolRegistry:
    return ToolRegistry(
        {
            ToolId.WEB_SEARCH: FakeWebSearchTool(),
            ToolId.FETCH_URL: FakeFetchUrlTool(),
        }
    )


def assert_live_fetch_config(settings: Settings) -> None:
    """Fail closed: live arbitrary-URL fetch is unavailable in Milestone 9."""
    if settings.tool_fetch_enabled:
        raise ToolAuthConfigError(
            "ATLAS_TOOL_FETCH_ENABLED requires a concurrency-safe fetch "
            "transport; live fetch is unavailable in Milestone 9"
        )


def build_live_registry(settings: Settings) -> ToolRegistry:
    """Build the live (Tavily) tool registry.

    Never substitutes ``FakeFetchUrlTool`` under a live provider. When fetch is
    disabled, ``fetch_url`` is omitted from the registry. When fetch is enabled,
    composition fails closed.
    """
    assert_live_fetch_config(settings)
    key = settings.tavily_api_key
    if key is None or not key.get_secret_value().strip():
        raise ToolAuthConfigError("Tavily API key is required for live search")
    search = TavilyWebSearchTool(
        api_key=key.get_secret_value(),
        timeout_seconds=settings.tool_attempt_timeout_seconds,
    )
    return ToolRegistry({ToolId.WEB_SEARCH: search})


def build_research_executor(
    settings: Settings,
    *,
    session_factory: sessionmaker[Session] | None = None,
    use_ledger: bool = True,
    evidence_ingest: EvidenceIngestService | None = None,
) -> ResearchPlanExecutor:
    """Compose the research-node executor.

    Default ``tool_provider=fake`` uses deterministic tools. When
    ``session_factory`` is provided and ``use_ledger`` is true, invocations are
    recorded in the tool ledger (including fake tools under the worker path).
    """
    if settings.tool_provider == "tavily":
        assert_live_fetch_config(settings)

    budgets = build_tool_budgets(settings)
    policy = default_permission_policy()
    # Live fetch is unavailable; only the fully-fake provider may use fake fetch.
    fetch_enabled = bool(
        settings.tool_fetch_enabled and settings.tool_provider == "fake"
    )

    if settings.tool_provider == "fake":
        registry = build_fake_registry()
        providers = {
            ToolId.WEB_SEARCH: ToolProviderId.FAKE,
            ToolId.FETCH_URL: ToolProviderId.FAKE,
        }
    elif settings.tool_provider == "tavily":
        registry = build_live_registry(settings)
        providers = {ToolId.WEB_SEARCH: ToolProviderId.TAVILY}
    else:
        raise ToolAuthConfigError("unsupported tool provider")

    if use_ledger and session_factory is not None:
        service = ToolInvocationService(
            session_factory=session_factory,
            registry=registry,
            policy=policy,
            provider_by_tool=providers,
            budgets=budgets,
            max_attempts_per_call=settings.tool_max_attempts_per_call,
            attempt_timeout_seconds=settings.tool_attempt_timeout_seconds,
        )
        return LedgerBackedResearchExecutor(
            service=service,
            fetch_enabled=fetch_enabled and ToolId.FETCH_URL in registry.list_ids(),
            evidence_ingest=evidence_ingest,
        )

    fetch_tool = None
    if ToolId.FETCH_URL in registry.list_ids():
        fetch_tool = registry.get(ToolId.FETCH_URL)
    return SimpleResearchExecutor(
        search_tool=registry.get(ToolId.WEB_SEARCH),
        fetch_tool=fetch_tool,
        fetch_enabled=fetch_enabled,
        budgets=budgets,
        policy_assert=policy.assert_allowed,
        evidence_ingest=evidence_ingest,
    )


def build_mcp_tool_service(
    settings: Settings,
    *,
    session_factory: sessionmaker[Session],
) -> ToolInvocationService:
    """Compose a ledger-backed service for the MCP stdio server."""
    if settings.tool_provider == "fake":
        registry = build_fake_registry()
        providers = {
            ToolId.WEB_SEARCH: ToolProviderId.FAKE,
            ToolId.FETCH_URL: ToolProviderId.FAKE,
        }
    elif settings.tool_provider == "tavily":
        registry = build_live_registry(settings)
        providers = {ToolId.WEB_SEARCH: ToolProviderId.TAVILY}
    else:
        raise ToolAuthConfigError("unsupported tool provider")
    return ToolInvocationService(
        session_factory=session_factory,
        registry=registry,
        policy=default_permission_policy(),
        provider_by_tool=providers,
        budgets=None,
        max_attempts_per_call=settings.tool_max_attempts_per_call,
        attempt_timeout_seconds=settings.tool_attempt_timeout_seconds,
    )
