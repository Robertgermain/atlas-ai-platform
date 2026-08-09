"""Composition helpers for planner/drafter capabilities."""

from __future__ import annotations

from typing import TYPE_CHECKING

from atlas.models.contracts import ProviderId
from atlas.models.errors import ModelAuthConfigError
from atlas.models.fakes import (
    DeterministicResearchDrafter,
    DeterministicResearchPlanner,
)
from atlas.models.langchain import build_chat_model
from atlas.models.ports import ResearchDrafter, ResearchPlanner
from atlas.models.service import (
    LedgerBackedDrafter,
    LedgerBackedPlanner,
    ModelInvocationService,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker

    from atlas.config.settings import Settings


def resolve_model_name(settings: Settings) -> str:
    """Resolve the configured model name, applying provider defaults when unset."""
    if settings.model_name and settings.model_name.strip():
        return settings.model_name.strip()
    provider = ProviderId(settings.model_provider)
    if provider is ProviderId.FAKE:
        return "deterministic-fake"
    if provider is ProviderId.OPENAI:
        return "gpt-4o-mini"
    if provider is ProviderId.ANTHROPIC:
        return "claude-haiku-4-5"
    raise ModelAuthConfigError("unsupported model provider")


def build_planner_and_drafter(
    settings: Settings,
    *,
    session_factory: sessionmaker[Session],
    workflow_execution_id: str,
) -> tuple[ResearchPlanner, ResearchDrafter]:
    """Compose Atlas planner/drafter ports from settings.

    Fake provider returns deterministic Protocol implementations directly.
    Real providers use LangChain chat models only inside this composition path
    and the ledger-backed service.
    """
    provider = ProviderId(settings.model_provider)
    if provider is ProviderId.FAKE:
        return DeterministicResearchPlanner(), DeterministicResearchDrafter()

    chat_model = build_chat_model(settings)
    service = ModelInvocationService(
        session_factory=session_factory,
        chat_model=chat_model,
        provider=provider,
        model_name=resolve_model_name(settings),
        call_timeout_seconds=settings.model_call_timeout_seconds,
    )
    return (
        LedgerBackedPlanner(service, workflow_execution_id=workflow_execution_id),
        LedgerBackedDrafter(service, workflow_execution_id=workflow_execution_id),
    )
