"""Worker-only semantic-grader composition (not a global Settings validator)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from atlas.config.settings import Settings
from atlas.evaluation.errors import SemanticGraderConfigurationError
from atlas.evaluation.graders import FakeSemanticGroundednessGrader
from atlas.evaluation.llm_grader import LangChainSemanticGroundednessGrader
from atlas.evaluation.ports import SemanticGroundednessGrader
from atlas.models.composition import resolve_model_name
from atlas.models.contracts import ProviderId
from atlas.models.langchain import build_chat_model
from atlas.models.service import ModelInvocationService

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker

_SANITIZED_LIVE_FAKE_PROVIDER = (
    "Live semantic grader requires a non-fake model provider"
)


def require_semantic_grader_mode(settings: Settings) -> None:
    """Fail worker startup when live semantic grading is misconfigured.

    ``live`` is never inferred from provider selection. The API constructs
    the same Settings class and must not fail on this combination.
    """
    if settings.semantic_grader_mode != "live":
        return
    if settings.model_provider == "fake":
        raise SemanticGraderConfigurationError(_SANITIZED_LIVE_FAKE_PROVIDER)


def build_semantic_grader(
    settings: Settings,
    *,
    session_factory: sessionmaker[Session],
    workflow_execution_id: str,
) -> SemanticGroundednessGrader | None:
    """Compose the optional semantic grader from explicit mode only."""
    mode = settings.semantic_grader_mode
    if mode == "skipped":
        return None
    if mode == "fake":
        return FakeSemanticGroundednessGrader()
    require_semantic_grader_mode(settings)
    chat_model = build_chat_model(settings)
    service = ModelInvocationService(
        session_factory=session_factory,
        chat_model=chat_model,
        provider=ProviderId(settings.model_provider),
        model_name=resolve_model_name(settings),
        call_timeout_seconds=settings.model_call_timeout_seconds,
    )
    return LangChainSemanticGroundednessGrader(
        service,
        workflow_execution_id=workflow_execution_id,
    )
