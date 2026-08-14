"""Worker-only evaluation-profile and semantic-grader composition.

Not a global Settings validator: the API constructs the same Settings class
and must remain constructible without live credentials.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from atlas.config.settings import Settings
from atlas.evaluation.contracts import (
    EVALUATION_PROFILE_CANDIDATE,
    EVALUATION_PROFILE_CANDIDATE_FAKE,
    EVALUATION_PROFILE_V1,
    PROFILE_FOR_SEMANTIC_MODE,
    SEMANTIC_MODE_FOR_PROFILE,
    EvaluationProfile,
)
from atlas.evaluation.errors import SemanticGraderConfigurationError
from atlas.evaluation.graders import FakeSemanticGroundednessGrader
from atlas.evaluation.llm_grader import LangChainSemanticGroundednessGrader
from atlas.evaluation.ports import SemanticGroundednessGrader
from atlas.evaluation.semantic_contracts import (
    FROZEN_LIVE_SEMANTIC_MODEL,
    FROZEN_LIVE_SEMANTIC_PROVIDER,
    FROZEN_LIVE_SEMANTIC_TEMPERATURE,
)
from atlas.models.composition import resolve_model_name
from atlas.models.contracts import ProviderId
from atlas.models.langchain import build_chat_model
from atlas.models.service import ModelInvocationService

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker

_SANITIZED_PROFILE_MODE_MISMATCH = (
    "Evaluation profile does not match semantic grader mode"
)
_SANITIZED_LIVE_PROVIDER = "evaluation.v1 requires the openai provider"
_SANITIZED_LIVE_MODEL = "evaluation.v1 requires model gpt-4o-mini"
_SANITIZED_LIVE_OPENAI_KEY = "evaluation.v1 requires an OpenAI credential"
_SANITIZED_LIVE_LANGSMITH_KEY = "evaluation.v1 requires a LangSmith credential"
_SANITIZED_LIVE_FAKE_PROVIDER = (
    "Live semantic grader requires a non-fake model provider"
)


def resolved_evaluation_profile(settings: Settings) -> EvaluationProfile:
    """Return the effective evaluation profile.

    An unset ``Settings.evaluation_profile`` is derived from
    ``semantic_grader_mode``. An explicit profile is returned as stored so
    worker composition can fail closed on disagreement.
    """
    explicit = settings.evaluation_profile
    if explicit is None:
        return PROFILE_FOR_SEMANTIC_MODE[settings.semantic_grader_mode]
    if explicit == EVALUATION_PROFILE_V1:
        return EVALUATION_PROFILE_V1
    if explicit == EVALUATION_PROFILE_CANDIDATE_FAKE:
        return EVALUATION_PROFILE_CANDIDATE_FAKE
    return EVALUATION_PROFILE_CANDIDATE


def require_evaluation_composition(settings: Settings) -> None:
    """Fail worker startup when profile, mode, and live pins disagree.

    ``live`` is never inferred from provider selection. Credential checks
    run here, not in global Settings validation. Unset profile derives from
    mode; an explicit profile must match the mode.
    """
    profile = resolved_evaluation_profile(settings)
    expected_mode = SEMANTIC_MODE_FOR_PROFILE[profile]
    if settings.semantic_grader_mode != expected_mode:
        raise SemanticGraderConfigurationError(_SANITIZED_PROFILE_MODE_MISMATCH)
    if profile != EVALUATION_PROFILE_V1:
        return
    require_frozen_live_v1(settings)


def require_frozen_live_v1(settings: Settings) -> None:
    """Fail closed when ``evaluation.v1`` is missing its frozen live pins."""
    if settings.semantic_grader_mode != "live":
        raise SemanticGraderConfigurationError(_SANITIZED_PROFILE_MODE_MISMATCH)
    if settings.model_provider != FROZEN_LIVE_SEMANTIC_PROVIDER:
        if settings.model_provider == "fake":
            raise SemanticGraderConfigurationError(_SANITIZED_LIVE_FAKE_PROVIDER)
        raise SemanticGraderConfigurationError(_SANITIZED_LIVE_PROVIDER)
    if resolve_model_name(settings) != FROZEN_LIVE_SEMANTIC_MODEL:
        raise SemanticGraderConfigurationError(_SANITIZED_LIVE_MODEL)
    openai_key = settings.openai_api_key
    openai_secret = (
        openai_key.get_secret_value().strip() if openai_key is not None else ""
    )
    if not openai_secret:
        raise SemanticGraderConfigurationError(_SANITIZED_LIVE_OPENAI_KEY)
    langsmith_key = settings.langsmith_api_key
    langsmith_secret = (
        langsmith_key.get_secret_value().strip() if langsmith_key is not None else ""
    )
    if not langsmith_secret:
        raise SemanticGraderConfigurationError(_SANITIZED_LIVE_LANGSMITH_KEY)


def require_semantic_grader_mode(settings: Settings) -> None:
    """Backward-compatible alias for worker evaluation composition checks."""
    require_evaluation_composition(settings)


def frozen_live_semantic_identity() -> dict[str, str | float]:
    """Canonical live fingerprint identity for ``evaluation.v1``."""
    return {
        "provider": FROZEN_LIVE_SEMANTIC_PROVIDER,
        "model_name": FROZEN_LIVE_SEMANTIC_MODEL,
        "temperature": FROZEN_LIVE_SEMANTIC_TEMPERATURE,
    }


def build_semantic_grader(
    settings: Settings,
    *,
    session_factory: sessionmaker[Session],
    workflow_execution_id: str,
) -> SemanticGroundednessGrader | None:
    """Compose the optional semantic grader from the resolved profile/mode."""
    require_evaluation_composition(settings)
    mode = settings.semantic_grader_mode
    if mode == "skipped":
        return None
    if mode == "fake":
        return FakeSemanticGroundednessGrader()
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
