"""Worker AI-composition LangSmith key requirement (not global Settings)."""

from __future__ import annotations

from atlas.config.settings import Settings
from atlas.observability.langsmith.errors import LangSmithConfigurationError

_SANITIZED_MESSAGE = (
    "LangSmith API key is required when a live model, tool, or embedding "
    "provider is configured"
)


def live_ai_provider_configured(settings: Settings) -> bool:
    """Return True when the worker would make a real model, tool, or embedding call."""
    return (
        settings.model_provider != "fake"
        or settings.tool_provider != "fake"
        or settings.embedding_provider != "fake"
    )


def require_langsmith_for_live_ai(settings: Settings) -> None:
    """Fail worker startup when live AI is configured without a LangSmith key.

    Fake-only configuration without a key is allowed (offline). This is
    not a Settings validator: the API constructs the same Settings class
    and does not run the AI workflow.
    """
    if not live_ai_provider_configured(settings):
        return
    key = settings.langsmith_api_key
    secret = key.get_secret_value().strip() if key is not None else ""
    if not secret:
        raise LangSmithConfigurationError(_SANITIZED_MESSAGE)
