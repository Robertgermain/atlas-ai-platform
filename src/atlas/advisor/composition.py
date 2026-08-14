"""CLI-only advisory composition. Not imported by the worker.

Live pins: openai / gpt-4o-mini / temperature 0 / LangSmith key.
Fake mode requires no credentials. This is not a global Settings validator.
"""

from __future__ import annotations

from contextlib import AbstractContextManager

from sqlalchemy.orm import Session, sessionmaker

from atlas.advisor.catalogs import (
    FROZEN_LIVE_ADVISORY_MODEL,
    FROZEN_LIVE_ADVISORY_PROVIDER,
)
from atlas.advisor.db import advisory_read_only_scope
from atlas.advisor.errors import AdvisoryConfigurationError
from atlas.advisor.fakes import DeterministicAdvisoryAnalyst
from atlas.advisor.invoker import AdvisoryStructuredInvoker
from atlas.advisor.ports import AdvisoryAnalystPort
from atlas.advisor.service import AdvisoryService
from atlas.config.settings import Settings
from atlas.models.composition import resolve_model_name
from atlas.models.contracts import ProviderId
from atlas.models.langchain import build_chat_model
from atlas.observability.langsmith.errors import LangSmithConfigurationError
from atlas.observability.metrics import AtlasMetrics, default_metrics
from atlas.persistence.repositories.advisory_snapshot import (
    SqlAlchemyAdvisorySnapshotReader,
)

_SANITIZED_LIVE_PROVIDER = "advisory live mode requires the openai provider"
_SANITIZED_LIVE_MODEL = "advisory live mode requires model gpt-4o-mini"
_SANITIZED_LIVE_OPENAI = "advisory live mode requires an OpenAI credential"
_SANITIZED_LIVE_LANGSMITH = "advisory live mode requires a LangSmith credential"


def require_advisory_composition(settings: Settings) -> None:
    """Fail CLI startup when live advisory pins disagree.

    Fake mode is the default and requires no keys. Worker startup must not
    call this function.
    """
    if settings.advisory_mode != "live":
        return
    if settings.model_provider != FROZEN_LIVE_ADVISORY_PROVIDER:
        raise AdvisoryConfigurationError(_SANITIZED_LIVE_PROVIDER)
    if resolve_model_name(settings) != FROZEN_LIVE_ADVISORY_MODEL:
        raise AdvisoryConfigurationError(_SANITIZED_LIVE_MODEL)
    openai_key = settings.openai_api_key
    openai_secret = (
        openai_key.get_secret_value().strip() if openai_key is not None else ""
    )
    if not openai_secret:
        raise AdvisoryConfigurationError(_SANITIZED_LIVE_OPENAI)
    langsmith_key = settings.langsmith_api_key
    langsmith_secret = (
        langsmith_key.get_secret_value().strip() if langsmith_key is not None else ""
    )
    if not langsmith_secret:
        raise LangSmithConfigurationError(_SANITIZED_LIVE_LANGSMITH)


def build_analyst(settings: Settings, *, metrics: AtlasMetrics) -> AdvisoryAnalystPort:
    """Compose fake or live analyst. Live uses invoke_structured, not the ledger."""
    if settings.advisory_mode != "live":
        return DeterministicAdvisoryAnalyst()
    chat_model = build_chat_model(settings)
    return AdvisoryStructuredInvoker(
        chat_model=chat_model,
        provider=ProviderId.OPENAI,
        model_name=FROZEN_LIVE_ADVISORY_MODEL,
        call_timeout_seconds=settings.model_call_timeout_seconds,
        metrics=metrics,
    )


def build_advisory_service(
    settings: Settings,
    *,
    session_factory: sessionmaker[Session],
    metrics: AtlasMetrics | None = None,
) -> AdvisoryService:
    resolved_metrics = metrics if metrics is not None else default_metrics()
    require_advisory_composition(settings)
    analyst = build_analyst(settings, metrics=resolved_metrics)

    def scope() -> AbstractContextManager[Session]:
        return advisory_read_only_scope(session_factory)

    return AdvisoryService(
        read_scope=scope,
        snapshot=SqlAlchemyAdvisorySnapshotReader(),
        analyst=analyst,
        metrics=resolved_metrics,
        mode=settings.advisory_mode,
    )
