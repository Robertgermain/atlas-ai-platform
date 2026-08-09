"""Opt-in live provider tests (skipped unless explicitly enabled)."""

from __future__ import annotations

import os

import pytest

from atlas.config.settings import Settings
from atlas.models.contracts import PlanStructuredOutput, ProviderId
from atlas.models.langchain import build_chat_model, invoke_structured, plan_prompts

pytestmark = pytest.mark.skipif(
    os.environ.get("ATLAS_ENABLE_LIVE_MODEL_TESTS", "").lower()
    not in {"1", "true", "yes"},
    reason=(
        "Live model tests require ATLAS_ENABLE_LIVE_MODEL_TESTS=1 "
        "and provider credentials"
    ),
)


def test_live_openai_structured_plan() -> None:
    settings = Settings(
        model_provider="openai",
        model_name=os.environ.get("ATLAS_MODEL_NAME", "gpt-4o-mini"),
        openai_api_key=Settings().openai_api_key,
        model_call_timeout_seconds=25.0,
    )
    if settings.openai_api_key is None:
        pytest.skip("ATLAS_OPENAI_API_KEY not configured")
    chat = build_chat_model(settings)
    system, user = plan_prompts("What are three risks of distributed locking?")
    validated, meta = invoke_structured(
        chat_model=chat,
        provider=ProviderId.OPENAI,
        model_name=settings.model_name or "gpt-4o-mini",
        prompt_version="plan.v1",
        system_prompt=system,
        user_prompt=user,
        schema=PlanStructuredOutput,
    )
    assert len(validated.tasks) == 3
    assert meta.provider is ProviderId.OPENAI


def test_live_anthropic_structured_plan() -> None:
    settings = Settings(
        model_provider="anthropic",
        model_name=os.environ.get("ATLAS_MODEL_NAME", "claude-haiku-4-5"),
        anthropic_api_key=Settings().anthropic_api_key,
        model_call_timeout_seconds=25.0,
    )
    if settings.anthropic_api_key is None:
        pytest.skip("ATLAS_ANTHROPIC_API_KEY not configured")
    chat = build_chat_model(settings)
    system, user = plan_prompts("What are three risks of distributed locking?")
    validated, meta = invoke_structured(
        chat_model=chat,
        provider=ProviderId.ANTHROPIC,
        model_name=settings.model_name or "claude-haiku-4-5",
        prompt_version="plan.v1",
        system_prompt=system,
        user_prompt=user,
        schema=PlanStructuredOutput,
    )
    assert len(validated.tasks) == 3
    assert meta.provider is ProviderId.ANTHROPIC
