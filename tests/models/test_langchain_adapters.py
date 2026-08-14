"""Mocked LangChain provider adapter tests (no live network)."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage
from pydantic import SecretStr

from atlas.config.settings import Settings
from atlas.models.contracts import PlanStructuredOutput, ProviderId
from atlas.models.errors import (
    ModelAuthConfigError,
    ModelInvalidStructuredOutputError,
    ModelRateLimitedError,
    ModelTimeoutError,
)
from atlas.models.langchain import build_chat_model, invoke_structured


def test_build_chat_model_rejects_fake_provider() -> None:
    settings = Settings(model_provider="fake")
    with pytest.raises(ModelAuthConfigError):
        build_chat_model(settings)


def test_build_openai_requires_api_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # Isolate from developer .env / env credentials (never assert on secret values).
    monkeypatch.delenv("ATLAS_OPENAI_API_KEY", raising=False)
    monkeypatch.chdir(tmp_path)
    settings = Settings(
        model_provider="openai",
        model_name="gpt-4o-mini",
        openai_api_key=None,
    )
    with pytest.raises(ModelAuthConfigError):
        build_chat_model(settings)


def test_build_openai_uses_responses_api_flag() -> None:
    settings = Settings(
        model_provider="openai",
        model_name="gpt-4o-mini",
        openai_api_key=SecretStr("sk-test"),
        model_call_timeout_seconds=25.0,
    )
    with patch("langchain_openai.ChatOpenAI") as mock_cls:
        mock_cls.return_value = MagicMock()
        build_chat_model(settings)
        kwargs = mock_cls.call_args.kwargs
        assert kwargs["use_responses_api"] is True
        assert kwargs["model"] == "gpt-4o-mini"
        assert kwargs["temperature"] == 0
        assert kwargs["max_retries"] == 0
        assert kwargs["request_timeout"] == 25.0


def test_build_anthropic_requires_api_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # Isolate from developer .env / env credentials (never assert on secret values).
    monkeypatch.delenv("ATLAS_ANTHROPIC_API_KEY", raising=False)
    monkeypatch.chdir(tmp_path)
    settings = Settings(
        model_provider="anthropic",
        model_name="claude-haiku-4-5",
        anthropic_api_key=None,
    )
    with pytest.raises(ModelAuthConfigError):
        build_chat_model(settings)


def test_build_anthropic_wires_timeout() -> None:
    settings = Settings(
        model_provider="anthropic",
        model_name="claude-haiku-4-5",
        anthropic_api_key=SecretStr("sk-ant-test"),
        model_call_timeout_seconds=25.0,
    )
    with patch("langchain_anthropic.ChatAnthropic") as mock_cls:
        mock_cls.return_value = MagicMock()
        build_chat_model(settings)
        kwargs = mock_cls.call_args.kwargs
        assert kwargs["model_name"] == "claude-haiku-4-5"
        assert kwargs["default_request_timeout"] == 25.0
        assert kwargs["max_retries"] == 0


def test_invoke_structured_success_extracts_usage_and_cost() -> None:
    chat_model = MagicMock()
    structured = MagicMock()
    chat_model.with_structured_output.return_value = structured
    raw_message = AIMessage(
        content="ok",
        usage_metadata={
            "input_tokens": 100,
            "output_tokens": 50,
            "total_tokens": 150,
        },
        response_metadata={"id": "req_123"},
    )
    structured.invoke.return_value = {
        "raw": raw_message,
        "parsed": PlanStructuredOutput(tasks=["a", "b", "c"]),
        "parsing_error": None,
    }

    validated, meta = invoke_structured(
        chat_model=chat_model,
        provider=ProviderId.OPENAI,
        model_name="gpt-4o-mini",
        prompt_version="plan.v1",
        system_prompt="sys",
        user_prompt="user",
        schema=PlanStructuredOutput,
    )
    assert validated.tasks == ["a", "b", "c"]
    assert meta.provider_request_id == "req_123"
    assert meta.input_tokens == 100
    assert meta.output_tokens == 50
    assert meta.total_tokens == 150
    assert meta.estimated_cost_usd is not None
    assert meta.pricing_version is not None
    dumped = meta.model_dump()
    assert "sys" not in dumped.values()
    assert "user" not in dumped.values()


def test_invoke_structured_parsing_error_is_atlas_category() -> None:
    chat_model = MagicMock()
    structured = MagicMock()
    chat_model.with_structured_output.return_value = structured
    structured.invoke.return_value = {
        "raw": AIMessage(content="nope"),
        "parsed": None,
        "parsing_error": ValueError("bad json"),
    }
    with pytest.raises(ModelInvalidStructuredOutputError):
        invoke_structured(
            chat_model=chat_model,
            provider=ProviderId.OPENAI,
            model_name="gpt-4o-mini",
            prompt_version="plan.v1",
            system_prompt="sys",
            user_prompt="user",
            schema=PlanStructuredOutput,
        )


def test_invoke_structured_translates_timeout() -> None:
    chat_model = MagicMock()
    structured = MagicMock()
    chat_model.with_structured_output.return_value = structured
    structured.invoke.side_effect = TimeoutError()
    with pytest.raises(ModelTimeoutError):
        invoke_structured(
            chat_model=chat_model,
            provider=ProviderId.OPENAI,
            model_name="gpt-4o-mini",
            prompt_version="plan.v1",
            system_prompt="sys",
            user_prompt="user",
            schema=PlanStructuredOutput,
        )


def test_invoke_structured_translates_openai_rate_limit() -> None:
    openai = pytest.importorskip("openai")
    chat_model = MagicMock()
    structured = MagicMock()
    chat_model.with_structured_output.return_value = structured

    response = MagicMock()
    response.status_code = 429
    response.headers = {}
    response.request = MagicMock()
    exc: Any = openai.RateLimitError(
        message="rate",
        response=response,
        body=None,
    )
    structured.invoke.side_effect = exc
    with pytest.raises(ModelRateLimitedError):
        invoke_structured(
            chat_model=chat_model,
            provider=ProviderId.OPENAI,
            model_name="gpt-4o-mini",
            prompt_version="plan.v1",
            system_prompt="sys",
            user_prompt="user",
            schema=PlanStructuredOutput,
        )
