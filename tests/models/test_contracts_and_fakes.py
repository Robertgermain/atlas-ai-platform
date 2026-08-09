"""Unit tests for Atlas model contracts, fakes, pricing, and error mapping."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from atlas.models.contracts import (
    DraftRequest,
    DraftStructuredOutput,
    PlanRequest,
    PlanStructuredOutput,
    ProviderId,
)
from atlas.models.errors import (
    ModelAuthConfigError,
    ModelInvalidStructuredOutputError,
    ModelRateLimitedError,
    ModelTimeoutError,
    sanitize_model_error,
)
from atlas.models.fakes import (
    DeterministicResearchDrafter,
    DeterministicResearchPlanner,
)
from atlas.models.langchain import translate_provider_exception
from atlas.models.pricing import PRICING_VERSION, estimate_cost_usd


def test_plan_structured_output_requires_three_non_empty_tasks() -> None:
    valid = PlanStructuredOutput(tasks=["a", "b", "c"])
    assert valid.tasks == ["a", "b", "c"]
    with pytest.raises(ValidationError):
        PlanStructuredOutput(tasks=["a", "b"])
    with pytest.raises(ValidationError):
        PlanStructuredOutput(tasks=["a", " ", "c"])


def test_draft_structured_output_strips_and_rejects_blank() -> None:
    assert DraftStructuredOutput(draft="  hello  ").draft == "hello"
    with pytest.raises(ValidationError):
        DraftStructuredOutput(draft="   ")


def test_deterministic_planner_and_drafter_are_stable() -> None:
    planner = DeterministicResearchPlanner()
    drafter = DeterministicResearchDrafter()
    request = PlanRequest(job_id="job-1", question="Q?", prompt_version="plan.v1")
    plan_a = planner.plan(request)
    plan_b = planner.plan(request)
    assert plan_a.tasks == plan_b.tasks
    assert len(plan_a.tasks) == 3
    assert plan_a.meta.provider is ProviderId.FAKE
    draft = drafter.draft(
        DraftRequest(
            job_id="job-1",
            question="Q?",
            plan=plan_a.tasks,
            findings=["f1", "f2", "f3"],
            prompt_version="draft.v1",
        )
    )
    assert "Q?" in draft.draft
    assert draft.meta.provider is ProviderId.FAKE


def test_estimate_cost_known_and_unknown_models() -> None:
    cost, version = estimate_cost_usd(
        provider=ProviderId.OPENAI,
        model="gpt-4o-mini",
        input_tokens=1_000_000,
        output_tokens=1_000_000,
    )
    assert cost == pytest.approx(0.75)
    assert version == PRICING_VERSION

    unknown_cost, unknown_version = estimate_cost_usd(
        provider=ProviderId.OPENAI,
        model="totally-unknown-model",
        input_tokens=100,
        output_tokens=100,
    )
    assert unknown_cost is None
    assert unknown_version is None


def test_translate_provider_exception_maps_timeout_and_rate_limit() -> None:
    assert isinstance(translate_provider_exception(TimeoutError()), ModelTimeoutError)

    class RateLimitError(Exception):
        pass

    RateLimitError.__name__ = "RateLimitError"
    assert isinstance(
        translate_provider_exception(RateLimitError()),
        ModelRateLimitedError,
    )


def test_sanitize_model_error_is_class_only() -> None:
    secret = "sk-live-secret"
    text = sanitize_model_error(ModelAuthConfigError(f"key={secret}"))
    assert text == "ModelAuthConfigError: model invocation failed"
    assert secret not in text


def test_invalid_structured_output_error_category() -> None:
    err = ModelInvalidStructuredOutputError()
    assert err.retry_class.value == "invalid_structured_output"
