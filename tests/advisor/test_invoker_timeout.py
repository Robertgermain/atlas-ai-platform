"""Malformed retry cap, no retry for other errors, whole-analysis deadline."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from prometheus_client import CollectorRegistry

from atlas.advisor.errors import (
    AdvisoryAnalysisTimeoutError,
    AdvisoryOutputRejectedError,
)
from atlas.advisor.fakes import DeterministicAdvisoryAnalyst
from atlas.advisor.invoker import AdvisoryStructuredInvoker
from atlas.models.contracts import ProviderId
from atlas.models.errors import (
    ModelAuthConfigError,
    ModelInvalidStructuredOutputError,
    ModelRateLimitedError,
    ModelRefusalError,
    ModelTimeoutError,
    ModelUnknownError,
)
from atlas.observability.metrics.catalog import AtlasMetrics
from tests.advisor.fakes import minimal_facts


class _Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def test_malformed_output_retries_once_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    facts = minimal_facts()
    calls = {"n": 0}

    def _invoke(**kwargs: object) -> tuple[object, object]:
        del kwargs
        calls["n"] += 1
        if calls["n"] == 1:
            raise ModelInvalidStructuredOutputError()
        return DeterministicAdvisoryAnalyst().analyze(facts), SimpleNamespace(
            input_tokens=1,
            output_tokens=1,
            estimated_cost_usd=0.0,
        )

    monkeypatch.setattr("atlas.advisor.invoker.invoke_structured", _invoke)
    metrics = AtlasMetrics(CollectorRegistry())
    invoker = AdvisoryStructuredInvoker(
        chat_model=object(),
        provider=ProviderId.OPENAI,
        model_name="gpt-4o-mini",
        call_timeout_seconds=25.0,
        metrics=metrics,
    )
    analysis = invoker.analyze(
        facts, analysis_id="00000000-0000-0000-0000-000000000001"
    )
    assert analysis.schema_version == "advisory.analysis.v1"
    assert calls["n"] == 2


def test_timeout_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    def _invoke(**kwargs: object) -> tuple[object, object]:
        del kwargs
        calls["n"] += 1
        raise ModelTimeoutError()

    monkeypatch.setattr("atlas.advisor.invoker.invoke_structured", _invoke)
    invoker = AdvisoryStructuredInvoker(
        chat_model=object(),
        provider=ProviderId.OPENAI,
        model_name="gpt-4o-mini",
        call_timeout_seconds=25.0,
        metrics=AtlasMetrics(CollectorRegistry()),
    )
    with pytest.raises(ModelTimeoutError):
        invoker.analyze(minimal_facts())
    assert calls["n"] == 1


def test_refusal_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    def _invoke(**kwargs: object) -> tuple[object, object]:
        del kwargs
        calls["n"] += 1
        raise ModelRefusalError()

    monkeypatch.setattr("atlas.advisor.invoker.invoke_structured", _invoke)
    invoker = AdvisoryStructuredInvoker(
        chat_model=object(),
        provider=ProviderId.OPENAI,
        model_name="gpt-4o-mini",
        call_timeout_seconds=25.0,
        metrics=AtlasMetrics(CollectorRegistry()),
    )
    with pytest.raises(ModelRefusalError):
        invoker.analyze(minimal_facts())
    assert calls["n"] == 1


def test_deadline_blocks_second_malformed_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    calls = {"n": 0}

    def _invoke(**kwargs: object) -> tuple[object, object]:
        del kwargs
        calls["n"] += 1
        clock.advance(100.0)
        raise ModelInvalidStructuredOutputError()

    monkeypatch.setattr("atlas.advisor.invoker.invoke_structured", _invoke)
    invoker = AdvisoryStructuredInvoker(
        chat_model=object(),
        provider=ProviderId.OPENAI,
        model_name="gpt-4o-mini",
        call_timeout_seconds=25.0,
        metrics=AtlasMetrics(CollectorRegistry()),
        monotonic=clock,
    )
    with pytest.raises(AdvisoryAnalysisTimeoutError):
        invoker.analyze(minimal_facts())
    assert calls["n"] == 1


def _failing_invoker(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> tuple[AdvisoryStructuredInvoker, dict[str, int]]:
    calls = {"n": 0}

    def _invoke(**kwargs: object) -> tuple[object, object]:
        del kwargs
        calls["n"] += 1
        raise error

    monkeypatch.setattr("atlas.advisor.invoker.invoke_structured", _invoke)
    invoker = AdvisoryStructuredInvoker(
        chat_model=object(),
        provider=ProviderId.OPENAI,
        model_name="gpt-4o-mini",
        call_timeout_seconds=25.0,
        metrics=AtlasMetrics(CollectorRegistry()),
    )
    return invoker, calls


def test_two_malformed_calls_then_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    invoker, calls = _failing_invoker(monkeypatch, ModelInvalidStructuredOutputError())
    with pytest.raises(ModelInvalidStructuredOutputError):
        invoker.analyze(minimal_facts())
    assert calls["n"] == 2


def test_rate_limited_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    invoker, calls = _failing_invoker(monkeypatch, ModelRateLimitedError())
    with pytest.raises(ModelRateLimitedError):
        invoker.analyze(minimal_facts())
    assert calls["n"] == 1


def test_auth_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    invoker, calls = _failing_invoker(monkeypatch, ModelAuthConfigError())
    with pytest.raises(ModelAuthConfigError):
        invoker.analyze(minimal_facts())
    assert calls["n"] == 1


def test_unknown_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    invoker, calls = _failing_invoker(monkeypatch, ModelUnknownError())
    with pytest.raises(ModelUnknownError):
        invoker.analyze(minimal_facts())
    assert calls["n"] == 1


def test_output_policy_rejection_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    facts = minimal_facts()
    calls = {"n": 0}

    def _invoke(**kwargs: object) -> tuple[object, object]:
        del kwargs
        calls["n"] += 1
        parsed = DeterministicAdvisoryAnalyst().analyze(facts)
        parsed = parsed.model_copy(
            update={"incident_summary": "See https://evil.example for details."}
        )
        return parsed, SimpleNamespace(
            input_tokens=1,
            output_tokens=1,
            estimated_cost_usd=0.0,
        )

    monkeypatch.setattr("atlas.advisor.invoker.invoke_structured", _invoke)
    invoker = AdvisoryStructuredInvoker(
        chat_model=object(),
        provider=ProviderId.OPENAI,
        model_name="gpt-4o-mini",
        call_timeout_seconds=25.0,
        metrics=AtlasMetrics(CollectorRegistry()),
    )
    with pytest.raises(AdvisoryOutputRejectedError):
        invoker.analyze(facts)
    assert calls["n"] == 1


def test_tracing_context_enter_failure_still_analyzes_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from atlas.observability.events import Event
    from atlas.observability.langsmith import reset_langsmith_for_tests
    from atlas.observability.testing import capture_logs
    from tests.observability.langsmith_fakes import arm_dummy_langsmith

    facts = minimal_facts()
    calls = {"n": 0}

    def _invoke(**kwargs: object) -> tuple[object, object]:
        del kwargs
        calls["n"] += 1
        return DeterministicAdvisoryAnalyst().analyze(facts), SimpleNamespace(
            input_tokens=1,
            output_tokens=1,
            estimated_cost_usd=0.0,
        )

    monkeypatch.setattr("atlas.advisor.invoker.invoke_structured", _invoke)
    metrics = AtlasMetrics(CollectorRegistry())
    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path, metrics=metrics)

    def _boom(**_kwargs: object) -> object:
        raise RuntimeError("sk-planted-secret")

    monkeypatch.setattr("atlas.observability.langsmith.tracing.tracing_context", _boom)
    invoker = AdvisoryStructuredInvoker(
        chat_model=object(),
        provider=ProviderId.OPENAI,
        model_name="gpt-4o-mini",
        call_timeout_seconds=25.0,
        metrics=AtlasMetrics(CollectorRegistry()),
    )
    try:
        with capture_logs("atlas.observability.langsmith.tracing") as captured:
            analysis = invoker.analyze(facts)
        assert analysis.schema_version == "advisory.analysis.v1"
        assert calls["n"] == 1
        assert Event.LANGSMITH_EXPORT_FAILED.value in captured.events
        assert captured.json(0)["error_class"] == "RuntimeError"
        assert captured.json(0)["outcome"] == "enqueue"
        assert "sk-planted-secret" not in captured.text
        total = 0.0
        for family in metrics.registry.collect():
            for sample in family.samples:
                if sample.name != "atlas_langsmith_operations_total":
                    continue
                if (
                    sample.labels.get("operation") == "enqueue"
                    and sample.labels.get("outcome") == "error"
                ):
                    total += sample.value
        assert total >= 1.0
    finally:
        handle.close()
        reset_langsmith_for_tests()
