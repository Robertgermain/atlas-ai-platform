"""Metric observations from ``ModelInvocationService._execute`` attempt/outcome paths.

Uses a fake repository (in-memory, no real PostgreSQL) and monkeypatches
``invoke_structured`` so this stays a fast unit test; the durable ledger
fencing/reclaim behavior already has dedicated real-database integration
coverage under ``tests/integration/test_model_invocation_ledger.py``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast

import pytest
from prometheus_client import CollectorRegistry
from pydantic import BaseModel
from sqlalchemy.orm import Session

import atlas.models.service as model_service
from atlas.models.contracts import (
    FinishOutcome,
    ModelCallMeta,
    PlanRequest,
    ProviderId,
)
from atlas.models.errors import ModelAttemptOwnershipLostError, ModelTimeoutError
from atlas.models.service import ModelInvocationService
from atlas.observability.metrics.catalog import AtlasMetrics
from atlas.persistence.repositories.model_invocation import (
    ModelInvocationRecord,
)


class _FakeSession:
    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


class _FakeSessionFactory:
    def __call__(self) -> _FakeSession:
        return _FakeSession()


class _FakeModelInvocationRepository:
    def __init__(self) -> None:
        self.fail_attempt_ok = True
        self.mark_invocation_failed_ok = True
        self.complete_attempt_ok = True
        self.complete_invocation_ok = True

    def get_by_key(
        self, session: Session, invocation_key: str, *, for_update: bool = False
    ) -> ModelInvocationRecord | None:
        del session, invocation_key, for_update
        return None

    def create_invocation(self, session: Session, **kwargs: object) -> None:
        del session, kwargs

    def reopen_invocation(self, session: Session, **kwargs: object) -> None:
        del session, kwargs

    def begin_attempt(self, session: Session, **kwargs: object) -> int:
        del session, kwargs
        return 1

    def latest_attempt(self, session: Session, *, invocation_id: str) -> None:
        del session, invocation_id
        return None

    def complete_attempt(self, session: Session, **kwargs: object) -> bool:
        del session, kwargs
        return self.complete_attempt_ok

    def fail_attempt(self, session: Session, **kwargs: object) -> bool:
        del session, kwargs
        return self.fail_attempt_ok

    def complete_invocation_for_attempt(
        self, session: Session, **kwargs: object
    ) -> bool:
        del session, kwargs
        return self.complete_invocation_ok

    def mark_invocation_failed_for_attempt(
        self, session: Session, **kwargs: object
    ) -> bool:
        del session, kwargs
        return self.mark_invocation_failed_ok

    def job_has_valid_claim(
        self, session: Session, *, research_job_id: str, now: datetime
    ) -> bool:
        del session, research_job_id, now
        return False


def _sample_count(metrics: AtlasMetrics, metric_name: str, **labels: str) -> float:
    total = 0.0
    for family in metrics.registry.collect():
        for sample in family.samples:
            if sample.name != metric_name:
                continue
            if all(sample.labels.get(k) == v for k, v in labels.items()):
                total += sample.value
    return total


def _build_service(
    repo: _FakeModelInvocationRepository, metrics: AtlasMetrics
) -> ModelInvocationService:
    return ModelInvocationService(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        chat_model=cast(Any, object()),
        provider=ProviderId.OPENAI,
        model_name="gpt-test",
        call_timeout_seconds=25.0,
        repository=cast(Any, repo),
        metrics=metrics,
    )


class _StubSchema(BaseModel):
    tasks: list[str] = ["a", "b", "c"]


def test_successful_plan_observes_attempt_invocation_and_token_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    repo = _FakeModelInvocationRepository()
    service = _build_service(repo, metrics)

    validated = _StubSchema()
    meta = ModelCallMeta(
        provider=ProviderId.OPENAI,
        model="gpt-test",
        prompt_version="plan.v1",
        latency_ms=120,
        input_tokens=10,
        output_tokens=5,
        estimated_cost_usd=0.02,
        finish_outcome=FinishOutcome.COMPLETED,
    )

    def _fake_invoke_structured(**_kwargs: object) -> tuple[BaseModel, ModelCallMeta]:
        return validated, meta

    monkeypatch.setattr(model_service, "invoke_structured", _fake_invoke_structured)
    monkeypatch.setattr(
        model_service,
        "PlanStructuredOutput",
        _StubSchema,
    )

    result = service.plan(
        PlanRequest(
            job_id="job-1", question="What is Atlas?", prompt_version="plan.v1"
        ),
        workflow_execution_id="exec-1",
    )

    assert result.tasks == ["a", "b", "c"]
    assert (
        _sample_count(
            metrics,
            "atlas_model_attempts_total",
            node_name="plan",
            provider="openai",
            outcome="succeeded",
            retry_class="none",
        )
        == 1
    )
    assert (
        _sample_count(
            metrics,
            "atlas_model_invocations_total",
            node_name="plan",
            provider="openai",
            outcome="succeeded",
        )
        == 1
    )
    assert (
        _sample_count(
            metrics, "atlas_model_tokens_total", node_name="plan", token_type="input"
        )
        == 10
    )
    assert (
        _sample_count(
            metrics, "atlas_model_tokens_total", node_name="plan", token_type="output"
        )
        == 5
    )
    assert _sample_count(metrics, "atlas_model_cost_usd_total", node_name="plan") == (
        pytest.approx(0.02)
    )


def test_provider_failure_observes_failed_attempt_and_invocation_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    repo = _FakeModelInvocationRepository()
    service = _build_service(repo, metrics)

    def _fake_invoke_structured(**_kwargs: object) -> tuple[BaseModel, ModelCallMeta]:
        raise ModelTimeoutError("provider timed out")

    monkeypatch.setattr(model_service, "invoke_structured", _fake_invoke_structured)

    with pytest.raises(ModelTimeoutError):
        service.plan(
            PlanRequest(
                job_id="job-2", question="What is Atlas?", prompt_version="plan.v1"
            ),
            workflow_execution_id="exec-2",
        )

    assert (
        _sample_count(
            metrics,
            "atlas_model_attempts_total",
            node_name="plan",
            provider="openai",
            outcome="failed",
            retry_class="timeout",
        )
        == 1
    )
    assert (
        _sample_count(
            metrics,
            "atlas_model_invocations_total",
            node_name="plan",
            provider="openai",
            outcome="failed",
        )
        == 1
    )


def test_attempt_ownership_lost_observes_no_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    repo = _FakeModelInvocationRepository()
    repo.fail_attempt_ok = False
    service = _build_service(repo, metrics)

    def _fake_invoke_structured(**_kwargs: object) -> tuple[BaseModel, ModelCallMeta]:
        raise ModelTimeoutError("provider timed out")

    monkeypatch.setattr(model_service, "invoke_structured", _fake_invoke_structured)

    with pytest.raises(ModelAttemptOwnershipLostError):
        service.plan(
            PlanRequest(
                job_id="job-3", question="What is Atlas?", prompt_version="plan.v1"
            ),
            workflow_execution_id="exec-3",
        )

    assert _sample_count(metrics, "atlas_model_attempts_total") == 0
    assert _sample_count(metrics, "atlas_model_invocations_total") == 0
