"""Metric observations from ``ToolInvocationService._execute_once`` attempt paths.

Uses a fake repository and fake tool (in-memory, no real PostgreSQL) so this
stays a fast unit test; the durable ledger fencing/reclaim/retry behavior
already has dedicated real-database integration coverage under
``tests/integration/test_tool_invocation_ledger.py``.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy.orm import Session

from atlas.observability.metrics.catalog import AtlasMetrics
from atlas.tools.contracts import (
    ToolCallContext,
    ToolId,
    ToolInvocationResult,
    ToolOrigin,
    ToolProviderId,
    ToolResultMeta,
    ToolRetryClass,
)
from atlas.tools.errors import ToolAttemptOwnershipLostError, ToolTimeoutError
from atlas.tools.registry import ToolRegistry, default_permission_policy
from atlas.tools.service import ToolInvocationService


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


class _FakeToolInvocationRepository:
    def __init__(self) -> None:
        self.fail_attempt_ok = True
        self.mark_invocation_failed_ok = True
        self.complete_attempt_ok = True
        self.complete_invocation_ok = True

    def get_by_key(
        self, session: Session, invocation_key: str, *, for_update: bool = False
    ) -> None:
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


class _FakeSearchTool:
    tool_id = ToolId.WEB_SEARCH

    def __init__(self, *, raises: Exception | None = None) -> None:
        self._raises = raises

    def invoke(
        self, raw_input: dict[str, object], *, context: ToolCallContext
    ) -> ToolInvocationResult:
        del raw_input, context
        if self._raises is not None:
            raise self._raises
        return ToolInvocationResult(
            output={"results": []},
            meta=ToolResultMeta(
                tool_id=ToolId.WEB_SEARCH,
                provider=ToolProviderId.TAVILY,
                tool_version="v1",
                tool_policy_version="v1",
                latency_ms=50,
                status="succeeded",
                retry_class=ToolRetryClass.NONE,
            ),
            finding_text="finding",
        )


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
    tool: _FakeSearchTool,
    repo: _FakeToolInvocationRepository,
    metrics: AtlasMetrics,
) -> ToolInvocationService:
    return ToolInvocationService(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        registry=ToolRegistry({ToolId.WEB_SEARCH: tool}),
        policy=default_permission_policy(),
        provider_by_tool={ToolId.WEB_SEARCH: ToolProviderId.TAVILY},
        repository=repo,  # type: ignore[arg-type]
        max_attempts_per_call=1,
        metrics=metrics,
    )


def _workflow_context() -> ToolCallContext:
    return ToolCallContext(
        origin=ToolOrigin.WORKFLOW,
        research_job_id="job-1",
        workflow_execution_id="exec-1",
        node_name="research",
        workflow_node_attempt=1,
    )


def test_successful_invoke_observes_attempt_and_invocation_metrics() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    repo = _FakeToolInvocationRepository()
    service = _build_service(_FakeSearchTool(), repo, metrics)

    result = service.invoke(
        tool_id=ToolId.WEB_SEARCH,
        raw_input={"query": "atlas"},
        context=_workflow_context(),
    )

    assert result.finding_text == "finding"
    assert (
        _sample_count(
            metrics,
            "atlas_tool_attempts_total",
            tool_id="web_search",
            provider="tavily",
            outcome="succeeded",
            retry_class="none",
        )
        == 1
    )
    assert (
        _sample_count(
            metrics,
            "atlas_tool_invocations_total",
            tool_id="web_search",
            provider="tavily",
            outcome="succeeded",
        )
        == 1
    )


def test_tool_error_observes_failed_attempt_and_invocation_metrics() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    repo = _FakeToolInvocationRepository()
    service = _build_service(
        _FakeSearchTool(raises=ToolTimeoutError("timed out")), repo, metrics
    )

    with pytest.raises(ToolTimeoutError):
        service.invoke(
            tool_id=ToolId.WEB_SEARCH,
            raw_input={"query": "atlas"},
            context=_workflow_context(),
        )

    assert (
        _sample_count(
            metrics,
            "atlas_tool_attempts_total",
            tool_id="web_search",
            provider="tavily",
            outcome="failed",
            retry_class="timeout",
        )
        == 1
    )
    assert (
        _sample_count(
            metrics,
            "atlas_tool_invocations_total",
            tool_id="web_search",
            provider="tavily",
            outcome="failed",
        )
        == 1
    )


def test_unexpected_adapter_exception_observes_failed_metrics_as_unknown() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    repo = _FakeToolInvocationRepository()
    service = _build_service(
        _FakeSearchTool(raises=RuntimeError("adapter crashed")), repo, metrics
    )

    with pytest.raises(Exception):  # noqa: B017 - ToolUnknownError wraps RuntimeError
        service.invoke(
            tool_id=ToolId.WEB_SEARCH,
            raw_input={"query": "atlas"},
            context=_workflow_context(),
        )

    assert (
        _sample_count(
            metrics,
            "atlas_tool_attempts_total",
            tool_id="web_search",
            outcome="failed",
            retry_class="unknown",
        )
        == 1
    )


def test_attempt_ownership_lost_observes_no_metric() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    repo = _FakeToolInvocationRepository()
    repo.fail_attempt_ok = False
    service = _build_service(
        _FakeSearchTool(raises=ToolTimeoutError("timed out")), repo, metrics
    )

    with pytest.raises(ToolAttemptOwnershipLostError):
        service.invoke(
            tool_id=ToolId.WEB_SEARCH,
            raw_input={"query": "atlas"},
            context=_workflow_context(),
        )

    assert _sample_count(metrics, "atlas_tool_attempts_total") == 0
    assert _sample_count(metrics, "atlas_tool_invocations_total") == 0
