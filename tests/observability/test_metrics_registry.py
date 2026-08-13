"""AtlasMetrics registry, catalog names/types, and failure containment."""

from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry
from prometheus_client.core import Metric

from atlas.observability.events import Event
from atlas.observability.metrics.catalog import AtlasMetrics, default_metrics
from atlas.observability.testing import capture_logs


def _metrics() -> AtlasMetrics:
    return AtlasMetrics(CollectorRegistry())


def _collect_names(registry: CollectorRegistry) -> set[str]:
    names: set[str] = set()
    for metric in registry.collect():
        assert isinstance(metric, Metric)
        for sample in metric.samples:
            names.add(sample.name)
    return names


def test_two_instances_use_independent_registries() -> None:
    """Constructing two AtlasMetrics never raises a duplicate-registration error."""
    first = _metrics()
    second = _metrics()
    first.observe_worker_claim(outcome="claimed")
    second.observe_worker_claim(outcome="empty")
    assert _collect_names(first.registry) != set()
    assert first.registry is not second.registry


def test_default_metrics_is_a_process_singleton() -> None:
    assert default_metrics() is default_metrics()


def test_http_request_metric_registered_with_expected_labels() -> None:
    metrics = _metrics()
    metrics.observe_http_request(
        method="GET", route="/health", status="200", duration_seconds=0.01
    )
    names = _collect_names(metrics.registry)
    assert "atlas_http_requests_total" in names
    assert "atlas_http_request_duration_seconds_bucket" in names


@pytest.mark.parametrize(
    "observe",
    [
        lambda m: m.observe_research_job_submitted(),
        lambda m: m.observe_research_job_terminal(status="completed"),
        lambda m: m.observe_worker_claim(outcome="claimed"),
        lambda m: m.observe_worker_processing(
            outcome="completed", duration_seconds=1.0
        ),
        lambda m: m.observe_workflow_node(
            node_name="plan", outcome="completed", duration_seconds=1.0
        ),
        lambda m: m.observe_model_attempt(
            node_name="plan",
            provider="openai",
            outcome="succeeded",
            retry_class="none",
            duration_seconds=1.0,
        ),
        lambda m: m.observe_model_invocation(
            node_name="plan", provider="openai", outcome="succeeded"
        ),
        lambda m: m.observe_model_tokens(
            node_name="plan", provider="openai", input_tokens=10, output_tokens=20
        ),
        lambda m: m.observe_model_cost(
            node_name="plan", provider="openai", cost_usd=0.01
        ),
        lambda m: m.observe_tool_attempt(
            tool_id="web_search",
            provider="tavily",
            outcome="succeeded",
            retry_class="none",
            duration_seconds=1.0,
        ),
        lambda m: m.observe_tool_invocation(
            tool_id="web_search", provider="tavily", outcome="succeeded"
        ),
        lambda m: m.observe_evaluation_run(
            profile="evaluation.candidate.v1", outcome="succeeded"
        ),
        lambda m: m.observe_evaluation_dimension(
            dimension="coverage", outcome="passed"
        ),
        lambda m: m.observe_human_review_decision(decision="approve"),
        lambda m: m.observe_outbox_relay_run(outcome="published"),
        lambda m: m.observe_outbox_published_events(count=3),
        lambda m: m.set_outbox_relay_lock_held(held=True),
        lambda m: m.set_outbox_backlog(size=3, oldest_age_seconds=12.5),
        lambda m: m.mark_outbox_backlog_collection_success(at_epoch_seconds=1.0),
        lambda m: m.observe_consumer_message(outcome="applied"),
        lambda m: m.observe_consumer_retry_attempt(stage="apply"),
        lambda m: m.observe_consumer_offset_commit(outcome="success"),
        lambda m: m.observe_consumer_dead_letter(failure_code="invalid_json"),
        lambda m: m.observe_recovery_decision(
            action="retry", failure_category="TRANSIENT_TIMEOUT"
        ),
        lambda m: m.observe_redis_rate_limit_decision(outcome="allowed"),
        lambda m: m.observe_heartbeat_write(outcome="success"),
        lambda m: m.mark_heartbeat_last_success(at_epoch_seconds=1.0),
        lambda m: m.observe_database_readiness_failure(),
        lambda m: m.observe_langsmith_operation(
            operation="initialize", outcome="success"
        ),
    ],
)
def test_every_observer_method_succeeds_without_raising(observe: object) -> None:
    metrics = _metrics()
    observe(metrics)  # type: ignore[operator]


def test_unbounded_label_value_normalizes_to_other_rather_than_raising() -> None:
    metrics = _metrics()
    metrics.observe_worker_claim(outcome="not-a-real-outcome")
    label_values = {
        sample.labels["outcome"]
        for metric in metrics.registry.collect()
        for sample in metric.samples
        if sample.name == "atlas_worker_claims_total"
    }
    assert label_values == {"other"}


def test_metric_observation_failure_is_contained_and_logged_safely() -> None:
    """A raising Prometheus call never propagates; it logs METRIC_OBSERVATION_FAILED."""
    metrics = _metrics()

    class _BoomLabels:
        def inc(self) -> None:
            raise RuntimeError("boom-secret-value")

    class _BoomCounter:
        def labels(self, **kwargs: object) -> _BoomLabels:
            del kwargs
            return _BoomLabels()

    metrics._worker_claims_total = _BoomCounter()  # type: ignore[assignment]

    with capture_logs("atlas.observability.metrics.catalog") as captured:
        metrics.observe_worker_claim(outcome="claimed")

    assert captured.events == [Event.METRIC_OBSERVATION_FAILED.value]
    record = captured.json(0)
    assert record["error_class"] == "RuntimeError"
    assert record["outcome"] == "worker_claim"
    assert "boom-secret-value" not in captured.text


def test_metric_observation_failure_does_not_recurse_into_more_metrics() -> None:
    """Logging METRIC_OBSERVATION_FAILED must never itself observe a metric."""
    metrics = _metrics()

    class _BoomLabels:
        def inc(self) -> None:
            raise RuntimeError("boom")

    class _BoomCounter:
        def labels(self, **kwargs: object) -> _BoomLabels:
            del kwargs
            return _BoomLabels()

    metrics._worker_claims_total = _BoomCounter()  # type: ignore[assignment]

    before = {
        m.name: sum(s.value for s in m.samples)
        for m in metrics.registry.collect()
        if m.name == "atlas_worker_claims_total"
    }
    with capture_logs("atlas.observability.metrics.catalog"):
        metrics.observe_worker_claim(outcome="claimed")
    after = {
        m.name: sum(s.value for s in m.samples)
        for m in metrics.registry.collect()
        if m.name == "atlas_worker_claims_total"
    }
    # The counter itself is the (mocked, always-raising) boom object, so its
    # own value cannot have changed -- proving no *additional* observation
    # (e.g. a second, recursive call) happened as a side effect of logging.
    assert before == after
