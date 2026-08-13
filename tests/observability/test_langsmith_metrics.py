"""Bounded LangSmith metric labels (Slice 15B)."""

from __future__ import annotations

from prometheus_client import CollectorRegistry

from atlas.observability.metrics.catalog import AtlasMetrics


def _metrics() -> AtlasMetrics:
    return AtlasMetrics(CollectorRegistry())


def _labels(metrics: AtlasMetrics, name: str, label: str) -> set[str]:
    values: set[str] = set()
    for family in metrics.registry.collect():
        for sample in family.samples:
            if sample.name != name:
                continue
            values.add(sample.labels[label])
    return values


def test_langsmith_observer_succeeds() -> None:
    metrics = _metrics()
    metrics.observe_langsmith_operation(operation="initialize", outcome="success")
    assert "atlas_langsmith_operations_total" in {
        sample.name
        for family in metrics.registry.collect()
        for sample in family.samples
    }


def test_unknown_langsmith_operation_and_outcome_map_to_other() -> None:
    metrics = _metrics()
    metrics.observe_langsmith_operation(
        operation="not-a-real-operation", outcome="not-a-real-outcome"
    )
    assert _labels(metrics, "atlas_langsmith_operations_total", "operation") == {
        "other"
    }
    assert _labels(metrics, "atlas_langsmith_operations_total", "outcome") == {"other"}


def test_allowed_langsmith_labels_are_preserved() -> None:
    metrics = _metrics()
    metrics.observe_langsmith_operation(operation="export", outcome="timeout")
    assert _labels(metrics, "atlas_langsmith_operations_total", "operation") == {
        "export"
    }
    assert _labels(metrics, "atlas_langsmith_operations_total", "outcome") == {
        "timeout"
    }
