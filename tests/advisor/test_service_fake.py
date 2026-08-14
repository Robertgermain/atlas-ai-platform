"""Session is closed before the analyst port is invoked."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy.orm import Session

from atlas.advisor.errors import AdvisorySnapshotRejectedError
from atlas.advisor.fakes import DeterministicAdvisoryAnalyst
from atlas.advisor.service import AdvisoryService
from atlas.advisor.snapshot import SnapshotLoad
from atlas.observability.metrics.catalog import AtlasMetrics
from tests.advisor.fakes import (
    FakeAdvisorySnapshotPort,
    pending_job_load,
    snapshot_load_with_signal_count,
)


def test_read_scope_exits_before_analyst() -> None:
    events: list[str] = []
    loaded = pending_job_load()
    port = FakeAdvisorySnapshotPort(loaded)

    @contextmanager
    def tracking_scope() -> Iterator[Session]:
        events.append("enter")
        session = SimpleNamespace(closed_marker=False)
        try:
            yield session  # type: ignore[misc]
        finally:
            events.append("rollback")
            events.append("close")
            session.closed_marker = True
            events.append("exit")

    class TrackingAnalyst(DeterministicAdvisoryAnalyst):
        def analyze(self, facts, *, analysis_id=None):  # type: ignore[no-untyped-def]
            events.append("analyze")
            assert "exit" in events
            assert events.index("close") < events.index("analyze")
            assert events.index("rollback") < events.index("analyze")
            return super().analyze(facts, analysis_id=analysis_id)

    service = AdvisoryService(
        read_scope=tracking_scope,
        snapshot=port,
        analyst=TrackingAnalyst(),
        metrics=AtlasMetrics(CollectorRegistry()),
        analysis_id_factory=lambda: "00000000-0000-0000-0000-000000000001",
        mode="fake",
    )
    envelope = service.analyze_job("job-1")
    assert envelope.analysis_id == "00000000-0000-0000-0000-000000000001"
    assert events[:5] == ["enter", "rollback", "close", "exit", "analyze"]


def test_repository_double_marks_closed_before_analyze() -> None:
    events: list[str] = []

    class ClosedMarkerSession:
        def __init__(self) -> None:
            self.closed_marker = False

        def close(self) -> None:
            self.closed_marker = True
            events.append("factory-close")

    session = ClosedMarkerSession()

    @contextmanager
    def factory_scope() -> Iterator[Session]:
        try:
            yield session  # type: ignore[misc]
        finally:
            session.close()
            events.append("scope-exit")

    class TrackingAnalyst(DeterministicAdvisoryAnalyst):
        def analyze(self, facts, *, analysis_id=None):  # type: ignore[no-untyped-def]
            events.append("analyze")
            assert session.closed_marker is True
            return super().analyze(facts, analysis_id=analysis_id)

    metrics = AtlasMetrics(CollectorRegistry())
    service = AdvisoryService(
        read_scope=factory_scope,
        snapshot=FakeAdvisorySnapshotPort(pending_job_load()),
        analyst=TrackingAnalyst(),
        metrics=metrics,
        mode="fake",
    )
    envelope = service.analyze_job("job-1")
    assert events.index("factory-close") < events.index("analyze")
    assert events.index("scope-exit") < events.index("analyze")
    names = {
        sample.name
        for metric in metrics.registry.collect()
        for sample in metric.samples
    }
    assert "atlas_advisory_analyses_total" in names
    assert "atlas_model_attempts_total" not in names
    assert envelope.analysis.schema_version == "advisory.analysis.v1"


def test_snapshot_duration_is_excluded_from_advisory_analysis() -> None:
    clock = _Clock(100.0)

    class SlowSnapshot(FakeAdvisorySnapshotPort):
        def load(self, session: Session, research_job_id: str) -> SnapshotLoad:
            clock.advance(10.0)
            return super().load(session, research_job_id)

    class TimedAnalyst(DeterministicAdvisoryAnalyst):
        def analyze(self, facts, *, analysis_id=None):  # type: ignore[no-untyped-def]
            clock.advance(1.5)
            return super().analyze(facts, analysis_id=analysis_id)

    @contextmanager
    def closed_scope() -> Iterator[Session]:
        yield SimpleNamespace(closed_marker=True)  # type: ignore[misc]

    metrics = AtlasMetrics(CollectorRegistry())
    service = AdvisoryService(
        read_scope=closed_scope,
        snapshot=SlowSnapshot(pending_job_load()),
        analyst=TimedAnalyst(),
        metrics=metrics,
        monotonic=clock,
        mode="fake",
    )
    service.analyze_job("job-1")
    observed = _histogram_sum(metrics, "atlas_advisory_analysis_duration_seconds")
    assert observed == pytest.approx(1.5)
    assert observed < 10.0


def test_signal_overflow_rejects_before_analyst() -> None:
    events: list[str] = []

    @contextmanager
    def closed_scope() -> Iterator[Session]:
        yield SimpleNamespace(closed_marker=True)  # type: ignore[misc]

    class TrackingAnalyst(DeterministicAdvisoryAnalyst):
        def analyze(self, facts, *, analysis_id=None):  # type: ignore[no-untyped-def]
            events.append("analyze")
            return super().analyze(facts, analysis_id=analysis_id)

    metrics = AtlasMetrics(CollectorRegistry())
    service = AdvisoryService(
        read_scope=closed_scope,
        snapshot=FakeAdvisorySnapshotPort(snapshot_load_with_signal_count(65)),
        analyst=TrackingAnalyst(),
        metrics=metrics,
        mode="fake",
    )
    with pytest.raises(AdvisorySnapshotRejectedError, match="signal bound"):
        service.analyze_job("job-1")
    assert events == []
    outcomes = {
        sample.labels.get("outcome")
        for metric in metrics.registry.collect()
        for sample in metric.samples
        if sample.name == "atlas_advisory_snapshot_assemblies_total"
        and sample.value > 0
    }
    assert outcomes == {"rejected"}


class _Clock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def _histogram_sum(metrics: AtlasMetrics, name: str) -> float:
    total = 0.0
    for metric in metrics.registry.collect():
        for sample in metric.samples:
            if sample.name == f"{name}_sum":
                total += float(sample.value)
    return total
