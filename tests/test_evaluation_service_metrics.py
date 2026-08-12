"""Metric observations from ``EvaluationService.finalize_success``/``finalize_failure``.

Uses a fake repository (in-memory, no real PostgreSQL) so this stays a fast
unit test; ``EvaluationService.begin_or_resume`` and full fencing behavior
already have dedicated real-database integration coverage under
``tests/integration``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy.orm import Session

from atlas.evaluation.contracts import DimensionResult, EvaluationRunResult
from atlas.evaluation.errors import EvaluationOwnershipLostError
from atlas.evaluation.repository import SqlAlchemyEvaluationRepository
from atlas.evaluation.service import EvaluationService
from atlas.observability.metrics.catalog import AtlasMetrics
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository


@dataclass
class _FakeRunRow:
    id: str
    status: str
    ownership_token: str
    evaluation_profile: str = "evaluation.candidate.v1"
    dimensions: list[DimensionResult] = field(default_factory=list)


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


class _RaisingCommitSession(_FakeSession):
    """Simulates ``session_scope``'s commit itself failing (Slice 15A2 correction)."""

    def commit(self) -> None:
        raise RuntimeError("commit-secret-failure")


class _RaisingCommitSessionFactory:
    def __call__(self) -> _RaisingCommitSession:
        return _RaisingCommitSession()


class _FakeEvaluationRepository(SqlAlchemyEvaluationRepository):
    """Overrides only the methods ``finalize_success``/``finalize_failure`` use."""

    def __init__(self) -> None:
        self.rows: dict[str, _FakeRunRow] = {}

    def seed(self, row: _FakeRunRow) -> None:
        self.rows[row.id] = row

    def get_by_id(  # type: ignore[override]
        self,
        session: Session,
        run_id: str,
        *,
        for_update: bool = False,
    ) -> _FakeRunRow | None:
        del session, for_update
        return self.rows.get(run_id)

    def replace_dimensions(
        self,
        session: Session,
        *,
        evaluation_run_id: str,
        dimensions: list[DimensionResult],
    ) -> None:
        del session
        self.rows[evaluation_run_id].dimensions = list(dimensions)

    def clear_dimensions(self, session: Session, *, evaluation_run_id: str) -> None:
        del session
        self.rows[evaluation_run_id].dimensions = []

    def finalize_success(
        self,
        session: Session,
        *,
        run_id: str,
        ownership_token: str,
        aggregate_score: float,
        passed: bool,
        disposition_hint: str,
        grader_versions: dict[str, str],
        at: datetime,
    ) -> bool:
        del session, aggregate_score, disposition_hint, grader_versions, at
        row = self.rows.get(run_id)
        if (
            row is None
            or row.status != "IN_PROGRESS"
            or row.ownership_token != (ownership_token)
        ):
            return False
        row.status = "SUCCEEDED"
        return True

    def finalize_failure(
        self,
        session: Session,
        *,
        run_id: str,
        ownership_token: str,
        at: datetime,
        grader_versions: dict[str, object] | None = None,
    ) -> bool:
        del session, at, grader_versions
        row = self.rows.get(run_id)
        if (
            row is None
            or row.status != "IN_PROGRESS"
            or row.ownership_token != (ownership_token)
        ):
            return False
        row.status = "FAILED"
        return True

    def to_result(
        self,
        session: Session,
        record: _FakeRunRow,  # type: ignore[override]
        *,
        include_ownership_token: bool = False,
    ) -> EvaluationRunResult:
        del session, include_ownership_token
        return EvaluationRunResult(
            run_id=record.id,
            research_job_id="job-1",
            workflow_execution_id="exec-1",
            evaluation_profile=record.evaluation_profile,  # type: ignore[arg-type]
            evaluation_attempt=1,
            status=record.status,  # type: ignore[arg-type]
            input_fingerprint="0" * 64,
            passed=record.status == "SUCCEEDED",
            aggregate_score=1.0 if record.status == "SUCCEEDED" else None,
            disposition_hint="complete" if record.status == "SUCCEEDED" else "terminal",
            dimensions=record.dimensions,
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


def _passing_dimension() -> DimensionResult:
    return DimensionResult(
        name="coverage",
        score=1.0,
        passed=True,
        method="deterministic",
        is_hard=False,
        is_provisional=False,
    )


def _failing_dimension() -> DimensionResult:
    return DimensionResult(
        name="citation_integrity",
        score=0.0,
        passed=False,
        method="deterministic",
        is_hard=True,
        is_provisional=False,
    )


def _build_service(
    repo: _FakeEvaluationRepository, metrics: AtlasMetrics
) -> EvaluationService:
    return EvaluationService(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        job_repository=SqlAlchemyResearchJobRepository(),
        metrics=metrics,
    )


def test_finalize_success_observes_run_and_dimension_metrics() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    repo = _FakeEvaluationRepository()
    repo.seed(_FakeRunRow(id="run-1", status="IN_PROGRESS", ownership_token="tok-1"))
    service = _build_service(repo, metrics)

    service.finalize_success(
        run_id="run-1",
        ownership_token="tok-1",
        aggregate=0.9,
        passed=True,
        dimensions=[_passing_dimension(), _failing_dimension()],
        disposition_hint="complete",
        grader_versions={},
    )

    assert (
        _sample_count(
            metrics,
            "atlas_evaluation_runs_total",
            profile="evaluation.candidate.v1",
            outcome="succeeded",
        )
        == 1
    )
    assert (
        _sample_count(
            metrics,
            "atlas_evaluation_dimension_outcomes_total",
            dimension="coverage",
            outcome="passed",
        )
        == 1
    )
    assert (
        _sample_count(
            metrics,
            "atlas_evaluation_dimension_outcomes_total",
            dimension="citation_integrity",
            outcome="failed",
        )
        == 1
    )


def test_finalize_failure_observes_failed_run_metric_only() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    repo = _FakeEvaluationRepository()
    repo.seed(_FakeRunRow(id="run-2", status="IN_PROGRESS", ownership_token="tok-2"))
    service = _build_service(repo, metrics)

    service.finalize_failure(
        run_id="run-2", ownership_token="tok-2", error_class="RuntimeError"
    )

    assert (
        _sample_count(
            metrics,
            "atlas_evaluation_runs_total",
            profile="evaluation.candidate.v1",
            outcome="failed",
        )
        == 1
    )
    assert _sample_count(metrics, "atlas_evaluation_dimension_outcomes_total") == 0


def test_finalize_success_commit_failure_observes_no_metric() -> None:
    """A ``session_scope`` commit failure must never emit a durable-transition metric.

    ``finalize_success`` observes its metrics only after the enclosing
    ``with session_scope(...)`` block above it has exited successfully; a
    commit failure there raises out of the block before that point is ever
    reached (Slice 15A2 correction).
    """
    metrics = AtlasMetrics(CollectorRegistry())
    repo = _FakeEvaluationRepository()
    repo.seed(_FakeRunRow(id="run-4", status="IN_PROGRESS", ownership_token="tok-4"))
    service = EvaluationService(
        session_factory=_RaisingCommitSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        job_repository=SqlAlchemyResearchJobRepository(),
        metrics=metrics,
    )

    with pytest.raises(RuntimeError):
        service.finalize_success(
            run_id="run-4",
            ownership_token="tok-4",
            aggregate=0.9,
            passed=True,
            dimensions=[_passing_dimension()],
            disposition_hint="complete",
            grader_versions={},
        )

    assert _sample_count(metrics, "atlas_evaluation_runs_total") == 0
    assert _sample_count(metrics, "atlas_evaluation_dimension_outcomes_total") == 0


def test_finalize_failure_commit_failure_observes_no_metric() -> None:
    """Mirrors the success-path commit-failure regression above for failures."""
    metrics = AtlasMetrics(CollectorRegistry())
    repo = _FakeEvaluationRepository()
    repo.seed(_FakeRunRow(id="run-5", status="IN_PROGRESS", ownership_token="tok-5"))
    service = EvaluationService(
        session_factory=_RaisingCommitSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        job_repository=SqlAlchemyResearchJobRepository(),
        metrics=metrics,
    )

    with pytest.raises(RuntimeError):
        service.finalize_failure(
            run_id="run-5", ownership_token="tok-5", error_class="RuntimeError"
        )

    assert _sample_count(metrics, "atlas_evaluation_runs_total") == 0


def test_finalize_success_ownership_lost_observes_no_metric() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    repo = _FakeEvaluationRepository()
    repo.seed(
        _FakeRunRow(id="run-3", status="IN_PROGRESS", ownership_token="real-token")
    )
    service = _build_service(repo, metrics)

    with pytest.raises(EvaluationOwnershipLostError):
        service.finalize_success(
            run_id="run-3",
            ownership_token="stale-token",
            aggregate=0.9,
            passed=True,
            dimensions=[],
            disposition_hint="complete",
            grader_versions={},
        )

    assert _sample_count(metrics, "atlas_evaluation_runs_total") == 0
