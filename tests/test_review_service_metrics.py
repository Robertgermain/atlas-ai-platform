"""Metric observation ordering from ``ReviewService.submit_decision`` (Slice 15A2).

Uses fake repositories/session factory (no real PostgreSQL) so this stays a
fast unit test, isolating only the metric-emission-vs-commit ordering this
correction pass fixes; full review-decision fencing/readiness behavior
already has dedicated real-database/API coverage elsewhere.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from prometheus_client import CollectorRegistry

from atlas.application.review import ReviewConflictError, ReviewService
from atlas.evaluation.contracts import EvaluationRunResult
from atlas.evaluation.service import EvaluationService
from atlas.observability.metrics.catalog import AtlasMetrics
from atlas.persistence.repositories.recovery import SqlAlchemyRecoveryRepository
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository


@dataclass
class _JobModel:
    id: str
    status: str
    active_workflow_execution_id: str | None


@dataclass
class _ExecModel:
    id: str
    research_job_id: str
    status: str


class _FakeSession:
    """Duck-typed stand-in for the one ``session.get(ResearchJobModel, ...)``
    call ``ReviewService.submit_decision`` makes directly (not through a
    repository)."""

    def __init__(self, job_model: _JobModel) -> None:
        self._job_model = job_model

    def get(
        self, model_cls: object, pk: str, with_for_update: bool = False
    ) -> _JobModel | None:
        del model_cls, with_for_update
        return self._job_model if pk == self._job_model.id else None

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


class _RaisingCommitSession(_FakeSession):
    """Simulates the durable-write ``session_scope``'s own commit failing."""

    def commit(self) -> None:
        raise RuntimeError("commit-secret-failure")


class _FakeSessionFactory:
    """First call (the read-only readiness check) always gets a normal
    session; every call after that (the durable-write ``session_scope``)
    gets ``commit_session_cls`` -- matching ``submit_decision``'s own two
    separate ``session_scope`` blocks.
    """

    def __init__(
        self,
        job_model: _JobModel,
        *,
        commit_session_cls: type[_FakeSession] = _FakeSession,
    ) -> None:
        self._job_model = job_model
        self._commit_session_cls = commit_session_cls
        self._calls = 0

    def __call__(self) -> _FakeSession:
        self._calls += 1
        cls = _FakeSession if self._calls == 1 else self._commit_session_cls
        return cls(self._job_model)


class _FakeRecoveryRepository(SqlAlchemyRecoveryRepository):
    def __init__(self) -> None:
        self.inserted_ids: list[str] = []
        self.existing: object | None = None

    def get_review_decision_by_idempotency(  # type: ignore[override]
        self, session: object, *, research_job_id: str, idempotency_key: str
    ) -> object | None:
        del session, research_job_id, idempotency_key
        return self.existing

    def insert_review_decision(self, session: object, **kwargs: object) -> None:
        del session
        self.inserted_ids.append(str(kwargs["id"]))


class _FakeWorkflowRepository(SqlAlchemyWorkflowRepository):
    def __init__(self, exec_model: _ExecModel) -> None:
        self._exec_model = exec_model

    def get_execution(  # type: ignore[override]
        self, session: object, *, execution_id: str
    ) -> _ExecModel | None:
        del session
        return self._exec_model if execution_id == self._exec_model.id else None


class _FakeJobRepository(SqlAlchemyResearchJobRepository):
    def __init__(self) -> None:
        self.approved = False

    def approve_review_to_pending(
        self,
        session: object,
        *,
        job_id: str,
        execution_id: str,
        next_attempt_at: object,
        at: object,
    ) -> bool:
        del session, job_id, execution_id, next_attempt_at, at
        self.approved = True
        return True

    def fail_from_review(
        self, session: object, *, job_id: str, reason: str, at: object
    ) -> bool:
        del session, job_id, reason, at
        return True


class _FakeEvaluationService(EvaluationService):
    def __init__(self, runs: list[EvaluationRunResult]) -> None:
        self._runs = runs

    def get_by_job(self, job_id: str) -> list[EvaluationRunResult]:
        del job_id
        return self._runs


def _sample_count(metrics: AtlasMetrics, metric_name: str, **labels: str) -> float:
    total = 0.0
    for family in metrics.registry.collect():
        for sample in family.samples:
            if sample.name != metric_name:
                continue
            if all(sample.labels.get(k) == v for k, v in labels.items()):
                total += sample.value
    return total


def _failed_run() -> EvaluationRunResult:
    return EvaluationRunResult(
        run_id="run-1",
        research_job_id="job-1",
        workflow_execution_id="exec-1",
        evaluation_profile="evaluation.candidate.v1",
        evaluation_attempt=1,
        status="SUCCEEDED",
        input_fingerprint="0" * 64,
        passed=False,
        aggregate_score=0.1,
        disposition_hint="await_review",
        dimensions=[],
    )


def _build_service(
    metrics: AtlasMetrics,
    job_model: _JobModel,
    exec_model: _ExecModel,
    *,
    monkeypatch: pytest.MonkeyPatch,
    commit_session_cls: type[_FakeSession] = _FakeSession,
) -> tuple[ReviewService, _FakeJobRepository, _FakeRecoveryRepository]:
    monkeypatch.setattr(
        ReviewService, "_checkpoint_ready_for_complete", lambda self, execution_id: True
    )
    job_repo = _FakeJobRepository()
    recovery_repo = _FakeRecoveryRepository()
    service = ReviewService(
        session_factory=_FakeSessionFactory(  # type: ignore[arg-type]
            job_model, commit_session_cls=commit_session_cls
        ),
        database_url="postgresql://unused/unused",
        job_repo=job_repo,
        recovery_repo=recovery_repo,
        workflow_repo=_FakeWorkflowRepository(exec_model),
        evaluation_service=_FakeEvaluationService([_failed_run()]),
        metrics=metrics,
    )
    return service, job_repo, recovery_repo


def test_submit_decision_observes_metric_only_after_durable_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    job = _JobModel(
        id="job-1", status="AWAITING_REVIEW", active_workflow_execution_id="exec-1"
    )
    exec_model = _ExecModel(id="exec-1", research_job_id="job-1", status="RUNNING")
    service, job_repo, _ = _build_service(
        metrics, job, exec_model, monkeypatch=monkeypatch
    )

    decision_id, outcome = service.submit_decision(
        job_id="job-1",
        decision="approve",
        actor_id="operator-1",
        idempotency_key="idem-1",
    )

    assert outcome == "created"
    assert decision_id
    assert job_repo.approved is True
    assert (
        _sample_count(metrics, "atlas_human_review_decisions_total", decision="approve")
        == 1
    )


def test_submit_decision_commit_failure_observes_no_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A durable-write ``session_scope`` commit failure must never emit a metric.

    ``submit_decision`` observes ``atlas_human_review_decisions_total`` only
    after its second (durable-write) ``session_scope`` block has exited
    successfully; a commit failure there raises before that point is ever
    reached (Slice 15A2 correction).
    """
    metrics = AtlasMetrics(CollectorRegistry())
    job = _JobModel(
        id="job-1", status="AWAITING_REVIEW", active_workflow_execution_id="exec-1"
    )
    exec_model = _ExecModel(id="exec-1", research_job_id="job-1", status="RUNNING")
    service, _, _ = _build_service(
        metrics,
        job,
        exec_model,
        monkeypatch=monkeypatch,
        commit_session_cls=_RaisingCommitSession,
    )

    with pytest.raises(RuntimeError):
        service.submit_decision(
            job_id="job-1",
            decision="approve",
            actor_id="operator-1",
            idempotency_key="idem-2",
        )

    assert _sample_count(metrics, "atlas_human_review_decisions_total") == 0


def test_submit_decision_idempotent_replay_observes_no_second_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An idempotent replay of an already-decided key never double-counts."""
    metrics = AtlasMetrics(CollectorRegistry())
    job = _JobModel(
        id="job-1", status="AWAITING_REVIEW", active_workflow_execution_id="exec-1"
    )
    exec_model = _ExecModel(id="exec-1", research_job_id="job-1", status="RUNNING")
    service, _, recovery_repo = _build_service(
        metrics, job, exec_model, monkeypatch=monkeypatch
    )

    decision_id, outcome = service.submit_decision(
        job_id="job-1",
        decision="approve",
        actor_id="operator-1",
        idempotency_key="idem-3",
    )
    assert outcome == "created"
    assert (
        _sample_count(metrics, "atlas_human_review_decisions_total", decision="approve")
        == 1
    )

    @dataclass
    class _ExistingDecision:
        id: str
        request_fingerprint: str

    from atlas.application.review import ReviewService as _RS

    fingerprint = _RS._fingerprint_request(
        decision="approve", actor_id="operator-1", evaluation_run_id=None
    )
    recovery_repo.existing = _ExistingDecision(
        id=decision_id, request_fingerprint=fingerprint
    )

    replay_id, replay_outcome = service.submit_decision(
        job_id="job-1",
        decision="approve",
        actor_id="operator-1",
        idempotency_key="idem-3",
    )

    assert replay_outcome == "replayed"
    assert replay_id == decision_id
    assert (
        _sample_count(metrics, "atlas_human_review_decisions_total", decision="approve")
        == 1
    )


def test_submit_decision_conflicting_replay_raises_and_observes_no_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    job = _JobModel(
        id="job-1", status="AWAITING_REVIEW", active_workflow_execution_id="exec-1"
    )
    exec_model = _ExecModel(id="exec-1", research_job_id="job-1", status="RUNNING")
    service, _, recovery_repo = _build_service(
        metrics, job, exec_model, monkeypatch=monkeypatch
    )

    @dataclass
    class _ExistingDecision:
        id: str
        request_fingerprint: str

    recovery_repo.existing = _ExistingDecision(
        id="prior-decision", request_fingerprint="0" * 64
    )

    with pytest.raises(ReviewConflictError):
        service.submit_decision(
            job_id="job-1",
            decision="approve",
            actor_id="operator-1",
            idempotency_key="idem-4",
        )

    assert _sample_count(metrics, "atlas_human_review_decisions_total") == 0
