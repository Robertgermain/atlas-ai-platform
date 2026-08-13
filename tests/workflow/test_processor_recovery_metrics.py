"""Recovery-decision metric ordering from ``LangGraphResearchProcessor``.

Constructs the processor via ``object.__new__`` with fake repositories/session
factory (no LangGraph graph, no real PostgreSQL) so this stays a fast unit
test isolating only the metric-emission-vs-commit ordering this correction
pass fixes; full retry/fencing/idempotency behavior already has dedicated
integration coverage in ``tests/integration/test_policy_decision_replay.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from prometheus_client import CollectorRegistry

from atlas.config.settings import get_settings
from atlas.observability.metrics.catalog import AtlasMetrics
from atlas.workflow.processor import LangGraphResearchProcessor


class _FakeSession:
    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


class _RaisingCommitSession(_FakeSession):
    """Simulates the durable policy-decision ``session_scope``'s own commit failing."""

    def commit(self) -> None:
        raise RuntimeError("commit-secret-failure")


class _FakeSessionFactory:
    """First call is the read-only attempt-counts lookup; every later call is
    the durable policy-decision write -- matching ``_handle_exception``'s own
    two separate ``session_scope`` blocks on its retry path."""

    def __init__(self, *, raise_on_write: bool = False) -> None:
        self._raise_on_write = raise_on_write
        self._calls = 0

    def __call__(self) -> _FakeSession:
        self._calls += 1
        if self._calls >= 2 and self._raise_on_write:
            return _RaisingCommitSession()
        return _FakeSession()


class _FakeJobRepo:
    def __init__(self) -> None:
        self.schedule_retry_called = False

    def get_attempt_counts(
        self, session: object, *, job_id: str
    ) -> tuple[int, int, int]:
        del session, job_id
        return (0, 0, 0)

    def schedule_retry(self, session: object, **kwargs: object) -> bool:
        del session, kwargs
        self.schedule_retry_called = True
        return True


@dataclass
class _FakeRecoveryAttempt:
    next_attempt_at: datetime
    attempt_number: int


class _FakeRecoveryRepo:
    """``authoritative_id`` pre-seeded to a *different* id than the fresh
    ``decision_id`` simulates ``insert_policy_decision``'s own idempotent-
    replay return value; left ``None`` simulates a genuinely fresh insert."""

    def __init__(self) -> None:
        self.authoritative_id: str | None = None
        self.insert_recovery_attempt_called = False

    def insert_policy_decision(
        self, session: object, *, id: str, **kwargs: object
    ) -> str:
        del session, kwargs
        if self.authoritative_id is None:
            self.authoritative_id = id
        return self.authoritative_id

    def insert_recovery_attempt(self, session: object, **kwargs: object) -> None:
        del session, kwargs
        self.insert_recovery_attempt_called = True

    def get_recovery_attempt_by_policy(
        self, session: object, *, policy_decision_id: str
    ) -> _FakeRecoveryAttempt | None:
        del session, policy_decision_id
        return _FakeRecoveryAttempt(next_attempt_at=datetime.now(UTC), attempt_number=1)


class _FakeOutbox:
    def enqueue(self, session: object, event: object) -> None:
        del session, event


def _sample_count(metrics: AtlasMetrics, metric_name: str, **labels: str) -> float:
    total = 0.0
    for family in metrics.registry.collect():
        for sample in family.samples:
            if sample.name != metric_name:
                continue
            if all(sample.labels.get(k) == v for k, v in labels.items()):
                total += sample.value
    return total


def _build_processor(
    *,
    session_factory: _FakeSessionFactory,
    metrics: AtlasMetrics,
    recovery_repo: _FakeRecoveryRepo,
    job_repo: _FakeJobRepo,
) -> LangGraphResearchProcessor:
    processor = object.__new__(LangGraphResearchProcessor)
    processor._session_factory = session_factory  # type: ignore[assignment]
    processor._settings = get_settings()
    processor._repository = None  # type: ignore[assignment]
    processor._job_repo = job_repo  # type: ignore[assignment]
    processor._recovery_repo = recovery_repo  # type: ignore[assignment]
    processor._outbox = _FakeOutbox()
    processor._metrics = metrics
    return processor


def test_handle_exception_retry_observes_metric_only_after_durable_commit() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    job_repo = _FakeJobRepo()
    processor = _build_processor(
        session_factory=_FakeSessionFactory(),
        metrics=metrics,
        recovery_repo=_FakeRecoveryRepo(),
        job_repo=job_repo,
    )

    outcome = processor._handle_exception(
        exc=TimeoutError("timed out"),
        job_id="job-1",
        claim_token="claim-1",
        execution_id="exec-1",
    )

    assert outcome.__class__.__name__ == "RetryScheduled"
    assert job_repo.schedule_retry_called is True
    assert (
        _sample_count(
            metrics,
            "atlas_recovery_decisions_total",
            action="retry",
            failure_category="TRANSIENT_TIMEOUT",
        )
        == 1
    )


def test_handle_exception_commit_failure_observes_no_metric() -> None:
    """A durable policy-decision commit failure must never emit a metric.

    ``_handle_exception`` observes ``atlas_recovery_decisions_total`` only
    after its second (durable-write) ``session_scope`` block has exited
    successfully; a commit failure there raises before that point is ever
    reached (Slice 15A2 correction).
    """
    metrics = AtlasMetrics(CollectorRegistry())
    processor = _build_processor(
        session_factory=_FakeSessionFactory(raise_on_write=True),
        metrics=metrics,
        recovery_repo=_FakeRecoveryRepo(),
        job_repo=_FakeJobRepo(),
    )

    with pytest.raises(RuntimeError):
        processor._handle_exception(
            exc=TimeoutError("timed out"),
            job_id="job-1",
            claim_token="claim-1",
            execution_id="exec-1",
        )

    assert _sample_count(metrics, "atlas_recovery_decisions_total") == 0


def test_handle_exception_idempotent_replay_observes_no_second_metric() -> None:
    """A replayed decision fingerprint (``created`` False) must never double-count."""
    metrics = AtlasMetrics(CollectorRegistry())
    recovery_repo = _FakeRecoveryRepo()
    recovery_repo.authoritative_id = "existing-decision-id"
    job_repo = _FakeJobRepo()
    processor = _build_processor(
        session_factory=_FakeSessionFactory(),
        metrics=metrics,
        recovery_repo=recovery_repo,
        job_repo=job_repo,
    )

    outcome = processor._handle_exception(
        exc=TimeoutError("timed out"),
        job_id="job-1",
        claim_token="claim-1",
        execution_id="exec-1",
    )

    assert outcome.__class__.__name__ == "RetryScheduled"
    assert job_repo.schedule_retry_called is False
    assert recovery_repo.insert_recovery_attempt_called is False
    assert _sample_count(metrics, "atlas_recovery_decisions_total") == 0
