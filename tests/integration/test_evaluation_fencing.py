"""Evaluation ownership fencing, reclaim, conflict, and replay."""

from __future__ import annotations

import secrets
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.orm import Session, sessionmaker

from atlas.domain import ResearchJob
from atlas.evaluation.claim_fingerprint import fingerprint_job_claim_token
from atlas.evaluation.contracts import EVALUATION_PROFILE, DimensionResult
from atlas.evaluation.errors import (
    EvaluationConflictError,
    EvaluationInProgressError,
    EvaluationOwnershipLostError,
    EvaluationValidationError,
)
from atlas.evaluation.repository import SqlAlchemyEvaluationRepository
from atlas.evaluation.service import EvaluationService
from atlas.persistence.db import session_scope
from atlas.persistence.models.evaluation import (
    EvaluationDimensionResultModel,
    EvaluationRunModel,
)
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository


def _create_job_and_execution(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
) -> str:
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, "Evaluation fencing question"),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="a" * 64,
        )
        session.execute(
            text(
                "UPDATE research_jobs SET evaluation_profile = :profile WHERE id = :id"
            ),
            {"profile": EVALUATION_PROFILE, "id": job_id},
        )
        return workflow_repo.create_execution(
            session,
            research_job_id=job_id,
            thread_id=job_id,
            at=at,
        )


def _set_job_claim(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
    claim_token: str | None,
    lease_expires_at: datetime | None,
    status: str = "RUNNING",
) -> None:
    with session_scope(session_factory) as session:
        session.execute(
            text(
                """
                UPDATE research_jobs
                SET status = :status,
                    started_at = COALESCE(started_at, NOW()),
                    updated_at = NOW(),
                    claim_token = :token,
                    lease_expires_at = :lease
                WHERE id = :job_id
                """
            ),
            {
                "status": status,
                "token": claim_token,
                "lease": lease_expires_at,
                "job_id": job_id,
            },
        )


def _claim_for_job(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
    claim_token: str | None = None,
    lease_seconds: int = 300,
) -> str:
    token = claim_token or secrets.token_hex(32)
    _set_job_claim(
        session_factory,
        job_id=job_id,
        claim_token=token,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=lease_seconds),
        status="RUNNING",
    )
    return token


def _dimensions(*, aggregate_marker: float) -> list[DimensionResult]:
    """Build a full dimension set; coverage score encodes the aggregate marker."""
    return [
        DimensionResult(
            name="citation_integrity",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=True,
            is_provisional=False,
            weight=0.25,
        ),
        DimensionResult(
            name="tool_use",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=True,
            is_provisional=False,
            weight=0.10,
        ),
        DimensionResult(
            name="report_structure",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=True,
            is_provisional=True,
            weight=0.15,
        ),
        DimensionResult(
            name="coverage",
            score=aggregate_marker,
            passed=aggregate_marker >= 0.70,
            method="deterministic",
            is_hard=False,
            is_provisional=True,
            weight=0.15,
        ),
        DimensionResult(
            name="completeness",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=False,
            is_provisional=True,
            weight=0.15,
        ),
        DimensionResult(
            name="lexical_id_groundedness",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=False,
            is_provisional=True,
            weight=0.20,
        ),
        DimensionResult(
            name="semantic_groundedness",
            score=0.0,
            passed=True,
            method="skipped",
            is_hard=False,
            is_provisional=True,
            weight=0.0,
        ),
    ]


def test_stale_reclaim_ownership_lost_preserves_winner_dimensions(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-fence-reclaim"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    claim_a = _claim_for_job(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)
    fingerprint = "b" * 64
    future = datetime.now(UTC) + timedelta(minutes=5)

    run_id_a, token_a, replay_a = service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint=fingerprint,
        job_id=job_id,
        deadline=future,
        job_claim_token=claim_a,
    )
    assert replay_a is None
    assert token_a
    assert run_id_a

    past = datetime.now(UTC) - timedelta(seconds=5)
    with session_scope(session_factory) as session:
        session.execute(
            update(EvaluationRunModel)
            .where(EvaluationRunModel.id == run_id_a)
            .values(deadline_at=past)
        )

    # Originating claim expires; new processing claim may reclaim.
    _set_job_claim(
        session_factory,
        job_id=job_id,
        claim_token=None,
        lease_expires_at=None,
        status="RUNNING",
    )
    claim_b = _claim_for_job(session_factory, job_id=job_id)

    run_id_b, token_b, replay_b = service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint=fingerprint,
        job_id=job_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=claim_b,
    )
    assert replay_b is None
    assert run_id_b == run_id_a
    assert token_b
    assert token_b != token_a

    winner = service.finalize_success(
        run_id=run_id_b,
        ownership_token=token_b,
        aggregate=0.91,
        passed=True,
        dimensions=_dimensions(aggregate_marker=0.91),
        disposition_hint="complete",
        grader_versions={"citation_integrity": "deterministic.v1"},
    )
    assert winner.status == "SUCCEEDED"
    assert winner.aggregate_score == pytest.approx(0.91)

    with pytest.raises(EvaluationOwnershipLostError):
        service.finalize_success(
            run_id=run_id_a,
            ownership_token=token_a,
            aggregate=0.42,
            passed=False,
            dimensions=_dimensions(aggregate_marker=0.42),
            disposition_hint="terminal",
            grader_versions={"citation_integrity": "deterministic.v1"},
        )

    repo = SqlAlchemyEvaluationRepository()
    with session_scope(session_factory) as session:
        scores = {
            row.dimension_name: row.score
            for row in session.execute(
                select(EvaluationDimensionResultModel).where(
                    EvaluationDimensionResultModel.evaluation_run_id == run_id_b
                )
            ).scalars()
        }
        record = repo.get_by_id(session, run_id_b)
        row = session.get(EvaluationRunModel, run_id_b)
    assert record is not None
    assert record.status == "SUCCEEDED"
    assert record.aggregate_score == pytest.approx(0.91)
    assert scores["coverage"] == pytest.approx(0.91)
    assert row is not None
    assert row.job_claim_fingerprint == fingerprint_job_claim_token(claim_b)
    assert claim_a not in str(row.job_claim_fingerprint)
    assert claim_b not in str(row.__dict__)


def test_fingerprint_conflict_after_succeeded(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-fence-conflict"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    claim = _claim_for_job(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)
    fingerprint = "c" * 64
    run_id, token, replay = service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint=fingerprint,
        job_id=job_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=claim,
    )
    assert replay is None
    service.finalize_success(
        run_id=run_id,
        ownership_token=token,
        aggregate=1.0,
        passed=True,
        dimensions=_dimensions(aggregate_marker=1.0),
        disposition_hint="complete",
        grader_versions={},
    )
    with pytest.raises(EvaluationConflictError):
        service.begin_or_resume(
            execution_id=execution_id,
            profile=EVALUATION_PROFILE,
            attempt=1,
            fingerprint="d" * 64,
            job_id=job_id,
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            job_claim_token=claim,
        )


def test_in_progress_begin_raises(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-fence-in-progress"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    claim = _claim_for_job(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)
    fingerprint = "e" * 64
    service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint=fingerprint,
        job_id=job_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=claim,
    )
    with pytest.raises(EvaluationInProgressError):
        service.begin_or_resume(
            execution_id=execution_id,
            profile=EVALUATION_PROFILE,
            attempt=1,
            fingerprint=fingerprint,
            job_id=job_id,
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            job_claim_token=claim,
        )


def test_replay_succeeded_same_fingerprint_returns_same_run_id(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-fence-replay"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    claim = _claim_for_job(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)
    fingerprint = "f" * 64
    run_id, token, replay = service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint=fingerprint,
        job_id=job_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=claim,
    )
    assert replay is None
    first = service.finalize_success(
        run_id=run_id,
        ownership_token=token,
        aggregate=1.0,
        passed=True,
        dimensions=_dimensions(aggregate_marker=1.0),
        disposition_hint="complete",
        grader_versions={},
    )
    # Succeeded replay does not require a live claim.
    _set_job_claim(
        session_factory,
        job_id=job_id,
        claim_token=None,
        lease_expires_at=None,
        status="RUNNING",
    )
    replay_run_id, replay_token, replay_result = service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint=fingerprint,
        job_id=job_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token="0" * 64,
    )
    assert replay_run_id == first.run_id
    assert replay_token == ""
    assert replay_result is not None
    assert replay_result.run_id == first.run_id
    assert replay_result.status == "SUCCEEDED"


def test_parent_claim_attribution_reclaim_sequence(
    session_factory: sessionmaker[Session],
) -> None:
    """Worker A owns eval; competing owners blocked until claim A expires."""
    job_id = "eval-parent-claim-seq"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    claim_a = _claim_for_job(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)
    fingerprint = "11" * 32

    run_id, token_a, _ = service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint=fingerprint,
        job_id=job_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=claim_a,
    )

    past = datetime.now(UTC) - timedelta(seconds=5)
    with session_scope(session_factory) as session:
        session.execute(
            update(EvaluationRunModel)
            .where(EvaluationRunModel.id == run_id)
            .values(deadline_at=past)
        )
        row = session.get(EvaluationRunModel, run_id)
        assert row is not None
        assert row.job_claim_fingerprint == fingerprint_job_claim_token(claim_a)
        assert claim_a not in row.job_claim_fingerprint

    # Claim A remains valid: a caller without that claim cannot reclaim.
    with pytest.raises(EvaluationValidationError):
        service.begin_or_resume(
            execution_id=execution_id,
            profile=EVALUATION_PROFILE,
            attempt=1,
            fingerprint=fingerprint,
            job_id=job_id,
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            job_claim_token=secrets.token_hex(32),
        )

    # Claim A expires; Worker B obtains claim B and reclaims.
    _set_job_claim(
        session_factory,
        job_id=job_id,
        claim_token=None,
        lease_expires_at=None,
        status="RUNNING",
    )
    claim_b = _claim_for_job(session_factory, job_id=job_id)
    _, token_b, _ = service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint=fingerprint,
        job_id=job_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=claim_b,
    )
    assert token_b != token_a

    with pytest.raises(EvaluationOwnershipLostError):
        service.finalize_success(
            run_id=run_id,
            ownership_token=token_a,
            aggregate=0.2,
            passed=False,
            dimensions=_dimensions(aggregate_marker=0.2),
            disposition_hint="terminal",
            grader_versions={},
        )

    winner = service.finalize_success(
        run_id=run_id,
        ownership_token=token_b,
        aggregate=0.95,
        passed=True,
        dimensions=_dimensions(aggregate_marker=0.95),
        disposition_hint="complete",
        grader_versions={},
    )
    assert winner.status == "SUCCEEDED"


def test_caller_without_current_claim_cannot_create(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-no-claim-create"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)
    with pytest.raises(EvaluationValidationError):
        service.begin_or_resume(
            execution_id=execution_id,
            profile=EVALUATION_PROFILE,
            attempt=1,
            fingerprint="22" * 32,
            job_id=job_id,
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            job_claim_token=secrets.token_hex(32),
        )


def test_anonymous_reclaim_rejected_when_no_valid_job_claim(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-anon-reclaim"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    claim_a = _claim_for_job(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)
    fingerprint = "33" * 32
    run_id, _, _ = service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint=fingerprint,
        job_id=job_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=claim_a,
    )
    past = datetime.now(UTC) - timedelta(seconds=5)
    with session_scope(session_factory) as session:
        session.execute(
            update(EvaluationRunModel)
            .where(EvaluationRunModel.id == run_id)
            .values(deadline_at=past)
        )
    _set_job_claim(
        session_factory,
        job_id=job_id,
        claim_token=None,
        lease_expires_at=None,
        status="RUNNING",
    )
    with pytest.raises(EvaluationValidationError):
        service.begin_or_resume(
            execution_id=execution_id,
            profile=EVALUATION_PROFILE,
            attempt=1,
            fingerprint=fingerprint,
            job_id=job_id,
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            job_claim_token=secrets.token_hex(32),
        )


def test_failed_attempt_rejects_different_fingerprint(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-fence-failed-fp"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    claim = _claim_for_job(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)
    fingerprint = "44" * 32
    run_id, token, replay = service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint=fingerprint,
        job_id=job_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=claim,
    )
    assert replay is None
    service.finalize_failure(
        run_id=run_id,
        ownership_token=token,
        error_class="EvaluationValidationError",
    )
    with pytest.raises(EvaluationConflictError):
        service.begin_or_resume(
            execution_id=execution_id,
            profile=EVALUATION_PROFILE,
            attempt=1,
            fingerprint="55" * 32,
            job_id=job_id,
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            job_claim_token=claim,
        )


def test_mismatched_job_execution_raises_validation(
    session_factory: sessionmaker[Session],
) -> None:
    job_a = "eval-fence-job-a"
    job_b = "eval-fence-job-b"
    execution_a = _create_job_and_execution(session_factory, job_id=job_a)
    _create_job_and_execution(session_factory, job_id=job_b)
    claim_b = _claim_for_job(session_factory, job_id=job_b)
    service = EvaluationService(session_factory=session_factory)
    with pytest.raises(EvaluationValidationError):
        service.begin_or_resume(
            execution_id=execution_a,
            profile=EVALUATION_PROFILE,
            attempt=1,
            fingerprint="66" * 32,
            job_id=job_b,
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            job_claim_token=claim_b,
        )


def test_raw_claim_token_not_in_evaluation_row(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-no-raw-token"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    claim = _claim_for_job(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)
    run_id, _, _ = service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint="77" * 32,
        job_id=job_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=claim,
    )
    with session_scope(session_factory) as session:
        row = session.get(EvaluationRunModel, run_id)
        assert row is not None
        payload = {c.name: getattr(row, c.name) for c in row.__table__.columns}
    assert claim not in str(payload)
    assert payload["job_claim_fingerprint"] == fingerprint_job_claim_token(claim)
