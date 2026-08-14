"""Durable job-level evaluation-profile binding (Slice 15C1 freeze)."""

from __future__ import annotations

import secrets
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from atlas.api.deps import provide_session_factory
from atlas.application.job_processing import ContinuationMode
from atlas.application.worker import ResearchJobWorker
from atlas.config.settings import Settings
from atlas.domain import ResearchJob
from atlas.evaluation.aggregation import weight_for
from atlas.evaluation.composition import resolved_evaluation_profile
from atlas.evaluation.contracts import (
    EVALUATION_PROFILE_CANDIDATE,
    EVALUATION_PROFILE_CANDIDATE_FAKE,
    EVALUATION_PROFILE_V1,
    DimensionResult,
)
from atlas.evaluation.errors import EvaluationProfileMismatchError
from atlas.evaluation.semantic_contracts import (
    FROZEN_LIVE_SEMANTIC_MODEL,
    FROZEN_LIVE_SEMANTIC_PROVIDER,
    LIVE_SEMANTIC_GRADER_VERSION,
    SemanticGradeRequest,
)
from atlas.main import app
from atlas.models.fakes import (
    DeterministicResearchDrafter,
    DeterministicResearchPlanner,
)
from atlas.persistence.db import reset_engine_cache, session_scope
from atlas.persistence.models import ResearchJobModel
from atlas.persistence.models.evaluation import (
    EvaluationDimensionResultModel,
    EvaluationRunModel,
)
from atlas.persistence.models.workflow import WorkflowExecutionModel
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.workflow import LangGraphResearchProcessor, create_checkpoint_runtime
from tests.integration.research_job_fixtures import bind_profile_and_start_claimed_job

T0 = datetime(2026, 8, 13, 12, 0, 0, tzinfo=UTC)


def _seed_pending(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
) -> None:
    repo = SqlAlchemyResearchJobRepository()
    with session_scope(session_factory) as session:
        repo.add(
            session,
            ResearchJob.create(job_id, "Bind profile", at=T0),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="a" * 64,
        )


def _profile_of(session_factory: sessionmaker[Session], job_id: str) -> str | None:
    with session_factory() as session:
        value = session.execute(
            text("SELECT evaluation_profile FROM research_jobs WHERE id = :id"),
            {"id": job_id},
        ).scalar_one()
    if value is None:
        return None
    return str(value)


def test_never_started_pending_job_may_remain_unbound(
    session_factory: sessionmaker[Session],
) -> None:
    _seed_pending(session_factory, job_id="unbound-pending")
    assert _profile_of(session_factory, "unbound-pending") is None


def test_first_claim_binds_worker_profile_atomically(
    session_factory: sessionmaker[Session],
) -> None:
    _seed_pending(session_factory, job_id="bind-v1")
    repo = SqlAlchemyResearchJobRepository()
    now = T0 + timedelta(seconds=1)
    with session_scope(session_factory) as session:
        claimed = repo.claim_next(
            session,
            now=now,
            lease_expires_at=now + timedelta(seconds=30),
            claim_token=secrets.token_hex(32),
            evaluation_profile=EVALUATION_PROFILE_V1,
        )
    assert claimed is not None
    assert claimed.evaluation_profile == EVALUATION_PROFILE_V1
    assert _profile_of(session_factory, "bind-v1") == EVALUATION_PROFILE_V1


def test_mismatched_worker_cannot_claim_or_rebind(
    session_factory: sessionmaker[Session],
) -> None:
    _seed_pending(session_factory, job_id="bound-v1")
    repo = SqlAlchemyResearchJobRepository()
    now = T0 + timedelta(seconds=1)
    with session_scope(session_factory) as session:
        first = repo.claim_next(
            session,
            now=now,
            lease_expires_at=now + timedelta(seconds=30),
            claim_token=secrets.token_hex(32),
            evaluation_profile=EVALUATION_PROFILE_V1,
        )
    assert first is not None
    with session_factory() as session:
        session.execute(
            text("UPDATE research_jobs SET lease_expires_at = :expired WHERE id = :id"),
            {"id": "bound-v1", "expired": now - timedelta(seconds=1)},
        )
        session.commit()
    later = now + timedelta(seconds=60)
    with session_scope(session_factory) as session:
        skipped = repo.claim_next(
            session,
            now=later,
            lease_expires_at=later + timedelta(seconds=30),
            claim_token=secrets.token_hex(32),
            evaluation_profile=EVALUATION_PROFILE_CANDIDATE,
        )
    assert skipped is None
    assert _profile_of(session_factory, "bound-v1") == EVALUATION_PROFILE_V1
    with session_scope(session_factory) as session:
        reclaimed = repo.claim_next(
            session,
            now=later,
            lease_expires_at=later + timedelta(seconds=30),
            claim_token=secrets.token_hex(32),
            evaluation_profile=EVALUATION_PROFILE_V1,
        )
    assert reclaimed is not None
    assert reclaimed.evaluation_profile == EVALUATION_PROFILE_V1
    assert reclaimed.continuation_mode is ContinuationMode.NONE


def test_concurrent_different_profiles_cannot_race_binding(
    session_factory: sessionmaker[Session],
) -> None:
    _seed_pending(session_factory, job_id="race-bind")
    repo = SqlAlchemyResearchJobRepository()
    now = T0 + timedelta(seconds=1)
    a_locked = Event()
    b_finished = Event()
    outcomes: dict[str, object] = {}

    def claimer_v1() -> None:
        session = session_factory()
        try:
            claimed = repo.claim_next(
                session,
                now=now,
                lease_expires_at=now + timedelta(seconds=30),
                claim_token=secrets.token_hex(32),
                evaluation_profile=EVALUATION_PROFILE_V1,
            )
            outcomes["v1"] = claimed
            a_locked.set()
            assert b_finished.wait(timeout=5.0)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def claimer_candidate() -> None:
        assert a_locked.wait(timeout=5.0)
        with session_scope(session_factory) as session:
            claimed = repo.claim_next(
                session,
                now=now,
                lease_expires_at=now + timedelta(seconds=30),
                claim_token=secrets.token_hex(32),
                evaluation_profile=EVALUATION_PROFILE_CANDIDATE,
            )
        outcomes["candidate"] = claimed
        b_finished.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        future_a = pool.submit(claimer_v1)
        future_b = pool.submit(claimer_candidate)
        future_a.result(timeout=10.0)
        future_b.result(timeout=10.0)

    assert outcomes["v1"] is not None
    assert outcomes["candidate"] is None
    assert _profile_of(session_factory, "race-bind") == EVALUATION_PROFILE_V1


def test_started_job_without_profile_is_rejected(
    session_factory: sessionmaker[Session],
) -> None:
    _seed_pending(session_factory, job_id="need-profile")
    with pytest.raises(IntegrityError):
        with session_factory() as session:
            session.execute(
                text(
                    """
                    UPDATE research_jobs
                    SET status = 'RUNNING', started_at = :at, updated_at = :at
                    WHERE id = :id
                    """
                ),
                {"id": "need-profile", "at": T0 + timedelta(seconds=1)},
            )
            session.commit()


def test_save_started_unbound_job_fails_started_profile_check(
    session_factory: sessionmaker[Session],
) -> None:
    _seed_pending(session_factory, job_id="save-unbound")
    repo = SqlAlchemyResearchJobRepository()
    with pytest.raises(IntegrityError) as exc_info:
        with session_scope(session_factory) as session:
            job = repo.get(session, "save-unbound")
            assert job is not None
            job.start(at=T0 + timedelta(seconds=1))
            repo.save(session, job)
    assert "ck_research_jobs_started_has_evaluation_profile" in str(exc_info.value)
    assert _profile_of(session_factory, "save-unbound") is None


@pytest.mark.parametrize(
    ("job_id", "profile"),
    [
        ("keep-v1", EVALUATION_PROFILE_V1),
        ("keep-fake", EVALUATION_PROFILE_CANDIDATE_FAKE),
    ],
)
def test_save_does_not_replace_bound_v1_or_candidate_fake(
    session_factory: sessionmaker[Session],
    job_id: str,
    profile: str,
) -> None:
    _seed_pending(session_factory, job_id=job_id)
    repo = SqlAlchemyResearchJobRepository()
    with session_scope(session_factory) as session:
        model = session.get(ResearchJobModel, job_id)
        assert model is not None
        model.evaluation_profile = profile
    with session_scope(session_factory) as session:
        job = repo.get(session, job_id)
        assert job is not None
        job.start(at=T0 + timedelta(seconds=1))
        repo.save(session, job)
    assert _profile_of(session_factory, job_id) == profile


def test_explicit_candidate_fixture_helper_starts_bound_job(
    session_factory: sessionmaker[Session],
) -> None:
    _seed_pending(session_factory, job_id="legacy-candidate")
    at = T0 + timedelta(seconds=1)
    with session_scope(session_factory) as session:
        model = session.get(ResearchJobModel, "legacy-candidate")
        assert model is not None
        bind_profile_and_start_claimed_job(
            model,
            at=at,
            claim_token="a" * 64,
            lease_expires_at=at + timedelta(seconds=30),
        )
    assert _profile_of(session_factory, "legacy-candidate") == (
        EVALUATION_PROFILE_CANDIDATE
    )
    with session_factory() as session:
        status = session.execute(
            text("SELECT status FROM research_jobs WHERE id = :id"),
            {"id": "legacy-candidate"},
        ).scalar_one()
    assert status == "RUNNING"


def test_processor_fails_closed_on_profile_mismatch_before_workflow_mutation(
    session_factory: sessionmaker[Session],
    test_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("ATLAS_EVALUATION_PROFILE", raising=False)
    monkeypatch.delenv("ATLAS_SEMANTIC_GRADER_MODE", raising=False)
    monkeypatch.chdir(tmp_path)
    _seed_pending(session_factory, job_id="mismatch-processor")
    repo = SqlAlchemyResearchJobRepository()
    now = T0 + timedelta(seconds=1)
    token = secrets.token_hex(32)
    with session_scope(session_factory) as session:
        claimed = repo.claim_next(
            session,
            now=now,
            lease_expires_at=now + timedelta(seconds=90),
            claim_token=token,
            evaluation_profile=EVALUATION_PROFILE_V1,
        )
    assert claimed is not None
    runtime = create_checkpoint_runtime(test_database_url)
    try:
        processor = LangGraphResearchProcessor(
            checkpointer=runtime.checkpointer,
            session_factory=session_factory,
            settings=Settings(
                evaluation_profile=EVALUATION_PROFILE_CANDIDATE,
                semantic_grader_mode="skipped",
            ),
        )
        with pytest.raises(EvaluationProfileMismatchError):
            processor(
                "mismatch question",
                job_id="mismatch-processor",
                claim_token=token,
            )
    finally:
        runtime.close()
    with session_factory() as session:
        executions = session.execute(
            select(func.count()).select_from(WorkflowExecutionModel)
        ).scalar_one()
    assert executions == 0
    assert _profile_of(session_factory, "mismatch-processor") == EVALUATION_PROFILE_V1


def test_fake_and_v1_profiles_are_readable_distinct_bindings(
    session_factory: sessionmaker[Session],
) -> None:
    _seed_pending(session_factory, job_id="fake-job")
    _seed_pending(session_factory, job_id="v1-job")
    repo = SqlAlchemyResearchJobRepository()
    now = T0 + timedelta(seconds=1)
    with session_scope(session_factory) as session:
        fake = repo.claim_next(
            session,
            now=now,
            lease_expires_at=now + timedelta(seconds=30),
            claim_token=secrets.token_hex(32),
            evaluation_profile=EVALUATION_PROFILE_CANDIDATE_FAKE,
        )
    with session_scope(session_factory) as session:
        live = repo.claim_next(
            session,
            now=now,
            lease_expires_at=now + timedelta(seconds=30),
            claim_token=secrets.token_hex(32),
            evaluation_profile=EVALUATION_PROFILE_V1,
        )
    assert fake is not None
    assert live is not None
    assert fake.job.id == "fake-job"
    assert live.job.id == "v1-job"
    assert _profile_of(session_factory, "fake-job") == EVALUATION_PROFILE_CANDIDATE_FAKE
    assert _profile_of(session_factory, "v1-job") == EVALUATION_PROFILE_V1


class _OfflineLiveSemanticGrader:
    """Network-free stand-in for live semantic grading in restart/replay tests."""

    version = LIVE_SEMANTIC_GRADER_VERSION

    def grade(self, request: SemanticGradeRequest) -> DimensionResult:
        del request
        return DimensionResult(
            name="semantic_groundedness",
            score=1.0,
            passed=True,
            method="llm",
            is_hard=False,
            is_provisional=True,
            failure_codes=[],
            weight=weight_for("semantic_groundedness", semantic_present=True),
        )


def _v1_live_settings() -> Settings:
    return Settings(
        evaluation_profile=EVALUATION_PROFILE_V1,
        semantic_grader_mode="live",
        model_provider=FROZEN_LIVE_SEMANTIC_PROVIDER,
        model_name=FROZEN_LIVE_SEMANTIC_MODEL,
        openai_api_key=SecretStr("sk-test-not-a-real-key"),
        langsmith_api_key=SecretStr("lsv2_test_not_a_real_key"),
    )


def _eval_inventory(
    session_factory: sessionmaker[Session], job_id: str
) -> tuple[list[str], int]:
    with session_factory() as session:
        runs = (
            session.execute(
                select(EvaluationRunModel).where(
                    EvaluationRunModel.research_job_id == job_id
                )
            )
            .scalars()
            .all()
        )
        dimensions = 0
        if runs:
            dimensions = session.execute(
                select(func.count())
                .select_from(EvaluationDimensionResultModel)
                .where(
                    EvaluationDimensionResultModel.evaluation_run_id.in_(
                        [run.id for run in runs]
                    ),
                    EvaluationDimensionResultModel.dimension_name
                    == "semantic_groundedness",
                )
            ).scalar_one()
        return [str(run.evaluation_profile) for run in runs], int(dimensions)


def test_bound_v1_job_survives_worker_restart_resume_and_rejects_candidate(
    session_factory: sessionmaker[Session],
    test_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("ATLAS_EVALUATION_PROFILE", raising=False)
    monkeypatch.delenv("ATLAS_SEMANTIC_GRADER_MODE", raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ATLAS_DATABASE_URL", test_database_url)
    monkeypatch.setattr(
        "atlas.workflow.processor.build_semantic_grader",
        lambda *args, **kwargs: _OfflineLiveSemanticGrader(),
    )
    settings = _v1_live_settings()
    assert resolved_evaluation_profile(settings) == EVALUATION_PROFILE_V1
    reset_engine_cache()
    app.dependency_overrides[provide_session_factory] = lambda: session_factory
    client = TestClient(app)
    runtime_a = create_checkpoint_runtime(test_database_url)
    processor_a = LangGraphResearchProcessor(
        checkpointer=runtime_a.checkpointer,
        session_factory=session_factory,
        settings=settings,
        planner=DeterministicResearchPlanner(),
        drafter=DeterministicResearchDrafter(),
    )
    worker_a = ResearchJobWorker(
        session_factory=session_factory,
        repository=SqlAlchemyResearchJobRepository(),
        processor=processor_a,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=30.0,
        lease_seconds=60.0,
        evaluation_profile=resolved_evaluation_profile(settings),
    )

    def _skip_finalize(
        claimed: object, *, result: str, duration_seconds: float
    ) -> bool:
        del claimed, result, duration_seconds
        return False

    worker_a._finalize_completion = _skip_finalize  # type: ignore[method-assign]
    try:
        created = client.post(
            "/v1/research-jobs",
            json={"question": "Offline evaluation.v1 restart replay"},
            headers={"Idempotency-Key": "eval-v1-restart-replay"},
        )
        assert created.status_code == 202
        job_id = created.json()["id"]
        assert worker_a.run_once() is True
        assert _profile_of(session_factory, job_id) == EVALUATION_PROFILE_V1
        profiles_before, semantic_before = _eval_inventory(session_factory, job_id)
        assert profiles_before == [EVALUATION_PROFILE_V1]
        assert semantic_before == 1
        with session_factory() as session:
            status = session.execute(
                text("SELECT status FROM research_jobs WHERE id = :id"),
                {"id": job_id},
            ).scalar_one()
        assert status == "RUNNING"
        expired = datetime.now(UTC) - timedelta(seconds=1)
        with session_factory() as session:
            session.execute(
                text(
                    "UPDATE research_jobs SET lease_expires_at = :expired "
                    "WHERE id = :id"
                ),
                {"id": job_id, "expired": expired},
            )
            session.commit()
    finally:
        worker_a.close()
        runtime_a.close()

    runtime_candidate = create_checkpoint_runtime(test_database_url)
    worker_candidate = ResearchJobWorker(
        session_factory=session_factory,
        repository=SqlAlchemyResearchJobRepository(),
        processor=LangGraphResearchProcessor(
            checkpointer=runtime_candidate.checkpointer,
            session_factory=session_factory,
            settings=Settings(
                evaluation_profile=EVALUATION_PROFILE_CANDIDATE,
                semantic_grader_mode="skipped",
            ),
            planner=DeterministicResearchPlanner(),
            drafter=DeterministicResearchDrafter(),
        ),
        poll_interval_seconds=0.01,
        processing_timeout_seconds=30.0,
        lease_seconds=60.0,
        evaluation_profile=EVALUATION_PROFILE_CANDIDATE,
    )
    try:
        assert worker_candidate.run_once() is False
        assert _profile_of(session_factory, job_id) == EVALUATION_PROFILE_V1
        profiles_after_mismatch, semantic_after_mismatch = _eval_inventory(
            session_factory, job_id
        )
        assert profiles_after_mismatch == [EVALUATION_PROFILE_V1]
        assert semantic_after_mismatch == 1
    finally:
        worker_candidate.close()
        runtime_candidate.close()

    runtime_b = create_checkpoint_runtime(test_database_url)
    worker_b = ResearchJobWorker(
        session_factory=session_factory,
        repository=SqlAlchemyResearchJobRepository(),
        processor=LangGraphResearchProcessor(
            checkpointer=runtime_b.checkpointer,
            session_factory=session_factory,
            settings=settings,
            planner=DeterministicResearchPlanner(),
            drafter=DeterministicResearchDrafter(),
        ),
        poll_interval_seconds=0.01,
        processing_timeout_seconds=30.0,
        lease_seconds=60.0,
        evaluation_profile=resolved_evaluation_profile(settings),
    )
    try:
        assert worker_b.run_once() is True
        with session_factory() as session:
            status = session.execute(
                text("SELECT status FROM research_jobs WHERE id = :id"),
                {"id": job_id},
            ).scalar_one()
        assert status == "COMPLETED"
        profiles_after, semantic_after = _eval_inventory(session_factory, job_id)
        assert profiles_after == [EVALUATION_PROFILE_V1]
        assert semantic_after == semantic_before == 1
        detail = client.get(f"/v1/research-jobs/{job_id}/evaluation")
        assert detail.status_code == 200
        body = detail.json()
        assert body["evaluation_profile"] == EVALUATION_PROFILE_V1
        assert "input_fingerprint" not in body
    finally:
        worker_b.close()
        runtime_b.close()
        app.dependency_overrides.clear()
        reset_engine_cache()
