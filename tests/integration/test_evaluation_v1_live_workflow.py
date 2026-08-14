"""Opt-in live evaluation.v1 workflow acceptance (never enabled in CI).

Requires ``ATLAS_ENABLE_LIVE_EVALUATION_V1_WORKFLOW_TESTS=1``, OpenAI and
LangSmith credentials, and the integration PostgreSQL fixture. Deterministic
planner/drafter keep plan/draft offline; only semantic grading is live.

This module proves live grading and persistence only. Durable restart,
resume, replay, and mismatched-worker claim behavior are covered offline in
``test_evaluation_profile_binding.py`` so a second provider call is not
required.
"""

from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import select, text
from sqlalchemy.orm import Session, sessionmaker

from atlas.api.deps import provide_session_factory
from atlas.application.worker import ResearchJobWorker
from atlas.config.settings import Settings
from atlas.evaluation.composition import resolved_evaluation_profile
from atlas.evaluation.contracts import EVALUATION_PROFILE_V1
from atlas.evaluation.semantic_contracts import (
    FROZEN_LIVE_SEMANTIC_MODEL,
    FROZEN_LIVE_SEMANTIC_PROVIDER,
)
from atlas.main import app
from atlas.models.fakes import (
    DeterministicResearchDrafter,
    DeterministicResearchPlanner,
)
from atlas.persistence.db import reset_engine_cache
from atlas.persistence.models.evaluation import (
    EvaluationDimensionResultModel,
    EvaluationRunModel,
)
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.workflow import LangGraphResearchProcessor, create_checkpoint_runtime

LIVE_WORKFLOW_FLAG = "ATLAS_ENABLE_LIVE_EVALUATION_V1_WORKFLOW_TESTS"

pytestmark = pytest.mark.skipif(
    os.environ.get(LIVE_WORKFLOW_FLAG) != "1"
    or (os.environ.get("ATLAS_MODEL_PROVIDER") or "fake").strip() == "fake"
    or not (os.environ.get("ATLAS_OPENAI_API_KEY") or "").strip()
    or not (os.environ.get("ATLAS_LANGSMITH_API_KEY") or "").strip(),
    reason=(
        "Live evaluation.v1 workflow acceptance requires "
        f"{LIVE_WORKFLOW_FLAG}=1, ATLAS_MODEL_PROVIDER=openai, "
        "ATLAS_OPENAI_API_KEY, and ATLAS_LANGSMITH_API_KEY"
    ),
)


def test_live_evaluation_v1_job_binds_and_persists_semantic_grade(
    session_factory: sessionmaker[Session],
    test_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ATLAS_DATABASE_URL", test_database_url)
    monkeypatch.setenv("ATLAS_EVALUATION_PROFILE", EVALUATION_PROFILE_V1)
    monkeypatch.setenv("ATLAS_SEMANTIC_GRADER_MODE", "live")
    settings = Settings(
        evaluation_profile=EVALUATION_PROFILE_V1,
        semantic_grader_mode="live",
        model_provider=FROZEN_LIVE_SEMANTIC_PROVIDER,
        model_name=FROZEN_LIVE_SEMANTIC_MODEL,
        openai_api_key=SecretStr(os.environ["ATLAS_OPENAI_API_KEY"]),
        langsmith_api_key=SecretStr(os.environ["ATLAS_LANGSMITH_API_KEY"]),
    )
    assert resolved_evaluation_profile(settings) == EVALUATION_PROFILE_V1
    reset_engine_cache()
    app.dependency_overrides[provide_session_factory] = lambda: session_factory
    client = TestClient(app)
    runtime = create_checkpoint_runtime(test_database_url)
    processor = LangGraphResearchProcessor(
        checkpointer=runtime.checkpointer,
        session_factory=session_factory,
        settings=settings,
        planner=DeterministicResearchPlanner(),
        drafter=DeterministicResearchDrafter(),
    )
    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=SqlAlchemyResearchJobRepository(),
        processor=processor,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=120.0,
        lease_seconds=180.0,
        evaluation_profile=resolved_evaluation_profile(settings),
    )
    try:
        created = client.post(
            "/v1/research-jobs",
            json={"question": "Slice 15C1 live evaluation.v1 cited report"},
            headers={"Idempotency-Key": "eval-v1-live-workflow-once"},
        )
        assert created.status_code == 202
        job_id = created.json()["id"]
        with session_factory() as session:
            unbound = session.execute(
                text("SELECT evaluation_profile FROM research_jobs WHERE id = :id"),
                {"id": job_id},
            ).scalar_one()
        assert unbound is None
        assert worker.run_once() is True

        with session_factory() as session:
            bound = session.execute(
                text("SELECT evaluation_profile FROM research_jobs WHERE id = :id"),
                {"id": job_id},
            ).scalar_one()
            runs = (
                session.execute(
                    select(EvaluationRunModel)
                    .where(EvaluationRunModel.research_job_id == job_id)
                    .order_by(EvaluationRunModel.started_at.desc())
                )
                .scalars()
                .all()
            )
            assert runs
            assert {run.evaluation_profile for run in runs} == {EVALUATION_PROFILE_V1}
            run = next(
                (item for item in runs if item.status == "SUCCEEDED"),
                runs[0],
            )
            semantic = session.execute(
                select(EvaluationDimensionResultModel).where(
                    EvaluationDimensionResultModel.evaluation_run_id == run.id,
                    EvaluationDimensionResultModel.dimension_name
                    == "semantic_groundedness",
                )
            ).scalar_one()
        assert bound == EVALUATION_PROFILE_V1
        assert run.evaluation_profile == EVALUATION_PROFILE_V1
        assert semantic.method == "llm"

        detail = client.get(f"/v1/research-jobs/{job_id}/evaluation")
        assert detail.status_code == 200
        body = detail.json()
        assert body["evaluation_profile"] == EVALUATION_PROFILE_V1
        assert "input_fingerprint" not in body
        assert any(
            item["name"] == "semantic_groundedness" for item in body["dimensions"]
        )
    finally:
        worker.close()
        runtime.close()
        app.dependency_overrides.clear()
        reset_engine_cache()
