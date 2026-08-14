"""Evaluation runner failure finalization and execution-scoped tool loading."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import Session, sessionmaker

from atlas.domain import ResearchJob
from atlas.evaluation.contracts import (
    EVALUATION_PROFILE,
    EVALUATION_PROFILE_V1,
    DimensionResult,
    EvaluationCandidateInput,
    ToolSummaryRow,
)
from atlas.evaluation.errors import (
    EvaluationOwnershipLostError,
    EvaluationTerminalError,
    EvaluationValidationError,
)
from atlas.evaluation.runner import EvaluationRunner
from atlas.evaluation.semantic_contracts import (
    FROZEN_LIVE_SEMANTIC_MODEL,
    FROZEN_LIVE_SEMANTIC_PROVIDER,
    FROZEN_LIVE_SEMANTIC_TEMPERATURE,
)
from atlas.evaluation.service import EvaluationService
from atlas.persistence.db import session_scope
from atlas.persistence.models.evaluation import EvaluationRunModel
from atlas.persistence.models.tool_invocation import ToolInvocationModel
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository


def _create_job_and_execution(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
    evaluation_profile: str = EVALUATION_PROFILE,
) -> str:
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, "Evaluation runner question"),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="a" * 64,
        )
        session.execute(
            text(
                "UPDATE research_jobs SET evaluation_profile = :profile WHERE id = :id"
            ),
            {"profile": evaluation_profile, "id": job_id},
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
    from sqlalchemy import text

    with session_scope(session_factory) as session:
        session.execute(
            text(
                """
                UPDATE research_jobs
                SET status = :status,
                    started_at = COALESCE(started_at, NOW()),
                    updated_at = NOW(),
                    claim_token = :token,
                    lease_expires_at = :lease,
                    evaluation_profile = COALESCE(
                        evaluation_profile, 'evaluation.candidate.v1'
                    )
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
) -> str:
    import secrets

    token = claim_token or secrets.token_hex(32)
    _set_job_claim(
        session_factory,
        job_id=job_id,
        claim_token=token,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        status="RUNNING",
    )
    return token


def _candidate(job_id: str) -> EvaluationCandidateInput:
    return EvaluationCandidateInput(
        job_id=job_id,
        question="Runner finalization probe",
        plan=["Clarify research scope carefully"],
        findings=["Clarify research scope carefully in findings"],
        draft="Clarify research scope carefully in the draft.",
        claims=[],
        evidence_item_ids=[],
        tool_summary=[],
        evaluation_profile=EVALUATION_PROFILE,
    )


class _RaisingSemanticGrader:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc
        self.version = "test.raising"

    def grade(self, request: object) -> DimensionResult:
        del request
        raise self._exc


def test_typed_evaluation_error_finalizes_failed(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-runner-typed-fail"
    execution_id = _create_job_and_execution(
        session_factory,
        job_id=job_id,
        evaluation_profile=EVALUATION_PROFILE_V1,
    )
    claim = _claim_for_job(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)
    runner = EvaluationRunner(
        evaluation_service=service,
        semantic_grader=_RaisingSemanticGrader(
            EvaluationValidationError("sanitized validation failure")
        ),
        semantic_model_provider=FROZEN_LIVE_SEMANTIC_PROVIDER,
        semantic_model_name=FROZEN_LIVE_SEMANTIC_MODEL,
        semantic_temperature=FROZEN_LIVE_SEMANTIC_TEMPERATURE,
    )
    with pytest.raises(EvaluationValidationError):
        runner.run(
            candidate=_candidate(job_id).model_copy(
                update={"evaluation_profile": EVALUATION_PROFILE_V1}
            ),
            workflow_execution_id=execution_id,
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            job_claim_token=claim,
        )

    with session_scope(session_factory) as session:
        row = session.execute(select(EvaluationRunModel)).scalar_one()
        assert row.status == "FAILED"
        assert row.grader_versions_json == {"error_class": "EvaluationValidationError"}


def test_unexpected_grader_error_sanitized_failed(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-runner-unexpected"
    execution_id = _create_job_and_execution(
        session_factory,
        job_id=job_id,
        evaluation_profile=EVALUATION_PROFILE_V1,
    )
    claim = _claim_for_job(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)
    runner = EvaluationRunner(
        evaluation_service=service,
        semantic_grader=_RaisingSemanticGrader(
            RuntimeError("raw provider secret must not persist")
        ),
        semantic_model_provider=FROZEN_LIVE_SEMANTIC_PROVIDER,
        semantic_model_name=FROZEN_LIVE_SEMANTIC_MODEL,
        semantic_temperature=FROZEN_LIVE_SEMANTIC_TEMPERATURE,
    )
    with pytest.raises(EvaluationTerminalError):
        runner.run(
            candidate=_candidate(job_id).model_copy(
                update={"evaluation_profile": EVALUATION_PROFILE_V1}
            ),
            workflow_execution_id=execution_id,
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            job_claim_token=claim,
        )

    with session_scope(session_factory) as session:
        row = session.execute(select(EvaluationRunModel)).scalar_one()
        assert row.status == "FAILED"
        assert row.grader_versions_json == {"error_class": "EvaluationUnexpectedError"}
        blob = str(row.grader_versions_json)
        assert "secret" not in blob
        assert "RuntimeError" not in blob


def test_ownership_lost_during_error_handling_leaves_newer_owner(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-runner-own-lost"
    execution_id = _create_job_and_execution(session_factory, job_id=job_id)
    claim = _claim_for_job(session_factory, job_id=job_id)
    service = EvaluationService(session_factory=session_factory)
    fingerprint = "66" * 32
    run_id, token_a, _ = service.begin_or_resume(
        execution_id=execution_id,
        profile=EVALUATION_PROFILE,
        attempt=1,
        fingerprint=fingerprint,
        job_id=job_id,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=claim,
    )

    past = datetime.now(UTC) - timedelta(seconds=5)
    with session_scope(session_factory) as session:
        row = session.get(EvaluationRunModel, run_id)
        assert row is not None
        row.deadline_at = past
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
        service.finalize_failure(
            run_id=run_id,
            ownership_token=token_a,
            error_class="EvaluationValidationError",
        )

    with session_scope(session_factory) as session:
        row = session.get(EvaluationRunModel, run_id)
        assert row is not None
        assert row.status == "IN_PROGRESS"
        assert row.ownership_token == token_b


def test_previous_execution_tool_rows_do_not_affect_current(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "eval-runner-tool-scope"
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, "Tool scope question"),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="b" * 64,
        )
        session.execute(
            text(
                "UPDATE research_jobs SET evaluation_profile = :profile WHERE id = :id"
            ),
            {"profile": EVALUATION_PROFILE, "id": job_id},
        )
        old_execution = workflow_repo.create_execution(
            session,
            research_job_id=job_id,
            thread_id=f"{job_id}-old",
            at=at,
        )
        new_execution = workflow_repo.create_execution(
            session,
            research_job_id=job_id,
            thread_id=f"{job_id}-new",
            at=at,
        )
        session.add(
            ToolInvocationModel(
                id=str(uuid4()),
                invocation_key="c" * 64,
                origin="WORKFLOW",
                research_job_id=job_id,
                workflow_execution_id=old_execution,
                node_name="draft",
                workflow_node_attempt=1,
                actor_id=None,
                tool_id="web_search",
                tool_version="1",
                provider="fake",
                tool_policy_version="tool-policy.v1",
                input_fingerprint="d" * 64,
                status="SUCCEEDED",
                output_summary_json={"result_count": 1},
                content_digest="1" * 64,
                byte_length=12,
                latency_ms=1,
                started_at=at,
                finished_at=at,
            )
        )
        session.add(
            ToolInvocationModel(
                id=str(uuid4()),
                invocation_key="e" * 64,
                origin="WORKFLOW",
                research_job_id=job_id,
                workflow_execution_id=new_execution,
                node_name="research",
                workflow_node_attempt=1,
                actor_id=None,
                tool_id="web_search",
                tool_version="1",
                provider="fake",
                tool_policy_version="tool-policy.v1",
                input_fingerprint="f" * 64,
                status="SUCCEEDED",
                output_summary_json={"result_count": 1},
                content_digest="2" * 64,
                byte_length=12,
                latency_ms=1,
                started_at=at,
                finished_at=at,
            )
        )

    def load_tool_rows(
        candidate: EvaluationCandidateInput,
        workflow_execution_id: str,
    ) -> list[ToolSummaryRow]:
        with session_scope(session_factory) as session:
            rows = session.execute(
                select(
                    ToolInvocationModel.node_name,
                    ToolInvocationModel.origin,
                    ToolInvocationModel.tool_id,
                    ToolInvocationModel.status,
                ).where(
                    ToolInvocationModel.research_job_id == candidate.job_id,
                    ToolInvocationModel.workflow_execution_id == workflow_execution_id,
                )
            ).all()
            return [
                ToolSummaryRow(
                    node_name=str(node_name),
                    origin=str(origin),
                    tool_id=str(tool_id),
                    status=str(status),
                )
                for node_name, origin, tool_id, status in rows
                if node_name
            ]

    service = EvaluationService(session_factory=session_factory)
    runner = EvaluationRunner(
        evaluation_service=service,
        load_tool_rows=load_tool_rows,
    )
    claim = _claim_for_job(session_factory, job_id=job_id)
    result = runner.run(
        candidate=_candidate(job_id),
        workflow_execution_id=new_execution,
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        job_claim_token=claim,
    )
    assert result.passed is True
    tool_dim = next(item for item in result.dimensions if item.name == "tool_use")
    assert tool_dim.passed is True
