"""Integration tests for the model invocation ledger."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from langchain_core.messages import AIMessage
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from atlas.domain.research_job import ResearchJob
from atlas.models.contracts import PlanRequest, PlanStructuredOutput, ProviderId
from atlas.models.errors import ModelInvocationInProgressError, ModelTimeoutError
from atlas.models.service import ModelInvocationService
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository


def _seed_job_and_execution(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
    claim_token: str | None = "a" * 64,
    lease_expires_at: datetime | None = None,
) -> str:
    repo = SqlAlchemyResearchJobRepository()
    workflow = SqlAlchemyWorkflowRepository()
    now = datetime.now(UTC)
    if lease_expires_at is None and claim_token is not None:
        lease_expires_at = now + timedelta(seconds=90)
    with session_scope(session_factory) as session:
        job = ResearchJob.create(
            id=job_id,
            question="ledger question",
            at=now,
        )
        if claim_token is not None:
            job.start(at=now)
        repo.add(
            session,
            job,
            idempotency_key=f"idem-{job_id}",
            request_fingerprint="a" * 64,
        )
        if claim_token is not None:
            session.execute(
                text(
                    """
                    UPDATE research_jobs
                    SET claim_token = :token, lease_expires_at = :lease
                    WHERE id = :id
                    """
                ),
                {
                    "token": claim_token,
                    "lease": lease_expires_at,
                    "id": job_id,
                },
            )
        return workflow.create_execution(
            session,
            research_job_id=job_id,
            thread_id=job_id,
            at=now,
        )


def _mock_chat_model(
    *, tasks: list[str] | None = None, error: Exception | None = None
) -> MagicMock:
    chat_model = MagicMock()
    structured = MagicMock()
    chat_model.with_structured_output.return_value = structured
    if error is not None:
        structured.invoke.side_effect = error
        return chat_model
    raw_message = AIMessage(
        content="ok",
        usage_metadata={
            "input_tokens": 10,
            "output_tokens": 5,
            "total_tokens": 15,
        },
        response_metadata={"id": "req_ledger"},
    )
    structured.invoke.return_value = {
        "raw": raw_message,
        "parsed": PlanStructuredOutput(tasks=tasks or ["t1", "t2", "t3"]),
        "parsing_error": None,
    }
    return chat_model


def test_successful_invocation_persists_ledger_and_replays(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = f"ledger-success-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    chat_model = _mock_chat_model()
    service = ModelInvocationService(
        session_factory=session_factory,
        chat_model=chat_model,
        provider=ProviderId.OPENAI,
        model_name="gpt-4o-mini",
        call_timeout_seconds=25.0,
    )
    request = PlanRequest(job_id=job_id, question="Q?", prompt_version="plan.v1")
    first = service.plan(request, workflow_execution_id=execution_id)
    assert first.tasks == ["t1", "t2", "t3"]
    assert chat_model.with_structured_output.call_count == 1

    second = service.plan(request, workflow_execution_id=execution_id)
    assert second.tasks == first.tasks
    assert chat_model.with_structured_output.call_count == 1

    with session_scope(session_factory) as session:
        inv = (
            session.execute(
                text(
                    "SELECT status, output_json, provider, model FROM model_invocations"
                )
            )
            .mappings()
            .one()
        )
        attempts = (
            session.execute(text("SELECT status FROM model_invocation_attempts"))
            .mappings()
            .all()
        )
    assert inv["status"] == "SUCCEEDED"
    assert inv["output_json"]["tasks"] == ["t1", "t2", "t3"]
    assert inv["provider"] == "openai"
    assert len(attempts) == 1
    assert attempts[0]["status"] == "SUCCEEDED"


def test_concurrent_same_key_fails_fast(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = f"ledger-inflight-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = ModelInvocationService(
        session_factory=session_factory,
        chat_model=_mock_chat_model(),
        provider=ProviderId.OPENAI,
        model_name="gpt-4o-mini",
        call_timeout_seconds=25.0,
    )
    request = PlanRequest(job_id=job_id, question="Q?", prompt_version="plan.v1")

    # Seed an in-progress invocation with a future deadline and valid claim.
    from atlas.models.service import build_invocation_key, fingerprint_payload
    from atlas.persistence.repositories.model_invocation import (
        SqlAlchemyModelInvocationRepository,
    )

    fingerprint = fingerprint_payload(
        {"question": request.question, "prompt_version": request.prompt_version}
    )
    key = build_invocation_key(
        research_job_id=job_id,
        node_name="plan",
        prompt_version="plan.v1",
        provider="openai",
        model="gpt-4o-mini",
        input_fingerprint=fingerprint,
    )
    repo = SqlAlchemyModelInvocationRepository()
    now = datetime.now(UTC)
    with session_scope(session_factory) as session:
        repo.create_invocation(
            session,
            invocation_id=str(uuid4()),
            invocation_key=key,
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            node_name="plan",
            provider="openai",
            model="gpt-4o-mini",
            prompt_version="plan.v1",
            input_fingerprint=fingerprint,
            at=now,
        )
        inv = repo.get_by_key(session, key)
        assert inv is not None
        repo.begin_attempt(
            session,
            attempt_id=str(uuid4()),
            invocation_id=inv.id,
            deadline_at=now + timedelta(seconds=25),
            at=now,
        )

    with pytest.raises(ModelInvocationInProgressError):
        service.plan(request, workflow_execution_id=execution_id)


def test_stale_invocation_reclaimed_after_deadline_and_invalid_claim(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = f"ledger-stale-{uuid4()}"
    execution_id = _seed_job_and_execution(
        session_factory,
        job_id=job_id,
        claim_token=None,
        lease_expires_at=None,
    )
    # Job remains PENDING without a valid claim.
    from atlas.models.service import build_invocation_key, fingerprint_payload
    from atlas.persistence.repositories.model_invocation import (
        SqlAlchemyModelInvocationRepository,
    )

    request = PlanRequest(job_id=job_id, question="Q?", prompt_version="plan.v1")
    fingerprint = fingerprint_payload(
        {"question": request.question, "prompt_version": request.prompt_version}
    )
    key = build_invocation_key(
        research_job_id=job_id,
        node_name="plan",
        prompt_version="plan.v1",
        provider="openai",
        model="gpt-4o-mini",
        input_fingerprint=fingerprint,
    )
    repo = SqlAlchemyModelInvocationRepository()
    now = datetime.now(UTC)
    with session_scope(session_factory) as session:
        repo.create_invocation(
            session,
            invocation_id=str(uuid4()),
            invocation_key=key,
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            node_name="plan",
            provider="openai",
            model="gpt-4o-mini",
            prompt_version="plan.v1",
            input_fingerprint=fingerprint,
            at=now - timedelta(seconds=120),
        )
        inv = repo.get_by_key(session, key)
        assert inv is not None
        repo.begin_attempt(
            session,
            attempt_id=str(uuid4()),
            invocation_id=inv.id,
            deadline_at=now - timedelta(seconds=30),
            at=now - timedelta(seconds=120),
        )

    service = ModelInvocationService(
        session_factory=session_factory,
        chat_model=_mock_chat_model(tasks=["r1", "r2", "r3"]),
        provider=ProviderId.OPENAI,
        model_name="gpt-4o-mini",
        call_timeout_seconds=25.0,
    )
    result = service.plan(request, workflow_execution_id=execution_id)
    assert result.tasks == ["r1", "r2", "r3"]

    with session_scope(session_factory) as session:
        attempts = (
            session.execute(
                text(
                    """
                SELECT attempt, status
                FROM model_invocation_attempts
                ORDER BY attempt
                """
                )
            )
            .mappings()
            .all()
        )
        inv_row = (
            session.execute(text("SELECT status FROM model_invocations"))
            .mappings()
            .one()
        )
    assert [dict(row) for row in attempts] == [
        {"attempt": 1, "status": "FAILED"},
        {"attempt": 2, "status": "SUCCEEDED"},
    ]
    assert inv_row["status"] == "SUCCEEDED"


def test_provider_failure_records_atlas_error_class(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = f"ledger-fail-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = ModelInvocationService(
        session_factory=session_factory,
        chat_model=_mock_chat_model(error=TimeoutError("provider timeout detail")),
        provider=ProviderId.OPENAI,
        model_name="gpt-4o-mini",
        call_timeout_seconds=25.0,
    )
    request = PlanRequest(job_id=job_id, question="Q?", prompt_version="plan.v1")
    with pytest.raises(ModelTimeoutError):
        service.plan(request, workflow_execution_id=execution_id)

    with session_scope(session_factory) as session:
        inv_row = (
            session.execute(
                text(
                    """
                    SELECT status, error_class, retry_class, output_json
                    FROM model_invocations
                    """
                )
            )
            .mappings()
            .one()
        )
        attempt = (
            session.execute(
                text("SELECT status, error_class FROM model_invocation_attempts")
            )
            .mappings()
            .one()
        )
    assert inv_row["status"] == "FAILED"
    assert inv_row["error_class"] == "ModelTimeoutError"
    assert inv_row["retry_class"] == "timeout"
    assert inv_row["output_json"] is None
    assert attempt["status"] == "FAILED"
    assert attempt["error_class"] == "ModelTimeoutError"


def test_late_stale_attempt_cannot_overwrite_reclaimed_invocation(
    session_factory: sessionmaker[Session],
) -> None:
    """Attempt 1 becomes stale; attempt 2 reclaims; attempt 1 returns too late."""
    job_id = f"ledger-race-{uuid4()}"
    execution_id = _seed_job_and_execution(
        session_factory,
        job_id=job_id,
        claim_token=None,
        lease_expires_at=None,
    )
    from atlas.models.service import build_invocation_key, fingerprint_payload
    from atlas.persistence.repositories.model_invocation import (
        SqlAlchemyModelInvocationRepository,
    )

    request = PlanRequest(job_id=job_id, question="Q?", prompt_version="plan.v1")
    fingerprint = fingerprint_payload(
        {"question": request.question, "prompt_version": request.prompt_version}
    )
    key = build_invocation_key(
        research_job_id=job_id,
        node_name="plan",
        prompt_version="plan.v1",
        provider="openai",
        model="gpt-4o-mini",
        input_fingerprint=fingerprint,
    )
    repo = SqlAlchemyModelInvocationRepository()
    now = datetime.now(UTC)
    attempt_1_id = str(uuid4())
    invocation_id = str(uuid4())
    with session_scope(session_factory) as session:
        repo.create_invocation(
            session,
            invocation_id=invocation_id,
            invocation_key=key,
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            node_name="plan",
            provider="openai",
            model="gpt-4o-mini",
            prompt_version="plan.v1",
            input_fingerprint=fingerprint,
            at=now - timedelta(seconds=120),
        )
        repo.begin_attempt(
            session,
            attempt_id=attempt_1_id,
            invocation_id=invocation_id,
            deadline_at=now - timedelta(seconds=30),
            at=now - timedelta(seconds=120),
        )

    service = ModelInvocationService(
        session_factory=session_factory,
        chat_model=_mock_chat_model(tasks=["a2", "b2", "c2"]),
        provider=ProviderId.OPENAI,
        model_name="gpt-4o-mini",
        call_timeout_seconds=25.0,
    )
    result = service.plan(request, workflow_execution_id=execution_id)
    assert result.tasks == ["a2", "b2", "c2"]

    late = datetime.now(UTC)
    with session_scope(session_factory) as session:
        owned_attempt = repo.complete_attempt(
            session,
            attempt_id=attempt_1_id,
            provider_request_id="stale-req",
            input_tokens=999,
            output_tokens=999,
            total_tokens=1998,
            latency_ms=1,
            estimated_cost_usd=9.99,
            pricing_version="stale",
            finish_outcome="completed",
            at=late,
        )
        assert owned_attempt is False

        owned_invocation = repo.complete_invocation_for_attempt(
            session,
            invocation_id=invocation_id,
            attempt_id=attempt_1_id,
            output_json={"tasks": ["stale1", "stale2", "stale3"]},
            provider_request_id="stale-req",
            input_tokens=999,
            output_tokens=999,
            total_tokens=1998,
            latency_ms=1,
            estimated_cost_usd=9.99,
            pricing_version="stale",
            finish_outcome="completed",
            at=late,
        )
        assert owned_invocation is False

        attempts = (
            session.execute(
                text(
                    """
                    SELECT attempt, status, provider_request_id, error_class
                    FROM model_invocation_attempts
                    ORDER BY attempt
                    """
                )
            )
            .mappings()
            .all()
        )
        inv_row = (
            session.execute(
                text(
                    """
                    SELECT status, output_json, provider_request_id,
                           input_tokens, estimated_cost_usd
                    FROM model_invocations
                    """
                )
            )
            .mappings()
            .one()
        )

    assert attempts[0]["attempt"] == 1
    assert attempts[0]["status"] == "FAILED"
    assert attempts[0]["error_class"] == "ModelInvocationStaleError"
    assert attempts[0]["provider_request_id"] is None
    assert attempts[1]["attempt"] == 2
    assert attempts[1]["status"] == "SUCCEEDED"
    assert inv_row["status"] == "SUCCEEDED"
    assert inv_row["output_json"]["tasks"] == ["a2", "b2", "c2"]
    assert inv_row["provider_request_id"] == "req_ledger"
    assert inv_row["input_tokens"] == 10
    assert inv_row["estimated_cost_usd"] != 9.99
