"""Integration tests for the tool invocation ledger."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from atlas.domain.research_job import ResearchJob
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.tool_invocation import (
    SqlAlchemyToolInvocationRepository,
)
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository
from atlas.tools.contracts import (
    ToolCallContext,
    ToolId,
    ToolInvocationResult,
    ToolOrigin,
    ToolProviderId,
)
from atlas.tools.errors import (
    ToolAttemptOwnershipLostError,
    ToolInvocationInProgressError,
    ToolTemporaryError,
)
from atlas.tools.fakes import FakeWebSearchTool
from atlas.tools.ports import ResearchTool
from atlas.tools.registry import ToolRegistry, default_permission_policy
from atlas.tools.service import ToolInvocationService, fingerprint_payload


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
            question="tool ledger question",
            at=now,
        )
        if claim_token is not None:
            job.start(at=now)
        repo.add(
            session,
            job,
            idempotency_key=f"idem-{job_id}",
            request_fingerprint="b" * 64,
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


def _service(
    session_factory: sessionmaker[Session],
    *,
    tool: ResearchTool | None = None,
) -> ToolInvocationService:
    registry = ToolRegistry({ToolId.WEB_SEARCH: tool or FakeWebSearchTool()})
    return ToolInvocationService(
        session_factory=session_factory,
        registry=registry,
        policy=default_permission_policy(),
        provider_by_tool={ToolId.WEB_SEARCH: ToolProviderId.FAKE},
        budgets=None,
        max_attempts_per_call=2,
        attempt_timeout_seconds=8.0,
    )


def _ctx(job_id: str, execution_id: str) -> ToolCallContext:
    return ToolCallContext(
        origin=ToolOrigin.WORKFLOW,
        research_job_id=job_id,
        workflow_execution_id=execution_id,
        node_name="research",
    )


def test_tool_ledger_success_and_replay(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = f"tool-success-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = _service(session_factory)
    raw = {"query": "Atlas tools", "max_results": 2}
    first = service.invoke(
        tool_id=ToolId.WEB_SEARCH,
        raw_input=raw,
        context=_ctx(job_id, execution_id),
    )
    second = service.invoke(
        tool_id=ToolId.WEB_SEARCH,
        raw_input=raw,
        context=_ctx(job_id, execution_id),
    )
    assert first.finding_text == second.finding_text

    with session_scope(session_factory) as session:
        inv = (
            session.execute(
                text("SELECT status, origin, tool_id FROM tool_invocations")
            )
            .mappings()
            .one()
        )
        attempts = (
            session.execute(text("SELECT status FROM tool_invocation_attempts"))
            .mappings()
            .all()
        )
    assert inv["status"] == "SUCCEEDED"
    assert inv["origin"] == "WORKFLOW"
    assert inv["tool_id"] == "web_search"
    assert len(attempts) == 1


def test_tool_in_progress_fails_fast(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = f"tool-inflight-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = _service(session_factory)
    raw = {"query": "inflight", "max_results": 1}
    fp = fingerprint_payload({"tool_id": "web_search", "input": raw})
    from atlas.tools.service import build_workflow_invocation_key

    key = build_workflow_invocation_key(
        research_job_id=job_id,
        node_name="research",
        tool_id="web_search",
        tool_version="tools.v1",
        provider="fake",
        input_fingerprint=fp,
        tool_policy_version="2026-08-09.tools.v1",
    )
    repo = SqlAlchemyToolInvocationRepository()
    now = datetime.now(UTC)
    with session_scope(session_factory) as session:
        inv_id = str(uuid4())
        repo.create_invocation(
            session,
            invocation_id=inv_id,
            invocation_key=key,
            origin="WORKFLOW",
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            node_name="research",
            workflow_node_attempt=1,
            actor_id=None,
            tool_id="web_search",
            tool_version="tools.v1",
            provider="fake",
            tool_policy_version="2026-08-09.tools.v1",
            input_fingerprint=fp,
            at=now,
        )
        repo.begin_attempt(
            session,
            attempt_id=str(uuid4()),
            invocation_id=inv_id,
            deadline_at=now + timedelta(seconds=30),
            at=now,
        )

    import pytest

    with pytest.raises(ToolInvocationInProgressError):
        service.invoke(
            tool_id=ToolId.WEB_SEARCH,
            raw_input=raw,
            context=_ctx(job_id, execution_id),
        )


def test_tool_stale_reclaim_after_deadline(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = f"tool-stale-{uuid4()}"
    execution_id = _seed_job_and_execution(
        session_factory,
        job_id=job_id,
        claim_token=None,
    )
    service = _service(session_factory)
    raw = {"query": "stale reclaim", "max_results": 1}
    fp = fingerprint_payload({"tool_id": "web_search", "input": raw})
    from atlas.tools.service import build_workflow_invocation_key

    key = build_workflow_invocation_key(
        research_job_id=job_id,
        node_name="research",
        tool_id="web_search",
        tool_version="tools.v1",
        provider="fake",
        input_fingerprint=fp,
        tool_policy_version="2026-08-09.tools.v1",
    )
    repo = SqlAlchemyToolInvocationRepository()
    now = datetime.now(UTC)
    with session_scope(session_factory) as session:
        inv_id = str(uuid4())
        repo.create_invocation(
            session,
            invocation_id=inv_id,
            invocation_key=key,
            origin="WORKFLOW",
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            node_name="research",
            workflow_node_attempt=1,
            actor_id=None,
            tool_id="web_search",
            tool_version="tools.v1",
            provider="fake",
            tool_policy_version="2026-08-09.tools.v1",
            input_fingerprint=fp,
            at=now,
        )
        repo.begin_attempt(
            session,
            attempt_id=str(uuid4()),
            invocation_id=inv_id,
            deadline_at=now - timedelta(seconds=1),
            at=now - timedelta(seconds=10),
        )

    result = service.invoke(
        tool_id=ToolId.WEB_SEARCH,
        raw_input=raw,
        context=_ctx(job_id, execution_id),
    )
    assert result.meta.status == "succeeded"


class _FlakyThenOkTool:
    def __init__(self) -> None:
        self.calls = 0

    @property
    def tool_id(self) -> ToolId:
        return ToolId.WEB_SEARCH

    def invoke(
        self, raw_input: dict[str, object], *, context: ToolCallContext
    ) -> ToolInvocationResult:
        self.calls += 1
        if self.calls == 1:
            raise ToolTemporaryError("transient")
        return FakeWebSearchTool().invoke(raw_input, context=context)


def test_tool_retries_transient_once(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = f"tool-retry-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    flaky = _FlakyThenOkTool()
    service = _service(session_factory, tool=flaky)
    result = service.invoke(
        tool_id=ToolId.WEB_SEARCH,
        raw_input={"query": "retry me", "max_results": 1},
        context=_ctx(job_id, execution_id),
    )
    assert result.meta.status == "succeeded"
    assert flaky.calls == 2


def test_late_attempt_cannot_overwrite(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = f"tool-fence-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    repo = SqlAlchemyToolInvocationRepository()
    now = datetime.now(UTC)
    inv_id = str(uuid4())
    attempt1 = str(uuid4())
    attempt2 = str(uuid4())
    with session_scope(session_factory) as session:
        repo.create_invocation(
            session,
            invocation_id=inv_id,
            invocation_key="a" * 64,
            origin="WORKFLOW",
            research_job_id=job_id,
            workflow_execution_id=execution_id,
            node_name="research",
            workflow_node_attempt=1,
            actor_id=None,
            tool_id="web_search",
            tool_version="tools.v1",
            provider="fake",
            tool_policy_version="2026-08-09.tools.v1",
            input_fingerprint="c" * 64,
            at=now,
        )
        repo.begin_attempt(
            session,
            attempt_id=attempt1,
            invocation_id=inv_id,
            deadline_at=now + timedelta(seconds=30),
            at=now,
        )
        repo.begin_attempt(
            session,
            attempt_id=attempt2,
            invocation_id=inv_id,
            deadline_at=now + timedelta(seconds=30),
            at=now,
        )
        assert repo.complete_attempt(session, attempt_id=attempt2, latency_ms=1, at=now)
        assert repo.complete_invocation_for_attempt(
            session,
            invocation_id=inv_id,
            attempt_id=attempt2,
            output_summary_json={"output": {}, "finding_text": "ok"},
            content_digest="d" * 64,
            byte_length=2,
            latency_ms=1,
            at=now,
        )
        # Late attempt1 may still leave STARTED→SUCCEEDED on its own row, but
        # must not overwrite the logical invocation owned by attempt2.
        assert repo.complete_attempt(
            session, attempt_id=attempt1, latency_ms=99, at=now
        )
        assert not repo.complete_invocation_for_attempt(
            session,
            invocation_id=inv_id,
            attempt_id=attempt1,
            output_summary_json={"output": {}, "finding_text": "late"},
            content_digest="e" * 64,
            byte_length=4,
            latency_ms=99,
            at=now,
        )

    with session_scope(session_factory) as session:
        inv = (
            session.execute(
                text("SELECT status, output_summary_json FROM tool_invocations")
            )
            .mappings()
            .one()
        )
    assert inv["status"] == "SUCCEEDED"
    assert inv["output_summary_json"]["finding_text"] == "ok"


# Keep import used for fencing test documentation.
_ = ToolAttemptOwnershipLostError
