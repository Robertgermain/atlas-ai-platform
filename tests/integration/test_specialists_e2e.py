"""Slice 11B end-to-end specialist workflow, ledger attribution, and bounds."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableConfig
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, sessionmaker

from atlas.api.deps import provide_session_factory
from atlas.application.worker import ResearchJobWorker
from atlas.config.settings import Settings, get_settings
from atlas.domain import ResearchJob
from atlas.evidence.contracts import ClaimStructured
from atlas.evidence.service import (
    CitationValidator,
    EvidenceIngestService,
    ReportArtifactService,
)
from atlas.main import app
from atlas.models.contracts import (
    DraftStructuredOutput,
    PlanStructuredOutput,
    ProviderId,
)
from atlas.models.fakes import DeterministicResearchPlanner
from atlas.models.service import (
    LedgerBackedDrafter,
    LedgerBackedPlanner,
    ModelInvocationService,
)
from atlas.persistence.db import reset_engine_cache, session_scope
from atlas.persistence.models.evidence import (
    CitationModel,
    ClaimModel,
    EvidenceJobLinkModel,
    ReportArtifactModel,
)
from atlas.persistence.models.workflow import (
    WorkflowExecutionModel,
    WorkflowNodeExecutionModel,
)
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository
from atlas.specialists.citation_verifier import DurableCitationVerifier
from atlas.specialists.contracts import ResearchSpecialistOutput, SynthesizerInput
from atlas.specialists.errors import SpecialistValidationError
from atlas.specialists.planner import BoundedPlannerSpecialist
from atlas.specialists.research import GovernedResearchRetrievalSpecialist
from atlas.specialists.synthesizer import BoundedReportSynthesizer
from atlas.tools.composition import build_tool_budgets
from atlas.workflow import LangGraphResearchProcessor, create_checkpoint_runtime
from atlas.workflow.graph import (
    NODE_NAMES,
    WorkflowRuntimeContext,
    build_research_graph,
    initial_graph_state,
)
from atlas.workflow.processor import RepositoryNodeAuditHooks


def _api_client(session_factory: sessionmaker[Session]) -> TestClient:
    reset_engine_cache()
    app.dependency_overrides[provide_session_factory] = lambda: session_factory
    return TestClient(app)


def _ai_message() -> AIMessage:
    return AIMessage(
        content="ok",
        usage_metadata={
            "input_tokens": 10,
            "output_tokens": 5,
            "total_tokens": 15,
        },
        response_metadata={"id": "req-specialist-e2e"},
    )


def _mock_plan_and_draft_chat_model() -> MagicMock:
    """Mock chat model that returns structured plan/draft without live calls."""
    chat_model = MagicMock()

    def with_structured_output(schema: type[Any], **kwargs: object) -> MagicMock:
        del kwargs
        structured = MagicMock()
        if schema is PlanStructuredOutput:

            def plan_invoke(_messages: object) -> dict[str, Any]:
                return {
                    "raw": _ai_message(),
                    "parsed": PlanStructuredOutput(
                        tasks=["Clarify scope", "Gather evidence", "Identify risks"]
                    ),
                    "parsing_error": None,
                }

            structured.invoke.side_effect = plan_invoke
        elif schema is DraftStructuredOutput:

            def draft_invoke(messages: list[Any]) -> dict[str, Any]:
                user = str(messages[1].content)
                evidence_ids = re.findall(r"id=([^\s]+)", user)
                claims: list[ClaimStructured] = []
                if evidence_ids:
                    claims = [
                        ClaimStructured(
                            text=f"Supported by evidence {evidence_ids[0]}",
                            evidence_item_ids=[evidence_ids[0]],
                        )
                    ]
                return {
                    "raw": _ai_message(),
                    "parsed": DraftStructuredOutput(
                        draft="Ledger-backed draft synthesis",
                        claims=claims,
                    ),
                    "parsing_error": None,
                }

            structured.invoke.side_effect = draft_invoke
        else:
            raise AssertionError(f"unexpected schema: {schema}")
        return structured

    chat_model.with_structured_output.side_effect = with_structured_output
    return chat_model


class _LedgerAttributedProcessor(LangGraphResearchProcessor):
    """Processor that records plan/draft model ledger rows with a fake chat model."""

    def _build_context(
        self,
        *,
        workflow_execution_id: str,
        hooks: object,
        job_claim_token: str,
        job_id: str = "",
        evaluation_profile: str | None = None,
    ) -> WorkflowRuntimeContext:
        from dataclasses import replace

        base = super()._build_context(
            workflow_execution_id=workflow_execution_id,
            hooks=hooks,  # type: ignore[arg-type]
            job_claim_token=job_claim_token,
            job_id=job_id,
            evaluation_profile=evaluation_profile,  # type: ignore[arg-type]
        )
        settings = self._settings
        service = ModelInvocationService(
            session_factory=self._session_factory,
            chat_model=_mock_plan_and_draft_chat_model(),
            provider=ProviderId.OPENAI,
            model_name="gpt-4o-mini",
            call_timeout_seconds=settings.model_call_timeout_seconds,
        )
        planner = LedgerBackedPlanner(
            service, workflow_execution_id=workflow_execution_id
        )
        drafter = LedgerBackedDrafter(
            service, workflow_execution_id=workflow_execution_id
        )
        synthesizer = base.synthesizer
        assert isinstance(synthesizer, BoundedReportSynthesizer)
        return replace(
            base,
            planner_specialist=BoundedPlannerSpecialist(planner),
            synthesizer=BoundedReportSynthesizer(
                drafter=drafter,
                evidence_ingest=synthesizer._evidence_ingest,
            ),
        )


def _artifact_counts(
    session_factory: sessionmaker[Session], *, job_id: str
) -> dict[str, int]:
    with session_factory() as session:
        return {
            "reports": session.execute(
                select(func.count())
                .select_from(ReportArtifactModel)
                .where(ReportArtifactModel.research_job_id == job_id)
            ).scalar_one(),
            "claims": session.execute(
                select(func.count())
                .select_from(ClaimModel)
                .where(ClaimModel.research_job_id == job_id)
            ).scalar_one(),
            "citations": session.execute(
                select(func.count())
                .select_from(CitationModel)
                .where(CitationModel.research_job_id == job_id)
            ).scalar_one(),
            "links": session.execute(
                select(func.count())
                .select_from(EvidenceJobLinkModel)
                .where(EvidenceJobLinkModel.research_job_id == job_id)
            ).scalar_one(),
        }


def test_specialists_e2e_cited_report_ledger_and_idempotent_replay(
    test_database_url: str,
    session_factory: sessionmaker[Session],
) -> None:
    """Full path: POST → worker → specialists → citations + ledger + replay."""
    client = _api_client(session_factory)
    runtime = create_checkpoint_runtime(test_database_url)
    counters: dict[str, int] = {}
    processor = _LedgerAttributedProcessor(
        checkpointer=runtime.checkpointer,
        session_factory=session_factory,
        node_counters=counters,
    )
    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=SqlAlchemyResearchJobRepository(),
        processor=processor,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=30.0,
        lease_seconds=60.0,
    )
    question = "Slice 11B specialist end-to-end cited report"
    try:
        created = client.post(
            "/v1/research-jobs",
            json={"question": question},
            headers={"Idempotency-Key": "specialists-e2e-key"},
        )
        assert created.status_code == 202
        job_id = created.json()["id"]

        assert worker.run_once() is True

        fetched = client.get(f"/v1/research-jobs/{job_id}")
        assert fetched.status_code == 200
        body = fetched.json()
        assert body["status"] == "COMPLETED"
        assert "Question:" in body["result"]
        assert "Citations:" in body["result"]

        citations = client.get(f"/v1/research-jobs/{job_id}/citations")
        assert citations.status_code == 200
        cite_body = citations.json()
        assert cite_body["report_artifact_id"] is not None
        assert len(cite_body["citations"]) >= 1
        first = cite_body["citations"][0]
        assert first["evidence_item_id"]
        assert first["document_id"]
        assert first["source_id"]
        assert first["source_canonical_uri"]
        cited_ids = {item["evidence_item_id"] for item in cite_body["citations"]}

        with session_factory() as session:
            executions = session.scalars(
                select(WorkflowExecutionModel).where(
                    WorkflowExecutionModel.research_job_id == job_id
                )
            ).all()
            assert len(executions) == 1
            execution_id = executions[0].id
            nodes = session.scalars(
                select(WorkflowNodeExecutionModel).where(
                    WorkflowNodeExecutionModel.workflow_execution_id == execution_id
                )
            ).all()
            assert {node.node_name for node in nodes} == set(NODE_NAMES) - {
                "terminal",
                "repair",
                "await_review",
            }
            assert all(node.status == "COMPLETED" for node in nodes)
            assert all(node.attempt == 1 for node in nodes)

            model_rows = (
                session.execute(
                    text(
                        """
                        SELECT node_name, status
                        FROM model_invocations
                        WHERE research_job_id = :job_id
                        ORDER BY node_name
                        """
                    ),
                    {"job_id": job_id},
                )
                .mappings()
                .all()
            )
            assert {row["node_name"] for row in model_rows} == {"plan", "draft"}
            assert all(row["status"] == "SUCCEEDED" for row in model_rows)
            assert len(model_rows) == 2

            tool_rows = (
                session.execute(
                    text(
                        """
                        SELECT node_name, origin
                        FROM tool_invocations
                        WHERE research_job_id = :job_id
                        """
                    ),
                    {"job_id": job_id},
                )
                .mappings()
                .all()
            )
            assert tool_rows
            assert all(row["node_name"] == "research" for row in tool_rows)
            assert all(row["origin"] == "WORKFLOW" for row in tool_rows)
            tool_row_count = len(tool_rows)

            verify_models = session.execute(
                text(
                    """
                    SELECT COUNT(*) AS n
                    FROM model_invocations
                    WHERE research_job_id = :job_id AND node_name = 'verify_citations'
                    """
                ),
                {"job_id": job_id},
            ).scalar_one()
            verify_tools = session.execute(
                text(
                    """
                    SELECT COUNT(*) AS n
                    FROM tool_invocations
                    WHERE research_job_id = :job_id AND node_name = 'verify_citations'
                    """
                ),
                {"job_id": job_id},
            ).scalar_one()
            assert verify_models == 0
            assert verify_tools == 0

            links = session.scalars(
                select(EvidenceJobLinkModel).where(
                    EvidenceJobLinkModel.research_job_id == job_id
                )
            ).all()
            linked_ids = {link.evidence_item_id for link in links}
            assert cited_ids <= linked_ids

        first_counts = _artifact_counts(session_factory, job_id=job_id)
        assert first_counts["reports"] == 1
        assert first_counts["claims"] >= 1
        assert first_counts["citations"] >= 1
        assert first_counts["links"] >= 1

        # Replay / resume short-circuit: no duplicate artifacts or side effects.
        second = processor(question, job_id=job_id, claim_token="d" * 64)
        from atlas.application.job_processing import CompletedProcessing

        assert isinstance(second, CompletedProcessing)
        assert "Question:" in second.result
        assert counters["complete"] == 1
        assert counters["plan"] == 1
        assert counters["research"] == 1
        assert counters["draft"] == 1
        assert counters["verify_citations"] == 1
        assert counters["evaluate"] == 1
        assert counters["policy"] == 1
        second_counts = _artifact_counts(session_factory, job_id=job_id)
        assert second_counts == first_counts

        with session_factory() as session:
            model_count = session.execute(
                text(
                    "SELECT COUNT(*) FROM model_invocations WHERE research_job_id = :j"
                ),
                {"j": job_id},
            ).scalar_one()
            tool_count = session.execute(
                text(
                    "SELECT COUNT(*) FROM tool_invocations WHERE research_job_id = :j"
                ),
                {"j": job_id},
            ).scalar_one()
        assert model_count == 2
        assert tool_count == tool_row_count
    finally:
        worker.close()
        runtime.close()
        app.dependency_overrides.clear()
        reset_engine_cache()


def test_processor_wires_isolated_specialist_capabilities(
    test_database_url: str,
    session_factory: sessionmaker[Session],
) -> None:
    runtime = create_checkpoint_runtime(test_database_url)
    try:
        processor = LangGraphResearchProcessor(
            checkpointer=runtime.checkpointer,
            session_factory=session_factory,
        )
        context = processor._build_context(
            workflow_execution_id="cap-wire-1",
            hooks=None,
            job_claim_token="e" * 64,
        )
    finally:
        runtime.close()

    assert isinstance(context.planner_specialist, BoundedPlannerSpecialist)
    assert isinstance(context.research_specialist, GovernedResearchRetrievalSpecialist)
    assert isinstance(context.synthesizer, BoundedReportSynthesizer)
    assert isinstance(context.citation_verifier, DurableCitationVerifier)
    assert context.research_specialist._research_executor is not None
    assert context.research_specialist._evidence_retriever is not None
    assert not hasattr(context.planner_specialist, "_research_executor")
    assert not hasattr(context.synthesizer, "_research_executor")
    assert not hasattr(context.citation_verifier, "_drafter")
    assert context.report_service is not None


def test_draft_failure_blocks_complete_and_report_persist(
    test_database_url: str,
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "draft-blocks-complete"
    question = "Draft failure must not complete"
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, question),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="e" * 64,
        )
        execution_id = workflow_repo.create_execution(
            session,
            research_job_id=job_id,
            thread_id=job_id,
            at=at,
        )

    class _FailingSynthesizer:
        def run(self, request: SynthesizerInput) -> object:
            del request
            raise SpecialistValidationError("draft boundary failed")

    class _NoResearch:
        def run(self, request: object) -> ResearchSpecialistOutput:
            del request
            return ResearchSpecialistOutput(findings=["f"], evidence_item_ids=[])

    hooks = RepositoryNodeAuditHooks(
        session_factory=session_factory,
        repository=workflow_repo,
        workflow_execution_id=execution_id,
    )
    context = WorkflowRuntimeContext(
        planner_specialist=BoundedPlannerSpecialist(DeterministicResearchPlanner()),
        research_specialist=_NoResearch(),
        synthesizer=_FailingSynthesizer(),  # type: ignore[arg-type]
        citation_verifier=DurableCitationVerifier(
            citation_validator=CitationValidator(session_factory=session_factory),
            evidence_ingest=EvidenceIngestService(session_factory=session_factory),
        ),
        plan_prompt_version="plan.v1",
        draft_prompt_version="draft.v2",
        workflow_execution_id=execution_id,
        hooks=hooks,
        report_service=ReportArtifactService(session_factory=session_factory),
    )
    runtime = create_checkpoint_runtime(test_database_url)
    config: RunnableConfig = {"configurable": {"thread_id": job_id}}
    try:
        graph = build_research_graph(checkpointer=runtime.checkpointer)
        with pytest.raises(SpecialistValidationError):
            graph.invoke(
                initial_graph_state(job_id=job_id, question=question),
                config,
                context=context,
            )
    finally:
        runtime.close()

    with session_factory() as session:
        reports = session.execute(
            select(func.count()).select_from(ReportArtifactModel)
        ).scalar_one()
        draft_row = session.scalars(
            select(WorkflowNodeExecutionModel).where(
                WorkflowNodeExecutionModel.workflow_execution_id == execution_id,
                WorkflowNodeExecutionModel.node_name == "draft",
            )
        ).one()
        complete_rows = session.scalars(
            select(WorkflowNodeExecutionModel).where(
                WorkflowNodeExecutionModel.workflow_execution_id == execution_id,
                WorkflowNodeExecutionModel.node_name == "complete",
            )
        ).all()
        verify_rows = session.scalars(
            select(WorkflowNodeExecutionModel).where(
                WorkflowNodeExecutionModel.workflow_execution_id == execution_id,
                WorkflowNodeExecutionModel.node_name == "verify_citations",
            )
        ).all()
    assert reports == 0
    assert draft_row.status == "FAILED"
    assert complete_rows == []
    assert verify_rows == []


def test_bounded_execution_settings_and_specialist_contracts() -> None:
    """Confirm research budgets and retrieval/findings bounds remain capped."""
    settings = get_settings()
    budgets = build_tool_budgets(settings)
    assert (
        budgets.max_logical_calls == settings.tool_max_logical_calls_per_research_node
    )
    assert settings.tool_max_logical_calls_per_research_node == 6
    assert settings.retrieval_default_k <= 8
    assert NODE_NAMES == (
        "validate",
        "plan",
        "research",
        "draft",
        "verify_citations",
        "evaluate",
        "policy",
        "repair",
        "await_review",
        "complete",
        "terminal",
    )
    from atlas.evidence.bounds import (
        MAX_CLAIMS_PER_DRAFT,
        MAX_EVIDENCE_ITEMS_TO_DRAFTER,
    )
    from atlas.specialists.contracts import MAX_RESEARCH_FINDINGS

    assert MAX_RESEARCH_FINDINGS == 6
    assert MAX_CLAIMS_PER_DRAFT == 20
    assert MAX_EVIDENCE_ITEMS_TO_DRAFTER == 8
    assert Settings().retrieval_default_k == 5
