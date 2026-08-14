"""Checkpoint resume through verify_citations and fail-closed citation checks."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from langchain_core.runnables import RunnableConfig
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, sessionmaker

from atlas.domain import ResearchJob
from atlas.evidence.contracts import ClaimStructured, IngestDocumentRequest, MediaType
from atlas.evidence.service import (
    CitationValidator,
    EvidenceIngestService,
    ReportArtifactService,
)
from atlas.models.fakes import (
    DeterministicResearchDrafter,
    DeterministicResearchPlanner,
)
from atlas.persistence.db import session_scope
from atlas.persistence.models.evidence import ClaimModel, ReportArtifactModel
from atlas.persistence.models.workflow import WorkflowNodeExecutionModel
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository
from atlas.specialists.citation_verifier import DurableCitationVerifier
from atlas.specialists.contracts import (
    CitationVerifierInput,
    ResearchSpecialistInput,
    ResearchSpecialistOutput,
)
from atlas.specialists.errors import SpecialistCitationError
from atlas.specialists.planner import BoundedPlannerSpecialist
from atlas.specialists.synthesizer import BoundedReportSynthesizer
from atlas.workflow.graph import (
    WorkflowRuntimeContext,
    build_research_graph,
    default_fake_runtime_context,
    initial_graph_state,
)
from atlas.workflow.processor import (
    LangGraphResearchProcessor,
    RepositoryNodeAuditHooks,
    create_checkpoint_runtime,
)


def test_interrupt_after_draft_resumes_through_verify_and_complete(
    test_database_url: str,
    session_factory: sessionmaker[Session],
) -> None:
    del session_factory
    job_id = "resume-after-draft-1"
    question = "How does Atlas verify citations?"
    config: RunnableConfig = {"configurable": {"thread_id": job_id}}
    counters: dict[str, int] = {}
    context = default_fake_runtime_context(node_counters=counters)

    runtime_a = create_checkpoint_runtime(test_database_url)
    try:
        graph_a = build_research_graph(
            checkpointer=runtime_a.checkpointer,
            interrupt_after=["draft"],
        )
        interrupted = graph_a.invoke(
            initial_graph_state(job_id=job_id, question=question),
            config,
            context=context,
        )
        snapshot_a = graph_a.get_state(config)
    finally:
        runtime_a.close()
        del graph_a
        del runtime_a

    assert interrupted["draft"]
    assert interrupted["result"] == ""
    assert snapshot_a.next == ("verify_citations",)
    assert counters == {
        "validate": 1,
        "plan": 1,
        "research": 1,
        "draft": 1,
    }

    runtime_b = create_checkpoint_runtime(test_database_url)
    try:
        graph_b = build_research_graph(checkpointer=runtime_b.checkpointer)
        completed = graph_b.invoke(None, config, context=context)
        snapshot_b = graph_b.get_state(config)
    finally:
        runtime_b.close()

    assert snapshot_b.next == ()
    assert "Question:" in completed["result"]
    assert "Draft:" in completed["result"]
    assert counters["verify_citations"] == 1
    assert counters["evaluate"] == 1
    assert counters["policy"] == 1
    assert counters["complete"] == 1
    assert counters["draft"] == 1


def test_completed_workflow_is_not_unexpectedly_rerun(
    test_database_url: str,
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "completed-no-rerun-1"
    question = "Completed workflows stay complete"
    job_repo = SqlAlchemyResearchJobRepository()
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, question),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="c" * 64,
        )
    with session_scope(session_factory) as session:
        session.execute(
            text(
                """
                UPDATE research_jobs
                SET status = 'RUNNING',
                    started_at = NOW(),
                    updated_at = NOW(),
                    claim_token = :token,
                    lease_expires_at = NOW() + interval '5 minutes',
                    evaluation_profile = COALESCE(
                        evaluation_profile, 'evaluation.candidate.v1'
                    )
                WHERE id = :job_id
                """
            ),
            {"token": "c" * 64, "job_id": job_id},
        )
    runtime = create_checkpoint_runtime(test_database_url)
    counters: dict[str, int] = {}
    try:
        processor = LangGraphResearchProcessor(
            checkpointer=runtime.checkpointer,
            session_factory=session_factory,
            node_counters=counters,
        )
        first = processor(question, job_id=job_id, claim_token="c" * 64)
        second = processor(question, job_id=job_id, claim_token="c" * 64)
    finally:
        runtime.close()

    assert first == second
    from atlas.application.job_processing import CompletedProcessing

    assert isinstance(first, CompletedProcessing)
    assert first.result.strip()
    assert counters["complete"] == 1


def test_citation_verifier_fails_closed_on_unlinked_evidence(
    session_factory: sessionmaker[Session],
) -> None:
    ingest = EvidenceIngestService(session_factory=session_factory)
    verifier = DurableCitationVerifier(
        citation_validator=CitationValidator(session_factory=session_factory),
        evidence_ingest=ingest,
    )
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    job_id = "verify-fail-job"
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, "Verify fail closed"),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="a" * 64,
        )
        workflow_repo.create_execution(
            session,
            research_job_id=job_id,
            thread_id=job_id,
            at=at,
        )
    doc = ingest.ingest_document(
        IngestDocumentRequest(
            corpus_key="verify-unlinked",
            text="Unlinked corpus evidence for citation verifier.",
            media_type=MediaType.TEXT_PLAIN,
        )
    )
    with pytest.raises(SpecialistCitationError):
        verifier.run(
            CitationVerifierInput(
                research_job_id=job_id,
                claims=[
                    ClaimStructured(
                        text="Claims unlinked evidence",
                        evidence_item_ids=[doc.evidence_item_ids[0]],
                    )
                ],
            )
        )


def test_verify_citations_failure_blocks_complete_and_report_persist(
    test_database_url: str,
    session_factory: sessionmaker[Session],
) -> None:
    job_id = "verify-blocks-complete"
    question = "Should not complete with unlinked citations"
    job_repo = SqlAlchemyResearchJobRepository()
    workflow_repo = SqlAlchemyWorkflowRepository()
    ingest = EvidenceIngestService(session_factory=session_factory)
    at = datetime.now(UTC)
    with session_scope(session_factory) as session:
        job_repo.add(
            session,
            ResearchJob.create(job_id, question),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="b" * 64,
        )
        execution_id = workflow_repo.create_execution(
            session,
            research_job_id=job_id,
            thread_id=job_id,
            at=at,
        )
    doc = ingest.ingest_document(
        IngestDocumentRequest(
            corpus_key="unlinked-for-graph",
            text="Evidence exists but is not linked to the research job.",
            media_type=MediaType.TEXT_PLAIN,
        )
    )
    unlinked_id = doc.evidence_item_ids[0]

    class _UnlinkedResearch:
        def run(self, request: ResearchSpecialistInput) -> ResearchSpecialistOutput:
            del request
            return ResearchSpecialistOutput(
                findings=["finding from unlinked path"],
                evidence_item_ids=[unlinked_id],
            )

    hooks = RepositoryNodeAuditHooks(
        session_factory=session_factory,
        repository=workflow_repo,
        workflow_execution_id=execution_id,
    )
    context = WorkflowRuntimeContext(
        planner_specialist=BoundedPlannerSpecialist(DeterministicResearchPlanner()),
        research_specialist=_UnlinkedResearch(),
        synthesizer=BoundedReportSynthesizer(
            drafter=DeterministicResearchDrafter(),
            evidence_ingest=ingest,
        ),
        citation_verifier=DurableCitationVerifier(
            citation_validator=CitationValidator(session_factory=session_factory),
            evidence_ingest=ingest,
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
        with pytest.raises(SpecialistCitationError):
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
        claims = session.execute(
            select(func.count()).select_from(ClaimModel)
        ).scalar_one()
        verify_row = session.scalars(
            select(WorkflowNodeExecutionModel).where(
                WorkflowNodeExecutionModel.workflow_execution_id == execution_id,
                WorkflowNodeExecutionModel.node_name == "verify_citations",
            )
        ).one()
        complete_rows = session.scalars(
            select(WorkflowNodeExecutionModel).where(
                WorkflowNodeExecutionModel.workflow_execution_id == execution_id,
                WorkflowNodeExecutionModel.node_name == "complete",
            )
        ).all()
    assert reports == 0
    assert claims == 0
    assert verify_row.status == "FAILED"
    assert verify_row.error == "SpecialistCitationError: node execution failed"
    assert complete_rows == []
